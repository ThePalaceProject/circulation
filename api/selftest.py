import sys
from abc import ABC
from typing import Iterable, Optional, Tuple, Union

from sqlalchemy.orm.session import Session

from core.config import IntegrationException
from core.exceptions import BaseError
from core.model import Collection, ExternalIntegration, Library, LicensePool, Patron
from core.opds_import import OPDSImporter, OPDSImportMonitor
from core.scripts import LibraryInputScript
from core.selftest import HasSelfTests as CoreHasSelfTests
from core.selftest import SelfTestResult
from core.util.problem_detail import ProblemDetail

from .authenticator import LibraryAuthenticator
from .circulation import CirculationAPI


class HasSelfTests(CoreHasSelfTests, ABC):
    """Circulation-specific enhancements for HasSelfTests.

    Circulation self-tests frequently need to test the ability to act
    on behalf of a specific patron.
    """

    class _NoValidLibrarySelfTestPatron(BaseError):
        """Exception raised when no valid self-test patron found for library.

        Attributes:
            message -- primary error message.
            detail (optional) -- additional explanation of the error
        """

        def __init__(self, message: str, *, detail: Optional[str] = None):
            super().__init__(message=message)
            self.message = message
            self.detail = detail

    @classmethod
    def default_patrons(
        cls, collection: Collection
    ) -> Iterable[Union[Tuple[Library, Patron, Optional[str]], SelfTestResult]]:
        """Find a usable default Patron for each of the libraries associated
        with the given Collection.

        :yield: If the collection has no associated libraries, yields a single
            failure SelfTestResult. Otherwise, for EACH associated library,
            yields either:
                - a (Library, Patron, (optional) password) 3-tuple, when a
                default patron can be determined; or
                - a failure SelfTestResult when it cannot.
        """
        _db = Session.object_session(collection)
        if not collection.libraries:
            yield cls.test_failure(
                "Acquiring test patron credentials.",
                "Collection is not associated with any libraries.",
                "Add the collection to a library that has a patron authentication service.",
            )
            # Not strictly necessary, but makes it obvious that we won't do anything else.
            return

        for library in collection.libraries:
            task = "Acquiring test patron credentials for library %s" % library.name
            try:
                patron, password = cls._determine_self_test_patron(library, _db=_db)
                yield library, patron, password
            except cls._NoValidLibrarySelfTestPatron as e:
                yield cls.test_failure(task, e.message, e.detail)
            except IntegrationException as e:
                yield cls.test_failure(task, e)
            except Exception as e:
                yield cls.test_failure(task, "Exception getting default patron: %r" % e)

    @classmethod
    def _determine_self_test_patron(
        cls, library: Library, _db=None
    ) -> Tuple[Patron, Optional[str]]:
        """Obtain the test Patron and optional password for a library's self-tests.

        :param library: The library being tested.
        :param _db: Database session object.
        :return: A 2-tuple with either (1) a patron and optional password.
        :raise: _NoValidLibrarySelfTestPatron when a valid patron is not found.
        """
        _db = _db or Session.object_session(library)
        library_authenticator = LibraryAuthenticator.from_config(_db, library)
        auth = library_authenticator.basic_auth_provider
        if auth is None:
            patron, password = None, None
        else:
            patron, password = auth.testing_patron(_db)
        if isinstance(patron, Patron):
            return patron, password

        # If we get here, then we have failed to find a valid test patron
        # and will raise an exception.
        if patron is None:
            message = "Library has no test patron configured."
            detail = (
                "You can specify a test patron when you configure "
                "the library's patron authentication service."
            )
        elif isinstance(patron, ProblemDetail):
            message = patron.detail
            detail = patron.debug_message
        else:
            message = (  # type: ignore[unreachable]
                "Authentication provider returned unexpected type "
                f"({type(patron)}) instead of patron."
            )
            detail = None
        raise cls._NoValidLibrarySelfTestPatron(message, detail=detail)


class RunSelfTestsScript(LibraryInputScript):
    """Run the self-tests for every collection in the given library
    where that's possible.
    """

    def __init__(self, _db=None, output=sys.stdout):
        super().__init__(_db)
        self.out = output

    def do_run(self, *args, **kwargs):
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        for library in parsed.libraries:
            api_map = CirculationAPI(self._db, library).default_api_map
            api_map[ExternalIntegration.OPDS_IMPORT] = OPDSImportMonitor
            self.out.write("Testing %s\n" % library.name)
            for collection in library.collections:
                try:
                    self.test_collection(collection, api_map)
                except Exception as e:
                    self.out.write("  Exception while running self-test: '%s'\n" % e)

    def test_collection(self, collection, api_map, extra_args=None):
        tester = api_map.get(collection.protocol)
        if not tester:
            self.out.write(
                " Cannot find a self-test for %s, ignoring.\n" % collection.name
            )
            return

        self.out.write(" Running self-test for %s.\n" % collection.name)
        # Some HasSelfTests classes require extra arguments to their
        # constructors.
        extra_args = extra_args or {
            OPDSImportMonitor: [OPDSImporter],
        }
        extra = extra_args.get(tester, [])
        constructor_args = [self._db, collection] + list(extra)
        results_dict, results_list = tester.run_self_tests(
            self._db, None, *constructor_args
        )
        for result in results_list:
            self.process_result(result)

    def process_result(self, result):
        """Process a single TestResult object."""
        if result.success:
            success = "SUCCESS"
        else:
            success = "FAILURE"
        self.out.write(f"  {success} {result.name} ({result.duration:.1f}sec)\n")
        if isinstance(result.result, (bytes, str)):
            self.out.write("   Result: %s\n" % result.result)
        if result.exception:
            self.out.write("   Exception: '%s'\n" % result.exception)


class HasCollectionSelfTests(HasSelfTests):
    """Extra tests to verify the integrity of imported
    collections of books.

    This is a mixin method that requires that `self.collection`
    point to the Collection to be tested.
    """

    def _no_delivery_mechanisms_test(self):
        # Find works in the tested collection that have no delivery
        # mechanisms.
        titles = []

        qu = self.collection.pools_with_no_delivery_mechanisms
        qu = qu.filter(LicensePool.licenses_owned > 0)
        for lp in qu:
            edition = lp.presentation_edition
            if edition:
                title = edition.title
            else:
                title = "[title unknown]"
            identifier = lp.identifier.identifier
            titles.append(f"{title} (ID: {identifier})")

        if titles:
            return titles
        else:
            return "All titles in this collection have delivery mechanisms."

    def _run_self_tests(self):
        yield self.run_test(
            "Checking for titles that have no delivery mechanisms.",
            self._no_delivery_mechanisms_test,
        )
