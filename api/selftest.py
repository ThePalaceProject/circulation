from __future__ import annotations

from abc import ABC
from typing import Generator, Iterable, Optional, Tuple, Union

from sqlalchemy.orm.session import Session

from core.config import IntegrationException
from core.exceptions import BaseError
from core.model import Collection, Library, LicensePool, Patron
from core.model.integration import IntegrationConfiguration
from core.selftest import BaseHasSelfTests
from core.selftest import HasSelfTests as CoreHasSelfTests
from core.selftest import HasSelfTestsIntegrationConfiguration, SelfTestResult
from core.util.problem_detail import ProblemDetail


class HasPatronSelfTests(BaseHasSelfTests, ABC):
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

        def __init__(self, message: Optional[str], *, detail: Optional[str] = None):
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
        from api.authenticator import LibraryAuthenticator

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
        message: Optional[str]
        detail: Optional[str]
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


class HasSelfTests(CoreHasSelfTests, HasPatronSelfTests):
    """Circulation specific self-tests, with the external integration paradigm"""


class HasCollectionSelfTests(HasSelfTestsIntegrationConfiguration, HasPatronSelfTests):
    """Extra tests to verify the integrity of imported
    collections of books.

    This is a mixin method that requires that `self.collection`
    point to the Collection to be tested.
    """

    def integration(self, _db: Session) -> IntegrationConfiguration | None:
        return self.collection.integration_configuration

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

    def _run_self_tests(self, _db: Session) -> Generator[SelfTestResult, None, None]:
        yield self.run_test(
            "Checking for titles that have no delivery mechanisms.",
            self._no_delivery_mechanisms_test,
        )
