from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Generator, Iterable

from sqlalchemy.orm.session import Session

from palace.manager.core.config import IntegrationException
from palace.manager.core.exceptions import BasePalaceException
from palace.manager.core.selftest import HasSelfTests, SelfTestResult
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.problem_detail import ProblemDetail


class HasPatronSelfTests(HasSelfTests, ABC):
    """Circulation-specific enhancements for HasSelfTests.

    Circulation self-tests frequently need to test the ability to act
    on behalf of a specific patron.
    """

    class _NoValidLibrarySelfTestPatron(BasePalaceException):
        """Exception raised when no valid self-test patron found for library.

        Attributes:
            message -- primary error message.
            detail (optional) -- additional explanation of the error
        """

        def __init__(self, message: str | None, *, detail: str | None = None):
            super().__init__(message=message)
            self.message = message
            self.detail = detail

    @classmethod
    def default_patrons(
        cls, collection: Collection
    ) -> Iterable[tuple[Library, Patron, str | None] | SelfTestResult]:
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
    ) -> tuple[Patron, str | None]:
        """Obtain the test Patron and optional password for a library's self-tests.

        :param library: The library being tested.
        :param _db: Database session object.
        :return: A 2-tuple with either (1) a patron and optional password.
        :raise: _NoValidLibrarySelfTestPatron when a valid patron is not found.
        """
        _db = _db or Session.object_session(library)
        from palace.manager.api.authenticator import LibraryAuthenticator

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
        message: str | None
        detail: str | None
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


class HasCollectionSelfTests(HasPatronSelfTests, ABC):
    """Extra tests to verify the integrity of imported
    collections of books.

    This is a mixin method that requires that `self.collection`
    point to the Collection to be tested.
    """

    @property
    @abstractmethod
    def collection(self) -> Collection | None: ...

    def integration(self, _db: Session) -> IntegrationConfiguration | None:
        if not self.collection:
            return None
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

    def _run_self_tests(self, _db: Session) -> Generator[SelfTestResult]:
        yield self.run_test(
            "Checking for titles that have no delivery mechanisms.",
            self._no_delivery_mechanisms_test,
        )
