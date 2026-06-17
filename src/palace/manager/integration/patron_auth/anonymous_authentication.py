from __future__ import annotations

from collections.abc import Generator
from typing import Any

from sqlalchemy.orm import Session
from werkzeug.datastructures import Authorization

from palace.manager.api.authentication.base import (
    AuthenticationProvider,
    AuthProviderLibrarySettings,
    AuthProviderSettings,
    PatronData,
)
from palace.manager.core.selftest import SelfTestResult
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.problem_detail import ProblemDetail


class AnonymousAuthenticationProvider(
    AuthenticationProvider[AuthProviderSettings, AuthProviderLibrarySettings]
):
    """An explicit marker that a library allows anonymous access.

    This provider never authenticates anyone -- anonymous requests carry no
    credentials, so there is no individual patron to identify. Its sole purpose
    is to record, as a deliberate configuration choice, that a library intends
    to serve its open-access content without authentication.

    Without this marker the mere *absence* of authentication providers is
    ambiguous: it could mean the library is intentionally anonymous, but it
    could equally mean the library is still being set up or is being
    decommissioned. By requiring an explicit provider, the default for a
    library with no configured authentication becomes "deny."

    It cannot be combined with any other authentication provider; the
    :class:`~palace.manager.api.authenticator.LibraryAuthenticator` enforces
    that mutual exclusion at configuration-load time.

    Unlike the identifying providers, this one is deliberately *not* an
    :class:`~palace.manager.api.authentication.opds.OPDSAuthenticationFlow`: it
    has no authentication flow to advertise, so it never appears in a library's
    Authentication For OPDS document.
    """

    @classmethod
    def label(cls) -> str:
        return "Anonymous Access"

    @classmethod
    def description(cls) -> str:
        return (
            "Explicitly allow anonymous (unauthenticated) access to this library's "
            "open-access content. Patrons are not identified and cannot borrow, place "
            "holds, or sync a bookshelf; they may only directly fulfill titles that are "
            "available without a loan. This provider cannot be combined with any other "
            "authentication provider."
        )

    @classmethod
    def settings_class(cls) -> type[AuthProviderSettings]:
        return AuthProviderSettings

    @classmethod
    def library_settings_class(cls) -> type[AuthProviderLibrarySettings]:
        return AuthProviderLibrarySettings

    @property
    def identifies_individuals(self) -> bool:
        return False

    def _run_self_tests(self, _db: Session) -> Generator[SelfTestResult]:
        # Anonymous access has nothing to test.
        yield from ()

    def authenticated_patron(
        self, _db: Session, header: dict[str, Any] | str
    ) -> Patron | ProblemDetail | None:
        # Anonymous access never authenticates an individual patron.
        return None

    def get_credential_from_header(self, auth: Authorization) -> str | None:
        return None

    def remote_patron_lookup(
        self, patron_or_patrondata: PatronData | Patron
    ) -> PatronData | ProblemDetail | None:
        return None
