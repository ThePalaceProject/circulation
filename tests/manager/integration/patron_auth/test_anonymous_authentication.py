from __future__ import annotations

from unittest.mock import MagicMock

from palace.manager.api.authentication.base import (
    AuthProviderLibrarySettings,
    AuthProviderSettings,
)
from palace.manager.api.authentication.opds import OPDSAuthenticationFlow
from palace.manager.integration.patron_auth.anonymous_authentication import (
    AnonymousAuthenticationProvider,
)


class TestAnonymousAuthenticationProvider:
    def _provider(self) -> AnonymousAuthenticationProvider:
        return AnonymousAuthenticationProvider(
            library_id=1,
            integration_id=2,
            settings=AuthProviderSettings(),
            library_settings=AuthProviderLibrarySettings(),
        )

    def test_metadata(self) -> None:
        assert AnonymousAuthenticationProvider.label() == "Anonymous Access"
        assert "anonymous" in AnonymousAuthenticationProvider.description().lower()
        assert AnonymousAuthenticationProvider.settings_class() is AuthProviderSettings
        assert (
            AnonymousAuthenticationProvider.library_settings_class()
            is AuthProviderLibrarySettings
        )

    def test_does_not_identify_individuals(self) -> None:
        # Anonymous access never identifies an individual patron.
        assert self._provider().identifies_individuals is False

    def test_never_authenticates(self) -> None:
        provider = self._provider()
        db = MagicMock()
        # No header value ever produces a Patron.
        assert provider.authenticated_patron(db, "Bearer abc") is None
        assert provider.authenticated_patron(db, {"username": "u"}) is None

    def test_no_credential_or_lookup(self) -> None:
        provider = self._provider()
        assert provider.get_credential_from_header(MagicMock()) is None
        assert provider.remote_patron_lookup(MagicMock()) is None

    def test_self_tests_are_empty(self) -> None:
        # The provider has nothing to self-test.
        assert list(self._provider()._run_self_tests(MagicMock())) == []

    def test_is_not_an_opds_authentication_flow(self) -> None:
        # Anonymous access has no authentication flow to advertise, so unlike
        # the identifying providers it is deliberately not an
        # OPDSAuthenticationFlow and never appears in an Authentication For
        # OPDS document.
        assert not isinstance(self._provider(), OPDSAuthenticationFlow)
