from __future__ import annotations

from typing import TYPE_CHECKING

from core.integration.goals import Goals
from core.integration.registry import IntegrationRegistry

if TYPE_CHECKING:
    from api.authentication.base import AuthenticationProvider  # noqa: autoflake


class PatronAuthRegistry(IntegrationRegistry["AuthenticationProvider"]):
    def __init__(self) -> None:
        super().__init__(Goals.PATRON_AUTH_GOAL)

        from api.firstbook2 import FirstBookAuthenticationAPI
        from api.kansas_patron import KansasAuthenticationAPI
        from api.millenium_patron import MilleniumPatronAPI
        from api.saml.provider import SAMLWebSSOAuthenticationProvider
        from api.simple_authentication import SimpleAuthenticationProvider
        from api.sip import SIP2AuthenticationProvider
        from api.sirsidynix_authentication_provider import (
            SirsiDynixHorizonAuthenticationProvider,
        )

        self.register(
            SimpleAuthenticationProvider, canonical="api.simple_authentication"
        )
        self.register(MilleniumPatronAPI, canonical="api.millenium_patron")
        self.register(SIP2AuthenticationProvider, canonical="api.sip")
        self.register(FirstBookAuthenticationAPI, canonical="api.firstbook2")
        self.register(KansasAuthenticationAPI, canonical="api.kansas_patron")
        self.register(SAMLWebSSOAuthenticationProvider, canonical="api.saml.provider")
        self.register(
            SirsiDynixHorizonAuthenticationProvider,
            canonical="api.sirsidynix_authentication_provider",
        )
