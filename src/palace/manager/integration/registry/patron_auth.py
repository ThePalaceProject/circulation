from __future__ import annotations

from typing import TYPE_CHECKING

from palace.manager.integration.goals import Goals
from palace.manager.integration.registry.base import IntegrationRegistry

if TYPE_CHECKING:
    from palace.manager.api.authentication.base import (  # noqa: autoflake
        AuthenticationProviderType,
    )


class PatronAuthRegistry(IntegrationRegistry["AuthenticationProviderType"]):
    def __init__(self) -> None:
        super().__init__(Goals.PATRON_AUTH_GOAL)
        from palace.manager.api.kansas_patron import KansasAuthenticationAPI
        from palace.manager.api.millenium_patron import MilleniumPatronAPI
        from palace.manager.api.saml.provider import SAMLWebSSOAuthenticationProvider
        from palace.manager.api.simple_authentication import (
            SimpleAuthenticationProvider,
        )
        from palace.manager.api.sip import SIP2AuthenticationProvider
        from palace.manager.api.sirsidynix_authentication_provider import (
            SirsiDynixHorizonAuthenticationProvider,
        )

        self.register(
            SimpleAuthenticationProvider, canonical="api.simple_authentication"
        )
        self.register(MilleniumPatronAPI, canonical="api.millenium_patron")
        self.register(SIP2AuthenticationProvider, canonical="api.sip")
        self.register(KansasAuthenticationAPI, canonical="api.kansas_patron")
        self.register(SAMLWebSSOAuthenticationProvider, canonical="api.saml.provider")
        self.register(
            SirsiDynixHorizonAuthenticationProvider,
            canonical="api.sirsidynix_authentication_provider",
        )
