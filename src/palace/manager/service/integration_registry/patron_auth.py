from __future__ import annotations

from typing import TYPE_CHECKING

from palace.manager.integration.goals import Goals
from palace.manager.integration.patron_auth.minimal_authentication import (
    MinimalAuthenticationProvider,
)
from palace.manager.service.integration_registry.base import IntegrationRegistry

if TYPE_CHECKING:
    from palace.manager.api.authentication.base import (  # noqa: autoflake
        AuthenticationProviderType,
    )


class PatronAuthRegistry(IntegrationRegistry["AuthenticationProviderType"]):
    def __init__(self) -> None:
        super().__init__(Goals.PATRON_AUTH_GOAL)
        from palace.manager.integration.patron_auth.kansas_patron import (
            KansasAuthenticationAPI,
        )
        from palace.manager.integration.patron_auth.millenium_patron import (
            MilleniumPatronAPI,
        )
        from palace.manager.integration.patron_auth.oidc.provider import (
            OIDCAuthenticationProvider,
        )
        from palace.manager.integration.patron_auth.saml.provider import (
            SAMLWebSSOAuthenticationProvider,
        )
        from palace.manager.integration.patron_auth.simple_authentication import (
            SimpleAuthenticationProvider,
        )
        from palace.manager.integration.patron_auth.sip2.provider import (
            SIP2AuthenticationProvider,
        )
        from palace.manager.integration.patron_auth.sirsidynix_authentication_provider import (
            SirsiDynixHorizonAuthenticationProvider,
        )

        self.register(
            SimpleAuthenticationProvider, canonical="api.simple_authentication"
        )
        self.register(
            MinimalAuthenticationProvider, canonical="api.minimal_authentication"
        )
        self.register(MilleniumPatronAPI, canonical="api.millenium_patron")
        self.register(SIP2AuthenticationProvider, canonical="api.sip")
        self.register(KansasAuthenticationAPI, canonical="api.kansas_patron")
        self.register(OIDCAuthenticationProvider, canonical="api.oidc.provider")
        self.register(SAMLWebSSOAuthenticationProvider, canonical="api.saml.provider")
        self.register(
            SirsiDynixHorizonAuthenticationProvider,
            canonical="api.sirsidynix_authentication_provider",
        )
