from __future__ import annotations

from typing import TYPE_CHECKING

from core.integration.goals import Goals
from core.integration.registry import IntegrationRegistry, SingletonIntegrationRegistry

if TYPE_CHECKING:
    from api.authenticator import AuthenticationProvider


def create_patron_auth_registry() -> IntegrationRegistry[AuthenticationProvider]:
    from api.firstbook2 import FirstBookAuthenticationAPI
    from api.kansas_patron import KansasAuthenticationAPI
    from api.millenium_patron import MilleniumPatronAPI
    from api.saml.provider import SAMLWebSSOAuthenticationProvider
    from api.simple_authentication import SimpleAuthenticationProvider
    from api.sip import SIP2AuthenticationProvider
    from api.sirsidynix_authentication_provider import (
        SirsiDynixHorizonAuthenticationProvider,
    )

    registry: IntegrationRegistry[AuthenticationProvider] = IntegrationRegistry(
        Goals.PATRON_AUTH_GOAL
    )
    registry.register(
        SimpleAuthenticationProvider, canonical="api.simple_authentication"
    )
    registry.register(MilleniumPatronAPI, canonical="api.millenium_patron")
    registry.register(SIP2AuthenticationProvider, canonical="api.sip")
    registry.register(FirstBookAuthenticationAPI, canonical="api.firstbook2")
    registry.register(KansasAuthenticationAPI, canonical="api.kansas_patron")
    registry.register(SAMLWebSSOAuthenticationProvider, canonical="api.saml.provider")
    registry.register(
        SirsiDynixHorizonAuthenticationProvider,
        canonical="api.sirsidynix_authentication_provider",
    )

    return registry


patron_auth_registry: SingletonIntegrationRegistry[
    AuthenticationProvider
] = SingletonIntegrationRegistry(create_patron_auth_registry)
