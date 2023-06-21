from typing import Callable, Optional, Tuple, Type

import pytest

from api.authentication.base import AuthenticationProvider
from api.integration.registry.patron_auth import PatronAuthRegistry
from api.millenium_patron import MilleniumPatronAPI
from api.saml.provider import SAMLWebSSOAuthenticationProvider
from api.simple_authentication import SimpleAuthenticationProvider
from api.sip import SIP2AuthenticationProvider
from core.integration.goals import Goals
from core.model import Library
from core.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from tests.api.saml.saml_strings import CORRECT_XML_WITH_ONE_SP

AuthProviderFixture = Tuple[
    IntegrationConfiguration, Optional[IntegrationLibraryConfiguration]
]


@pytest.fixture
def create_auth_integration_configuration(
    create_integration_configuration,
    create_integration_library_configuration: Callable[
        ..., IntegrationLibraryConfiguration
    ],
) -> Callable[..., AuthProviderFixture]:
    def create_integration(
        protocol: str,
        library: Optional[Library],
        settings: Optional[dict] = None,
        library_settings: Optional[dict] = None,
    ) -> AuthProviderFixture:
        settings = settings or {}
        library_settings = library_settings or {}
        integration = create_integration_configuration(
            protocol,
            Goals.PATRON_AUTH_GOAL,
            settings,
        )
        if library is not None:
            library_integration = create_integration_library_configuration(
                library, integration, library_settings
            )
        else:
            library_integration = None
        return integration, library_integration

    return create_integration


@pytest.fixture()
def patron_auth_registry() -> PatronAuthRegistry:
    return PatronAuthRegistry()


@pytest.fixture
def get_auth_protocol(
    patron_auth_registry: PatronAuthRegistry,
) -> Callable[[Type[AuthenticationProvider]], Optional[str]]:
    return lambda x: patron_auth_registry.get_protocol(x)


@pytest.fixture
def create_simple_auth_integration(
    create_auth_integration_configuration: Callable[..., AuthProviderFixture],
    get_auth_protocol: Callable[[Type[AuthenticationProvider]], Optional[str]],
) -> Callable[..., AuthProviderFixture]:
    def create_integration(
        library: Optional[Library] = None,
        test_identifier: str = "username1",
        test_password: str = "password1",
    ) -> AuthProviderFixture:
        return create_auth_integration_configuration(
            get_auth_protocol(SimpleAuthenticationProvider),
            library,
            dict(
                test_identifier=test_identifier,
                test_password=test_password,
            ),
        )

    return create_integration


@pytest.fixture
def create_millenium_auth_integration(
    create_auth_integration_configuration: Callable[..., AuthProviderFixture],
    get_auth_protocol: Callable[[Type[AuthenticationProvider]], Optional[str]],
) -> Callable[..., AuthProviderFixture]:
    protocol = get_auth_protocol(MilleniumPatronAPI)

    def create_integration(
        library: Optional[Library] = None, **kwargs
    ) -> AuthProviderFixture:
        if "url" not in kwargs:
            kwargs["url"] = "http://url.com/"
        return create_auth_integration_configuration(
            protocol,
            library,
            kwargs,
        )

    return create_integration


@pytest.fixture
def create_sip2_auth_integration(
    create_auth_integration_configuration: Callable[..., AuthProviderFixture],
    get_auth_protocol: Callable[[Type[AuthenticationProvider]], Optional[str]],
) -> Callable[..., AuthProviderFixture]:
    protocol = get_auth_protocol(SIP2AuthenticationProvider)

    def create_integration(
        library: Optional[Library] = None, **kwargs
    ) -> AuthProviderFixture:
        if "url" not in kwargs:
            kwargs["url"] = "url.com"
        return create_auth_integration_configuration(
            protocol,
            library,
            kwargs,
        )

    return create_integration


@pytest.fixture
def create_saml_auth_integration(
    create_auth_integration_configuration: Callable[..., AuthProviderFixture],
    get_auth_protocol: Callable[[Type[AuthenticationProvider]], Optional[str]],
) -> Callable[..., AuthProviderFixture]:
    protocol = get_auth_protocol(SAMLWebSSOAuthenticationProvider)

    def create_integration(
        library: Optional[Library] = None, **kwargs
    ) -> AuthProviderFixture:
        if "service_provider_xml_metadata" not in kwargs:
            kwargs["service_provider_xml_metadata"] = CORRECT_XML_WITH_ONE_SP
        return create_auth_integration_configuration(
            protocol,
            library,
            kwargs,
        )

    return create_integration
