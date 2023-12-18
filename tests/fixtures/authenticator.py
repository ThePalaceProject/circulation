from typing import Optional

import pytest

from api.authentication.base import AuthenticationProviderType
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
from tests.fixtures.database import (
    IntegrationConfigurationFixture,
    IntegrationLibraryConfigurationFixture,
)

AuthProviderFixture = tuple[
    IntegrationConfiguration, Optional[IntegrationLibraryConfiguration]
]


class CreateAuthIntegrationFixture:
    def __init__(
        self,
        integration_configuration: IntegrationConfigurationFixture,
        integration_library_configuration: IntegrationLibraryConfigurationFixture,
    ):
        self.integration_configuration = integration_configuration
        self.integration_library_configuration = integration_library_configuration

    def __call__(
        self,
        protocol: str,
        library: Library | None,
        settings_dict: dict[str, str] | None = None,
        library_settings_dict: dict[str, str] | None = None,
    ) -> AuthProviderFixture:
        settings_dict = settings_dict or {}
        library_settings_dict = library_settings_dict or {}
        integration = self.integration_configuration(
            protocol,
            Goals.PATRON_AUTH_GOAL,
            settings_dict,
        )
        if library is not None:
            library_integration = self.integration_library_configuration(
                library, integration, library_settings_dict
            )
        else:
            library_integration = None
        return integration, library_integration


@pytest.fixture
def create_auth_integration_configuration(
    create_integration_configuration: IntegrationConfigurationFixture,
    create_integration_library_configuration: IntegrationLibraryConfigurationFixture,
) -> CreateAuthIntegrationFixture:
    return CreateAuthIntegrationFixture(
        create_integration_configuration, create_integration_library_configuration
    )


@pytest.fixture()
def patron_auth_registry() -> PatronAuthRegistry:
    return PatronAuthRegistry()


class AuthProtocolFixture:
    def __init__(self, registry: PatronAuthRegistry):
        self.registry = registry

    def __call__(self, protocol: type[AuthenticationProviderType]) -> str:
        return self.registry.get_protocol(protocol, "")


@pytest.fixture
def get_auth_protocol(
    patron_auth_registry: PatronAuthRegistry,
) -> AuthProtocolFixture:
    return AuthProtocolFixture(patron_auth_registry)


class SimpleAuthIntegrationFixture:
    def __init__(
        self,
        create_auth_integration_configuration: CreateAuthIntegrationFixture,
        get_auth_protocol: AuthProtocolFixture,
    ):
        self.create_auth_integration_configuration = (
            create_auth_integration_configuration
        )
        self.get_auth_protocol = get_auth_protocol

    def __call__(
        self,
        library: Library | None = None,
        test_identifier: str = "username1",
        test_password: str = "password1",
    ) -> AuthProviderFixture:
        return self.create_auth_integration_configuration(
            self.get_auth_protocol(SimpleAuthenticationProvider),
            library,
            dict(
                test_identifier=test_identifier,
                test_password=test_password,
            ),
        )


@pytest.fixture
def create_simple_auth_integration(
    create_auth_integration_configuration: CreateAuthIntegrationFixture,
    get_auth_protocol: AuthProtocolFixture,
) -> SimpleAuthIntegrationFixture:
    return SimpleAuthIntegrationFixture(
        create_auth_integration_configuration, get_auth_protocol
    )


class MilleniumAuthIntegrationFixture:
    def __init__(
        self,
        create_auth_integration_configuration: CreateAuthIntegrationFixture,
        get_auth_protocol: AuthProtocolFixture,
    ):
        self.create_auth_integration_configuration = (
            create_auth_integration_configuration
        )
        self.get_auth_protocol = get_auth_protocol

    def __call__(
        self, library: Library | None = None, **kwargs: str
    ) -> AuthProviderFixture:
        if "url" not in kwargs:
            kwargs["url"] = "http://url.com/"
        return self.create_auth_integration_configuration(
            self.get_auth_protocol(MilleniumPatronAPI),
            library,
            kwargs,
        )


@pytest.fixture
def create_millenium_auth_integration(
    create_auth_integration_configuration: CreateAuthIntegrationFixture,
    get_auth_protocol: AuthProtocolFixture,
) -> MilleniumAuthIntegrationFixture:
    return MilleniumAuthIntegrationFixture(
        create_auth_integration_configuration, get_auth_protocol
    )


class Sip2AuthIntegrationFixture:
    def __init__(
        self,
        create_auth_integration_configuration: CreateAuthIntegrationFixture,
        get_auth_protocol: AuthProtocolFixture,
    ):
        self.create_auth_integration_configuration = (
            create_auth_integration_configuration
        )
        self.get_auth_protocol = get_auth_protocol

    def __call__(
        self, library: Library | None = None, **kwargs: str
    ) -> AuthProviderFixture:
        if "url" not in kwargs:
            kwargs["url"] = "url.com"
        return self.create_auth_integration_configuration(
            self.get_auth_protocol(SIP2AuthenticationProvider),
            library,
            kwargs,
        )


@pytest.fixture
def create_sip2_auth_integration(
    create_auth_integration_configuration: CreateAuthIntegrationFixture,
    get_auth_protocol: AuthProtocolFixture,
) -> Sip2AuthIntegrationFixture:
    return Sip2AuthIntegrationFixture(
        create_auth_integration_configuration, get_auth_protocol
    )


class SamlAuthIntegrationFixture:
    def __init__(
        self,
        create_auth_integration_configuration: CreateAuthIntegrationFixture,
        get_auth_protocol: AuthProtocolFixture,
    ):
        self.create_auth_integration_configuration = (
            create_auth_integration_configuration
        )
        self.get_auth_protocol = get_auth_protocol

    def __call__(
        self, library: Library | None = None, **kwargs: str
    ) -> AuthProviderFixture:
        if "service_provider_xml_metadata" not in kwargs:
            kwargs["service_provider_xml_metadata"] = CORRECT_XML_WITH_ONE_SP
        return self.create_auth_integration_configuration(
            self.get_auth_protocol(SAMLWebSSOAuthenticationProvider),
            library,
            kwargs,
        )


@pytest.fixture
def create_saml_auth_integration(
    create_auth_integration_configuration: CreateAuthIntegrationFixture,
    get_auth_protocol: AuthProtocolFixture,
) -> SamlAuthIntegrationFixture:
    return SamlAuthIntegrationFixture(
        create_auth_integration_configuration, get_auth_protocol
    )
