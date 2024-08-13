from typing import Any

import pytest

from palace.manager.api.authentication.base import AuthenticationProvider
from palace.manager.api.millenium_patron import (
    MilleniumPatronAPI,
    MilleniumPatronSettings,
)
from palace.manager.api.saml.configuration.model import SAMLWebSSOAuthSettings
from palace.manager.api.saml.provider import SAMLWebSSOAuthenticationProvider
from palace.manager.api.simple_authentication import (
    SimpleAuthenticationProvider,
    SimpleAuthSettings,
)
from palace.manager.api.sip import SIP2AuthenticationProvider, SIP2Settings
from palace.manager.integration.goals import Goals
from palace.manager.integration.settings import BaseSettings
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from palace.manager.sqlalchemy.model.library import Library
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks.saml_strings import CORRECT_XML_WITH_ONE_SP


class AuthIntegrationFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db

    def auth_integration(
        self,
        protocol: type[AuthenticationProvider[Any, Any]],
        library: Library | None,
        settings: BaseSettings,
    ) -> tuple[IntegrationConfiguration, IntegrationLibraryConfiguration | None]:
        integration = self.db.integration_configuration(
            protocol,
            Goals.PATRON_AUTH_GOAL,
            libraries=[library] if library is not None else None,
            settings=settings,
        )
        library_integration = integration.for_library(library)
        return integration, library_integration

    def simple_auth(
        self,
        library: Library | None = None,
        test_identifier: str = "username1",
        test_password: str = "password1",
    ) -> tuple[IntegrationConfiguration, IntegrationLibraryConfiguration | None]:
        return self.auth_integration(
            SimpleAuthenticationProvider,
            library=library,
            settings=SimpleAuthSettings(
                test_identifier=test_identifier,
                test_password=test_password,
            ),
        )

    def millenium_patron(
        self,
        library: Library | None = None,
        url: str = "http://url.com/",
        **kwargs: str
    ) -> tuple[IntegrationConfiguration, IntegrationLibraryConfiguration | None]:
        return self.auth_integration(
            MilleniumPatronAPI,
            library=library,
            settings=MilleniumPatronSettings(url=url, **kwargs),
        )

    def sip2(
        self,
        library: Library | None = None,
        url: str = "http://url.com/",
        **kwargs: str
    ) -> tuple[IntegrationConfiguration, IntegrationLibraryConfiguration | None]:
        return self.auth_integration(
            SIP2AuthenticationProvider,
            library,
            settings=SIP2Settings(url=url, **kwargs),
        )

    def saml(
        self, library: Library | None = None, **kwargs: str
    ) -> tuple[IntegrationConfiguration, IntegrationLibraryConfiguration | None]:
        if "service_provider_xml_metadata" not in kwargs:
            kwargs["service_provider_xml_metadata"] = CORRECT_XML_WITH_ONE_SP
        return self.auth_integration(
            SAMLWebSSOAuthenticationProvider,
            library,
            settings=SAMLWebSSOAuthSettings(**kwargs),
        )


@pytest.fixture
def auth_integration_fixture(db: DatabaseTransactionFixture) -> AuthIntegrationFixture:
    return AuthIntegrationFixture(db)
