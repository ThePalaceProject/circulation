from __future__ import annotations

from collections.abc import Callable
from functools import partial
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from api.saml.configuration.model import (
    SAMLOneLoginConfiguration,
    SAMLWebSSOAuthSettings,
)
from api.saml.metadata.model import (
    SAMLIdentityProviderMetadata,
    SAMLServiceProviderMetadata,
)
from api.saml.provider import SAMLWebSSOAuthenticationProvider
from core.integration.settings import BaseSettings
from tests.api.saml.saml_strings import CORRECT_XML_WITH_ONE_SP

if TYPE_CHECKING:
    from tests.fixtures.api_controller import ControllerFixture


@pytest.fixture
def create_saml_configuration() -> Callable[..., SAMLWebSSOAuthSettings]:
    return partial(
        SAMLWebSSOAuthSettings,
        service_provider_xml_metadata=CORRECT_XML_WITH_ONE_SP,
    )


@pytest.fixture
def mock_integration_id() -> int:
    return 20


@pytest.fixture
def create_saml_provider(
    controller_fixture: ControllerFixture,
    mock_integration_id: int,
    create_saml_configuration: Callable[..., SAMLWebSSOAuthSettings],
) -> Callable[..., SAMLWebSSOAuthenticationProvider]:
    library = controller_fixture.db.default_library()
    return partial(
        SAMLWebSSOAuthenticationProvider,
        library_id=library.id,
        integration_id=mock_integration_id,
        settings=create_saml_configuration(),
        library_settings=BaseSettings(),
    )


@pytest.fixture
def create_mock_onelogin_configuration(
    create_saml_configuration,
) -> Callable[..., SAMLOneLoginConfiguration]:
    def _create_mock(
        service_provider: SAMLServiceProviderMetadata,
        identity_providers: list[SAMLIdentityProviderMetadata],
        configuration: SAMLWebSSOAuthSettings | None = None,
    ):
        if configuration is None:
            configuration = create_saml_configuration()
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        onelogin_configuration._load_identity_providers = MagicMock(
            return_value=identity_providers
        )
        onelogin_configuration._load_service_provider = MagicMock(
            return_value=service_provider
        )
        return onelogin_configuration

    return _create_mock
