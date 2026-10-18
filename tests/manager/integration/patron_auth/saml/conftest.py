from __future__ import annotations

from collections.abc import Callable, Generator
from functools import partial
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from palace.manager.integration.patron_auth.saml.configuration.model import (
    SAMLOneLoginConfiguration,
    SAMLWebSSOAuthLibrarySettings,
    SAMLWebSSOAuthSettings,
    sp_environment_check,
)
from palace.manager.integration.patron_auth.saml.configuration.promotion import (
    promotion_configuration,
)
from palace.manager.integration.patron_auth.saml.metadata.model import (
    SAMLIdentityProviderMetadata,
    SAMLServiceProviderMetadata,
)
from palace.manager.integration.patron_auth.saml.provider import (
    SAMLWebSSOAuthenticationProvider,
)
from tests.mocks.saml_strings import CORRECT_XML_WITH_ONE_SP

if TYPE_CHECKING:
    from tests.fixtures.api_controller import ControllerFixture


@pytest.fixture(autouse=True)
def reload_promotion_configuration() -> Generator[None]:
    """Reloads the site-wide promotion defaults for each test.

    They are cached once per process, so a test that sets their environment
    variables would otherwise see the values from an earlier test.
    """
    promotion_configuration.cache_clear()
    yield
    promotion_configuration.cache_clear()


@pytest.fixture(autouse=True)
def recheck_sp_environment() -> Generator[None]:
    """Clears the cached Service Provider environment check for each test.

    Without this, a test that sets the SP environment variables could see the
    result from an earlier test.
    """
    sp_environment_check.cache_clear()
    yield
    sp_environment_check.cache_clear()


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
    assert library.id is not None
    return partial(
        SAMLWebSSOAuthenticationProvider,
        library_id=library.id,
        integration_id=mock_integration_id,
        settings=create_saml_configuration(),
        library_settings=SAMLWebSSOAuthLibrarySettings(),
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
