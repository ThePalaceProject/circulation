from collections.abc import Callable
from datetime import datetime
from unittest.mock import MagicMock, call, create_autospec

import pytest
import sqlalchemy

from palace.manager.api.admin.problem_details import INCOMPLETE_CONFIGURATION
from palace.manager.integration.patron_auth.saml.configuration.model import (
    SAMLOneLoginConfiguration,
    SAMLWebSSOAuthSettings,
)
from palace.manager.integration.patron_auth.saml.configuration.problem_details import (
    SAML_INCORRECT_METADATA,
    SAML_INCORRECT_PRIVATE_KEY,
)
from palace.manager.integration.patron_auth.saml.metadata.federations import incommon
from palace.manager.integration.patron_auth.saml.metadata.model import (
    SAMLIdentityProviderMetadata,
    SAMLNameIDFormat,
    SAMLOrganization,
    SAMLService,
    SAMLServiceProviderMetadata,
    SAMLUIInfo,
)
from palace.manager.integration.patron_auth.saml.metadata.parser import (
    SAMLMetadataParser,
)
from palace.manager.sqlalchemy.model.saml import (
    SAMLFederatedIdentityProvider,
    SAMLFederation,
)
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.test_utils import MonkeyPatchEnvFixture
from tests.mocks import saml_strings

SERVICE_PROVIDER_WITHOUT_CERTIFICATE = SAMLServiceProviderMetadata(
    saml_strings.SP_ENTITY_ID,
    SAMLUIInfo(),
    SAMLOrganization(),
    SAMLNameIDFormat.UNSPECIFIED.value,
    SAMLService(saml_strings.SP_ACS_URL, saml_strings.SP_ACS_BINDING),
)

SERVICE_PROVIDER_WITH_CERTIFICATE = SAMLServiceProviderMetadata(
    saml_strings.SP_ENTITY_ID,
    SAMLUIInfo(),
    SAMLOrganization(),
    SAMLNameIDFormat.UNSPECIFIED.value,
    SAMLService(saml_strings.SP_ACS_URL, saml_strings.SP_ACS_BINDING),
    certificate=saml_strings.SIGNING_CERTIFICATE,
    private_key=saml_strings.PRIVATE_KEY,
)

IDENTITY_PROVIDERS = [
    SAMLIdentityProviderMetadata(
        saml_strings.IDP_1_ENTITY_ID,
        SAMLUIInfo(),
        SAMLOrganization(),
        SAMLNameIDFormat.UNSPECIFIED.value,
        SAMLService(saml_strings.IDP_1_SSO_URL, saml_strings.IDP_1_SSO_BINDING),
    ),
    SAMLIdentityProviderMetadata(
        saml_strings.IDP_2_ENTITY_ID,
        SAMLUIInfo(),
        SAMLOrganization(),
        SAMLNameIDFormat.UNSPECIFIED.value,
        SAMLService(saml_strings.IDP_2_SSO_URL, saml_strings.IDP_2_SSO_BINDING),
    ),
]


class TestSamlIdpConfiguration:
    def test_get_identity_providers_returns_non_federated_idps(
        self,
        db: DatabaseTransactionFixture,
        create_saml_configuration: Callable[..., SAMLWebSSOAuthSettings],
    ):
        # Arrange
        metadata_parser = SAMLMetadataParser()
        metadata_parser.parse = MagicMock(side_effect=metadata_parser.parse)
        configuration = create_saml_configuration(
            non_federated_identity_provider_xml_metadata=saml_strings.CORRECT_XML_WITH_MULTIPLE_IDPS
        )
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        onelogin_configuration._metadata_parser = metadata_parser

        # Act
        identity_providers = onelogin_configuration.get_identity_providers(db.session)

        # Assert
        assert 2 == len(identity_providers)

        assert True == isinstance(identity_providers[0], SAMLIdentityProviderMetadata)
        assert saml_strings.IDP_1_ENTITY_ID == identity_providers[0].entity_id

        assert True == isinstance(identity_providers[1], SAMLIdentityProviderMetadata)
        assert saml_strings.IDP_2_ENTITY_ID == identity_providers[1].entity_id
        metadata_parser.parse.assert_called_once_with(
            configuration.non_federated_identity_provider_xml_metadata
        )

    def test_get_identity_providers_returns_federated_idps(
        self,
        db: DatabaseTransactionFixture,
        create_saml_configuration: Callable[..., SAMLWebSSOAuthSettings],
    ):
        # Arrange
        federated_identity_provider_entity_ids = [
            saml_strings.IDP_1_ENTITY_ID,
            saml_strings.IDP_2_ENTITY_ID,
        ]

        metadata_parser = SAMLMetadataParser()
        metadata_parser.parse = MagicMock(side_effect=metadata_parser.parse)

        federation = SAMLFederation("Test federation", "http://localhost")
        federated_idp_1 = SAMLFederatedIdentityProvider(
            federation,
            saml_strings.IDP_1_ENTITY_ID,
            saml_strings.IDP_1_UI_INFO_EN_DISPLAY_NAME,
            saml_strings.CORRECT_XML_WITH_IDP_1,
        )
        federated_idp_2 = SAMLFederatedIdentityProvider(
            federation,
            saml_strings.IDP_2_ENTITY_ID,
            saml_strings.IDP_2_UI_INFO_EN_DISPLAY_NAME,
            saml_strings.CORRECT_XML_WITH_IDP_2,
        )

        db.session.add_all([federation, federated_idp_1, federated_idp_2])

        configuration = create_saml_configuration(
            federated_identity_provider_entity_ids=federated_identity_provider_entity_ids
        )
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        onelogin_configuration._metadata_parser = metadata_parser

        # Act
        identity_providers = onelogin_configuration.get_identity_providers(db.session)

        # Assert
        assert 2 == len(identity_providers)
        assert isinstance(identity_providers[0], SAMLIdentityProviderMetadata) is True
        assert saml_strings.IDP_1_ENTITY_ID == identity_providers[0].entity_id

        assert isinstance(identity_providers[1], SAMLIdentityProviderMetadata) is True
        assert saml_strings.IDP_2_ENTITY_ID == identity_providers[1].entity_id

        metadata_parser.parse.assert_has_calls(
            [call(federated_idp_1.xml_metadata), call(federated_idp_2.xml_metadata)]
        )

    def test_get_identity_providers_returns_both_non_federated_and_federated_idps(
        self,
        db: DatabaseTransactionFixture,
        create_saml_configuration: Callable[..., SAMLWebSSOAuthSettings],
    ):
        # Arrange
        federated_identity_provider_entity_ids = [
            saml_strings.IDP_1_ENTITY_ID,
            saml_strings.IDP_2_ENTITY_ID,
        ]

        metadata_parser = SAMLMetadataParser()
        metadata_parser.parse = MagicMock(side_effect=metadata_parser.parse)

        federation = SAMLFederation("Test federation", "http://localhost")
        federated_idp_1 = SAMLFederatedIdentityProvider(
            federation,
            saml_strings.IDP_1_ENTITY_ID,
            saml_strings.IDP_1_UI_INFO_EN_DISPLAY_NAME,
            saml_strings.CORRECT_XML_WITH_IDP_1,
        )
        federated_idp_2 = SAMLFederatedIdentityProvider(
            federation,
            saml_strings.IDP_2_ENTITY_ID,
            saml_strings.IDP_2_UI_INFO_EN_DISPLAY_NAME,
            saml_strings.CORRECT_XML_WITH_IDP_2,
        )

        db.session.add_all([federation, federated_idp_1, federated_idp_2])

        configuration = create_saml_configuration(
            non_federated_identity_provider_xml_metadata=saml_strings.CORRECT_XML_WITH_MULTIPLE_IDPS,
            federated_identity_provider_entity_ids=federated_identity_provider_entity_ids,
        )
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        onelogin_configuration._metadata_parser = metadata_parser

        # Act
        identity_providers = onelogin_configuration.get_identity_providers(db.session)

        # Assert
        assert 4 == len(identity_providers)
        assert isinstance(identity_providers[0], SAMLIdentityProviderMetadata) is True
        assert saml_strings.IDP_1_ENTITY_ID == identity_providers[0].entity_id

        assert isinstance(identity_providers[1], SAMLIdentityProviderMetadata) is True
        assert saml_strings.IDP_2_ENTITY_ID == identity_providers[1].entity_id

        assert isinstance(identity_providers[2], SAMLIdentityProviderMetadata) is True
        assert saml_strings.IDP_1_ENTITY_ID == identity_providers[2].entity_id

        assert isinstance(identity_providers[3], SAMLIdentityProviderMetadata) is True
        assert saml_strings.IDP_2_ENTITY_ID == identity_providers[3].entity_id

        metadata_parser.parse.assert_has_calls(
            [
                call(configuration.non_federated_identity_provider_xml_metadata),
                call(federated_idp_1.xml_metadata),
                call(federated_idp_2.xml_metadata),
            ]
        )


class TestSamlSpConfiguration:

    @pytest.mark.parametrize(
        "env_metadata,settings_metadata,env_key,settings_key",
        (
            pytest.param(
                None,
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                None,
                saml_strings.PRIVATE_KEY,
                id="metadata-from-settings-key-from-settings",
            ),
            pytest.param(
                None,
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.PRIVATE_KEY,
                None,
                id="metadata-from-settings-key-from-env",
            ),
            pytest.param(
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                None,
                None,
                saml_strings.PRIVATE_KEY,
                id="metadata-from-env-key-from-settings",
            ),
            pytest.param(
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                None,
                saml_strings.PRIVATE_KEY,
                None,
                id="metadata-from-env-key-from-env",
            ),
            pytest.param(
                None,
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.INVALID_PRIVATE_KEY,
                saml_strings.PRIVATE_KEY,
                id="metadata-from-settings-key-from-both",
            ),
            pytest.param(
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                None,
                saml_strings.INVALID_PRIVATE_KEY,
                saml_strings.PRIVATE_KEY,
                id="metadata-from-env-key-from-both",
            ),
            pytest.param(
                saml_strings.INVALID_XML,
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                None,
                saml_strings.PRIVATE_KEY,
                id="metadata-from-both-key-from-settings",
            ),
            pytest.param(
                saml_strings.INVALID_XML,
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.PRIVATE_KEY,
                None,
                id="metadata-from-both-key-from-env",
            ),
            pytest.param(
                saml_strings.INVALID_XML,
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.INVALID_PRIVATE_KEY,
                saml_strings.PRIVATE_KEY,
                id="metadata-from-both-key-from-both",
            ),
        ),
    )
    def test_get_service_provider_returns_correct_value(
        self,
        monkeypatch_env: MonkeyPatchEnvFixture,
        create_saml_configuration: Callable[..., SAMLWebSSOAuthSettings],
        env_metadata: str | None,
        settings_metadata: str | None,
        env_key: str | None,
        settings_key: str | None,
    ):
        """Test permutations of SP metadata and private key from settings vs environment.

        This test validates that:
        - SP XML metadata can come from integration settings, environment, or both
        - Private key is correctly loaded by either or both sources
        - When both sources present, integration settings take precedence
        - get_service_provider() returns correct SP metadata regardless of source
        """
        # Set/unset environment variables
        monkeypatch_env("PALACE_SAML_SP_METADATA", env_metadata)
        monkeypatch_env("PALACE_SAML_SP_PRIVATE_KEY", env_key)

        # Create configuration with integration settings.
        # This will fail if the selected data is invalid.
        configuration = create_saml_configuration(
            service_provider_xml_metadata=settings_metadata or "",
            service_provider_private_key=settings_key or "",
        )
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)

        # Get service provider and validate
        service_provider = onelogin_configuration.get_service_provider()

        assert isinstance(service_provider, SAMLServiceProviderMetadata)
        assert service_provider.entity_id == saml_strings.SP_ENTITY_ID

        # Verify private key is set (from either source)
        assert service_provider.private_key is not None
        assert service_provider.private_key.replace(
            "\n", ""
        ) == saml_strings.PRIVATE_KEY.replace("\n", "")

    @pytest.mark.parametrize(
        "env_metadata,settings_metadata,env_key,settings_key,expected_problem_detail",
        (
            pytest.param(
                None,
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.PRIVATE_KEY,
                saml_strings.INVALID_PRIVATE_KEY,
                SAML_INCORRECT_PRIVATE_KEY,
                id="metadata-from-settings-key-from-both",
            ),
            pytest.param(
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                None,
                saml_strings.PRIVATE_KEY,
                saml_strings.INVALID_PRIVATE_KEY,
                SAML_INCORRECT_PRIVATE_KEY,
                id="metadata-from-env-key-from-both",
            ),
            pytest.param(
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.INVALID_XML,
                None,
                saml_strings.PRIVATE_KEY,
                SAML_INCORRECT_METADATA,
                id="metadata-from-both-key-from-settings",
            ),
            pytest.param(
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.INVALID_XML,
                saml_strings.PRIVATE_KEY,
                None,
                SAML_INCORRECT_METADATA,
                id="metadata-from-both-key-from-env",
            ),
        ),
    )
    def test_create_saml_configuration_fails_when_superseding_value_invalid(
        self,
        monkeypatch_env: MonkeyPatchEnvFixture,
        create_saml_configuration: Callable[..., SAMLWebSSOAuthSettings],
        env_metadata: str | None,
        settings_metadata: str | None,
        env_key: str | None,
        settings_key: str | None,
        expected_problem_detail: ProblemDetail,
    ):
        """Verify that superseding config is used, even if it's invalid."""

        # Set/unset environment variables
        monkeypatch_env("PALACE_SAML_SP_METADATA", env_metadata)
        monkeypatch_env("PALACE_SAML_SP_PRIVATE_KEY", env_key)

        # Create configuration with integration settings should fail.
        with pytest.raises(ProblemDetailException) as exc:
            create_saml_configuration(
                service_provider_xml_metadata=settings_metadata or "",
                service_provider_private_key=settings_key or "",
            )

        assert exc.value.problem_detail.uri, expected_problem_detail.uri

    def test_no_settings_without_sp_xml(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        # Set a private key, but no SP XML metadata
        monkeypatch.setenv("PALACE_SAML_SP_PRIVATE_KEY", saml_strings.PRIVATE_KEY)
        monkeypatch.delenv("PALACE_SAML_SP_METADATA", raising=False)
        monkeypatch.delenv("PALACE_SAML_SP_METADATA_FILE", raising=False)

        # We cannot even create integration settings
        with pytest.raises(ProblemDetailException) as exc:
            SAMLWebSSOAuthSettings(
                service_provider_xml_metadata="",
                service_provider_private_key="",
            )

        problem_detail = exc.value.problem_detail

        assert problem_detail.uri == INCOMPLETE_CONFIGURATION.uri
        assert problem_detail.title == INCOMPLETE_CONFIGURATION.title
        assert problem_detail.status_code == INCOMPLETE_CONFIGURATION.status_code
        assert problem_detail.detail is not None
        assert problem_detail.detail.startswith(
            "Service Provider's XML Metadata is required."
        )

    def test_no_settings_with_invalid_sp_xml(
        self,
    ):
        with pytest.raises(ProblemDetailException) as exc:
            SAMLWebSSOAuthSettings(
                service_provider_xml_metadata=saml_strings.INVALID_XML,
                service_provider_private_key=saml_strings.PRIVATE_KEY,
            )

        assert exc.value.problem_detail == SAML_INCORRECT_METADATA.detailed(
            "Service Provider's XML metadata (from this setting) must contain exactly one declaration of SPSSODescriptor"
        )

    def test_no_settings_without_sp_private_key(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        # Set SP XML metadata, but no private key
        monkeypatch.setenv(
            "PALACE_SAML_SP_METADATA", saml_strings.CORRECT_XML_WITH_ONE_SP
        )
        monkeypatch.delenv("PALACE_SAML_SP_PRIVATE_KEY", raising=False)
        monkeypatch.delenv("PALACE_SAML_SP_PRIVATE_KEY_FILE", raising=False)

        # We cannot even create integration settings
        with pytest.raises(ProblemDetailException) as exc:
            SAMLWebSSOAuthSettings(
                service_provider_xml_metadata="",
                service_provider_private_key="",
            )

        problem_detail = exc.value.problem_detail

        assert problem_detail.uri == INCOMPLETE_CONFIGURATION.uri
        assert problem_detail.title == INCOMPLETE_CONFIGURATION.title
        assert problem_detail.status_code == INCOMPLETE_CONFIGURATION.status_code
        assert problem_detail.detail is not None
        assert problem_detail.detail.startswith(
            "Service Provider's Private Key is required."
        )

    def test_no_settings_with_invalid_sp_private_key(
        self,
    ):
        with pytest.raises(ProblemDetailException) as exc:
            SAMLWebSSOAuthSettings(
                service_provider_xml_metadata=saml_strings.CORRECT_XML_WITH_ONE_SP,
                service_provider_private_key="wrong-key",
            )

        assert exc.value.problem_detail == SAML_INCORRECT_PRIVATE_KEY.detailed(
            "Service Provider's Private Key (from this setting) is not in a valid format. "
            "The value must include the '-----BEGIN ...-----' header and '-----END ...-----' footer text."
        )

    def test_environment_sp_config_fallback(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Test that SP config falls back to environment variables when not in integration settings."""
        # Arrange - set environment variables
        monkeypatch.setenv(
            "PALACE_SAML_SP_METADATA", saml_strings.CORRECT_XML_WITH_ONE_SP
        )
        monkeypatch.setenv("PALACE_SAML_SP_PRIVATE_KEY", saml_strings.PRIVATE_KEY)

        # Create configuration WITHOUT SP metadata (should fall back to environment)
        # Use defaults for SP fields (empty strings).
        configuration = SAMLWebSSOAuthSettings(
            service_provider_xml_metadata="",
            service_provider_private_key="",
        )
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)

        # Act
        service_provider = onelogin_configuration.get_service_provider()

        # Assert
        assert isinstance(service_provider, SAMLServiceProviderMetadata)
        assert service_provider.entity_id == saml_strings.SP_ENTITY_ID
        assert service_provider.private_key.replace(
            "\n", ""
        ) == saml_strings.PRIVATE_KEY.replace("\n", "")

    def test_integration_settings_override_environment(
        self,
        create_saml_configuration: Callable[..., SAMLWebSSOAuthSettings],
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Test that integration settings take precedence over environment variables."""
        # Arrange - set environment variables (these should be ignored)
        monkeypatch.setenv("PALACE_SAML_SP_METADATA", "<invalid>xml</invalid>")
        monkeypatch.setenv("PALACE_SAML_SP_PRIVATE_KEY", "wrong-key")

        # Create configuration WITH SP metadata (should override environment)
        configuration = create_saml_configuration(
            service_provider_xml_metadata=saml_strings.CORRECT_XML_WITH_ONE_SP,
            service_provider_private_key=saml_strings.PRIVATE_KEY,
        )
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)

        # Act
        service_provider = onelogin_configuration.get_service_provider()

        # Assert - should use integration settings, not environment
        assert isinstance(service_provider, SAMLServiceProviderMetadata)
        assert service_provider.entity_id == saml_strings.SP_ENTITY_ID
        assert service_provider.private_key.replace(
            "\n", ""
        ) == saml_strings.PRIVATE_KEY.replace("\n", "")

    def test_mixed_configuration(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Test mixed configuration: metadata from integration, private key from environment."""
        # Arrange - set private key in environment
        monkeypatch.setenv("PALACE_SAML_SP_PRIVATE_KEY", saml_strings.PRIVATE_KEY)

        # Create configuration with metadata but no private key
        configuration = SAMLWebSSOAuthSettings(
            service_provider_xml_metadata=saml_strings.CORRECT_XML_WITH_ONE_SP,
            service_provider_private_key="",
        )
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)

        # Act
        service_provider = onelogin_configuration.get_service_provider()

        # Assert
        assert isinstance(service_provider, SAMLServiceProviderMetadata)
        assert service_provider.entity_id == saml_strings.SP_ENTITY_ID
        assert service_provider.private_key.replace(
            "\n", ""
        ) == saml_strings.PRIVATE_KEY.replace("\n", "")


class TestSAMLSettings:
    def test(self, db: DatabaseTransactionFixture):
        # Without loading anything into the database there are no federated IdPs and no options
        [federated_identity_provider_entity_ids] = [
            setting
            for setting in SAMLWebSSOAuthSettings.configuration_form(db.session)
            if setting["key"] == "federated_identity_provider_entity_ids"
        ]

        assert len(federated_identity_provider_entity_ids["options"]) == 0

        # Load a federated IdP into the database
        federation = SAMLFederation(
            incommon.FEDERATION_TYPE,
            "http://incommon.org/metadata",
        )
        federation.last_updated_at = datetime.now()
        federated_identity_provider = SAMLFederatedIdentityProvider(
            federation,
            saml_strings.IDP_1_ENTITY_ID,
            saml_strings.IDP_1_UI_INFO_EN_DISPLAY_NAME,
            saml_strings.CORRECT_XML_WITH_IDP_1,
        )

        db.session.add_all([federation, federated_identity_provider])

        [federated_identity_provider_entity_ids] = [
            setting
            for setting in SAMLWebSSOAuthSettings.configuration_form(db.session)
            if setting["key"] == "federated_identity_provider_entity_ids"
        ]

        # After getting an active database session options get initialized
        assert len(federated_identity_provider_entity_ids["options"]) == 1

        # A new idp shows up only after the last updated time
        federated_identity_provider_2 = SAMLFederatedIdentityProvider(
            federation,
            saml_strings.IDP_2_ENTITY_ID,
            saml_strings.IDP_2_UI_INFO_EN_DISPLAY_NAME,
            saml_strings.CORRECT_XML_WITH_IDP_2,
        )
        db.session.add(federated_identity_provider_2)

        [federated_identity_provider_entity_ids] = [
            setting
            for setting in SAMLWebSSOAuthSettings.configuration_form(db.session)
            if setting["key"] == "federated_identity_provider_entity_ids"
        ]

        # Only the first shows up yet
        assert 1 == len(federated_identity_provider_entity_ids["options"])

        federation.last_updated_at = datetime.now()
        [federated_identity_provider_entity_ids] = [
            setting
            for setting in SAMLWebSSOAuthSettings.configuration_form(db.session)
            if setting["key"] == "federated_identity_provider_entity_ids"
        ]
        assert 2 == len(federated_identity_provider_entity_ids["options"])


class TestSAMLOneLoginConfiguration:
    def test_get_identity_provider_settings_returns_correct_result(self):
        # Arrange
        configuration = create_autospec(spec=SAMLWebSSOAuthSettings)
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        onelogin_configuration.get_identity_providers = MagicMock(
            return_value=IDENTITY_PROVIDERS
        )
        expected_result = {
            "idp": {
                "entityId": IDENTITY_PROVIDERS[0].entity_id,
                "singleSignOnService": {
                    "url": IDENTITY_PROVIDERS[0].sso_service.url,
                    "binding": IDENTITY_PROVIDERS[0].sso_service.binding.value,
                },
            },
            "security": {
                "authnRequestsSigned": IDENTITY_PROVIDERS[0].want_authn_requests_signed
            },
        }
        db = create_autospec(spec=sqlalchemy.orm.session.Session)

        # Act
        result = onelogin_configuration.get_identity_provider_settings(
            db, IDENTITY_PROVIDERS[0].entity_id
        )

        # Assert
        assert result == expected_result
        onelogin_configuration.get_identity_providers.assert_called_once_with(db)

    @pytest.mark.parametrize(
        "service_provider,expected_result",
        [
            pytest.param(
                SERVICE_PROVIDER_WITHOUT_CERTIFICATE,
                {
                    "sp": {
                        "entityId": SERVICE_PROVIDER_WITH_CERTIFICATE.entity_id,
                        "assertionConsumerService": {
                            "url": SERVICE_PROVIDER_WITH_CERTIFICATE.acs_service.url,
                            "binding": SERVICE_PROVIDER_WITH_CERTIFICATE.acs_service.binding.value,
                        },
                        "NameIDFormat": SERVICE_PROVIDER_WITH_CERTIFICATE.name_id_format,
                        "x509cert": "",
                        "privateKey": "",
                    },
                    "security": {
                        "authnRequestsSigned": SERVICE_PROVIDER_WITH_CERTIFICATE.authn_requests_signed
                    },
                },
                id="service_provider_without_certificates",
            ),
            pytest.param(
                SERVICE_PROVIDER_WITH_CERTIFICATE,
                {
                    "sp": {
                        "entityId": SERVICE_PROVIDER_WITH_CERTIFICATE.entity_id,
                        "assertionConsumerService": {
                            "url": SERVICE_PROVIDER_WITH_CERTIFICATE.acs_service.url,
                            "binding": SERVICE_PROVIDER_WITH_CERTIFICATE.acs_service.binding.value,
                        },
                        "NameIDFormat": SERVICE_PROVIDER_WITH_CERTIFICATE.name_id_format,
                        "x509cert": saml_strings.strip_certificate(
                            SERVICE_PROVIDER_WITH_CERTIFICATE.certificate
                        ),
                        "privateKey": SERVICE_PROVIDER_WITH_CERTIFICATE.private_key,
                    },
                    "security": {
                        "authnRequestsSigned": SERVICE_PROVIDER_WITH_CERTIFICATE.authn_requests_signed
                    },
                },
                id="service_provider_with_certificate",
            ),
        ],
    )
    def test_get_service_provider_settings_returns_correct_result(
        self, service_provider, expected_result
    ):
        # Arrange
        configuration = create_autospec(spec=SAMLWebSSOAuthSettings)
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        onelogin_configuration.get_service_provider = MagicMock(
            return_value=service_provider
        )

        # Act
        result = onelogin_configuration.get_service_provider_settings()

        # Assert
        result["sp"]["x509cert"] = saml_strings.strip_certificate(
            result["sp"]["x509cert"]
        )

        assert result == expected_result
        onelogin_configuration.get_service_provider.assert_called_once()

    def test_get_settings_returns_correct_result(self, create_saml_configuration):
        # Arrange
        debug = 0
        strict = 0

        configuration = create_saml_configuration(
            service_provider_strict_mode=debug,
            service_provider_debug_mode=strict,
        )

        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        onelogin_configuration.get_service_provider = MagicMock(
            return_value=SERVICE_PROVIDER_WITH_CERTIFICATE
        )
        onelogin_configuration.get_identity_providers = MagicMock(
            return_value=IDENTITY_PROVIDERS
        )

        expected_result = {
            "debug": debug,
            "strict": strict,
            "idp": {
                "entityId": IDENTITY_PROVIDERS[0].entity_id,
                "singleSignOnService": {
                    "url": IDENTITY_PROVIDERS[0].sso_service.url,
                    "binding": IDENTITY_PROVIDERS[0].sso_service.binding.value,
                },
                "singleLogoutService": {},
                "x509cert": "",
                "certFingerprint": "",
                "certFingerprintAlgorithm": "sha1",
            },
            "sp": {
                "entityId": SERVICE_PROVIDER_WITH_CERTIFICATE.entity_id,
                "assertionConsumerService": {
                    "url": SERVICE_PROVIDER_WITH_CERTIFICATE.acs_service.url,
                    "binding": SERVICE_PROVIDER_WITH_CERTIFICATE.acs_service.binding.value,
                },
                "attributeConsumingService": {},
                "singleLogoutService": {
                    "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"
                },
                "NameIDFormat": SERVICE_PROVIDER_WITH_CERTIFICATE.name_id_format,
                "x509cert": saml_strings.strip_certificate(
                    SERVICE_PROVIDER_WITH_CERTIFICATE.certificate
                ),
                "privateKey": SERVICE_PROVIDER_WITH_CERTIFICATE.private_key,
            },
            "security": {
                "failOnAuthnContextMismatch": False,
                "requestedAuthnContextComparison": "exact",
                "wantNameIdEncrypted": False,
                "authnRequestsSigned": SERVICE_PROVIDER_WITH_CERTIFICATE.authn_requests_signed
                or IDENTITY_PROVIDERS[0].want_authn_requests_signed,
                "logoutResponseSigned": False,
                "wantMessagesSigned": False,
                "metadataCacheDuration": None,
                "requestedAuthnContext": True,
                "logoutRequestSigned": False,
                "wantAttributeStatement": True,
                "signMetadata": False,
                "digestAlgorithm": "http://www.w3.org/2001/04/xmlenc#sha256",
                "metadataValidUntil": None,
                "wantAssertionsSigned": False,
                "wantNameId": True,
                "wantAssertionsEncrypted": False,
                "nameIdEncrypted": False,
                "rejectDeprecatedAlgorithm": False,
                "signatureAlgorithm": "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256",
                "allowRepeatAttributeName": False,
            },
        }
        db = create_autospec(spec=sqlalchemy.orm.session.Session)

        # Act
        result = onelogin_configuration.get_settings(
            db, IDENTITY_PROVIDERS[0].entity_id
        )

        # Assert
        result["sp"]["x509cert"] = saml_strings.strip_certificate(
            result["sp"]["x509cert"]
        )

        assert result == expected_result
        onelogin_configuration.get_service_provider.assert_called_with()
        onelogin_configuration.get_identity_providers.assert_called_with(db)
