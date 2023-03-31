import json
from datetime import datetime
from unittest.mock import MagicMock, PropertyMock, call, create_autospec

import pytest
import sqlalchemy
from parameterized import parameterized

from api.app import initialize_database
from api.authenticator import BaseSAMLAuthenticationProvider
from api.saml.configuration.model import (
    SAMLConfiguration,
    SAMLConfigurationFactory,
    SAMLOneLoginConfiguration,
)
from api.saml.metadata.federations import incommon
from api.saml.metadata.federations.model import (
    SAMLFederatedIdentityProvider,
    SAMLFederation,
)
from api.saml.metadata.model import (
    SAMLIdentityProviderMetadata,
    SAMLNameIDFormat,
    SAMLOrganization,
    SAMLService,
    SAMLServiceProviderMetadata,
    SAMLUIInfo,
)
from api.saml.metadata.parser import SAMLMetadataParser
from core.model.configuration import (
    ConfigurationStorage,
    ExternalIntegration,
    HasExternalIntegration,
)
from tests.api.saml import saml_strings
from tests.fixtures.database import DatabaseTransactionFixture

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


class SAMLModelFixture:
    db: DatabaseTransactionFixture
    saml_provider_integration: ExternalIntegration
    saml_integration_association: MagicMock

    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db

        self.saml_provider_integration = self.db.external_integration(
            "api.saml.provider", ExternalIntegration.PATRON_AUTH_GOAL
        )

        self.saml_integration_association = create_autospec(spec=HasExternalIntegration)
        self.saml_integration_association.external_integration = MagicMock(
            return_value=self.saml_provider_integration
        )


@pytest.fixture(scope="function")
def saml_model_fixture(db: DatabaseTransactionFixture) -> SAMLModelFixture:
    return SAMLModelFixture(db)


class TestSAMLConfiguration:
    def test_get_service_provider_returns_correct_value(
        self, saml_model_fixture: SAMLModelFixture
    ):
        # Arrange
        service_provider_metadata = saml_strings.CORRECT_XML_WITH_ONE_SP

        metadata_parser = SAMLMetadataParser()
        metadata_parser.parse = MagicMock(side_effect=metadata_parser.parse)  # type: ignore

        configuration_storage = ConfigurationStorage(
            saml_model_fixture.saml_integration_association
        )
        configuration_storage.load = MagicMock(side_effect=configuration_storage.load)  # type: ignore

        saml_configuration_factory = SAMLConfigurationFactory(metadata_parser)

        with saml_configuration_factory.create(
            configuration_storage, saml_model_fixture.db.session, SAMLConfiguration
        ) as configuration:
            configuration.service_provider_xml_metadata = service_provider_metadata

            # Act
            service_provider = configuration.get_service_provider(
                saml_model_fixture.db.session
            )

            # Assert
            assert True == isinstance(service_provider, SAMLServiceProviderMetadata)
            assert saml_strings.SP_ENTITY_ID == service_provider.entity_id

            configuration_storage.load.assert_has_calls(
                [
                    call(
                        saml_model_fixture.db.session,
                        SAMLConfiguration.service_provider_xml_metadata.key,
                    ),
                    call(
                        saml_model_fixture.db.session,
                        SAMLConfiguration.service_provider_private_key.key,
                    ),
                ]
            )
            metadata_parser.parse.assert_called_once_with(service_provider_metadata)

    def test_get_identity_providers_returns_non_federated_idps(
        self, saml_model_fixture: SAMLModelFixture
    ):
        # Arrange
        identity_providers_metadata = saml_strings.CORRECT_XML_WITH_MULTIPLE_IDPS

        metadata_parser = SAMLMetadataParser()
        metadata_parser.parse = MagicMock(side_effect=metadata_parser.parse)  # type: ignore

        configuration_storage = ConfigurationStorage(
            saml_model_fixture.saml_integration_association
        )
        configuration_storage.load = MagicMock(side_effect=configuration_storage.load)  # type: ignore

        saml_configuration_factory = SAMLConfigurationFactory(metadata_parser)

        with saml_configuration_factory.create(
            configuration_storage, saml_model_fixture.db.session, SAMLConfiguration
        ) as configuration:
            configuration.non_federated_identity_provider_xml_metadata = (
                identity_providers_metadata
            )

            # Act
            identity_providers = configuration.get_identity_providers(
                saml_model_fixture.db.session
            )

            # Assert
            assert 2 == len(identity_providers)

            assert True == isinstance(
                identity_providers[0], SAMLIdentityProviderMetadata
            )
            assert saml_strings.IDP_1_ENTITY_ID == identity_providers[0].entity_id

            assert True == isinstance(
                identity_providers[1], SAMLIdentityProviderMetadata
            )
            assert saml_strings.IDP_2_ENTITY_ID == identity_providers[1].entity_id

            configuration_storage.load.assert_has_calls(
                [
                    call(
                        saml_model_fixture.db.session,
                        SAMLConfiguration.non_federated_identity_provider_xml_metadata.key,
                    ),
                    call(
                        saml_model_fixture.db.session,
                        SAMLConfiguration.federated_identity_provider_entity_ids.key,
                    ),
                ]
            )
            metadata_parser.parse.assert_called_once_with(identity_providers_metadata)

    def test_get_identity_providers_returns_federated_idps(
        self, saml_model_fixture: SAMLModelFixture
    ):
        # Arrange
        federated_identity_provider_entity_ids = json.dumps(
            [saml_strings.IDP_1_ENTITY_ID, saml_strings.IDP_2_ENTITY_ID]
        )

        metadata_parser = SAMLMetadataParser()
        metadata_parser.parse = MagicMock(side_effect=metadata_parser.parse)  # type: ignore

        configuration_storage = ConfigurationStorage(
            saml_model_fixture.saml_integration_association
        )
        configuration_storage.load = MagicMock(side_effect=configuration_storage.load)  # type: ignore

        saml_configuration_factory = SAMLConfigurationFactory(metadata_parser)

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

        saml_model_fixture.db.session.add_all(
            [federation, federated_idp_1, federated_idp_2]
        )

        with saml_configuration_factory.create(
            configuration_storage, saml_model_fixture.db.session, SAMLConfiguration
        ) as configuration:
            configuration.federated_identity_provider_entity_ids = (
                federated_identity_provider_entity_ids
            )

            # Act
            identity_providers = configuration.get_identity_providers(
                saml_model_fixture.db.session
            )

            # Assert
            assert 2 == len(identity_providers)
            assert True == isinstance(
                identity_providers[0], SAMLIdentityProviderMetadata
            )
            assert saml_strings.IDP_1_ENTITY_ID == identity_providers[0].entity_id

            assert True == isinstance(
                identity_providers[1], SAMLIdentityProviderMetadata
            )
            assert saml_strings.IDP_2_ENTITY_ID == identity_providers[1].entity_id

            configuration_storage.load.assert_has_calls(
                [
                    call(
                        saml_model_fixture.db.session,
                        SAMLConfiguration.non_federated_identity_provider_xml_metadata.key,
                    ),
                    call(
                        saml_model_fixture.db.session,
                        SAMLConfiguration.federated_identity_provider_entity_ids.key,
                    ),
                ]
            )
            metadata_parser.parse.assert_has_calls(
                [call(federated_idp_1.xml_metadata), call(federated_idp_2.xml_metadata)]
            )

    def test_get_identity_providers_returns_both_non_federated_and_federated_idps(
        self, saml_model_fixture: SAMLModelFixture
    ):
        # Arrange
        non_federated_identity_providers_metadata = (
            saml_strings.CORRECT_XML_WITH_MULTIPLE_IDPS
        )

        federated_identity_provider_entity_ids = json.dumps(
            [saml_strings.IDP_1_ENTITY_ID, saml_strings.IDP_2_ENTITY_ID]
        )

        metadata_parser = SAMLMetadataParser()
        metadata_parser.parse = MagicMock(side_effect=metadata_parser.parse)  # type: ignore

        configuration_storage = ConfigurationStorage(
            saml_model_fixture.saml_integration_association
        )
        configuration_storage.load = MagicMock(side_effect=configuration_storage.load)  # type: ignore

        saml_configuration_factory = SAMLConfigurationFactory(metadata_parser)

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

        saml_model_fixture.db.session.add_all(
            [federation, federated_idp_1, federated_idp_2]
        )

        with saml_configuration_factory.create(
            configuration_storage, saml_model_fixture.db.session, SAMLConfiguration
        ) as configuration:
            configuration.non_federated_identity_provider_xml_metadata = (
                non_federated_identity_providers_metadata
            )
            configuration.federated_identity_provider_entity_ids = (
                federated_identity_provider_entity_ids
            )

            # Act
            identity_providers = configuration.get_identity_providers(
                saml_model_fixture.db.session
            )

            # Assert
            assert 4 == len(identity_providers)
            assert True == isinstance(
                identity_providers[0], SAMLIdentityProviderMetadata
            )
            assert saml_strings.IDP_1_ENTITY_ID == identity_providers[0].entity_id

            assert True == isinstance(
                identity_providers[1], SAMLIdentityProviderMetadata
            )
            assert saml_strings.IDP_2_ENTITY_ID == identity_providers[1].entity_id

            assert True == isinstance(
                identity_providers[2], SAMLIdentityProviderMetadata
            )
            assert saml_strings.IDP_1_ENTITY_ID == identity_providers[2].entity_id

            assert True == isinstance(
                identity_providers[3], SAMLIdentityProviderMetadata
            )
            assert saml_strings.IDP_2_ENTITY_ID == identity_providers[3].entity_id

            configuration_storage.load.assert_has_calls(
                [
                    call(
                        saml_model_fixture.db.session,
                        SAMLConfiguration.non_federated_identity_provider_xml_metadata.key,
                    ),
                    call(
                        saml_model_fixture.db.session,
                        SAMLConfiguration.federated_identity_provider_entity_ids.key,
                    ),
                ]
            )
            metadata_parser.parse.assert_has_calls(
                [
                    call(non_federated_identity_providers_metadata),
                    call(federated_idp_1.xml_metadata),
                    call(federated_idp_2.xml_metadata),
                ]
            )


class TestSAMLSettings:
    def test(self):
        # Arrange

        # Act, assert
        [federated_identity_provider_entity_ids] = [
            setting
            for setting in BaseSAMLAuthenticationProvider.SETTINGS
            if setting["key"]
            == SAMLConfiguration.federated_identity_provider_entity_ids.key
        ]

        # Without an active database session there are no federated IdPs and no options
        assert None == federated_identity_provider_entity_ids["options"]

        initialize_database(autoinitialize=False)

        federation = SAMLFederation(
            incommon.FEDERATION_TYPE, "http://incommon.org/metadata"
        )
        federated_identity_provider = SAMLFederatedIdentityProvider(
            federation,
            saml_strings.IDP_1_ENTITY_ID,
            saml_strings.IDP_1_UI_INFO_EN_DISPLAY_NAME,
            saml_strings.CORRECT_XML_WITH_IDP_1,
        )

        from api.app import app

        app._db.add_all([federation, federated_identity_provider])

        [federated_identity_provider_entity_ids] = [
            setting
            for setting in BaseSAMLAuthenticationProvider.SETTINGS
            if setting["key"]
            == SAMLConfiguration.federated_identity_provider_entity_ids.key
        ]

        # After getting an active database session options get initialized
        assert 1 == len(federated_identity_provider_entity_ids["options"])

        # A new idp shows up only after the last updated time
        federated_identity_provider_2 = SAMLFederatedIdentityProvider(
            federation,
            saml_strings.IDP_2_ENTITY_ID,
            saml_strings.IDP_2_UI_INFO_EN_DISPLAY_NAME,
            saml_strings.CORRECT_XML_WITH_IDP_2,
        )
        app._db.add(federated_identity_provider_2)

        [federated_identity_provider_entity_ids] = [
            setting
            for setting in BaseSAMLAuthenticationProvider.SETTINGS
            if setting["key"]
            == SAMLConfiguration.federated_identity_provider_entity_ids.key
        ]

        # Only the first shows up yet
        assert 1 == len(federated_identity_provider_entity_ids["options"])

        federation.last_updated_at = datetime.now()
        [federated_identity_provider_entity_ids] = [
            setting
            for setting in BaseSAMLAuthenticationProvider.SETTINGS
            if setting["key"]
            == SAMLConfiguration.federated_identity_provider_entity_ids.key
        ]
        assert 2 == len(federated_identity_provider_entity_ids["options"])


class TestSAMLOneLoginConfiguration:
    def test_get_identity_provider_settings_returns_correct_result(self):
        # Arrange
        configuration = create_autospec(spec=SAMLConfiguration)
        configuration.get_identity_providers = MagicMock(
            return_value=IDENTITY_PROVIDERS
        )
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
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
        configuration.get_identity_providers.assert_called_once_with(db)

    @parameterized.expand(
        [
            (
                "service_provider_without_certificates",
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
            ),
            (
                "service_provider_with_certificate",
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
            ),
        ]
    )
    def test_get_service_provider_settings_returns_correct_result(
        self, _, service_provider, expected_result
    ):
        # Arrange
        configuration = create_autospec(spec=SAMLConfiguration)
        configuration.get_service_provider = MagicMock(return_value=service_provider)
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        db = create_autospec(spec=sqlalchemy.orm.session.Session)

        # Act
        result = onelogin_configuration.get_service_provider_settings(db)

        # Assert
        result["sp"]["x509cert"] = saml_strings.strip_certificate(
            result["sp"]["x509cert"]
        )

        assert result == expected_result
        configuration.get_service_provider.assert_called_once_with(db)

    def test_get_settings_returns_correct_result(self):
        # Arrange
        debug = False
        strict = False

        service_provider_debug_mode_mock = PropertyMock(return_value=debug)
        service_provider_strict_mode_mock = PropertyMock(return_value=strict)

        configuration = create_autospec(spec=SAMLConfiguration)
        type(
            configuration
        ).service_provider_debug_mode = service_provider_debug_mode_mock
        type(
            configuration
        ).service_provider_strict_mode = service_provider_strict_mode_mock
        configuration.get_service_provider = MagicMock(
            return_value=SERVICE_PROVIDER_WITH_CERTIFICATE
        )
        configuration.get_identity_providers = MagicMock(
            return_value=IDENTITY_PROVIDERS
        )

        onelogin_configuration = SAMLOneLoginConfiguration(configuration)

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
        service_provider_debug_mode_mock.assert_called_with()
        service_provider_strict_mode_mock.assert_called_with()
        configuration.get_service_provider.assert_called_with(db)
        configuration.get_identity_providers.assert_called_with(db)
