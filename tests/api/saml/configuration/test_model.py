from collections.abc import Callable
from datetime import datetime
from unittest.mock import MagicMock, call, create_autospec

import pytest
import sqlalchemy

from api.saml.configuration.model import (
    SAMLOneLoginConfiguration,
    SAMLWebSSOAuthSettings,
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


class TestSAMLConfiguration:
    def test_get_service_provider_returns_correct_value(
        self, create_saml_configuration: Callable[..., SAMLWebSSOAuthSettings]
    ):
        # Arrange
        metadata_parser = SAMLMetadataParser()
        metadata_parser.parse = MagicMock(side_effect=metadata_parser.parse)

        configuration = create_saml_configuration(
            service_provider_xml_metadata=saml_strings.CORRECT_XML_WITH_ONE_SP
        )
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        onelogin_configuration._metadata_parser = metadata_parser

        # Act
        service_provider = onelogin_configuration.get_service_provider()

        # Assert
        assert isinstance(service_provider, SAMLServiceProviderMetadata) is True
        assert saml_strings.SP_ENTITY_ID == service_provider.entity_id

        metadata_parser.parse.assert_called_once_with(
            configuration.service_provider_xml_metadata
        )

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
        metadata_parser.parse = MagicMock(side_effect=metadata_parser.parse)  # type: ignore

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
        "_,service_provider,expected_result",
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
        ],
    )
    def test_get_service_provider_settings_returns_correct_result(
        self, _, service_provider, expected_result
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
