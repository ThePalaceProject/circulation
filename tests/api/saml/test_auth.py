from copy import copy
from unittest.mock import MagicMock, PropertyMock, create_autospec, patch
from urllib.parse import parse_qs, urlsplit

import pytest
from freezegun import freeze_time
from onelogin.saml2.utils import OneLogin_Saml2_Utils, OneLogin_Saml2_XML
from onelogin.saml2.xmlparser import fromstring

from api.saml.auth import (
    SAML_NO_ACCESS_ERROR,
    SAMLAuthenticationManager,
    SAMLAuthenticationManagerFactory,
)
from api.saml.configuration.model import SAMLConfiguration, SAMLOneLoginConfiguration
from api.saml.metadata.filter import SAMLSubjectFilter
from api.saml.metadata.model import (
    SAMLAttribute,
    SAMLAttributeStatement,
    SAMLAttributeType,
    SAMLIdentityProviderMetadata,
    SAMLNameID,
    SAMLNameIDFormat,
    SAMLOrganization,
    SAMLService,
    SAMLServiceProviderMetadata,
    SAMLSubject,
    SAMLUIInfo,
)
from api.saml.metadata.parser import SAMLSubjectParser
from api.saml.provider import SAMLWebSSOAuthenticationProvider
from core.model.configuration import ExternalIntegration, HasExternalIntegration
from core.python_expression_dsl.evaluator import DSLEvaluationVisitor, DSLEvaluator
from core.python_expression_dsl.parser import DSLParser
from core.util.datetime_helpers import datetime_utc
from core.util.string_helpers import base64
from tests.api.saml import saml_strings
from tests.fixtures.api_controller import ControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture

SERVICE_PROVIDER_WITH_UNSIGNED_REQUESTS = SAMLServiceProviderMetadata(
    "http://opds.hilbertteam.net/metadata/",
    SAMLUIInfo(),
    SAMLOrganization(),
    SAMLNameIDFormat.UNSPECIFIED.value,
    SAMLService(saml_strings.SP_ACS_URL, saml_strings.SP_ACS_BINDING),
)

SERVICE_PROVIDER_WITH_SIGNED_REQUESTS = SAMLServiceProviderMetadata(
    saml_strings.SP_ENTITY_ID,
    SAMLUIInfo(),
    SAMLOrganization(),
    SAMLNameIDFormat.UNSPECIFIED.value,
    SAMLService(saml_strings.SP_ACS_URL, saml_strings.SP_ACS_BINDING),
    True,
    True,
    saml_strings.SIGNING_CERTIFICATE,
    saml_strings.PRIVATE_KEY,
)

IDENTITY_PROVIDERS = [
    SAMLIdentityProviderMetadata(
        saml_strings.IDP_1_ENTITY_ID,
        SAMLUIInfo(),
        SAMLOrganization(),
        SAMLNameIDFormat.UNSPECIFIED.value,
        SAMLService(saml_strings.IDP_1_SSO_URL, saml_strings.IDP_1_SSO_BINDING),
        signing_certificates=[saml_strings.SIGNING_CERTIFICATE],
    ),
    SAMLIdentityProviderMetadata(
        saml_strings.IDP_2_ENTITY_ID,
        SAMLUIInfo(),
        SAMLOrganization(),
        SAMLNameIDFormat.UNSPECIFIED.value,
        SAMLService(saml_strings.IDP_2_SSO_URL, saml_strings.IDP_2_SSO_BINDING),
    ),
]


class TestSAMLAuthenticationManager:
    @pytest.mark.parametrize(
        "_, service_provider, identity_providers",
        [
            (
                "with_unsigned_authentication_request",
                SERVICE_PROVIDER_WITH_UNSIGNED_REQUESTS,
                IDENTITY_PROVIDERS,
            ),
            (
                "with_signed_authentication_request",
                SERVICE_PROVIDER_WITH_SIGNED_REQUESTS,
                IDENTITY_PROVIDERS,
            ),
        ],
    )
    def test_start_authentication(
        self,
        controller_fixture: ControllerFixture,
        _,
        service_provider,
        identity_providers,
    ):
        configuration = create_autospec(spec=SAMLConfiguration)
        configuration.service_provider_debug_mode = MagicMock(return_value=False)
        configuration.service_provider_strict_mode = MagicMock(return_value=False)
        configuration.get_service_provider = MagicMock(return_value=service_provider)
        configuration.get_identity_providers = MagicMock(
            return_value=identity_providers
        )
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        subject_parser = SAMLSubjectParser()
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)
        authentication_manager = SAMLAuthenticationManager(
            onelogin_configuration, subject_parser, subject_filter
        )

        with controller_fixture.app.test_request_context("/"):
            result = authentication_manager.start_authentication(
                controller_fixture.db.session, saml_strings.IDP_1_ENTITY_ID, ""
            )

            query_items = parse_qs(urlsplit(result).query)
            saml_request = query_items["SAMLRequest"][0]
            decoded_saml_request = OneLogin_Saml2_Utils.decode_base64_and_inflate(
                saml_request
            )

            validation_result = OneLogin_Saml2_XML.validate_xml(
                decoded_saml_request, "saml-schema-protocol-2.0.xsd", False
            )
            assert isinstance(validation_result, OneLogin_Saml2_XML._element_class)

            saml_request_dom = fromstring(decoded_saml_request)

            acs_url = saml_request_dom.get("AssertionConsumerServiceURL")
            assert acs_url == SERVICE_PROVIDER_WITH_UNSIGNED_REQUESTS.acs_service.url

            acs_binding = saml_request_dom.get("ProtocolBinding")
            assert (
                acs_binding
                == SERVICE_PROVIDER_WITH_UNSIGNED_REQUESTS.acs_service.binding.value
            )

            sso_url = saml_request_dom.get("Destination")
            assert sso_url == IDENTITY_PROVIDERS[0].sso_service.url

            name_id_policy_nodes = OneLogin_Saml2_XML.query(
                saml_request_dom, "./samlp:NameIDPolicy"
            )

            assert name_id_policy_nodes is not None
            assert len(name_id_policy_nodes) == 1

            name_id_policy_node = name_id_policy_nodes[0]
            name_id_format = name_id_policy_node.get("Format")

            assert (
                name_id_format == SERVICE_PROVIDER_WITH_UNSIGNED_REQUESTS.name_id_format
            )

    @pytest.mark.parametrize(
        "_, saml_response, current_time, filter_expression, expected_value, mock_validation",
        [
            (
                "with_name_id_and_attributes",
                saml_strings.SAML_RESPONSE,
                datetime_utc(2020, 6, 7, 23, 43, 0),
                None,
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(
                        SAMLNameIDFormat.TRANSIENT.value,
                        "http://idp.hilbertteam.net/idp/shibboleth",
                        "http://opds.hilbertteam.net/metadata/",
                        "AAdzZWNyZXQxeAj5TZ2CQ6FkW//TigUE8kgDuJfVEw7mtnCAFq02hvot2hQzlCj5QqQOBRlsAs0dqp1oHoi/apPWmrC2G30BvrtXcDfZsCGQv9eTGSRDydTLVPEe+lfCc1yg3WlxTeiCbFazW6kcybVgUper",
                    ),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(SAMLAttributeType.uid.name, ["student1"]),
                            SAMLAttribute(
                                SAMLAttributeType.mail.name, ["student1@idptestbed.edu"]
                            ),
                            SAMLAttribute(SAMLAttributeType.surname.name, ["Ent"]),
                            SAMLAttribute(SAMLAttributeType.givenName.name, ["Stud"]),
                        ]
                    ),
                ),
                False,
            ),
            (
                "with_name_id_attributes_and_filter_expression_returning_false",
                saml_strings.SAML_RESPONSE,
                datetime_utc(2020, 6, 7, 23, 43, 0),
                "subject.attribute_statement.attributes['uid'].values[0] != 'student1'",
                SAML_NO_ACCESS_ERROR,
                False,
            ),
            (
                "with_name_id_attributes_and_filter_expression_returning_true",
                saml_strings.SAML_RESPONSE,
                datetime_utc(2020, 6, 7, 23, 43, 0),
                "subject.attribute_statement.attributes['uid'].values[0] == 'student1'",
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(
                        SAMLNameIDFormat.TRANSIENT.value,
                        "http://idp.hilbertteam.net/idp/shibboleth",
                        "http://opds.hilbertteam.net/metadata/",
                        "AAdzZWNyZXQxeAj5TZ2CQ6FkW//TigUE8kgDuJfVEw7mtnCAFq02hvot2hQzlCj5QqQOBRlsAs0dqp1oHoi/apPWmrC2G30BvrtXcDfZsCGQv9eTGSRDydTLVPEe+lfCc1yg3WlxTeiCbFazW6kcybVgUper",
                    ),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(SAMLAttributeType.uid.name, ["student1"]),
                            SAMLAttribute(
                                SAMLAttributeType.mail.name, ["student1@idptestbed.edu"]
                            ),
                            SAMLAttribute(SAMLAttributeType.surname.name, ["Ent"]),
                            SAMLAttribute(SAMLAttributeType.givenName.name, ["Stud"]),
                        ]
                    ),
                ),
                False,
            ),
            (
                "with_name_id_inside_edu_person_targeted_id_attribute",
                saml_strings.SAML_COLUMBIA_RESPONSE,
                datetime_utc(2020, 6, 7, 23, 43, 0),
                None,
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(
                        SAMLNameIDFormat.PERSISTENT.value,
                        "https://shibboleth-dev.cc.columbia.edu/idp/shibboleth",
                        None,
                        "0Mi3izMnex9L0sMt9wRfwY0pqQ8=",
                    ),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                SAMLAttributeType.eduPersonScopedAffiliation.name,
                                ["alum@columbia.edu"],
                            ),
                            SAMLAttribute(
                                SAMLAttributeType.eduPersonTargetedID.name,
                                ["0Mi3izMnex9L0sMt9wRfwY0pqQ8="],
                            ),
                            SAMLAttribute(
                                SAMLAttributeType.displayName.name, ["William Tester"]
                            ),
                        ]
                    ),
                ),
                True,
            ),
            (
                "with_name_id_inside_edu_person_targeted_id_attribute_and_filter_expression_returning_false",
                saml_strings.SAML_COLUMBIA_RESPONSE,
                datetime_utc(2020, 6, 7, 23, 43, 0),
                "subject.attribute_statement.attributes['eduPersonScopedAffiliation'].values[0] != 'alum@columbia.edu'",
                SAML_NO_ACCESS_ERROR,
                True,
            ),
            (
                "with_name_id_inside_edu_person_targeted_id_attribute_and_filter_expression_returning_true",
                saml_strings.SAML_COLUMBIA_RESPONSE,
                datetime_utc(2020, 6, 7, 23, 43, 0),
                "subject.attribute_statement.attributes['eduPersonScopedAffiliation'].values[0] == 'alum@columbia.edu'",
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(
                        SAMLNameIDFormat.PERSISTENT.value,
                        "https://shibboleth-dev.cc.columbia.edu/idp/shibboleth",
                        None,
                        "0Mi3izMnex9L0sMt9wRfwY0pqQ8=",
                    ),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                SAMLAttributeType.eduPersonScopedAffiliation.name,
                                ["alum@columbia.edu"],
                            ),
                            SAMLAttribute(
                                SAMLAttributeType.eduPersonTargetedID.name,
                                ["0Mi3izMnex9L0sMt9wRfwY0pqQ8="],
                            ),
                            SAMLAttribute(
                                SAMLAttributeType.displayName.name, ["William Tester"]
                            ),
                        ]
                    ),
                ),
                True,
            ),
        ],
    )
    def test_finish_authentication(
        self,
        controller_fixture: ControllerFixture,
        _,
        saml_response,
        current_time,
        filter_expression,
        expected_value,
        mock_validation,
    ):
        # Arrange
        identity_provider_entity_id = "http://idp.hilbertteam.net/idp/shibboleth"
        service_provider_host_name = "opds.hilbertteam.net"

        identity_providers = [
            copy(identity_provider) for identity_provider in IDENTITY_PROVIDERS
        ]
        identity_providers[0].entity_id = identity_provider_entity_id

        if mock_validation:
            validate_mock = MagicMock(return_value=True)
        else:
            real_validate_sign = OneLogin_Saml2_Utils.validate_sign
            validate_mock = MagicMock(
                side_effect=lambda *args, **kwargs: real_validate_sign(*args, **kwargs)
            )

        filter_expression_mock = PropertyMock(return_value=filter_expression)
        service_provider_debug_mode_mock = PropertyMock(return_value=False)
        service_provider_strict_mode = PropertyMock(return_value=False)

        configuration = create_autospec(spec=SAMLConfiguration)
        type(configuration).filter_expression = filter_expression_mock
        type(
            configuration
        ).service_provider_debug_mode = service_provider_debug_mode_mock
        type(configuration).service_provider_strict_mode = service_provider_strict_mode
        configuration.get_service_provider = MagicMock(
            return_value=SERVICE_PROVIDER_WITH_UNSIGNED_REQUESTS
        )
        configuration.get_identity_providers = MagicMock(
            return_value=identity_providers
        )
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        subject_parser = SAMLSubjectParser()
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)
        authentication_manager = SAMLAuthenticationManager(
            onelogin_configuration, subject_parser, subject_filter
        )
        saml_response = base64.b64encode(saml_response)

        # Act
        with freeze_time(current_time):
            with patch(
                "onelogin.saml2.response.OneLogin_Saml2_Utils.validate_sign",
                validate_mock,
            ):
                controller_fixture.app.config[
                    "SERVER_NAME"
                ] = service_provider_host_name

                with controller_fixture.app.test_request_context(
                    "/SAML2/POST", data={"SAMLResponse": saml_response}
                ):
                    result = authentication_manager.finish_authentication(
                        controller_fixture.db.session, identity_provider_entity_id
                    )

                    # Assert
                    assert expected_value == result


class SAMLAuthFixture:
    db: DatabaseTransactionFixture
    integration: ExternalIntegration
    authentication_provider: SAMLWebSSOAuthenticationProvider

    def __init__(self, db: DatabaseTransactionFixture):
        self.integration = db.external_integration(
            protocol=SAMLWebSSOAuthenticationProvider.NAME,
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
        )
        self.authentication_provider = SAMLWebSSOAuthenticationProvider(
            db.default_library(), self.integration
        )


@pytest.fixture(scope="function")
def saml_auth_fixture(db: DatabaseTransactionFixture) -> SAMLAuthFixture:
    return SAMLAuthFixture(db)


class TestSAMLAuthenticationManagerFactory:
    def test_create(self, saml_auth_fixture: SAMLAuthFixture):
        # Arrange
        factory = SAMLAuthenticationManagerFactory()
        integration_owner = create_autospec(spec=HasExternalIntegration)
        integration_owner.external_integration = MagicMock(
            return_value=saml_auth_fixture.integration
        )

        # Act
        result = factory.create(integration_owner)

        # Assert
        assert True == isinstance(result, SAMLAuthenticationManager)
