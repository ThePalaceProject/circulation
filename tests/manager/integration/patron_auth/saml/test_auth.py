from collections.abc import Callable
from copy import copy
from unittest.mock import MagicMock, create_autospec, patch
from urllib.parse import parse_qs, urlsplit

import pytest
from freezegun import freeze_time
from onelogin.saml2.utils import OneLogin_Saml2_Utils, OneLogin_Saml2_XML
from onelogin.saml2.xmlparser import fromstring

from palace.manager.integration.patron_auth.saml.auth import (
    SAML_NO_ACCESS_ERROR,
    SAMLAuthenticationManager,
    SAMLAuthenticationManagerFactory,
)
from palace.manager.integration.patron_auth.saml.configuration.model import (
    SAMLOneLoginConfiguration,
    SAMLWebSSOAuthSettings,
)
from palace.manager.integration.patron_auth.saml.metadata.filter import (
    SAMLSubjectFilter,
)
from palace.manager.integration.patron_auth.saml.metadata.model import (
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
from palace.manager.integration.patron_auth.saml.metadata.parser import (
    SAMLSubjectParser,
)
from palace.manager.integration.patron_auth.saml.python_expression_dsl.evaluator import (
    DSLEvaluationVisitor,
    DSLEvaluator,
)
from palace.manager.integration.patron_auth.saml.python_expression_dsl.parser import (
    DSLParser,
)
from palace.manager.util import base64
from palace.manager.util.datetime_helpers import datetime_utc
from palace.manager.util.problem_detail import ProblemDetail
from tests.fixtures.api_controller import ControllerFixture
from tests.mocks import saml_strings

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
        "service_provider, identity_providers",
        [
            pytest.param(
                SERVICE_PROVIDER_WITH_UNSIGNED_REQUESTS,
                IDENTITY_PROVIDERS,
                id="with_unsigned_authentication_request",
            ),
            pytest.param(
                SERVICE_PROVIDER_WITH_SIGNED_REQUESTS,
                IDENTITY_PROVIDERS,
                id="with_signed_authentication_request",
            ),
        ],
    )
    def test_start_authentication(
        self,
        controller_fixture: ControllerFixture,
        create_mock_onelogin_configuration: Callable[..., SAMLOneLoginConfiguration],
        service_provider,
        identity_providers,
    ):
        onelogin_configuration = create_mock_onelogin_configuration(
            service_provider, identity_providers
        )
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
        "saml_response, current_time, filter_expression, expected_value, mock_validation",
        [
            pytest.param(
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
                id="with_name_id_and_attributes",
            ),
            pytest.param(
                saml_strings.SAML_RESPONSE,
                datetime_utc(2020, 6, 7, 23, 43, 0),
                "subject.attribute_statement.attributes['uid'].values[0] != 'student1'",
                SAML_NO_ACCESS_ERROR,
                False,
                id="with_name_id_attributes_and_filter_expression_returning_false",
            ),
            pytest.param(
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
                id="with_name_id_attributes_and_filter_expression_returning_true",
            ),
            pytest.param(
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
                id="with_name_id_inside_edu_person_targeted_id_attribute",
            ),
            pytest.param(
                saml_strings.SAML_COLUMBIA_RESPONSE,
                datetime_utc(2020, 6, 7, 23, 43, 0),
                "subject.attribute_statement.attributes['eduPersonScopedAffiliation'].values[0] != 'alum@columbia.edu'",
                SAML_NO_ACCESS_ERROR,
                True,
                id="with_name_id_inside_edu_person_targeted_id_attribute_and_filter_expression_returning_false",
            ),
            pytest.param(
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
                id="with_name_id_inside_edu_person_targeted_id_attribute_and_filter_expression_returning_true",
            ),
        ],
    )
    def test_finish_authentication(
        self,
        controller_fixture: ControllerFixture,
        create_saml_configuration,
        create_mock_onelogin_configuration: Callable[..., SAMLOneLoginConfiguration],
        saml_response,
        current_time,
        filter_expression,
        expected_value,
        mock_validation,
    ):
        # Arrange
        identity_provider_entity_id = "http://idp.hilbertteam.net/idp/shibboleth"

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

        configuration = create_saml_configuration(
            filter_expression=filter_expression,
            service_provider_debug_mode=False,
            service_provider_strict_mode=False,
        )
        onelogin_configuration = create_mock_onelogin_configuration(
            SERVICE_PROVIDER_WITH_UNSIGNED_REQUESTS, identity_providers, configuration
        )

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
                with controller_fixture.app.test_request_context(
                    "/SAML2/POST", data={"SAMLResponse": saml_response}
                ):
                    result = authentication_manager.finish_authentication(
                        controller_fixture.db.session, identity_provider_entity_id
                    )

                    # Assert
                    assert expected_value == result

    def test_start_logout_success(
        self,
        controller_fixture: ControllerFixture,
        create_mock_onelogin_configuration: Callable[..., SAMLOneLoginConfiguration],
    ):
        """start_logout should generate a redirect URL to the IdP's SLO endpoint."""
        from palace.manager.integration.patron_auth.saml.metadata.model import (
            SAMLBinding,
            SAMLIdentityProviderMetadata as IDP,
            SAMLService,
        )

        slo_url = "http://idp.example.com/idp/profile/SAML2/Redirect/SLO"
        slo_binding = SAMLBinding.HTTP_REDIRECT
        idp_with_slo = IDP(
            saml_strings.IDP_1_ENTITY_ID,
            SAMLUIInfo(),
            SAMLOrganization(),
            SAMLNameIDFormat.UNSPECIFIED.value,
            SAMLService(saml_strings.IDP_1_SSO_URL, saml_strings.IDP_1_SSO_BINDING),
            slo_service=SAMLService(slo_url, slo_binding),
            signing_certificates=[saml_strings.SIGNING_CERTIFICATE],
        )
        onelogin_configuration = create_mock_onelogin_configuration(
            SERVICE_PROVIDER_WITH_UNSIGNED_REQUESTS, [idp_with_slo]
        )
        subject_parser = SAMLSubjectParser()
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)
        authentication_manager = SAMLAuthenticationManager(
            onelogin_configuration, subject_parser, subject_filter
        )

        name_id = SAMLNameID(
            SAMLNameIDFormat.PERSISTENT.value,
            name_qualifier="",
            sp_name_qualifier=None,
            name_id="patron-name-id",
        )

        with controller_fixture.app.test_request_context("/"):
            result = authentication_manager.start_logout(
                controller_fixture.db.session,
                saml_strings.IDP_1_ENTITY_ID,
                name_id,
                "https://cm.example.com/saml/logout_callback",
                "https://app.example.com/logout?library=default",
            )

        assert isinstance(result, str)
        assert "SAMLRequest" in result

    def test_start_logout_onelogin_error(
        self,
        controller_fixture: ControllerFixture,
        create_mock_onelogin_configuration: Callable[..., SAMLOneLoginConfiguration],
    ):
        """start_logout should return a ProblemDetail on OneLogin_Saml2_Error."""
        from unittest.mock import patch

        from onelogin.saml2.errors import OneLogin_Saml2_Error

        from palace.manager.integration.patron_auth.saml.auth import SAML_GENERIC_ERROR

        onelogin_configuration = create_mock_onelogin_configuration(
            SERVICE_PROVIDER_WITH_UNSIGNED_REQUESTS, IDENTITY_PROVIDERS
        )
        subject_parser = SAMLSubjectParser()
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)
        authentication_manager = SAMLAuthenticationManager(
            onelogin_configuration, subject_parser, subject_filter
        )

        name_id = SAMLNameID(
            SAMLNameIDFormat.PERSISTENT.value,
            name_qualifier="",
            sp_name_qualifier=None,
            name_id="patron-name-id",
        )

        with controller_fixture.app.test_request_context("/"):
            with patch.object(
                authentication_manager._configuration,
                "get_logout_settings",
                side_effect=OneLogin_Saml2_Error(
                    "logout error", OneLogin_Saml2_Error.SETTINGS_INVALID
                ),
            ):
                result = authentication_manager.start_logout(
                    controller_fixture.db.session,
                    saml_strings.IDP_1_ENTITY_ID,
                    name_id,
                    "https://cm.example.com/saml/logout_callback",
                    "https://app.example.com/logout",
                )

        assert isinstance(result, ProblemDetail)
        assert result.uri == SAML_GENERIC_ERROR.uri

    def test_finish_logout_success(
        self,
        controller_fixture: ControllerFixture,
        create_mock_onelogin_configuration: Callable[..., SAMLOneLoginConfiguration],
    ):
        """finish_logout should return True when the SAMLResponse is valid."""
        from unittest.mock import MagicMock, patch

        onelogin_configuration = create_mock_onelogin_configuration(
            SERVICE_PROVIDER_WITH_UNSIGNED_REQUESTS, IDENTITY_PROVIDERS
        )
        subject_parser = SAMLSubjectParser()
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)
        authentication_manager = SAMLAuthenticationManager(
            onelogin_configuration, subject_parser, subject_filter
        )

        mock_auth = MagicMock()
        mock_auth.get_errors.return_value = []

        with controller_fixture.app.test_request_context(
            "/saml/logout_callback?SAMLResponse=dummyresponse"
        ):
            with patch.object(
                authentication_manager,
                "_create_auth_object",
                return_value=mock_auth,
            ):
                with patch.object(
                    authentication_manager._configuration,
                    "get_logout_settings",
                    return_value={},
                ):
                    result = authentication_manager.finish_logout(
                        controller_fixture.db.session,
                        saml_strings.IDP_1_ENTITY_ID,
                        "https://cm.example.com/saml/logout_callback",
                    )

        assert result is True
        mock_auth.process_slo.assert_called_once_with(keep_local_session=True)

    def test_finish_logout_validation_errors(
        self,
        controller_fixture: ControllerFixture,
        create_mock_onelogin_configuration: Callable[..., SAMLOneLoginConfiguration],
    ):
        """finish_logout should return a ProblemDetail when the SAMLResponse contains errors."""
        from unittest.mock import MagicMock, patch

        from palace.manager.integration.patron_auth.saml.auth import SAML_GENERIC_ERROR

        onelogin_configuration = create_mock_onelogin_configuration(
            SERVICE_PROVIDER_WITH_UNSIGNED_REQUESTS, IDENTITY_PROVIDERS
        )
        subject_parser = SAMLSubjectParser()
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)
        authentication_manager = SAMLAuthenticationManager(
            onelogin_configuration, subject_parser, subject_filter
        )

        mock_auth = MagicMock()
        mock_auth.get_errors.return_value = ["invalid_logout_response"]
        mock_auth.get_last_error_reason.return_value = "Signature validation failed"

        with controller_fixture.app.test_request_context(
            "/saml/logout_callback?SAMLResponse=dummyresponse"
        ):
            with patch.object(
                authentication_manager,
                "_create_auth_object",
                return_value=mock_auth,
            ):
                with patch.object(
                    authentication_manager._configuration,
                    "get_logout_settings",
                    return_value={},
                ):
                    result = authentication_manager.finish_logout(
                        controller_fixture.db.session,
                        saml_strings.IDP_1_ENTITY_ID,
                        "https://cm.example.com/saml/logout_callback",
                    )

        assert isinstance(result, ProblemDetail)
        assert result.uri == SAML_GENERIC_ERROR.uri
        assert result.detail is not None
        assert "Signature validation failed" in result.detail

    def test_finish_logout_no_saml_response(
        self,
        controller_fixture: ControllerFixture,
        create_mock_onelogin_configuration: Callable[..., SAMLOneLoginConfiguration],
    ):
        """finish_logout should return a ProblemDetail when no SAMLResponse is present (IdP-Initiated SLO rejected)."""
        from palace.manager.integration.patron_auth.saml.auth import SAML_GENERIC_ERROR

        onelogin_configuration = create_mock_onelogin_configuration(
            SERVICE_PROVIDER_WITH_UNSIGNED_REQUESTS, IDENTITY_PROVIDERS
        )
        subject_parser = SAMLSubjectParser()
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)
        authentication_manager = SAMLAuthenticationManager(
            onelogin_configuration, subject_parser, subject_filter
        )

        # No SAMLResponse in request — only SAMLRequest (IdP-Initiated path).
        with controller_fixture.app.test_request_context(
            "/saml/logout_callback?SAMLRequest=dummyrequest"
        ):
            result = authentication_manager.finish_logout(
                controller_fixture.db.session,
                saml_strings.IDP_1_ENTITY_ID,
                "https://cm.example.com/saml/logout_callback",
            )

        assert isinstance(result, ProblemDetail)
        assert result.uri == SAML_GENERIC_ERROR.uri

    def test_finish_logout_onelogin_error(
        self,
        controller_fixture: ControllerFixture,
        create_mock_onelogin_configuration: Callable[..., SAMLOneLoginConfiguration],
    ):
        """finish_logout should return a ProblemDetail on OneLogin_Saml2_Error."""
        from unittest.mock import patch

        from onelogin.saml2.errors import OneLogin_Saml2_Error

        from palace.manager.integration.patron_auth.saml.auth import SAML_GENERIC_ERROR

        onelogin_configuration = create_mock_onelogin_configuration(
            SERVICE_PROVIDER_WITH_UNSIGNED_REQUESTS, IDENTITY_PROVIDERS
        )
        subject_parser = SAMLSubjectParser()
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)
        authentication_manager = SAMLAuthenticationManager(
            onelogin_configuration, subject_parser, subject_filter
        )

        with controller_fixture.app.test_request_context(
            "/saml/logout_callback?SAMLResponse=dummyresponse"
        ):
            with patch.object(
                authentication_manager._configuration,
                "get_logout_settings",
                side_effect=OneLogin_Saml2_Error(
                    "error", OneLogin_Saml2_Error.SETTINGS_INVALID
                ),
            ):
                result = authentication_manager.finish_logout(
                    controller_fixture.db.session,
                    saml_strings.IDP_1_ENTITY_ID,
                    "https://cm.example.com/saml/logout_callback",
                )

        assert isinstance(result, ProblemDetail)
        assert result.uri == SAML_GENERIC_ERROR.uri

    def test_start_logout_saml_configuration_error(
        self,
        controller_fixture: ControllerFixture,
        create_mock_onelogin_configuration: Callable[..., SAMLOneLoginConfiguration],
    ):
        """start_logout should return ProblemDetail on SAMLConfigurationError."""
        from unittest.mock import patch

        from palace.manager.integration.patron_auth.saml.auth import SAML_GENERIC_ERROR
        from palace.manager.integration.patron_auth.saml.configuration.model import (
            SAMLConfigurationError,
        )

        onelogin_configuration = create_mock_onelogin_configuration(
            SERVICE_PROVIDER_WITH_UNSIGNED_REQUESTS, IDENTITY_PROVIDERS
        )
        subject_parser = SAMLSubjectParser()
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)
        authentication_manager = SAMLAuthenticationManager(
            onelogin_configuration, subject_parser, subject_filter
        )

        name_id = SAMLNameID(
            SAMLNameIDFormat.PERSISTENT.value,
            name_qualifier="",
            sp_name_qualifier=None,
            name_id="patron-name-id",
        )

        with controller_fixture.app.test_request_context("/"):
            with patch.object(
                authentication_manager._configuration,
                "get_logout_settings",
                side_effect=SAMLConfigurationError("Invalid IdP configuration"),
            ):
                result = authentication_manager.start_logout(
                    controller_fixture.db.session,
                    saml_strings.IDP_1_ENTITY_ID,
                    name_id,
                    "https://cm.example.com/saml/logout_callback",
                    "https://app.example.com/logout",
                )

        assert isinstance(result, ProblemDetail)
        assert result.uri == SAML_GENERIC_ERROR.uri

    def test_finish_logout_saml_configuration_error(
        self,
        controller_fixture: ControllerFixture,
        create_mock_onelogin_configuration: Callable[..., SAMLOneLoginConfiguration],
    ):
        """finish_logout should return ProblemDetail on SAMLConfigurationError."""
        from unittest.mock import patch

        from palace.manager.integration.patron_auth.saml.auth import SAML_GENERIC_ERROR
        from palace.manager.integration.patron_auth.saml.configuration.model import (
            SAMLConfigurationError,
        )

        onelogin_configuration = create_mock_onelogin_configuration(
            SERVICE_PROVIDER_WITH_UNSIGNED_REQUESTS, IDENTITY_PROVIDERS
        )
        subject_parser = SAMLSubjectParser()
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)
        authentication_manager = SAMLAuthenticationManager(
            onelogin_configuration, subject_parser, subject_filter
        )

        with controller_fixture.app.test_request_context(
            "/saml/logout_callback?SAMLResponse=dummyresponse"
        ):
            with patch.object(
                authentication_manager._configuration,
                "get_logout_settings",
                side_effect=SAMLConfigurationError("Invalid IdP configuration"),
            ):
                result = authentication_manager.finish_logout(
                    controller_fixture.db.session,
                    saml_strings.IDP_1_ENTITY_ID,
                    "https://cm.example.com/saml/logout_callback",
                )

        assert isinstance(result, ProblemDetail)
        assert result.uri == SAML_GENERIC_ERROR.uri

    def test_finish_logout_http_post_returns_success(
        self,
        controller_fixture: ControllerFixture,
        create_mock_onelogin_configuration: Callable[..., SAMLOneLoginConfiguration],
    ):
        """HTTP-POST SLO responses return True without calling process_slo."""
        from unittest.mock import patch

        from onelogin.saml2.auth import OneLogin_Saml2_Auth

        onelogin_configuration = create_mock_onelogin_configuration(
            SERVICE_PROVIDER_WITH_UNSIGNED_REQUESTS, IDENTITY_PROVIDERS
        )
        subject_parser = SAMLSubjectParser()
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)
        authentication_manager = SAMLAuthenticationManager(
            onelogin_configuration, subject_parser, subject_filter
        )

        with controller_fixture.app.test_request_context(
            "/saml/logout_callback",
            method="POST",
            data={"SAMLResponse": "dummyresponse"},
        ):
            with patch.object(OneLogin_Saml2_Auth, "process_slo") as mock_process_slo:
                result = authentication_manager.finish_logout(
                    controller_fixture.db.session,
                    saml_strings.IDP_1_ENTITY_ID,
                    "https://cm.example.com/saml/logout_callback",
                )

        assert result is True
        mock_process_slo.assert_not_called()


class TestSAMLAuthenticationManagerFactory:
    def test_create(self):
        # Arrange
        factory = SAMLAuthenticationManagerFactory()
        configuration = create_autospec(spec=SAMLWebSSOAuthSettings)

        # Act
        result = factory.create(configuration)

        # Assert
        assert isinstance(result, SAMLAuthenticationManager)
