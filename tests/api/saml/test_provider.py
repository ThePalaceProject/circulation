import datetime
import json
from collections.abc import Callable
from unittest.mock import MagicMock, create_autospec, patch

import pytest
from freezegun import freeze_time
from werkzeug.datastructures import Authorization

from api.authentication.base import PatronData
from api.saml.auth import SAMLAuthenticationManager, SAMLAuthenticationManagerFactory
from api.saml.configuration.model import (
    SAMLOneLoginConfiguration,
    SAMLWebSSOAuthSettings,
)
from api.saml.metadata.filter import SAMLSubjectFilter
from api.saml.metadata.model import (
    SAMLAttribute,
    SAMLAttributeStatement,
    SAMLAttributeType,
    SAMLIdentityProviderMetadata,
    SAMLLocalizedMetadataItem,
    SAMLNameID,
    SAMLNameIDFormat,
    SAMLOrganization,
    SAMLService,
    SAMLServiceProviderMetadata,
    SAMLSubject,
    SAMLSubjectJSONEncoder,
    SAMLUIInfo,
)
from api.saml.metadata.parser import SAMLSubjectParser
from api.saml.provider import SAML_INVALID_SUBJECT, SAMLWebSSOAuthenticationProvider
from core.python_expression_dsl.evaluator import DSLEvaluationVisitor, DSLEvaluator
from core.python_expression_dsl.parser import DSLParser
from core.util.datetime_helpers import datetime_utc, utc_now
from core.util.problem_detail import ProblemDetail
from tests.api.saml import saml_strings
from tests.fixtures.api_controller import ControllerFixture

SERVICE_PROVIDER = SAMLServiceProviderMetadata(
    saml_strings.SP_ENTITY_ID,
    SAMLUIInfo(),
    SAMLOrganization(),
    SAMLNameIDFormat.UNSPECIFIED.value,
    SAMLService(saml_strings.SP_ACS_URL, saml_strings.SP_ACS_BINDING),
)

IDENTITY_PROVIDER_WITH_DISPLAY_NAME = SAMLIdentityProviderMetadata(
    saml_strings.IDP_2_ENTITY_ID,
    SAMLUIInfo(
        display_names=[
            SAMLLocalizedMetadataItem(saml_strings.IDP_1_UI_INFO_EN_DISPLAY_NAME, "en"),
            SAMLLocalizedMetadataItem(saml_strings.IDP_1_UI_INFO_ES_DISPLAY_NAME, "es"),
        ],
        descriptions=[
            SAMLLocalizedMetadataItem(saml_strings.IDP_1_UI_INFO_DESCRIPTION, "en"),
            SAMLLocalizedMetadataItem(saml_strings.IDP_1_UI_INFO_DESCRIPTION, "es"),
        ],
        information_urls=[
            SAMLLocalizedMetadataItem(saml_strings.IDP_1_UI_INFO_INFORMATION_URL, "en"),
            SAMLLocalizedMetadataItem(saml_strings.IDP_1_UI_INFO_INFORMATION_URL, "es"),
        ],
        privacy_statement_urls=[
            SAMLLocalizedMetadataItem(
                saml_strings.IDP_1_UI_INFO_PRIVACY_STATEMENT_URL, "en"
            ),
            SAMLLocalizedMetadataItem(
                saml_strings.IDP_1_UI_INFO_PRIVACY_STATEMENT_URL, "es"
            ),
        ],
        logo_urls=[
            SAMLLocalizedMetadataItem(saml_strings.IDP_1_UI_INFO_LOGO_URL, "en"),
            SAMLLocalizedMetadataItem(saml_strings.IDP_1_UI_INFO_LOGO_URL, "es"),
        ],
    ),
    SAMLOrganization(),
    SAMLNameIDFormat.UNSPECIFIED.value,
    SAMLService(saml_strings.IDP_2_SSO_URL, saml_strings.IDP_2_SSO_BINDING),
)

IDENTITY_PROVIDER_WITH_ORGANIZATION_DISPLAY_NAME = SAMLIdentityProviderMetadata(
    saml_strings.IDP_2_ENTITY_ID,
    SAMLUIInfo(),
    SAMLOrganization(
        organization_display_names=[
            SAMLLocalizedMetadataItem(
                saml_strings.IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME, "en"
            ),
            SAMLLocalizedMetadataItem(
                saml_strings.IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME, "es"
            ),
        ]
    ),
    SAMLNameIDFormat.UNSPECIFIED.value,
    SAMLService(saml_strings.IDP_2_SSO_URL, saml_strings.IDP_2_SSO_BINDING),
)

IDENTITY_PROVIDER_WITHOUT_DISPLAY_NAMES = SAMLIdentityProviderMetadata(
    saml_strings.IDP_1_ENTITY_ID,
    SAMLUIInfo(),
    SAMLOrganization(),
    SAMLNameIDFormat.UNSPECIFIED.value,
    SAMLService(saml_strings.IDP_1_SSO_URL, saml_strings.IDP_1_SSO_BINDING),
)


class TestSAMLWebSSOAuthenticationProvider:
    @pytest.mark.parametrize(
        "_, identity_providers, expected_result",
        [
            (
                "identity_provider_with_display_name",
                [IDENTITY_PROVIDER_WITH_DISPLAY_NAME],
                {
                    "type": "http://librarysimplified.org/authtype/SAML-2.0",
                    "description": SAMLWebSSOAuthenticationProvider.label(),
                    "links": [
                        {
                            "rel": "authenticate",
                            "href": "http://localhost/default/saml_authenticate?provider=SAML+2.0+Web+SSO&idp_entity_id=http://idp2.hilbertteam.net/idp/shibboleth",
                            "display_names": [
                                {
                                    "value": saml_strings.IDP_1_UI_INFO_EN_DISPLAY_NAME,
                                    "language": "en",
                                },
                                {
                                    "value": saml_strings.IDP_1_UI_INFO_ES_DISPLAY_NAME,
                                    "language": "es",
                                },
                            ],
                            "descriptions": [
                                {
                                    "value": saml_strings.IDP_1_UI_INFO_DESCRIPTION,
                                    "language": "en",
                                },
                                {
                                    "value": saml_strings.IDP_1_UI_INFO_DESCRIPTION,
                                    "language": "es",
                                },
                            ],
                            "information_urls": [
                                {
                                    "value": saml_strings.IDP_1_UI_INFO_INFORMATION_URL,
                                    "language": "en",
                                },
                                {
                                    "value": saml_strings.IDP_1_UI_INFO_INFORMATION_URL,
                                    "language": "es",
                                },
                            ],
                            "privacy_statement_urls": [
                                {
                                    "value": saml_strings.IDP_1_UI_INFO_PRIVACY_STATEMENT_URL,
                                    "language": "en",
                                },
                                {
                                    "value": saml_strings.IDP_1_UI_INFO_PRIVACY_STATEMENT_URL,
                                    "language": "es",
                                },
                            ],
                            "logo_urls": [
                                {
                                    "value": saml_strings.IDP_1_UI_INFO_LOGO_URL,
                                    "language": "en",
                                },
                                {
                                    "value": saml_strings.IDP_1_UI_INFO_LOGO_URL,
                                    "language": "es",
                                },
                            ],
                        }
                    ],
                },
            ),
            (
                "identity_provider_with_organization_display_name",
                [IDENTITY_PROVIDER_WITH_ORGANIZATION_DISPLAY_NAME],
                {
                    "type": "http://librarysimplified.org/authtype/SAML-2.0",
                    "description": SAMLWebSSOAuthenticationProvider.label(),
                    "links": [
                        {
                            "rel": "authenticate",
                            "href": "http://localhost/default/saml_authenticate?provider=SAML+2.0+Web+SSO&idp_entity_id=http://idp2.hilbertteam.net/idp/shibboleth",
                            "display_names": [
                                {
                                    "value": saml_strings.IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME,
                                    "language": "en",
                                },
                                {
                                    "value": saml_strings.IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME,
                                    "language": "es",
                                },
                            ],
                            "descriptions": [],
                            "information_urls": [],
                            "privacy_statement_urls": [],
                            "logo_urls": [],
                        }
                    ],
                },
            ),
            (
                "identity_provider_without_display_names_and_default_template",
                [
                    IDENTITY_PROVIDER_WITHOUT_DISPLAY_NAMES,
                    IDENTITY_PROVIDER_WITHOUT_DISPLAY_NAMES,
                ],
                {
                    "type": "http://librarysimplified.org/authtype/SAML-2.0",
                    "description": SAMLWebSSOAuthenticationProvider.label(),
                    "links": [
                        {
                            "rel": "authenticate",
                            "href": "http://localhost/default/saml_authenticate?provider=SAML+2.0+Web+SSO&idp_entity_id=http://idp1.hilbertteam.net/idp/shibboleth",
                            "display_names": [
                                {
                                    "value": "Identity Provider #1",
                                    "language": "en",
                                }
                            ],
                            "descriptions": [],
                            "information_urls": [],
                            "privacy_statement_urls": [],
                            "logo_urls": [],
                        },
                        {
                            "rel": "authenticate",
                            "href": "http://localhost/default/saml_authenticate?provider=SAML+2.0+Web+SSO&idp_entity_id=http://idp1.hilbertteam.net/idp/shibboleth",
                            "display_names": [
                                {
                                    "value": "Identity Provider #2",
                                    "language": "en",
                                }
                            ],
                            "descriptions": [],
                            "information_urls": [],
                            "privacy_statement_urls": [],
                            "logo_urls": [],
                        },
                    ],
                },
            ),
        ],
    )
    def test_authentication_document(
        self,
        controller_fixture: ControllerFixture,
        create_saml_configuration: Callable[..., SAMLWebSSOAuthSettings],
        create_saml_provider: Callable[..., SAMLWebSSOAuthenticationProvider],
        create_mock_onelogin_configuration: Callable[..., SAMLOneLoginConfiguration],
        _,
        identity_providers,
        expected_result,
    ):
        # Arrange
        configuration = create_saml_configuration(
            patron_id_use_name_id="true",
            patron_id_attributes=[],
            patron_id_regular_expression=None,
        )
        onelogin_configuration = create_mock_onelogin_configuration(
            SERVICE_PROVIDER, identity_providers, configuration
        )
        subject_parser = SAMLSubjectParser()
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)
        authentication_manager = SAMLAuthenticationManager(
            onelogin_configuration, subject_parser, subject_filter
        )

        authentication_manager_factory = create_autospec(
            spec=SAMLAuthenticationManagerFactory
        )
        authentication_manager_factory.create = MagicMock(
            return_value=authentication_manager
        )

        with patch(
            "api.saml.provider.SAMLAuthenticationManagerFactory"
        ) as authentication_manager_factory_constructor_mock:
            authentication_manager_factory_constructor_mock.return_value = (
                authentication_manager_factory
            )

            # Act
            provider = create_saml_provider(
                settings=configuration,
            )

            controller_fixture.app.config["SERVER_NAME"] = "localhost"

            with controller_fixture.app.test_request_context("/"):
                result = provider.authentication_flow_document(
                    controller_fixture.db.session
                )

            # Assert
            assert expected_result == result

    @pytest.mark.parametrize(
        "_, subject, expected_result, patron_id_use_name_id, patron_id_attributes, patron_id_regular_expression",
        [
            (
                "empty_subject",
                None,
                SAML_INVALID_SUBJECT.detailed("Subject is empty"),
                None,
                None,
                None,
            ),
            (
                "subject_is_patron_data",
                PatronData(permanent_id=12345),
                PatronData(permanent_id=12345),
                None,
                None,
                None,
            ),
            (
                "subject_does_not_have_unique_id",
                SAMLSubject("http://idp.example.com", None, None),
                SAML_INVALID_SUBJECT.detailed("Subject does not have a unique ID"),
                None,
                None,
                None,
            ),
            (
                "subject_has_unique_id",
                SAMLSubject(
                    "http://idp.example.com",
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            )
                        ]
                    ),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
                None,
                None,
                None,
            ),
            (
                "subject_has_unique_name_id_but_use_of_name_id_is_switched_off_using_string_literal",
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", "12345"),
                    SAMLAttributeStatement([]),
                ),
                SAML_INVALID_SUBJECT.detailed("Subject does not have a unique ID"),
                "false",
                None,
                None,
            ),
            (
                "subject_has_unique_name_id_and_use_of_name_id_is_switched_on_using_string_literal_true",
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", "12345"),
                    SAMLAttributeStatement([]),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
                "true",
                None,
                None,
            ),
            (
                "subject_has_unique_id_matching_the_regular_expression",
                SAMLSubject(
                    "http://idp.example.com",
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonPrincipalName.name,
                                values=["firstname.lastname@university.org"],
                            )
                        ]
                    ),
                ),
                PatronData(
                    permanent_id="firstname.lastname",
                    authorization_identifier="firstname.lastname",
                    external_type="A",
                    complete=True,
                ),
                False,
                [SAMLAttributeType.eduPersonPrincipalName.name],
                saml_strings.PATRON_ID_REGULAR_EXPRESSION_ORG,
            ),
            (
                "subject_has_unique_id_not_matching_the_regular_expression",
                SAMLSubject(
                    "http://idp.example.com",
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonPrincipalName.name,
                                values=["firstname.lastname@university.com"],
                            )
                        ]
                    ),
                ),
                SAML_INVALID_SUBJECT.detailed("Subject does not have a unique ID"),
                False,
                [SAMLAttributeType.eduPersonPrincipalName.name],
                saml_strings.PATRON_ID_REGULAR_EXPRESSION_ORG,
            ),
        ],
    )
    def test_remote_patron_lookup(
        self,
        create_saml_configuration: Callable[..., SAMLWebSSOAuthSettings],
        create_saml_provider: Callable[..., SAMLWebSSOAuthenticationProvider],
        _,
        subject,
        expected_result,
        patron_id_use_name_id,
        patron_id_attributes,
        patron_id_regular_expression,
    ):
        # Arrange
        configuration = create_saml_configuration(
            patron_id_use_name_id=patron_id_use_name_id or True,
            patron_id_attributes=patron_id_attributes,
            patron_id_regular_expression=patron_id_regular_expression,
        )

        provider = create_saml_provider(
            settings=configuration,
        )

        # Act
        result = provider.remote_patron_lookup(subject)

        # Assert
        if isinstance(result, ProblemDetail):
            assert result.response == expected_result.response
        else:
            assert result == expected_result

    @pytest.mark.parametrize(
        "_, subject, expected_patron_data, expected_credential, expected_expiration_time, cm_session_lifetime",
        [
            (
                "empty_subject",
                None,
                SAML_INVALID_SUBJECT.detailed("Subject is empty"),
                None,
                None,
                None,
            ),
            (
                "subject_does_not_have_unique_id",
                SAMLSubject("http://idp.example.com", None, None),
                SAML_INVALID_SUBJECT.detailed("Subject does not have a unique ID"),
                None,
                None,
                None,
            ),
            (
                "subject_has_unique_id",
                SAMLSubject(
                    "http://idp.example.com",
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            )
                        ]
                    ),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
                None,
                None,
                None,
            ),
            (
                "subject_has_unique_id_and_persistent_name_id",
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(
                        SAMLNameIDFormat.PERSISTENT.value,
                        "name-qualifier",
                        "sp-name-qualifier",
                        "12345",
                    ),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            )
                        ]
                    ),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
                None,
                None,
                None,
            ),
            (
                "subject_has_unique_id_and_transient_name_id",
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(
                        SAMLNameIDFormat.TRANSIENT.value,
                        "name-qualifier",
                        "sp-name-qualifier",
                        "12345",
                    ),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            )
                        ]
                    ),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
                '{"idp": "http://idp.example.com", "attributes": {"eduPersonUniqueId": ["12345"]}}',
                None,
                None,
            ),
            (
                "subject_has_unique_id_and_custom_session_lifetime",
                SAMLSubject(
                    "http://idp.example.com",
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            )
                        ]
                    ),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
                None,
                datetime_utc(2020, 1, 1) + datetime.timedelta(days=42),
                42,
            ),
            (
                "subject_has_unique_id_and_empty_session_lifetime",
                SAMLSubject(
                    "http://idp.example.com",
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            )
                        ]
                    ),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
                None,
                None,
                "",
            ),
            (
                "subject_has_unique_id_and_non_default_expiration_timeout",
                SAMLSubject(
                    "http://idp.example.com",
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            )
                        ]
                    ),
                    valid_till=datetime.timedelta(days=1),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
                None,
                None,
                None,
            ),
            (
                "subject_has_unique_id_non_default_expiration_timeout_and_custom_session_lifetime",
                SAMLSubject(
                    "http://idp.example.com",
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            )
                        ]
                    ),
                    valid_till=datetime.timedelta(days=1),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
                None,
                datetime_utc(2020, 1, 1) + datetime.timedelta(days=42),
                42,
            ),
            (
                "subject_has_unique_id_non_default_expiration_timeout_and_empty_session_lifetime",
                SAMLSubject(
                    "http://idp.example.com",
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["12345"],
                            )
                        ]
                    ),
                    valid_till=datetime.timedelta(days=1),
                ),
                PatronData(
                    permanent_id="12345",
                    authorization_identifier="12345",
                    external_type="A",
                    complete=True,
                ),
                None,
                None,
                "",
            ),
        ],
    )
    @freeze_time("2020-01-01 00:00:00")
    def test_saml_callback(
        self,
        controller_fixture: ControllerFixture,
        create_saml_configuration: Callable[..., SAMLWebSSOAuthSettings],
        create_saml_provider: Callable[..., SAMLWebSSOAuthenticationProvider],
        _,
        subject,
        expected_patron_data,
        expected_credential,
        expected_expiration_time,
        cm_session_lifetime,
    ):
        # This test makes sure that SAMLWebSSOAuthenticationProvider.saml_callback
        # correctly processes a SAML subject and returns right PatronData.

        # Arrange
        configuration = create_saml_configuration(session_lifetime=cm_session_lifetime)
        provider = create_saml_provider(settings=configuration)

        if expected_credential is None:
            expected_credential = json.dumps(subject, cls=SAMLSubjectJSONEncoder)

        if expected_expiration_time is None and subject is not None:
            expected_expiration_time = utc_now() + subject.valid_till

        # Act
        result = provider.saml_callback(controller_fixture.db.session, subject)

        # Assert
        if isinstance(result, ProblemDetail):
            assert result.response == expected_patron_data.response
        else:
            credential, patron, patron_data = result

            assert expected_credential == credential.credential
            assert expected_patron_data.permanent_id == patron.external_identifier
            assert expected_patron_data == patron_data
            assert expected_expiration_time == credential.expires

    def test_get_credential_from_header(
        self,
        create_saml_provider: Callable[..., SAMLWebSSOAuthenticationProvider],
    ):
        # This provider doesn't support getting the credential from the header.
        # so this method should always return None.
        auth = Authorization("")

        provider = create_saml_provider()
        assert provider.get_credential_from_header(auth) is None
