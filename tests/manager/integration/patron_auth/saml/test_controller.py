import json
import logging
from contextlib import nullcontext
from unittest.mock import MagicMock, PropertyMock, create_autospec, patch
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest
from flask import request

from palace.manager.api.authentication.base import PatronData
from palace.manager.api.authenticator import Authenticator
from palace.manager.integration.patron_auth.saml.auth import (
    SAML_INCORRECT_RESPONSE,
    SAMLAuthenticationManager,
)
from palace.manager.integration.patron_auth.saml.configuration.problem_details import (
    SAML_INCORRECT_METADATA,
    SAML_METADATA_NOT_CONFIGURED,
)
from palace.manager.integration.patron_auth.saml.controller import (
    SAML_INVALID_REQUEST,
    SAML_INVALID_RESPONSE,
    SAMLController,
)
from palace.manager.integration.patron_auth.saml.metadata.model import (
    SAMLIdentityProviderMetadata,
    SAMLNameIDFormat,
    SAMLOrganization,
    SAMLService,
    SAMLServiceProviderMetadata,
    SAMLUIInfo,
)
from palace.manager.integration.patron_auth.saml.provider import (
    SAML_CANNOT_DETERMINE_PATRON,
    SAMLWebSSOAuthenticationProvider,
)
from palace.manager.sqlalchemy.model.credential import Credential
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException
from tests.fixtures.api_controller import ControllerFixture
from tests.mocks import saml_strings

SERVICE_PROVIDER = SAMLServiceProviderMetadata(
    saml_strings.SP_ENTITY_ID,
    SAMLUIInfo(),
    SAMLOrganization(),
    SAMLNameIDFormat.UNSPECIFIED.value,
    SAMLService(saml_strings.SP_ACS_URL, saml_strings.SP_ACS_BINDING),
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


def create_patron_data_mock():
    patron_data_mock = create_autospec(spec=PatronData)
    type(patron_data_mock).to_response_parameters = PropertyMock(return_value="")

    return patron_data_mock


class TestSAMLController:
    @pytest.mark.parametrize(
        "provider_name, idp_entity_id, redirect_uri, expected_problem, expected_relay_state",
        [
            pytest.param(
                None,
                None,
                None,
                SAML_INVALID_REQUEST.detailed(
                    "Required parameter {} is missing".format(
                        SAMLController.PROVIDER_NAME
                    )
                ),
                None,
                id="with_missing_provider_name",
            ),
            pytest.param(
                SAMLWebSSOAuthenticationProvider.label(),
                None,
                None,
                SAML_INVALID_REQUEST.detailed(
                    "Required parameter {} is missing".format(
                        SAMLController.IDP_ENTITY_ID
                    )
                ),
                None,
                id="with_missing_idp_entity_id",
            ),
            pytest.param(
                SAMLWebSSOAuthenticationProvider.label(),
                IDENTITY_PROVIDERS[0].entity_id,
                None,
                SAML_INVALID_REQUEST.detailed(
                    "Required parameter {} is missing".format(
                        SAMLController.REDIRECT_URI
                    )
                ),
                "http://localhost?"
                + urlencode(
                    {
                        SAMLController.LIBRARY_SHORT_NAME: "default",
                        SAMLController.PROVIDER_NAME: SAMLWebSSOAuthenticationProvider.label(),
                        SAMLController.IDP_ENTITY_ID: IDENTITY_PROVIDERS[0].entity_id,
                    }
                ),
                id="with_missing_redirect_uri",
            ),
            pytest.param(
                SAMLWebSSOAuthenticationProvider.label(),
                IDENTITY_PROVIDERS[0].entity_id,
                "http://localhost",
                None,
                "http://localhost?"
                + urlencode(
                    {
                        SAMLController.LIBRARY_SHORT_NAME: "default",
                        SAMLController.PROVIDER_NAME: SAMLWebSSOAuthenticationProvider.label(),
                        SAMLController.IDP_ENTITY_ID: IDENTITY_PROVIDERS[0].entity_id,
                    }
                ),
                id="with_all_parameters_set",
            ),
            pytest.param(
                SAMLWebSSOAuthenticationProvider.label(),
                IDENTITY_PROVIDERS[0].entity_id,
                "http://localhost#fragment",
                None,
                "http://localhost?"
                + urlencode(
                    {
                        SAMLController.LIBRARY_SHORT_NAME: "default",
                        SAMLController.PROVIDER_NAME: SAMLWebSSOAuthenticationProvider.label(),
                        SAMLController.IDP_ENTITY_ID: IDENTITY_PROVIDERS[0].entity_id,
                    }
                )
                + "#fragment",
                id="with_all_parameters_set_and_fragment",
            ),
            pytest.param(
                SAMLWebSSOAuthenticationProvider.label(),
                IDENTITY_PROVIDERS[0].entity_id,
                "http://localhost?patron_info=%7B%7D&access_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
                None,
                "http://localhost?"
                + urlencode(
                    {
                        SAMLController.LIBRARY_SHORT_NAME: "default",
                        SAMLController.PROVIDER_NAME: SAMLWebSSOAuthenticationProvider.label(),
                        SAMLController.IDP_ENTITY_ID: IDENTITY_PROVIDERS[0].entity_id,
                        "patron_info": "{}",
                        "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
                    }
                ),
                id="with_all_parameters_set_and_redirect_uri_containing_other_parameters",
            ),
        ],
    )
    def test_saml_authentication_redirect(
        self,
        controller_fixture: ControllerFixture,
        provider_name,
        idp_entity_id,
        redirect_uri,
        expected_problem,
        expected_relay_state,
    ):
        """Make sure that SAMLController.saml_authentication_redirect creates a correct RelayState or
        returns a correct ProblemDetail object in the case of any error.

        :param provider_name: Name of the authentication provider which should be passed as a request parameter
        :type provider_name: str

        :param idp_entity_id: Identity Provider's ID which should be passed as a request parameter
        :type idp_entity_id: str

        :param expected_problem: (Optional) Expected ProblemDetail object describing the error occurred (if any)
        :type expected_problem: Optional[ProblemDetail]

        :param expected_relay_state: (Optional) String containing the expected RelayState value
        :type expected_relay_state: Optional[str]
        """
        # Arrange
        expected_authentication_redirect_uri = "https://idp.circulationmanager.org"
        authentication_manager = create_autospec(spec=SAMLAuthenticationManager)
        authentication_manager.start_authentication = MagicMock(
            return_value=expected_authentication_redirect_uri
        )
        provider = create_autospec(spec=SAMLWebSSOAuthenticationProvider)
        provider.label = MagicMock(
            return_value=SAMLWebSSOAuthenticationProvider.label()
        )
        provider.get_authentication_manager = MagicMock(
            return_value=authentication_manager
        )
        provider.library = MagicMock(
            return_value=controller_fixture.db.default_library()
        )
        authenticator = Authenticator(
            controller_fixture.db.session,
            controller_fixture.db.session.query(Library),
        )
        integration = create_autospec(spec=IntegrationConfiguration)
        type(integration).parent_id = PropertyMock()

        authenticator.library_authenticators["default"].register_saml_provider(provider)

        controller = SAMLController(controller_fixture.app.manager, authenticator)
        params = {}

        if provider_name:
            params[SAMLController.PROVIDER_NAME] = provider_name
        if idp_entity_id:
            params[SAMLController.IDP_ENTITY_ID] = idp_entity_id
        if redirect_uri:
            params[SAMLController.REDIRECT_URI] = redirect_uri

        query = urlencode(params)

        with controller_fixture.app.test_request_context("/saml_authenticate?" + query):
            request.library = controller_fixture.db.default_library()  # type: ignore[attr-defined]

            # Act
            result = controller.saml_authentication_redirect(
                request.args, controller_fixture.db.session
            )

            # Assert
            if expected_problem:
                assert isinstance(result, ProblemDetail)
                assert result.response == expected_problem.response
            else:
                assert 302 == result.status_code
                assert expected_authentication_redirect_uri == result.headers.get(
                    "Location"
                )

                authentication_manager.start_authentication.assert_called_once_with(
                    controller_fixture.db.session,
                    idp_entity_id,
                    expected_relay_state,
                )

    @pytest.mark.parametrize(
        "data, finish_authentication_result, saml_callback_result, bearer_token, expected_authentication_redirect_uri, expected_problem,",
        [
            pytest.param(
                None,
                None,
                None,
                None,
                None,
                SAML_INVALID_RESPONSE.detailed(
                    "Required parameter {} is missing from the response body".format(
                        SAMLController.RELAY_STATE
                    )
                ),
                id="with_missing_relay_state",
            ),
            pytest.param(
                {SAMLController.RELAY_STATE: "<>"},
                None,
                None,
                None,
                None,
                SAML_INVALID_RESPONSE.detailed(
                    "Required parameter {} is missing from RelayState".format(
                        SAMLController.LIBRARY_SHORT_NAME
                    )
                ),
                id="with_incorrect_relay_state",
            ),
            pytest.param(
                {
                    SAMLController.RELAY_STATE: "http://localhost?"
                    + urlencode({SAMLController.LIBRARY_SHORT_NAME: "default"})
                },
                None,
                None,
                None,
                None,
                SAML_INVALID_RESPONSE.detailed(
                    "Required parameter {} is missing from RelayState".format(
                        SAMLController.PROVIDER_NAME
                    )
                ),
                id="with_missing_provider_name",
            ),
            pytest.param(
                {
                    SAMLController.RELAY_STATE: "http://localhost?"
                    + urlencode(
                        {
                            SAMLController.LIBRARY_SHORT_NAME: "default",
                            SAMLController.PROVIDER_NAME: SAMLWebSSOAuthenticationProvider.label(),
                        }
                    )
                },
                None,
                None,
                None,
                None,
                SAML_INVALID_RESPONSE.detailed(
                    "Required parameter {} is missing from RelayState".format(
                        SAMLController.IDP_ENTITY_ID
                    )
                ),
                id="with_missing_idp_entity_id",
            ),
            pytest.param(
                {
                    SAMLController.RELAY_STATE: "http://localhost?"
                    + urlencode(
                        {
                            SAMLController.LIBRARY_SHORT_NAME: "default",
                            SAMLController.PROVIDER_NAME: SAMLWebSSOAuthenticationProvider.label(),
                            SAMLController.IDP_ENTITY_ID: IDENTITY_PROVIDERS[
                                0
                            ].entity_id,
                        }
                    )
                },
                SAML_INCORRECT_RESPONSE.detailed("Authentication failed"),
                None,
                None,
                None,
                None,
                id="when_finish_authentication_fails",
            ),
            pytest.param(
                {
                    SAMLController.RELAY_STATE: "http://localhost?"
                    + urlencode(
                        {
                            SAMLController.LIBRARY_SHORT_NAME: "default",
                            SAMLController.PROVIDER_NAME: SAMLWebSSOAuthenticationProvider.label(),
                            SAMLController.IDP_ENTITY_ID: IDENTITY_PROVIDERS[
                                0
                            ].entity_id,
                        }
                    )
                },
                None,
                SAML_CANNOT_DETERMINE_PATRON,
                None,
                None,
                None,
                id="when_saml_callback_fails",
            ),
            pytest.param(
                {
                    SAMLController.RELAY_STATE: "http://localhost?"
                    + urlencode(
                        {
                            SAMLController.LIBRARY_SHORT_NAME: "default",
                            SAMLController.PROVIDER_NAME: SAMLWebSSOAuthenticationProvider.label(),
                            SAMLController.IDP_ENTITY_ID: IDENTITY_PROVIDERS[
                                0
                            ].entity_id,
                        }
                    )
                },
                None,
                (create_autospec(spec=Credential), object(), create_patron_data_mock()),
                "ABCDEFG",
                "http://localhost?access_token=ABCDEFG&patron_info=%22%22",
                None,
                id="when_saml_callback_returns_correct_patron",
            ),
        ],
    )
    def test_saml_authentication_callback(
        self,
        controller_fixture: ControllerFixture,
        data,
        finish_authentication_result: ProblemDetail | None,
        saml_callback_result: tuple[Credential, object, PatronData] | None,
        bearer_token: str | None,
        expected_authentication_redirect_uri: str | None,
        expected_problem: ProblemDetail | None,
    ):
        # Arrange
        authentication_manager = create_autospec(spec=SAMLAuthenticationManager)
        authentication_manager.finish_authentication = MagicMock(
            return_value=finish_authentication_result
        )
        provider = create_autospec(spec=SAMLWebSSOAuthenticationProvider)
        provider.label = MagicMock(
            return_value=SAMLWebSSOAuthenticationProvider.label()
        )
        provider.get_authentication_manager = MagicMock(
            return_value=authentication_manager
        )
        provider.library = MagicMock(
            return_value=controller_fixture.db.default_library()
        )
        provider.saml_callback = (
            MagicMock(
                side_effect=ProblemDetailException(problem_detail=saml_callback_result)
            )
            if isinstance(saml_callback_result, ProblemDetail)
            else MagicMock(return_value=saml_callback_result)
        )
        authenticator = Authenticator(
            controller_fixture.db.session,
            libraries=controller_fixture.db.session.query(Library),
        )
        integration = create_autospec(spec=IntegrationConfiguration)
        type(integration).parent_id = PropertyMock()

        authenticator.library_authenticators["default"].register_saml_provider(provider)
        authenticator.library_authenticators["default"].bearer_token_signing_secret = (
            "test"
        )
        authenticator.create_bearer_token = MagicMock(return_value=bearer_token)

        controller = SAMLController(controller_fixture.app.manager, authenticator)

        with controller_fixture.app.test_request_context("/saml_callback", data=data):
            # Act
            result = controller.saml_authentication_callback(
                request, controller_fixture.db.session
            )

            # Assert
            if isinstance(finish_authentication_result, ProblemDetail) or isinstance(
                saml_callback_result, ProblemDetail
            ):
                assert result.status_code == 302

                query_items = parse_qs(urlsplit(result.location).query)

                assert SAMLController.ERROR in query_items

                error_str = query_items[SAMLController.ERROR][0]
                error = json.loads(error_str)

                problem = (
                    finish_authentication_result
                    if finish_authentication_result
                    else saml_callback_result
                )
                assert error["type"] == problem.uri
                assert error["status"] == problem.status_code
                assert error["title"] == problem.title
                assert error["detail"] == problem.detail
            elif expected_problem:
                assert isinstance(result, ProblemDetail)
                assert result.response == expected_problem.response
            else:
                assert result.status_code == 302
                assert (
                    result.headers.get("Location")
                    == expected_authentication_redirect_uri
                )

                authentication_manager.finish_authentication.assert_called_once_with(
                    controller_fixture.db.session,
                    IDENTITY_PROVIDERS[0].entity_id,
                )
                provider.saml_callback.assert_called_once_with(
                    controller_fixture.db.session,
                    finish_authentication_result,
                )

    @pytest.mark.parametrize(
        "library_in_context, register_provider, sp_metadata_xml, expected_problem, expected_log_message",
        [
            pytest.param(
                False,
                False,
                None,
                SAML_METADATA_NOT_CONFIGURED,
                "SAML metadata is not configured.",
                id="no-library-no-env-metadata",
            ),
            pytest.param(
                False,
                False,
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                None,
                None,
                id="no-library-env-metadata-configured",
            ),
            pytest.param(
                False,
                False,
                "<invalid",
                SAML_INCORRECT_METADATA,
                "SAML metadata has an incorrect format.",
                id="no-library-invalid-metadata",
            ),
            pytest.param(
                True,
                False,
                None,
                SAML_METADATA_NOT_CONFIGURED,
                "is not configured for SAML authentication.",
                id="library-no-saml-integration",
            ),
            pytest.param(
                True,
                True,
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                None,
                None,
                id="library-saml-integration-metadata-configured",
            ),
            pytest.param(
                True,
                True,
                None,
                SAML_METADATA_NOT_CONFIGURED,
                "SAML metadata is not configured.",
                id="library-saml-integration-no-metadata",
            ),
            pytest.param(
                True,
                True,
                "<invalid",
                SAML_INCORRECT_METADATA,
                "SAML metadata has an incorrect format.",
                id="library-saml-integration-invalid-metadata",
            ),
        ],
    )
    def test_saml_sp_metadata(
        self,
        controller_fixture: ControllerFixture,
        caplog: pytest.LogCaptureFixture,
        library_in_context: bool,
        register_provider: bool,
        sp_metadata_xml: str | None,
        expected_problem: ProblemDetail | None,
        expected_log_message: str | None,
    ):
        authenticator = Authenticator(
            controller_fixture.db.session,
            controller_fixture.db.session.query(Library),
        )

        if register_provider:
            provider = create_autospec(spec=SAMLWebSSOAuthenticationProvider)
            provider.label = MagicMock(
                return_value=SAMLWebSSOAuthenticationProvider.label()
            )
            provider.get_sp_metadata_xml = MagicMock(return_value=sp_metadata_xml)
            authenticator.library_authenticators["default"].register_saml_provider(
                provider
            )

        controller = SAMLController(controller_fixture.app.manager, authenticator)

        raises = (
            pytest.raises(ProblemDetailException) if expected_problem else nullcontext()
        )
        caplog.set_level(
            logging.ERROR,
            logger="palace.manager.integration.patron_auth.saml.controller",
        )
        with controller_fixture.app.test_request_context("/saml/metadata/sp"):
            if library_in_context:
                request.library = controller_fixture.db.default_library()  # type: ignore[attr-defined]

            with patch(
                "palace.manager.integration.patron_auth.saml.controller"
                ".SamlServiceProviderConfiguration.get_metadata",
                return_value=sp_metadata_xml,
            ):
                with raises as exc_info:
                    result = controller.saml_sp_metadata()

        if expected_problem:
            assert exc_info.value.problem_detail.uri == expected_problem.uri
            assert (
                exc_info.value.problem_detail.status_code
                == expected_problem.status_code
            )
        else:
            assert result.status_code == 200
            assert result.content_type == "application/xml"
            stripped = sp_metadata_xml.strip()
            expected_xml = (
                '<?xml version="1.0" encoding="UTF-8"?>\n' + stripped
                if not stripped.startswith("<?xml")
                else stripped
            )
            assert result.get_data(as_text=True) == expected_xml

        controller_errors = [
            r
            for r in caplog.records
            if r.levelno == logging.ERROR
            and r.name == "palace.manager.integration.patron_auth.saml.controller"
        ]
        if expected_log_message:
            assert len(controller_errors) == 1
            assert expected_log_message in controller_errors[0].message
        else:
            assert controller_errors == []
