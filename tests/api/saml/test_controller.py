import json
from unittest.mock import MagicMock, PropertyMock, create_autospec
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest
from flask import request

from api.authentication.base import PatronData
from api.authenticator import Authenticator
from api.saml.auth import SAML_INCORRECT_RESPONSE, SAMLAuthenticationManager
from api.saml.controller import (
    SAML_INVALID_REQUEST,
    SAML_INVALID_RESPONSE,
    SAMLController,
)
from api.saml.metadata.model import (
    SAMLIdentityProviderMetadata,
    SAMLNameIDFormat,
    SAMLOrganization,
    SAMLService,
    SAMLServiceProviderMetadata,
    SAMLUIInfo,
)
from api.saml.provider import SAML_INVALID_SUBJECT, SAMLWebSSOAuthenticationProvider
from core.model import Credential, Library
from core.model.integration import IntegrationConfiguration
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
        "_, provider_name, idp_entity_id, redirect_uri, expected_problem, expected_relay_state",
        [
            (
                "with_missing_provider_name",
                None,
                None,
                None,
                SAML_INVALID_REQUEST.detailed(
                    "Required parameter {} is missing".format(
                        SAMLController.PROVIDER_NAME
                    )
                ),
                None,
            ),
            (
                "with_missing_idp_entity_id",
                SAMLWebSSOAuthenticationProvider.label(),
                None,
                None,
                SAML_INVALID_REQUEST.detailed(
                    "Required parameter {} is missing".format(
                        SAMLController.IDP_ENTITY_ID
                    )
                ),
                None,
            ),
            (
                "with_missing_redirect_uri",
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
            ),
            (
                "with_all_parameters_set",
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
            ),
            (
                "with_all_parameters_set_and_fragment",
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
            ),
            (
                "with_all_parameters_set_and_redirect_uri_containing_other_parameters",
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
            ),
        ],
    )
    def test_saml_authentication_redirect(
        self,
        controller_fixture: ControllerFixture,
        _,
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

        with controller_fixture.app.test_request_context(
            "http://circulationmanager.org/saml_authenticate?" + query
        ):
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
        "_, data, finish_authentication_result, saml_callback_result, bearer_token, expected_authentication_redirect_uri, expected_problem,",
        [
            (
                "with_missing_relay_state",
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
            ),
            (
                "with_incorrect_relay_state",
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
            ),
            (
                "with_missing_provider_name",
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
            ),
            (
                "with_missing_idp_entity_id",
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
            ),
            (
                "when_finish_authentication_fails",
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
            ),
            (
                "when_saml_callback_fails",
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
                SAML_INVALID_SUBJECT.detailed("Authentication failed"),
                None,
                None,
                None,
            ),
            (
                "when_saml_callback_returns_correct_patron",
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
            ),
        ],
    )
    def test_saml_authentication_callback(
        self,
        controller_fixture: ControllerFixture,
        _,
        data,
        finish_authentication_result,
        saml_callback_result,
        bearer_token,
        expected_authentication_redirect_uri,
        expected_problem,
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
        provider.saml_callback = MagicMock(return_value=saml_callback_result)
        authenticator = Authenticator(
            controller_fixture.db.session,
            libraries=controller_fixture.db.session.query(Library),
        )
        integration = create_autospec(spec=IntegrationConfiguration)
        type(integration).parent_id = PropertyMock()

        authenticator.library_authenticators["default"].register_saml_provider(provider)
        authenticator.library_authenticators[
            "default"
        ].bearer_token_signing_secret = "test"
        authenticator.create_bearer_token = MagicMock(return_value=bearer_token)

        controller = SAMLController(controller_fixture.app.manager, authenticator)

        with controller_fixture.app.test_request_context(
            "http://circulationmanager.org/saml_callback", data=data
        ):
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
