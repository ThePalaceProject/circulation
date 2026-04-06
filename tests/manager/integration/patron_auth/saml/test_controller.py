import json
import logging
from contextlib import nullcontext
from unittest.mock import MagicMock, Mock, PropertyMock, create_autospec, patch
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest
from flask import request
from lxml import etree

from palace.manager.api.authentication.base import PatronData
from palace.manager.api.authenticator import Authenticator
from palace.manager.integration.patron_auth.constants import LOGOUT_REDIRECT_QUERY_PARAM
from palace.manager.integration.patron_auth.saml.auth import (
    SAML_GENERIC_ERROR,
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
    SAMLBinding,
    SAMLIdentityProviderMetadata,
    SAMLNameID,
    SAMLNameIDFormat,
    SAMLOrganization,
    SAMLService,
    SAMLServiceProviderMetadata,
    SAMLSubject,
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

CORRECT_XML_WITH_DECLARATION = (
    '<?xml version="1.0" encoding="UTF-8"?>\n' + saml_strings.CORRECT_XML_WITH_ONE_SP
)

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
        "library_in_request, register_provider, sp_metadata_xml, expected_problem, expected_log_message",
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
                id="library-has-saml-with-metadata",
            ),
            pytest.param(
                True,
                True,
                None,
                SAML_METADATA_NOT_CONFIGURED,
                "SAML metadata is not configured.",
                id="library-has-saml-no-metadata",
            ),
            pytest.param(
                True,
                True,
                "<invalid",
                SAML_INCORRECT_METADATA,
                "SAML metadata has an incorrect format.",
                id="library-has-saml-invalid-metadata",
            ),
            pytest.param(
                False,
                False,
                CORRECT_XML_WITH_DECLARATION,
                None,
                None,
                id="no-library-env-metadata-has-declaration",
            ),
            pytest.param(
                True,
                True,
                CORRECT_XML_WITH_DECLARATION,
                None,
                None,
                id="library-has-saml-metadata-has-declaration",
            ),
            pytest.param(
                False,
                False,
                "\ufeff" + saml_strings.CORRECT_XML_WITH_ONE_SP,
                None,
                None,
                id="no-library-env-metadata-with-bom",
            ),
        ],
    )
    def test_saml_sp_metadata(
        self,
        controller_fixture: ControllerFixture,
        caplog: pytest.LogCaptureFixture,
        library_in_request: bool,
        register_provider: bool,
        sp_metadata_xml: str | None,
        expected_problem: ProblemDetail | None,
        expected_log_message: str | None,
    ):
        SAMLController.clear_metadata_cache()
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
            if library_in_request:
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
            assert result.content_type == "application/samlmetadata+xml"
            root = etree.fromstring(sp_metadata_xml.encode())
            expected_xml = etree.tostring(
                root, xml_declaration=True, encoding="UTF-8"
            ).decode()
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

    def test_clear_metadata_cache(self):
        SAMLController.clear_metadata_cache()
        SAMLController._validated_sp_metadata(
            SAMLController._SITE_WIDE_METADATA_CACHE_KEY,
            saml_strings.CORRECT_XML_WITH_ONE_SP,
        )
        assert len(SAMLController._sp_metadata_cache) == 1

        SAMLController.clear_metadata_cache()
        assert len(SAMLController._sp_metadata_cache) == 0

    @pytest.mark.parametrize(
        "xml, expect_cached_value",
        [
            pytest.param(
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                True,
                id="valid-xml-cached",
            ),
            pytest.param(
                None,
                False,
                id="none-cached-as-none",
            ),
        ],
    )
    def test_validated_sp_metadata(
        self, xml: str | None, expect_cached_value: bool
    ) -> None:
        SAMLController.clear_metadata_cache()
        result = SAMLController._validated_sp_metadata("key", xml)

        assert "key" in SAMLController._sp_metadata_cache
        if expect_cached_value:
            assert result is not None
            assert result == SAMLController._sp_metadata_cache["key"]
            # Result should be normalized XML with declaration
            assert result.startswith("<?xml")
        else:
            assert result is None
            assert SAMLController._sp_metadata_cache["key"] is None

    def test_validated_sp_metadata_invalid_xml_not_cached(self) -> None:
        SAMLController.clear_metadata_cache()
        with pytest.raises(Exception):
            SAMLController._validated_sp_metadata("key", "<invalid")
        assert "key" not in SAMLController._sp_metadata_cache

    def test_saml_sp_metadata_uses_cache(
        self, controller_fixture: ControllerFixture
    ) -> None:
        """A second request for the same key returns the cached value without
        re-fetching from configuration."""
        SAMLController.clear_metadata_cache()
        authenticator = Authenticator(
            controller_fixture.db.session,
            controller_fixture.db.session.query(Library),
        )
        controller = SAMLController(controller_fixture.app.manager, authenticator)

        with controller_fixture.app.test_request_context("/saml/metadata/sp"):
            with patch.object(
                controller,
                "_load_sp_metadata_xml",
                return_value=saml_strings.CORRECT_XML_WITH_ONE_SP,
            ) as mock_load:
                controller.saml_sp_metadata()
                controller.saml_sp_metadata()

        mock_load.assert_called_once()

    @pytest.mark.parametrize(
        "library, provider_result, expected_xml",
        [
            pytest.param(
                False,
                None,
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                id="no-library-returns-env-metadata",
            ),
            pytest.param(
                True,
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                id="library-returns-provider-metadata",
            ),
            pytest.param(
                True,
                None,
                None,
                id="library-provider-returns-none",
            ),
        ],
    )
    def test_load_sp_metadata_xml(
        self,
        controller_fixture: ControllerFixture,
        library: bool,
        provider_result: str | None,
        expected_xml: str | None,
    ) -> None:
        authenticator = Authenticator(
            controller_fixture.db.session,
            controller_fixture.db.session.query(Library),
        )
        lib = controller_fixture.db.default_library() if library else None

        if library:
            provider = create_autospec(spec=SAMLWebSSOAuthenticationProvider)
            provider.label = MagicMock(
                return_value=SAMLWebSSOAuthenticationProvider.label()
            )
            provider.get_sp_metadata_xml = MagicMock(return_value=provider_result)
            authenticator.library_authenticators["default"].register_saml_provider(
                provider
            )

        controller = SAMLController(controller_fixture.app.manager, authenticator)

        with controller_fixture.app.test_request_context("/saml/metadata/sp"):
            if lib is not None:
                request.library = lib  # type: ignore[attr-defined]
            with patch(
                "palace.manager.integration.patron_auth.saml.controller"
                ".SamlServiceProviderConfiguration.get_metadata",
                return_value=saml_strings.CORRECT_XML_WITH_ONE_SP,
            ):
                result = controller._load_sp_metadata_xml(lib)

        assert result == expected_xml

    def test_load_sp_metadata_xml_library_no_saml(
        self, controller_fixture: ControllerFixture
    ) -> None:
        """Raises ProblemDetailException when a library has no SAML provider."""
        authenticator = Authenticator(
            controller_fixture.db.session,
            controller_fixture.db.session.query(Library),
        )
        controller = SAMLController(controller_fixture.app.manager, authenticator)
        lib = controller_fixture.db.default_library()

        with controller_fixture.app.test_request_context("/saml/metadata/sp"):
            request.library = lib  # type: ignore[attr-defined]
            with pytest.raises(ProblemDetailException) as exc_info:
                controller._load_sp_metadata_xml(lib)

        assert exc_info.value.problem_detail.uri == SAML_METADATA_NOT_CONFIGURED.uri


class TestSAMLControllerLogout:
    """Tests for SP-Initiated SAML SLO."""

    IDP_ENTITY_ID = saml_strings.IDP_1_ENTITY_ID
    SLO_URL = "http://idp.example.com/idp/profile/SAML2/Redirect/SLO"
    REDIRECT_URI = "https://app.example.com/after-logout"
    CALLBACK_URL = "https://cm.example.com/saml/logout_callback"
    PROVIDER_NAME = SAMLWebSSOAuthenticationProvider.label()

    def _make_name_id(self) -> SAMLNameID:
        return SAMLNameID(
            SAMLNameIDFormat.PERSISTENT.value,
            name_qualifier="",
            sp_name_qualifier=None,
            name_id="patron-name-id",
        )

    def _make_subject(self) -> SAMLSubject:
        return SAMLSubject(
            idp=self.IDP_ENTITY_ID,
            name_id=self._make_name_id(),
            attribute_statement=None,
        )

    def _make_idp_with_slo(self) -> SAMLIdentityProviderMetadata:
        return SAMLIdentityProviderMetadata(
            self.IDP_ENTITY_ID,
            SAMLUIInfo(),
            SAMLOrganization(),
            SAMLNameIDFormat.UNSPECIFIED.value,
            SAMLService(saml_strings.IDP_1_SSO_URL, saml_strings.IDP_1_SSO_BINDING),
            slo_service=SAMLService(self.SLO_URL, SAMLBinding.HTTP_REDIRECT),
            signing_certificates=[saml_strings.SIGNING_CERTIFICATE],
        )

    def _make_idp_without_slo(self) -> SAMLIdentityProviderMetadata:
        return SAMLIdentityProviderMetadata(
            self.IDP_ENTITY_ID,
            SAMLUIInfo(),
            SAMLOrganization(),
            SAMLNameIDFormat.UNSPECIFIED.value,
            SAMLService(saml_strings.IDP_1_SSO_URL, saml_strings.IDP_1_SSO_BINDING),
        )

    def _make_controller_and_mocks(self, controller_fixture: ControllerFixture):
        """Return (controller, mock_provider, mock_auth_manager, mock_credential_manager)."""
        mock_auth_manager = Mock(spec=SAMLAuthenticationManager)
        mock_auth_manager.start_logout.return_value = (
            f"{self.SLO_URL}?SAMLRequest=encoded"
        )
        mock_auth_manager.finish_logout.return_value = True

        mock_credential_manager = Mock()

        mock_provider = Mock(spec=SAMLWebSSOAuthenticationProvider)
        mock_provider.label.return_value = self.PROVIDER_NAME
        mock_provider.get_authentication_manager.return_value = mock_auth_manager
        mock_provider._credential_manager = mock_credential_manager

        authenticator = Authenticator(
            controller_fixture.db.session,
            controller_fixture.db.session.query(Library),
        )
        authenticator.library_authenticators["default"].register_saml_provider(
            mock_provider
        )

        controller = SAMLController(controller_fixture.app.manager, authenticator)
        return controller, mock_provider, mock_auth_manager, mock_credential_manager

    def test_saml_logout_redirect_success_with_slo(
        self, controller_fixture: ControllerFixture
    ):
        """Happy path: credential found, IdP supports SLO → redirect to IdP SLO endpoint."""
        controller, mock_provider, mock_auth_manager, mock_credential_manager = (
            self._make_controller_and_mocks(controller_fixture)
        )
        library = controller_fixture.db.default_library()

        subject = self._make_subject()
        mock_credential = Mock()
        mock_credential_manager.lookup_saml_token_by_value.return_value = (
            mock_credential
        )
        mock_credential_manager.extract_saml_token.return_value = subject

        idp_with_slo = self._make_idp_with_slo()
        mock_auth_manager.configuration.get_identity_providers.return_value = [
            idp_with_slo
        ]

        library_authenticator = controller._authenticator.library_authenticators[
            "default"
        ]
        library_authenticator.decode_bearer_token = Mock(
            return_value=(self.PROVIDER_NAME, "raw-token-value")
        )

        params = {
            SAMLController.PROVIDER_NAME: self.PROVIDER_NAME,
            LOGOUT_REDIRECT_QUERY_PARAM: self.REDIRECT_URI,
        }

        with controller_fixture.app.test_request_context(
            f"/default/saml/logout?provider={self.PROVIDER_NAME}&post_logout_redirect_uri={self.REDIRECT_URI}",
            headers={"Authorization": "Bearer valid.jwt.token"},
        ):
            request.library = library  # type: ignore[attr-defined]
            with patch(
                "palace.manager.integration.patron_auth.saml.controller.get_request_library",
                return_value=library,
            ):
                with patch(
                    "palace.manager.integration.patron_auth.saml.controller.url_for",
                    return_value=self.CALLBACK_URL,
                ):
                    result = controller.saml_logout_redirect(
                        params, controller_fixture.db.session
                    )

        assert result.status_code == 302
        assert self.SLO_URL in result.location
        mock_credential_manager.invalidate_saml_token.assert_called_once_with(
            controller_fixture.db.session, mock_credential
        )
        mock_auth_manager.start_logout.assert_called_once()

    def test_saml_logout_redirect_no_slo_service(
        self, controller_fixture: ControllerFixture
    ):
        """IdP has no SLO service → local-only logout, redirect with logout_status=success."""
        controller, mock_provider, mock_auth_manager, mock_credential_manager = (
            self._make_controller_and_mocks(controller_fixture)
        )
        library = controller_fixture.db.default_library()

        subject = self._make_subject()
        mock_credential = Mock()
        mock_credential_manager.lookup_saml_token_by_value.return_value = (
            mock_credential
        )
        mock_credential_manager.extract_saml_token.return_value = subject

        idp_no_slo = self._make_idp_without_slo()
        mock_auth_manager.configuration.get_identity_providers.return_value = [
            idp_no_slo
        ]

        library_authenticator = controller._authenticator.library_authenticators[
            "default"
        ]
        library_authenticator.decode_bearer_token = Mock(
            return_value=(self.PROVIDER_NAME, "raw-token-value")
        )

        params = {
            SAMLController.PROVIDER_NAME: self.PROVIDER_NAME,
            LOGOUT_REDIRECT_QUERY_PARAM: self.REDIRECT_URI,
        }

        with controller_fixture.app.test_request_context(
            f"/default/saml/logout",
            headers={"Authorization": "Bearer valid.jwt.token"},
        ):
            request.library = library  # type: ignore[attr-defined]
            with patch(
                "palace.manager.integration.patron_auth.saml.controller.get_request_library",
                return_value=library,
            ):
                result = controller.saml_logout_redirect(
                    params, controller_fixture.db.session
                )

        assert result.status_code == 302
        assert "logout_status=success" in result.location
        mock_auth_manager.start_logout.assert_not_called()

    def test_saml_logout_redirect_credential_not_found(
        self, controller_fixture: ControllerFixture
    ):
        """No credential found → local-only logout, redirect with logout_status=success."""
        controller, mock_provider, mock_auth_manager, mock_credential_manager = (
            self._make_controller_and_mocks(controller_fixture)
        )
        library = controller_fixture.db.default_library()

        mock_credential_manager.lookup_saml_token_by_value.return_value = None

        library_authenticator = controller._authenticator.library_authenticators[
            "default"
        ]
        library_authenticator.decode_bearer_token = Mock(
            return_value=(self.PROVIDER_NAME, "raw-token-value")
        )

        params = {
            SAMLController.PROVIDER_NAME: self.PROVIDER_NAME,
            LOGOUT_REDIRECT_QUERY_PARAM: self.REDIRECT_URI,
        }

        with controller_fixture.app.test_request_context(
            "/default/saml/logout",
            headers={"Authorization": "Bearer valid.jwt.token"},
        ):
            request.library = library  # type: ignore[attr-defined]
            with patch(
                "palace.manager.integration.patron_auth.saml.controller.get_request_library",
                return_value=library,
            ):
                result = controller.saml_logout_redirect(
                    params, controller_fixture.db.session
                )

        assert result.status_code == 302
        assert "logout_status=success" in result.location
        mock_credential_manager.invalidate_saml_token.assert_not_called()

    @pytest.mark.parametrize(
        "params, auth_header, expected_uri",
        [
            pytest.param(
                {},
                "Bearer token",
                SAML_INVALID_REQUEST.uri,
                id="missing-provider",
            ),
            pytest.param(
                {
                    SAMLController.PROVIDER_NAME: SAMLWebSSOAuthenticationProvider.label()
                },
                "Bearer token",
                SAML_INVALID_REQUEST.uri,
                id="missing-redirect-uri",
            ),
            pytest.param(
                {
                    SAMLController.PROVIDER_NAME: SAMLWebSSOAuthenticationProvider.label(),
                    LOGOUT_REDIRECT_QUERY_PARAM: "https://app.example.com/logout",
                },
                "NotBearer token",
                SAML_INVALID_REQUEST.uri,
                id="invalid-auth-header",
            ),
        ],
    )
    def test_saml_logout_redirect_invalid_params(
        self,
        controller_fixture: ControllerFixture,
        params: dict,
        auth_header: str,
        expected_uri: str,
    ):
        """Missing or invalid parameters return ProblemDetail."""
        controller, *_ = self._make_controller_and_mocks(controller_fixture)
        library = controller_fixture.db.default_library()

        with controller_fixture.app.test_request_context(
            "/default/saml/logout",
            headers={"Authorization": auth_header},
        ):
            request.library = library  # type: ignore[attr-defined]
            with patch(
                "palace.manager.integration.patron_auth.saml.controller.get_request_library",
                return_value=library,
            ):
                result = controller.saml_logout_redirect(
                    params, controller_fixture.db.session
                )

        assert isinstance(result, ProblemDetail)
        assert result.uri == expected_uri

    def test_saml_logout_redirect_invalid_bearer_token(
        self, controller_fixture: ControllerFixture
    ):
        """Invalid JWT bearer token returns ProblemDetail."""
        import jwt

        controller, mock_provider, mock_auth_manager, mock_credential_manager = (
            self._make_controller_and_mocks(controller_fixture)
        )
        library = controller_fixture.db.default_library()

        library_authenticator = controller._authenticator.library_authenticators[
            "default"
        ]
        library_authenticator.decode_bearer_token = Mock(
            side_effect=jwt.exceptions.InvalidTokenError("bad token")
        )

        params = {
            SAMLController.PROVIDER_NAME: self.PROVIDER_NAME,
            LOGOUT_REDIRECT_QUERY_PARAM: self.REDIRECT_URI,
        }

        with controller_fixture.app.test_request_context(
            "/default/saml/logout",
            headers={"Authorization": "Bearer bad.token"},
        ):
            request.library = library  # type: ignore[attr-defined]
            with patch(
                "palace.manager.integration.patron_auth.saml.controller.get_request_library",
                return_value=library,
            ):
                result = controller.saml_logout_redirect(
                    params, controller_fixture.db.session
                )

        assert isinstance(result, ProblemDetail)
        assert result.uri == SAML_INVALID_REQUEST.uri

    def test_saml_logout_redirect_provider_mismatch(
        self, controller_fixture: ControllerFixture
    ):
        """Bearer token from different provider returns ProblemDetail."""
        controller, mock_provider, mock_auth_manager, mock_credential_manager = (
            self._make_controller_and_mocks(controller_fixture)
        )
        library = controller_fixture.db.default_library()

        library_authenticator = controller._authenticator.library_authenticators[
            "default"
        ]
        library_authenticator.decode_bearer_token = Mock(
            return_value=("Different Provider", "token")
        )

        params = {
            SAMLController.PROVIDER_NAME: self.PROVIDER_NAME,
            LOGOUT_REDIRECT_QUERY_PARAM: self.REDIRECT_URI,
        }

        with controller_fixture.app.test_request_context(
            "/default/saml/logout",
            headers={"Authorization": "Bearer valid.jwt.token"},
        ):
            request.library = library  # type: ignore[attr-defined]
            with patch(
                "palace.manager.integration.patron_auth.saml.controller.get_request_library",
                return_value=library,
            ):
                result = controller.saml_logout_redirect(
                    params, controller_fixture.db.session
                )

        assert isinstance(result, ProblemDetail)
        assert result.uri == SAML_INVALID_REQUEST.uri

    def test_saml_logout_redirect_start_logout_fails_returns_partial(
        self, controller_fixture: ControllerFixture
    ):
        """When start_logout fails, fall back to partial logout redirect."""
        controller, mock_provider, mock_auth_manager, mock_credential_manager = (
            self._make_controller_and_mocks(controller_fixture)
        )
        library = controller_fixture.db.default_library()

        subject = self._make_subject()
        mock_credential = Mock()
        mock_credential_manager.lookup_saml_token_by_value.return_value = (
            mock_credential
        )
        mock_credential_manager.extract_saml_token.return_value = subject

        idp_with_slo = self._make_idp_with_slo()
        mock_auth_manager.configuration.get_identity_providers.return_value = [
            idp_with_slo
        ]
        mock_auth_manager.start_logout.return_value = SAML_GENERIC_ERROR.detailed(
            "SAML error"
        )

        library_authenticator = controller._authenticator.library_authenticators[
            "default"
        ]
        library_authenticator.decode_bearer_token = Mock(
            return_value=(self.PROVIDER_NAME, "raw-token-value")
        )

        params = {
            SAMLController.PROVIDER_NAME: self.PROVIDER_NAME,
            LOGOUT_REDIRECT_QUERY_PARAM: self.REDIRECT_URI,
        }

        with controller_fixture.app.test_request_context(
            "/default/saml/logout",
            headers={"Authorization": "Bearer valid.jwt.token"},
        ):
            request.library = library  # type: ignore[attr-defined]
            with patch(
                "palace.manager.integration.patron_auth.saml.controller.get_request_library",
                return_value=library,
            ):
                with patch(
                    "palace.manager.integration.patron_auth.saml.controller.url_for",
                    return_value=self.CALLBACK_URL,
                ):
                    result = controller.saml_logout_redirect(
                        params, controller_fixture.db.session
                    )

        assert result.status_code == 302
        assert "logout_status=partial" in result.location

    def test_saml_logout_callback_success(self, controller_fixture: ControllerFixture):
        """Happy path: valid SAMLResponse → redirect with logout_status=success."""
        controller, mock_provider, mock_auth_manager, mock_credential_manager = (
            self._make_controller_and_mocks(controller_fixture)
        )

        relay_state = urlencode(
            {
                SAMLController.LIBRARY_SHORT_NAME: "default",
                SAMLController.PROVIDER_NAME: self.PROVIDER_NAME,
                SAMLController.IDP_ENTITY_ID: self.IDP_ENTITY_ID,
            }
        )
        relay_state_url = f"https://app.example.com/logout?{relay_state}"
        qs = urlencode({"SAMLResponse": "encoded", "RelayState": relay_state_url})

        with controller_fixture.app.test_request_context(f"/saml/logout_callback?{qs}"):
            with patch(
                "palace.manager.integration.patron_auth.saml.controller.url_for",
                return_value=self.CALLBACK_URL,
            ):
                result = controller.saml_logout_callback(
                    request, controller_fixture.db.session
                )

        assert result.status_code == 302
        assert "logout_status=success" in result.location
        mock_auth_manager.finish_logout.assert_called_once_with(
            controller_fixture.db.session,
            self.IDP_ENTITY_ID,
            self.CALLBACK_URL,
        )

    def test_saml_logout_callback_missing_relay_state(
        self, controller_fixture: ControllerFixture
    ):
        """Missing RelayState returns ProblemDetail."""
        controller, *_ = self._make_controller_and_mocks(controller_fixture)

        with controller_fixture.app.test_request_context(
            "/saml/logout_callback?SAMLResponse=encoded"
        ):
            result = controller.saml_logout_callback(
                request, controller_fixture.db.session
            )

        assert isinstance(result, ProblemDetail)
        assert result.uri == SAML_INVALID_RESPONSE.uri

    def test_saml_logout_callback_post_binding(
        self, controller_fixture: ControllerFixture
    ):
        """Callback via HTTP-POST also works (RelayState in form body)."""
        controller, mock_provider, mock_auth_manager, mock_credential_manager = (
            self._make_controller_and_mocks(controller_fixture)
        )

        relay_state = urlencode(
            {
                SAMLController.LIBRARY_SHORT_NAME: "default",
                SAMLController.PROVIDER_NAME: self.PROVIDER_NAME,
                SAMLController.IDP_ENTITY_ID: self.IDP_ENTITY_ID,
            }
        )
        relay_state_url = f"https://app.example.com/logout?{relay_state}"

        with controller_fixture.app.test_request_context(
            "/saml/logout_callback",
            method="POST",
            data={"SAMLResponse": "encoded", "RelayState": relay_state_url},
        ):
            with patch(
                "palace.manager.integration.patron_auth.saml.controller.url_for",
                return_value=self.CALLBACK_URL,
            ):
                result = controller.saml_logout_callback(
                    request, controller_fixture.db.session
                )

        assert result.status_code == 302
        assert "logout_status=success" in result.location

    def test_saml_logout_callback_finish_logout_fails(
        self, controller_fixture: ControllerFixture
    ):
        """SLO validation failure redirects with error."""
        controller, mock_provider, mock_auth_manager, mock_credential_manager = (
            self._make_controller_and_mocks(controller_fixture)
        )
        mock_auth_manager.finish_logout.return_value = SAML_GENERIC_ERROR.detailed(
            "bad response"
        )

        relay_state = urlencode(
            {
                SAMLController.LIBRARY_SHORT_NAME: "default",
                SAMLController.PROVIDER_NAME: self.PROVIDER_NAME,
                SAMLController.IDP_ENTITY_ID: self.IDP_ENTITY_ID,
            }
        )
        relay_state_url = f"https://app.example.com/logout?{relay_state}"
        qs = urlencode({"SAMLResponse": "encoded", "RelayState": relay_state_url})

        with controller_fixture.app.test_request_context(f"/saml/logout_callback?{qs}"):
            with patch(
                "palace.manager.integration.patron_auth.saml.controller.url_for",
                return_value=self.CALLBACK_URL,
            ):
                result = controller.saml_logout_callback(
                    request, controller_fixture.db.session
                )

        assert result.status_code == 302
        assert "error" in result.location
