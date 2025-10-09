import pytest

from palace.manager.api.admin.problem_details import (
    INCOMPLETE_CONFIGURATION,
    INVALID_CONFIGURATION_OPTION,
)
from palace.manager.integration.patron_auth.saml.configuration.model import (
    SAMLWebSSOAuthSettings,
)
from palace.manager.integration.patron_auth.saml.configuration.problem_details import (
    SAML_INCORRECT_METADATA,
    SAML_INCORRECT_PATRON_ID_REGULAR_EXPRESSION,
)
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException
from tests.fixtures.test_utils import MonkeyPatchEnvFixture
from tests.mocks import saml_strings


class TestSAMLSettingsValidator:
    @pytest.mark.parametrize(
        "env_sp_xml_metadata,sp_xml_metadata,idp_xml_metadata,patron_id_regular_expression,expected_validation_result",
        [
            pytest.param(
                None,
                None,
                None,
                None,
                INCOMPLETE_CONFIGURATION,
                id="missing_sp_metadata_and_missing_idp_metadata",
            ),
            pytest.param(
                saml_strings.INCORRECT_XML_WITH_ONE_SP_METADATA_WITHOUT_ACS_SERVICE,
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.CORRECT_XML_WITH_IDP_1,
                None,
                None,
                id="env-sp-ignored-when-submitted-present",
            ),
            pytest.param(
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.INCORRECT_XML_WITH_ONE_SP_METADATA_WITHOUT_ACS_SERVICE,
                saml_strings.CORRECT_XML_WITH_IDP_1,
                None,
                SAML_INCORRECT_METADATA,
                id="submitted-sp-overrides-env-sp",
            ),
            pytest.param(
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                None,
                saml_strings.CORRECT_XML_WITH_IDP_1,
                None,
                None,
                id="env-sp-metadata-checked-if-submitted-missing",
            ),
            pytest.param(
                None,
                saml_strings.INCORRECT_XML,
                saml_strings.INCORRECT_XML,
                None,
                INCOMPLETE_CONFIGURATION,
                id="empty_sp_metadata_and_empty_idp_metadata",
            ),
            pytest.param(
                saml_strings.INCORRECT_XML_WITH_ONE_SP_METADATA_WITHOUT_ACS_SERVICE,
                saml_strings.INCORRECT_XML_WITH_ONE_SP_METADATA_WITHOUT_ACS_SERVICE,
                saml_strings.INCORRECT_XML_WITH_ONE_IDP_METADATA_WITHOUT_SSO_SERVICE,
                None,
                SAML_INCORRECT_METADATA,
                id="incorrect_sp_metadata_and_incorrect_idp_metadata",
            ),
            pytest.param(
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.INCORRECT_XML_WITH_ONE_IDP_METADATA_WITHOUT_SSO_SERVICE,
                None,
                SAML_INCORRECT_METADATA,
                id="correct_sp_metadata_and_incorrect_idp_metadata",
            ),
            pytest.param(
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.CORRECT_XML_WITH_IDP_1,
                None,
                None,
                id="correct_sp_and_idp_metadata",
            ),
            pytest.param(
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                None,
                saml_strings.CORRECT_XML_WITH_IDP_1,
                None,
                None,
                id="correct_env_and_submitted_sp_and_idp_metadata",
            ),
            pytest.param(
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                None,
                saml_strings.CORRECT_XML_WITH_IDP_1,
                None,
                None,
                id="correct_submitted_sp_and_idp_metadata",
            ),
            pytest.param(
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.CORRECT_XML_WITH_IDP_1,
                r"(?P<patron_id>.+)@university\.org",
                None,
                id="correct_patron_id_regular_expression",
            ),
            pytest.param(
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.CORRECT_XML_WITH_IDP_1,
                r"(?P<patron>.+)@university\.org",
                SAML_INCORRECT_PATRON_ID_REGULAR_EXPRESSION,
                id="correct_patron_id_regular_expression_without_patron_id_named_group",
            ),
            pytest.param(
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.CORRECT_XML_WITH_IDP_1,
                r"[",
                INVALID_CONFIGURATION_OPTION,
                id="incorrect_patron_id_regular_expression",
            ),
        ],
    )
    def test_validate(
        self,
        monkeypatch_env: MonkeyPatchEnvFixture,
        env_sp_xml_metadata: str | None,
        sp_xml_metadata: str | None,
        idp_xml_metadata: str | None,
        patron_id_regular_expression: str | None,
        expected_validation_result: ProblemDetail | str | None,
    ):
        """Ensure that SAMLSettingsValidator correctly validates the input data.

        :param env_sp_xml_metadata: SP SAML metadata from the environment
        :param sp_xml_metadata: SP SAML metadata submitted as a setting
        :param idp_xml_metadata: IdP SAML metadata
        :param patron_id_regular_expression: Regular expression used to extract a unique patron ID from SAML attributes
        :param expected_validation_result: Expected result: ProblemDetail object if validation must fail, None otherwise
        """
        monkeypatch_env("PALACE_SAML_SP_METADATA", env_sp_xml_metadata)
        # Arrange
        submitted_settings = {
            "service_provider_xml_metadata": sp_xml_metadata,
            "non_federated_identity_provider_xml_metadata": idp_xml_metadata,
            "patron_id_regular_expression": patron_id_regular_expression,
        }

        if isinstance(expected_validation_result, ProblemDetail):
            with pytest.raises(ProblemDetailException) as exception:
                SAMLWebSSOAuthSettings(**submitted_settings)

            assert (
                expected_validation_result.status_code
                == exception.value.problem_detail.status_code
            )
            assert (
                expected_validation_result.title == exception.value.problem_detail.title
            )
            assert expected_validation_result.uri == exception.value.problem_detail.uri
        else:
            SAMLWebSSOAuthSettings(**submitted_settings)
