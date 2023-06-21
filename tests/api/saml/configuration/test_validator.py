import pytest

from api.admin.problem_details import (
    INCOMPLETE_CONFIGURATION,
    INVALID_CONFIGURATION_OPTION,
)
from api.saml.configuration.model import SAMLWebSSOAuthSettings
from api.saml.configuration.problem_details import (
    SAML_INCORRECT_METADATA,
    SAML_INCORRECT_PATRON_ID_REGULAR_EXPRESSION,
)
from core.util.problem_detail import ProblemError
from tests.api.saml import saml_strings


class TestSAMLSettingsValidator:
    @pytest.mark.parametrize(
        "_,sp_xml_metadata,idp_xml_metadata,patron_id_regular_expression,expected_validation_result",
        [
            (
                "missing_sp_metadata_and_missing_idp_metadata",
                None,
                None,
                None,
                INCOMPLETE_CONFIGURATION,
            ),
            (
                "empty_sp_metadata_and_empty_idp_metadata",
                saml_strings.INCORRECT_XML,
                saml_strings.INCORRECT_XML,
                None,
                INCOMPLETE_CONFIGURATION,
            ),
            (
                "incorrect_sp_metadata_and_incorrect_idp_metadata",
                saml_strings.INCORRECT_XML_WITH_ONE_SP_METADATA_WITHOUT_ACS_SERVICE,
                saml_strings.INCORRECT_XML_WITH_ONE_IDP_METADATA_WITHOUT_SSO_SERVICE,
                None,
                SAML_INCORRECT_METADATA,
            ),
            (
                "correct_sp_metadata_and_incorrect_idp_metadata",
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.INCORRECT_XML_WITH_ONE_IDP_METADATA_WITHOUT_SSO_SERVICE,
                None,
                SAML_INCORRECT_METADATA,
            ),
            (
                "correct_sp_and_idp_metadata",
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.CORRECT_XML_WITH_IDP_1,
                None,
                None,
            ),
            (
                "correct_patron_id_regular_expression",
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.CORRECT_XML_WITH_IDP_1,
                r"(?P<patron_id>.+)@university\.org",
                None,
            ),
            (
                "correct_patron_id_regular_expression_without_patron_id_named_group",
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.CORRECT_XML_WITH_IDP_1,
                r"(?P<patron>.+)@university\.org",
                SAML_INCORRECT_PATRON_ID_REGULAR_EXPRESSION,
            ),
            (
                "incorrect_patron_id_regular_expression",
                saml_strings.CORRECT_XML_WITH_ONE_SP,
                saml_strings.CORRECT_XML_WITH_IDP_1,
                r"[",
                INVALID_CONFIGURATION_OPTION,
            ),
        ],
    )
    def test_validate(
        self,
        _,
        sp_xml_metadata,
        idp_xml_metadata,
        patron_id_regular_expression,
        expected_validation_result,
    ):
        """Ensure that SAMLSettingsValidator correctly validates the input data.

        :param sp_xml_metadata: SP SAML metadata
        :type sp_xml_metadata: str

        :param idp_xml_metadata: IdP SAML metadata
        :type idp_xml_metadata: str

        :param patron_id_regular_expression: Regular expression used to extract a unique patron ID from SAML attributes
        :type patron_id_regular_expression: str

        :param expected_validation_result: Expected result: ProblemDetail object if validation must fail, None otherwise
        :type expected_validation_result: Optional[ProblemDetail]
        """
        # Arrange
        submitted_settings = {}

        if sp_xml_metadata is not None:
            submitted_settings["service_provider_xml_metadata"] = sp_xml_metadata
        if idp_xml_metadata is not None:
            submitted_settings[
                "non_federated_identity_provider_xml_metadata"
            ] = idp_xml_metadata
        if patron_id_regular_expression is not None:
            submitted_settings[
                "patron_id_regular_expression"
            ] = patron_id_regular_expression

        if expected_validation_result is not None:
            with pytest.raises(ProblemError) as exception:
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
