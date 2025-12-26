import logging
import re

import pytest

from palace.manager.integration.patron_auth.saml.metadata.model import (
    SAMLAttribute,
    SAMLAttributeStatement,
    SAMLAttributeType,
    SAMLNameID,
    SAMLNameIDFormat,
    SAMLSubject,
    SAMLSubjectPatronIDExtractor,
)
from tests.mocks import saml_strings


class TestAttributeStatement:
    def test_init_accepts_list_of_attributes(self):
        # Arrange
        attributes = [
            SAMLAttribute(SAMLAttributeType.uid.name, [12345]),
            SAMLAttribute(SAMLAttributeType.eduPersonTargetedID.name, [12345]),
        ]

        # Act
        attribute_statement = SAMLAttributeStatement(attributes)

        # Assert
        assert True == (SAMLAttributeType.uid.name in attribute_statement.attributes)
        assert (
            attributes[0].values
            == attribute_statement.attributes[SAMLAttributeType.uid.name].values
        )

        assert True == (
            SAMLAttributeType.eduPersonTargetedID.name in attribute_statement.attributes
        )
        assert (
            attributes[1].values
            == attribute_statement.attributes[
                SAMLAttributeType.eduPersonTargetedID.name
            ].values
        )


class TestSAMLSubjectPatronIDExtractor:
    @pytest.mark.parametrize(
        "subject,expected_patron_id,use_name_id,patron_id_attributes,patron_id_regular_expression,expected_source_attribute",
        [
            pytest.param(
                SAMLSubject("http://idp.example.com", None, None),
                None,
                True,
                None,
                None,
                None,
                id="no-patron-id",
            ),
            pytest.param(
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonTargetedID.name,
                                values=["2"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                "3",
                True,
                None,
                None,
                "eduPersonUniqueId",
                id="edupersonuniqueid-priority",
            ),
            pytest.param(
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["2"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["3"]
                            ),
                        ]
                    ),
                ),
                "2",
                True,
                None,
                None,
                "eduPersonUniqueId",
                id="edupersonuniqueid",
            ),
            pytest.param(
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", "1"),
                    SAMLAttributeStatement(
                        [SAMLAttribute(name=SAMLAttributeType.uid.name, values=["2"])]
                    ),
                ),
                "2",
                True,
                None,
                None,
                "uid",
                id="uid",
            ),
            pytest.param(
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonPrincipalName.name,
                                values=["2"],
                            )
                        ]
                    ),
                ),
                "1",
                True,
                None,
                None,
                "NameID",
                id="name-id",
            ),
            pytest.param(
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonPrincipalName.name,
                                values=["2"],
                            )
                        ]
                    ),
                ),
                None,
                False,
                None,
                None,
                None,
                id="name-id-disabled",
            ),
            pytest.param(
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonTargetedID.name,
                                values=["2"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                "4",
                False,
                [SAMLAttributeType.uid.name],
                None,
                "uid",
                id="uid-configured",
            ),
            pytest.param(
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonTargetedID.name,
                                values=[None],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                "4",
                True,
                [
                    SAMLAttributeType.eduPersonTargetedID.name,
                    SAMLAttributeType.uid.name,
                ],
                None,
                "uid",
                id="uid-second-match",
            ),
            pytest.param(
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonTargetedID.name,
                                values=["2"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                None,
                False,
                [SAMLAttributeType.givenName.name],
                None,
                None,
                id="no-match",
            ),
            pytest.param(
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonTargetedID.name,
                                values=["2"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                "1",
                True,
                [SAMLAttributeType.givenName.name],
                None,
                "NameID",
                id="no-match-fallback-name-id",
            ),
            pytest.param(
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonPrincipalName.name,
                                values=["patron@university.org"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                "patron",
                False,
                [
                    SAMLAttributeType.eduPersonPrincipalName.name,
                    SAMLAttributeType.mail.name,
                ],
                saml_strings.PATRON_ID_REGULAR_EXPRESSION_ORG,
                "eduPersonPrincipalName",
                id="regex-match",
            ),
            pytest.param(
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonPrincipalName.name,
                                values=["patron@university.org"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                "patron",
                False,
                [
                    SAMLAttributeType.eduPersonUniqueId.name,
                    SAMLAttributeType.eduPersonPrincipalName.name,
                    SAMLAttributeType.mail.name,
                ],
                saml_strings.PATRON_ID_REGULAR_EXPRESSION_ORG,
                "eduPersonPrincipalName",
                id="regex-second-match",
            ),
            pytest.param(
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonPrincipalName.name,
                                values=["pątron@university.org"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                "pątron",
                False,
                [
                    SAMLAttributeType.eduPersonPrincipalName.name,
                    SAMLAttributeType.mail.name,
                ],
                saml_strings.PATRON_ID_REGULAR_EXPRESSION_ORG,
                "eduPersonPrincipalName",
                id="regex-unicode",
            ),
            pytest.param(
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonPrincipalName.name,
                                values=["patron@university.org"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                None,
                False,
                [
                    SAMLAttributeType.eduPersonPrincipalName.name,
                    SAMLAttributeType.mail.name,
                ],
                saml_strings.PATRON_ID_REGULAR_EXPRESSION_COM,
                None,
                id="regex-no-match",
            ),
            pytest.param(
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(
                        SAMLNameIDFormat.UNSPECIFIED.value,
                        "",
                        "",
                        "patron@university.com",
                    ),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonPrincipalName.name,
                                values=["patron@university.org"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonUniqueId.name,
                                values=["3"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["4"]
                            ),
                        ]
                    ),
                ),
                "patron",
                True,
                [
                    SAMLAttributeType.eduPersonPrincipalName.name,
                    SAMLAttributeType.mail.name,
                ],
                saml_strings.PATRON_ID_REGULAR_EXPRESSION_COM,
                "NameID",
                id="regex-fallback-name-id",
            ),
            pytest.param(
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.pairwiseId.name,
                                values=["abc123def456@university.org"],
                            )
                        ]
                    ),
                ),
                "abc123def456@university.org",
                False,
                [SAMLAttributeType.pairwiseId.name],
                None,
                "pairwiseId",
                id="subject-with-pairwise-id-attribute",
            ),
            pytest.param(
                SAMLSubject(
                    "http://idp.example.com",
                    SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", "1"),
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.pairwiseId.name,
                                values=["xyz789abc@university.org"],
                            ),
                            SAMLAttribute(
                                name=SAMLAttributeType.uid.name, values=["2"]
                            ),
                        ]
                    ),
                ),
                "xyz789abc",
                False,
                [SAMLAttributeType.pairwiseId.name],
                saml_strings.PATRON_ID_REGULAR_EXPRESSION_ORG,
                "pairwiseId",
                id="pairwise-id-with-regex-extraction",
            ),
        ],
    )
    def test(
        self,
        subject: SAMLSubject,
        expected_patron_id: str | None,
        use_name_id: bool,
        patron_id_attributes: list[str] | None,
        patron_id_regular_expression: re.Pattern | None,
        expected_source_attribute: str | None,
        caplog: pytest.LogCaptureFixture,
    ):
        """Make sure that SAMLSubjectUIDExtractor correctly extracts a unique patron ID from the SAML subject.

        :param expected_patron_id: Expected patron ID
        :param use_name_id: Boolean value indicating whether SAMLSubjectUIDExtractor
            is allowed to search for patron IDs in NameID
        :param patron_id_attributes: List of SAML attribute names that are used by
            SAMLSubjectUIDExtractor to search for a patron ID
        :param patron_id_regular_expression: Regular expression used to extract a patron ID from SAML attributes
        :param expected_source_attribute: Expected source attribute name in the log message
        """
        if expected_patron_id and expected_source_attribute:
            expected_message = (
                f"Extracted unique patron ID '{expected_patron_id}' "
                f"from attribute '{expected_source_attribute}' in "
            )
        else:
            expected_message = "Failed to extract a unique patron ID from "

        expect_log_level = logging.INFO if expected_patron_id else logging.ERROR

        # Arrange
        caplog.set_level(expect_log_level)
        extractor = SAMLSubjectPatronIDExtractor(
            use_name_id, patron_id_attributes, patron_id_regular_expression
        )

        # Act
        patron_id = extractor.extract(subject)

        # Assert
        assert expected_patron_id == patron_id
        assert expected_message in caplog.text
