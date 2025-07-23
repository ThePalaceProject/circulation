import pytest

from palace.manager.integration.patron_auth.saml.metadata.filter import (
    SAMLSubjectFilter,
    SAMLSubjectFilterError,
)
from palace.manager.integration.patron_auth.saml.metadata.model import (
    SAMLAttribute,
    SAMLAttributeStatement,
    SAMLAttributeType,
    SAMLSubject,
)
from palace.manager.integration.patron_auth.saml.python_expression_dsl.evaluator import (
    DSLEvaluationVisitor,
    DSLEvaluator,
)
from palace.manager.integration.patron_auth.saml.python_expression_dsl.parser import (
    DSLParser,
)


class TestSAMLSubjectFilter:
    @pytest.mark.parametrize(
        "expression,subject,expected_result,expected_exception",
        [
            pytest.param(
                'subject.attribute_statement.attributes["eduPersonEntitlement"].values[0 == "urn:mace:nyu.edu:entl:lib:eresources"',
                SAMLSubject(
                    "http://idp.example.com",
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonEntitlement.name,
                                values=["urn:mace:nyu.edu:entl:lib:eresources"],
                            )
                        ]
                    ),
                ),
                None,
                SAMLSubjectFilterError,
                id="fails_in_the_case_of_syntax_error",
            ),
            pytest.param(
                'subject.attribute_statement.attributes["mail"].values[0] == "urn:mace:nyu.edu:entl:lib:eresources"',
                SAMLSubject(
                    "http://idp.example.com",
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonEntitlement.name,
                                values=["urn:mace:nyu.edu:entl:lib:eresources"],
                            )
                        ]
                    ),
                ),
                None,
                SAMLSubjectFilterError,
                id="fails_in_the_case_of_unknown_attribute",
            ),
            pytest.param(
                'attributes["eduPersonEntitlement"].values[0] == "urn:mace:nyu.edu:entl:lib:eresources"',
                SAMLSubject(
                    "http://idp.example.com",
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonEntitlement.name,
                                values=["urn:mace:nyu.edu:entl:lib:eresources"],
                            )
                        ]
                    ),
                ),
                None,
                SAMLSubjectFilterError,
                id="fails_when_subject_is_not_used",
            ),
            pytest.param(
                '"urn:mace:nyu.edu:entl:lib:eresources" == subject.attribute_statement.attributes["eduPersonEntitlement"].values[0]',
                SAMLSubject(
                    "http://idp.example.com",
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonEntitlement.name,
                                values=["urn:mace:nyu.edu:entl:lib:eresources"],
                            )
                        ]
                    ),
                ),
                True,
                None,
                id="can_filter_when_attribute_has_one_value",
            ),
            pytest.param(
                '"urn:mace:nyu.edu:entl:lib:eresources" in subject.attribute_statement.attributes["eduPersonEntitlement"].values',
                SAMLSubject(
                    "http://idp.example.com",
                    None,
                    SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.eduPersonEntitlement.name,
                                values=[
                                    "urn:mace:nyu.edu:entl:lib:eresources",
                                    "urn:mace:nyu.edu:entl:lib:books",
                                ],
                            )
                        ]
                    ),
                ),
                True,
                None,
                id="can_filter_when_attribute_has_multiple_values",
            ),
        ],
    )
    def test_execute(self, expression, subject, expected_result, expected_exception):
        # Arrange
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)

        # Act
        if expected_exception:
            with pytest.raises(expected_exception):
                subject_filter.execute(expression, subject)
        else:
            result = subject_filter.execute(expression, subject)

            # Assert
            assert expected_result == result

    @pytest.mark.parametrize(
        "expression,expected_exception",
        [
            pytest.param(
                'subject.attribute_statement.attributes["eduPersonEntitlement"].values[0 == "urn:mace:nyu.edu:entl:lib:eresources"',
                SAMLSubjectFilterError,
                id="fails_in_the_case_of_syntax_error",
            ),
            pytest.param(
                'attributes["eduPersonEntitlement"].values[0] == "urn:mace:nyu.edu:entl:lib:eresources"',
                SAMLSubjectFilterError,
                id="fails_when_subject_is_not_used",
            ),
            pytest.param(
                'subject.attribute_statement.attributes["urn:oid:1.3.6.1.4.1.5923.1.8"].values[0] == "urn:mace:nyu.edu:entl:lib:eresources"',
                None,
                id="can_filter_by_attribute_oid",
            ),
            pytest.param(
                '"urn:mace:nyu.edu:entl:lib:eresources" == subject.attribute_statement.attributes["eduPersonEntitlement"].values[0]',
                None,
                id="can_filter_when_attribute_has_one_value",
            ),
            pytest.param(
                '"urn:mace:nyu.edu:entl:lib:eresources" in subject.attribute_statement.attributes["eduPersonEntitlement"].values',
                None,
                id="can_filter_when_attribute_has_multiple_values",
            ),
        ],
    )
    def test_validate(self, expression, expected_exception):
        # Arrange
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)

        # Act
        if expected_exception:
            with pytest.raises(expected_exception):
                subject_filter.validate(expression)
        else:
            subject_filter.validate(expression)
