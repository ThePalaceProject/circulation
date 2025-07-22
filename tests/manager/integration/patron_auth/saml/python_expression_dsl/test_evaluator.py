import pytest

from palace.manager.integration.patron_auth.saml.python_expression_dsl.evaluator import (
    DSLEvaluationError,
    DSLEvaluationVisitor,
    DSLEvaluator,
)
from palace.manager.integration.patron_auth.saml.python_expression_dsl.parser import (
    DSLParseError,
    DSLParser,
)


class Subject:
    """Dummy object designed for testing DSLEvaluator."""

    def __init__(self, attributes):
        """Initialize a new instance of Subject.

        :param attributes: List of attributes
        :type attributes: List[str]
        """
        self._attributes = attributes

    @property
    def attributes(self):
        """Return the list of attributes.

        :return: List of attributes
        :rtype: List
        """
        return self._attributes

    def get_attribute_value(self, index):
        """Dummy method to test method invocation.

        :param index: Attribute's index
        :type index: int

        :return: Attribute's value
        :rtype: Any
        """
        return self._attributes[index]


class TestDSLEvaluator:
    @pytest.mark.parametrize(
        "expression,expected_result,context,safe_classes,expected_exception",
        [
            pytest.param(
                "?", None, None, None, DSLParseError, id="incorrect_expression"
            ),
            pytest.param("9", 9, None, None, None, id="numeric_literal"),
            pytest.param("9.5", 9.5, None, None, None, id="numeric_float_literal"),
            pytest.param(
                "foo", None, None, None, DSLEvaluationError, id="unknown_identifier"
            ),
            pytest.param("foo", 9, {"foo": 9}, None, None, id="known_identifier"),
            pytest.param(
                "foo.bar",
                None,
                {"foo": 9},
                None,
                DSLEvaluationError,
                id="unknown_nested_identifier",
            ),
            pytest.param(
                "foo.bar",
                9,
                {"foo": {"bar": 9}},
                None,
                None,
                id="known_nested_identifier",
            ),
            pytest.param(
                "foo.bar.baz",
                9,
                {"foo": {"bar": {"baz": 9}}},
                None,
                None,
                id="known_nested_identifier",
            ),
            pytest.param(
                "foo.bar[0].baz",
                9,
                {"foo": {"bar": [{"baz": 9}]}},
                None,
                None,
                id="known_nested_identifier",
            ),
            pytest.param(
                "'eresources' in subject.attributes",
                True,
                {"subject": Subject(["eresources"])},
                None,
                None,
                id="identifier_pointing_to_the_object",
            ),
            pytest.param("-9", -9, None, None, None, id="simple_negation"),
            pytest.param(
                "-(9)",
                -(9),
                None,
                None,
                None,
                id="simple_parenthesized_expression_negation",
            ),
            pytest.param(
                "-(9 + 3)",
                -(9 + 3),
                None,
                None,
                None,
                id="parenthesized_expression_negation",
            ),
            pytest.param(
                "-(arr[1])",
                -12,
                {"arr": [1, 12, 3]},
                None,
                None,
                id="slice_expression_negation",
            ),
            pytest.param(
                "9 + 3", 9 + 3, None, None, None, id="addition_with_two_operands"
            ),
            pytest.param(
                "9 + 3 + 3",
                9 + 3 + 3,
                None,
                None,
                None,
                id="addition_with_three_operands",
            ),
            pytest.param(
                "9 + 3 + 3 + 3",
                9 + 3 + 3 + 3,
                None,
                None,
                None,
                id="addition_with_four_operands",
            ),
            pytest.param(
                "9 - 3", 9 - 3, None, None, None, id="subtraction_with_two_operands"
            ),
            pytest.param(
                "9 * 3", 9 * 3, None, None, None, id="multiplication_with_two_operands"
            ),
            pytest.param(
                "9 / 3", 9 / 3, None, None, None, id="division_with_two_operands"
            ),
            pytest.param(
                "9 / 4",
                9.0 / 4.0,
                None,
                None,
                None,
                id="division_with_two_operands_and_remainder",
            ),
            pytest.param(
                "9 ** 3",
                9**3,
                None,
                None,
                None,
                id="exponentiation_with_two_operands",
            ),
            pytest.param(
                "2 ** 3 ** 3",
                2**3**3,
                None,
                None,
                None,
                id="exponentiation_with_three_operands",
            ),
            pytest.param(
                "(a + b) + c == a + (b + c)",
                True,
                {"a": 9, "b": 3, "c": 3},
                None,
                None,
                id="associative_law_for_addition",
            ),
            pytest.param(
                "(a * b) * c == a * (b * c)",
                True,
                {"a": 9, "b": 3, "c": 3},
                None,
                None,
                id="associative_law_for_multiplication",
            ),
            pytest.param(
                "a + b == b + a",
                True,
                {"a": 9, "b": 3},
                None,
                None,
                id="commutative_law_for_addition",
            ),
            pytest.param(
                "a * b == b * a",
                True,
                {"a": 9, "b": 3},
                None,
                None,
                id="commutative_law_for_multiplication",
            ),
            pytest.param(
                "a * (b + c) == a * b + a * c",
                True,
                {"a": 9, "b": 3, "c": 3},
                None,
                None,
                id="distributive_law",
            ),
            pytest.param("9 < 3", 9 < 3, None, None, None, id="less_comparison"),
            pytest.param(
                "3 <= 3", 3 <= 3, None, None, None, id="less_or_equal_comparison"
            ),
            pytest.param("9 > 3", 9 > 3, None, None, None, id="greater_comparison"),
            pytest.param(
                "3 >= 2", 3 >= 2, None, None, None, id="greater_or_equal_comparison"
            ),
            pytest.param(
                "3 in list", True, {"list": [1, 2, 3]}, None, None, id="in_operator"
            ),
            pytest.param("not 9 < 3", not 9 < 3, None, None, None, id="inversion"),
            pytest.param(
                "not not 9 < 3", not not 9 < 3, None, None, None, id="double_inversion"
            ),
            pytest.param(
                "not not not 9 < 3",
                not not not 9 < 3,
                None,
                None,
                None,
                id="triple_inversion",
            ),
            pytest.param(
                "9 == 9 and 3 == 3",
                9 == 9 and 3 == 3,
                None,
                None,
                None,
                id="conjunction",
            ),
            pytest.param(
                "9 == 3 or 3 == 3", 9 == 3 or 3 == 3, None, None, None, id="disjunction"
            ),
            pytest.param(
                "(9 + 3)",
                (9 + 3),
                None,
                None,
                None,
                id="simple_parenthesized_expression",
            ),
            pytest.param(
                "2 * (9 + 3) * 2",
                2 * (9 + 3) * 2,
                None,
                None,
                None,
                id="arithmetic_parenthesized_expression",
            ),
            pytest.param(
                "arr[1] == 12",
                True,
                {"arr": [1, 12, 3]},
                None,
                None,
                id="slice_expression",
            ),
            pytest.param(
                "arr[1] + arr[2]",
                15,
                {"arr": [1, 12, 3]},
                None,
                None,
                id="complex_slice_expression",
            ),
            pytest.param(
                "string.upper()",
                "HELLO WORLD",
                {"string": "Hello World"},
                None,
                None,
                id="method_call",
            ),
            pytest.param(
                "min(1, 2)", min(1, 2), None, None, None, id="builtin_function_call"
            ),
            pytest.param(
                "subject.get_attribute_value(0)",
                "eresources",
                {"subject": Subject(["eresources"])},
                None,
                DSLEvaluationError,
                id="unsafe_class_method_call",
            ),
            pytest.param(
                "subject.get_attribute_value(0)",
                "eresources",
                {"subject": Subject(["eresources"])},
                [Subject],
                None,
                id="safe_class_method_call",
            ),
            pytest.param(
                "get_attribute_value(0)",
                "eresources",
                Subject(["eresources"]),
                [Subject],
                None,
                id="safe_class_method_call_with_direct_context",
            ),
        ],
    )
    def test(
        self,
        expression,
        expected_result,
        context,
        safe_classes,
        expected_exception,
    ):
        # Arrange
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)

        if safe_classes is None:
            safe_classes = []

        # Act
        if expected_exception:
            with pytest.raises(expected_exception):
                evaluator.evaluate(expression, context, safe_classes)
        else:
            result = evaluator.evaluate(expression, context, safe_classes)

            # Assert
            assert expected_result == result
