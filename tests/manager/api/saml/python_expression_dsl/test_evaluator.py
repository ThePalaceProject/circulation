import pytest

from palace.manager.api.saml.python_expression_dsl.evaluator import (
    DSLEvaluationError,
    DSLEvaluationVisitor,
    DSLEvaluator,
)
from palace.manager.api.saml.python_expression_dsl.parser import (
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
        "_,expression,expected_result,context,safe_classes,expected_exception",
        [
            ("incorrect_expression", "?", None, None, None, DSLParseError),
            ("numeric_literal", "9", 9, None, None, None),
            ("numeric_float_literal", "9.5", 9.5, None, None, None),
            ("unknown_identifier", "foo", None, None, None, DSLEvaluationError),
            ("known_identifier", "foo", 9, {"foo": 9}, None, None),
            (
                "unknown_nested_identifier",
                "foo.bar",
                None,
                {"foo": 9},
                None,
                DSLEvaluationError,
            ),
            ("known_nested_identifier", "foo.bar", 9, {"foo": {"bar": 9}}, None, None),
            (
                "known_nested_identifier",
                "foo.bar.baz",
                9,
                {"foo": {"bar": {"baz": 9}}},
                None,
                None,
            ),
            (
                "known_nested_identifier",
                "foo.bar[0].baz",
                9,
                {"foo": {"bar": [{"baz": 9}]}},
                None,
                None,
            ),
            (
                "identifier_pointing_to_the_object",
                "'eresources' in subject.attributes",
                True,
                {"subject": Subject(["eresources"])},
                None,
                None,
            ),
            ("simple_negation", "-9", -9, None, None, None),
            (
                "simple_parenthesized_expression_negation",
                "-(9)",
                -(9),
                None,
                None,
                None,
            ),
            (
                "parenthesized_expression_negation",
                "-(9 + 3)",
                -(9 + 3),
                None,
                None,
                None,
            ),
            (
                "slice_expression_negation",
                "-(arr[1])",
                -12,
                {"arr": [1, 12, 3]},
                None,
                None,
            ),
            ("addition_with_two_operands", "9 + 3", 9 + 3, None, None, None),
            ("addition_with_three_operands", "9 + 3 + 3", 9 + 3 + 3, None, None, None),
            (
                "addition_with_four_operands",
                "9 + 3 + 3 + 3",
                9 + 3 + 3 + 3,
                None,
                None,
                None,
            ),
            ("subtraction_with_two_operands", "9 - 3", 9 - 3, None, None, None),
            ("multiplication_with_two_operands", "9 * 3", 9 * 3, None, None, None),
            ("division_with_two_operands", "9 / 3", 9 / 3, None, None, None),
            (
                "division_with_two_operands_and_remainder",
                "9 / 4",
                9.0 / 4.0,
                None,
                None,
                None,
            ),
            ("exponentiation_with_two_operands", "9 ** 3", 9**3, None, None, None),
            (
                "exponentiation_with_three_operands",
                "2 ** 3 ** 3",
                2**3**3,
                None,
                None,
                None,
            ),
            (
                "associative_law_for_addition",
                "(a + b) + c == a + (b + c)",
                True,
                {"a": 9, "b": 3, "c": 3},
                None,
                None,
            ),
            (
                "associative_law_for_multiplication",
                "(a * b) * c == a * (b * c)",
                True,
                {"a": 9, "b": 3, "c": 3},
                None,
                None,
            ),
            (
                "commutative_law_for_addition",
                "a + b == b + a",
                True,
                {"a": 9, "b": 3},
                None,
                None,
            ),
            (
                "commutative_law_for_multiplication",
                "a * b == b * a",
                True,
                {"a": 9, "b": 3},
                None,
                None,
            ),
            (
                "distributive_law",
                "a * (b + c) == a * b + a * c",
                True,
                {"a": 9, "b": 3, "c": 3},
                None,
                None,
            ),
            ("less_comparison", "9 < 3", 9 < 3, None, None, None),
            ("less_or_equal_comparison", "3 <= 3", 3 <= 3, None, None, None),
            ("greater_comparison", "9 > 3", 9 > 3, None, None, None),
            ("greater_or_equal_comparison", "3 >= 2", 3 >= 2, None, None, None),
            ("in_operator", "3 in list", True, {"list": [1, 2, 3]}, None, None),
            ("inversion", "not 9 < 3", not 9 < 3, None, None, None),
            ("double_inversion", "not not 9 < 3", not not 9 < 3, None, None, None),
            (
                "triple_inversion",
                "not not not 9 < 3",
                not not not 9 < 3,
                None,
                None,
                None,
            ),
            ("conjunction", "9 == 9 and 3 == 3", 9 == 9 and 3 == 3, None, None, None),
            ("disjunction", "9 == 3 or 3 == 3", 9 == 3 or 3 == 3, None, None, None),
            ("simple_parenthesized_expression", "(9 + 3)", (9 + 3), None, None, None),
            (
                "arithmetic_parenthesized_expression",
                "2 * (9 + 3) * 2",
                2 * (9 + 3) * 2,
                None,
                None,
                None,
            ),
            ("slice_expression", "arr[1] == 12", True, {"arr": [1, 12, 3]}, None, None),
            (
                "complex_slice_expression",
                "arr[1] + arr[2]",
                15,
                {"arr": [1, 12, 3]},
                None,
                None,
            ),
            (
                "method_call",
                "string.upper()",
                "HELLO WORLD",
                {"string": "Hello World"},
                None,
                None,
            ),
            ("builtin_function_call", "min(1, 2)", min(1, 2), None, None, None),
            (
                "unsafe_class_method_call",
                "subject.get_attribute_value(0)",
                "eresources",
                {"subject": Subject(["eresources"])},
                None,
                DSLEvaluationError,
            ),
            (
                "safe_class_method_call",
                "subject.get_attribute_value(0)",
                "eresources",
                {"subject": Subject(["eresources"])},
                [Subject],
                None,
            ),
            (
                "safe_class_method_call_with_direct_context",
                "get_attribute_value(0)",
                "eresources",
                Subject(["eresources"]),
                [Subject],
                None,
            ),
        ],
    )
    def test(
        self,
        _,
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
