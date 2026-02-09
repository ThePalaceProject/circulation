import re

from pyparsing import (
    Forward,
    Group,
    Literal,
    ParseException,
    QuotedString,
    Regex,
    Suppress,
    Word,
    ZeroOrMore,
    alphanums,
    alphas,
)

from palace.manager.core.exceptions import BasePalaceException
from palace.manager.integration.patron_auth.saml.python_expression_dsl.ast import (
    Node,
    Operator,
)
from palace.manager.integration.patron_auth.saml.python_expression_dsl.util import (
    _parse_binary_arithmetic_expression,
    _parse_binary_boolean_expression,
    _parse_comparison_expression,
    _parse_dot_expression,
    _parse_function_call_expression,
    _parse_identifier,
    _parse_number,
    _parse_parenthesized_expression,
    _parse_slice_operation,
    _parse_string,
    _parse_unary_arithmetic_expression,
    _parse_unary_boolean_expression,
)


class DSLParseError(BasePalaceException):
    """Raised when expression has an incorrect format."""


class DSLParser:
    """Parses expressions into AST objects."""

    PARSE_ERROR_MESSAGE_REGEX = re.compile(r"found\s+('.+')\s+\(at\s+char\s+(\d+)\)")
    DEFAULT_ERROR_MESSAGE = "Could not parse the expression"

    # Auxiliary tokens
    LEFT_PAREN, RIGHT_PAREN = map(Suppress, "()")
    LEFT_BRACKET, RIGHT_BRACKET = map(Suppress, "[]")
    COMMA = Suppress(",")
    FULL_STOP = Suppress(".")

    # Unary arithmetic operators
    NEGATION_OPERATOR = Literal("-").set_parse_action(lambda _: Operator.NEGATION)

    # Binary additive arithmetic operators
    ADDITION_OPERATOR = Literal("+").set_parse_action(lambda _: Operator.ADDITION)
    SUBTRACTION_OPERATOR = Literal("-").set_parse_action(lambda _: Operator.SUBTRACTION)
    ADDITIVE_OPERATOR = ADDITION_OPERATOR | SUBTRACTION_OPERATOR

    # Binary multiplicative arithmetic operators
    MULTIPLICATION_OPERATOR = Literal("*").set_parse_action(
        lambda _: Operator.MULTIPLICATION
    )
    DIVISION_OPERATOR = Literal("/").set_parse_action(lambda _: Operator.DIVISION)
    MULTIPLICATIVE_OPERATOR = MULTIPLICATION_OPERATOR | DIVISION_OPERATOR

    # Power operator
    POWER_OPERATOR = Literal("**").set_parse_action(lambda _: Operator.EXPONENTIATION)

    # Comparison operators
    EQUAL_OPERATOR = Literal("==").set_parse_action(lambda _: Operator.EQUAL)
    NOT_EQUAL_OPERATOR = Literal("!=").set_parse_action(lambda _: Operator.NOT_EQUAL)
    GREATER_OPERATOR = Literal(">").set_parse_action(lambda _: Operator.GREATER)
    GREATER_OR_EQUAL_OPERATOR = Literal(">=").set_parse_action(
        lambda _: Operator.GREATER_OR_EQUAL
    )
    LESS_OPERATOR = Literal("<").set_parse_action(lambda _: Operator.LESS)
    LESS_OR_EQUAL_OPERATOR = Literal("<=").set_parse_action(
        lambda _: Operator.LESS_OR_EQUAL
    )
    IN_OPERATOR = Literal("in").set_parse_action(lambda _: Operator.IN)
    COMPARISON_OPERATOR = (
        EQUAL_OPERATOR
        | NOT_EQUAL_OPERATOR
        | GREATER_OR_EQUAL_OPERATOR
        | GREATER_OPERATOR
        | LESS_OR_EQUAL_OPERATOR
        | LESS_OPERATOR
        | IN_OPERATOR
    )

    NUMBER = Regex(r"[+-]?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?").set_parse_action(
        _parse_number
    )
    IDENTIFIER = Word(alphas, alphanums + "_$").set_parse_action(_parse_identifier)
    STRING = (QuotedString("'") | QuotedString('"')).set_parse_action(_parse_string)

    # Unary boolean operator
    INVERSION_OPERATOR = Literal("not").set_parse_action(lambda _: Operator.INVERSION)

    # Binary boolean operators
    CONJUNCTION_OPERATOR = Literal("and").set_parse_action(
        lambda _: Operator.CONJUNCTION
    )
    DISJUNCTION_OPERATOR = Literal("or").set_parse_action(
        lambda _: Operator.DISJUNCTION
    )

    arithmetic_expression = Forward()

    comparison_expression = (
        arithmetic_expression + ZeroOrMore(COMPARISON_OPERATOR + arithmetic_expression)
    ).set_parse_action(_parse_comparison_expression)

    inversion_expression = (
        ZeroOrMore(INVERSION_OPERATOR) + comparison_expression
    ).set_parse_action(_parse_unary_boolean_expression)
    conjunction_expression = (
        inversion_expression + ZeroOrMore(CONJUNCTION_OPERATOR + inversion_expression)
    ).set_parse_action(_parse_binary_boolean_expression)
    disjunction_expression = (
        conjunction_expression
        + ZeroOrMore(DISJUNCTION_OPERATOR + conjunction_expression)
    ).set_parse_action(_parse_binary_boolean_expression)

    expression = disjunction_expression

    dot_expression = Group(
        IDENTIFIER + ZeroOrMore(FULL_STOP + expression)
    ).set_parse_action(_parse_dot_expression)

    parenthesized_expression = Group(
        LEFT_PAREN + expression + RIGHT_PAREN
    ).set_parse_action(_parse_parenthesized_expression)

    slice = expression
    slice_expression = Group(
        IDENTIFIER + LEFT_BRACKET + slice + RIGHT_BRACKET
    ).set_parse_action(_parse_slice_operation)

    function_call_arguments = ZeroOrMore(expression + ZeroOrMore(COMMA + expression))
    function_call_expression = Group(
        IDENTIFIER + LEFT_PAREN + function_call_arguments + RIGHT_PAREN
    ).set_parse_action(_parse_function_call_expression)

    atom = (
        ZeroOrMore(NEGATION_OPERATOR)
        + (
            NUMBER
            | STRING
            | slice_expression
            | parenthesized_expression
            | function_call_expression
            | dot_expression
            | IDENTIFIER
        )
    ).set_parse_action(_parse_unary_arithmetic_expression)

    factor = Forward()
    factor << (atom + ZeroOrMore(POWER_OPERATOR + factor)).set_parse_action(
        _parse_binary_arithmetic_expression
    )
    term = (factor + ZeroOrMore(MULTIPLICATIVE_OPERATOR + factor)).set_parse_action(
        _parse_binary_arithmetic_expression
    )
    arithmetic_expression << (
        term + ZeroOrMore(ADDITIVE_OPERATOR + term)
    ).set_parse_action(_parse_binary_arithmetic_expression)

    def _parse_error_message(self, parse_exception: ParseException) -> str:
        """Transform the standard error description into a readable concise message.

        :param parse_exception: Exception thrown by pyparsing

        :return: Error message
        """
        error_message = str(parse_exception)
        match = self.PARSE_ERROR_MESSAGE_REGEX.search(error_message)

        if not match:
            return self.DEFAULT_ERROR_MESSAGE

        found = match.group(1).strip("'")
        position = match.group(2)

        return f"Unexpected symbol '{found}' at position {position}"

    def parse(self, expression: str) -> Node:
        """Parse the expression and transform it into AST.

        :param expression: String containing the expression

        :return: AST node
        """
        try:
            results = self.expression.parse_string(expression, parse_all=True)

            return results[0]  # type: ignore[no-any-return]
        except ParseException as exception:
            error_message = self._parse_error_message(exception)

            raise DSLParseError(error_message) from exception
