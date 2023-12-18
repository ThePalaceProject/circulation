from typing import TypeVar

from pyparsing import ParseResults

from core.python_expression_dsl.ast import (
    BinaryArithmeticExpression,
    BinaryBooleanExpression,
    BinaryExpression,
    ComparisonExpression,
    DotExpression,
    Expression,
    FunctionCallExpression,
    Identifier,
    Number,
    SliceExpression,
    String,
    UnaryArithmeticExpression,
    UnaryBooleanExpression,
    UnaryExpression,
)
from core.util import chunks

UE = TypeVar("UE", bound=UnaryExpression)
BE = TypeVar("BE", bound=BinaryExpression)


def _parse_identifier(tokens: ParseResults) -> Identifier:
    """Transform the token into an Identifier node.

    :param tokens: ParseResults objects

    :return: Identifier node
    """
    return Identifier(tokens[0])


def _parse_string(tokens: ParseResults) -> String:
    """Transform the token into a String node.

    :param tokens: ParseResults objects

    :return: Identifier node
    """
    return String(tokens[0])


def _parse_number(tokens: ParseResults) -> Number:
    """Transform the token into a Number node.

    :param tokens: ParseResults objects

    :return: Number node
    """
    return Number(tokens[0])


def _parse_unary_expression(
    expression_type: type[UE], tokens: ParseResults
) -> UE | None:
    """Transform the token into an unary expression.

    :param tokens: ParseResults objects

    :return: UnaryExpression node
    """
    if len(tokens) >= 2:
        token_list = list(reversed(tokens))
        argument = token_list[0]
        operator_type = token_list[1]
        expression = expression_type(operator_type, argument)

        for tokens_chunk in chunks(token_list, 1, 2):
            operator_type = tokens_chunk[0]
            expression = expression_type(operator_type, expression)

        return expression
    else:
        return None


def _parse_unary_arithmetic_expression(
    tokens: ParseResults,
) -> UnaryArithmeticExpression | None:
    """Transform the token into an UnaryArithmeticExpression node.

    :param tokens: ParseResults objects

    :return: UnaryArithmeticExpression node
    """
    return _parse_unary_expression(UnaryArithmeticExpression, tokens)


def _parse_unary_boolean_expression(
    tokens: ParseResults,
) -> UnaryBooleanExpression | None:
    """Transform the token into an UnaryBooleanExpression node.

    :param tokens: ParseResults objects

    :return: UnaryBooleanExpression node
    """
    return _parse_unary_expression(UnaryBooleanExpression, tokens)


def _parse_binary_expression(
    expression_type: type[BE], tokens: ParseResults
) -> BE | None:
    """Transform the token into a BinaryExpression node.

    :param tokens: ParseResults objects

    :return: BinaryExpression node
    """
    if len(tokens) >= 3:
        left_argument = tokens[0]
        operator_type = tokens[1]
        right_argument = tokens[2]
        expression = expression_type(operator_type, left_argument, right_argument)

        for tokens_chunk in chunks(tokens, 2, 3):
            operator_type = tokens_chunk[0]
            right_argument = tokens_chunk[1]
            expression = expression_type(operator_type, expression, right_argument)

        return expression
    else:
        return None


def _parse_binary_arithmetic_expression(
    tokens: ParseResults,
) -> BinaryArithmeticExpression | None:
    """Transform the token into a BinaryArithmeticExpression node.

    :param tokens: ParseResults objects

    :return: BinaryArithmeticExpression node
    """
    return _parse_binary_expression(BinaryArithmeticExpression, tokens)


def _parse_binary_boolean_expression(
    tokens: ParseResults,
) -> BinaryBooleanExpression | None:
    """Transform the token into a BinaryBooleanExpression node.

    :param tokens: ParseResults objects

    :return: BinaryBooleanExpression node
    """
    return _parse_binary_expression(BinaryBooleanExpression, tokens)


def _parse_comparison_expression(
    tokens: ParseResults,
) -> ComparisonExpression | None:
    """Transform the token into a ComparisonExpression node.

    :param tokens: ParseResults objects

    :return: ComparisonExpression node
    """
    return _parse_binary_expression(ComparisonExpression, tokens)


def _parse_dot_expression(tokens: ParseResults) -> DotExpression:
    """Transform the token into a DotExpression node.

    :param tokens: ParseResults objects

    :return: ComparisonExpression node
    """
    return DotExpression(list(tokens[0]))


def _parse_parenthesized_expression(tokens: ParseResults) -> Expression:
    """Transform the token into a Expression node.

    :param tokens: ParseResults objects

    :return: ComparisonExpression node
    """
    return tokens[0]


def _parse_function_call_expression(tokens: ParseResults) -> FunctionCallExpression:
    """Transform the token into a FunctionCallExpression node.

    :param tokens: ParseResults objects

    :return: ComparisonExpression node
    """
    function_identifier = tokens[0][0]
    arguments = tokens[0][1:]

    return FunctionCallExpression(function_identifier, arguments)


def _parse_slice_operation(tokens: ParseResults) -> SliceExpression:
    """Transform the token into a SliceExpression node.

    :param tokens: ParseResults objects

    :return: SliceExpression node
    """
    array_expression = tokens[0][0]
    slice_expression = tokens[0][1]

    return SliceExpression(array_expression, slice_expression)
