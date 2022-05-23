from __future__ import annotations

from abc import ABCMeta, abstractmethod
from enum import Enum


class Visitor(metaclass=ABCMeta):
    """Interface for visitors walking through abstract syntax trees (AST)."""

    @abstractmethod
    def visit(self, node: Node):
        """Process the specified node.

        :param node: AST node
        """
        raise NotImplementedError()


class Visitable(metaclass=ABCMeta):
    """Interface for objects walkable by AST visitors."""

    @abstractmethod
    def accept(self, visitor: Visitor):
        """Accept  the specified visitor.

        :param visitor: Visitor object

        :return: Evaluated result
        """
        raise NotImplementedError()


class Node(Visitable):
    """Base class for all AST nodes."""

    def accept(self, visitor: Visitor):
        """Accept  the specified visitor.

        :param visitor: Visitor object

        :return: Evaluated result
        """
        return visitor.visit(self)


class ScalarValue(Node):
    """Represents a scalar value."""

    def __init__(self, value):
        """Initialize a new instance of ScalarValue class.

        :param value: Value
        """
        self._value = value

    @property
    def value(self):
        """Return the value.

        :return: Value
        """
        return self._value


class Identifier(ScalarValue):
    """Represents an identifier."""


class String(ScalarValue):
    """Represents an string."""


class Number(ScalarValue):
    """Represents a number."""


class Expression(Node):
    """Base class for AST nodes representing different types of expressions."""


class DotExpression(Expression):
    """Represents a dotted expression."""

    def __init__(self, expressions: list[Expression]):
        """Initialize a new instance of DotExpression class.

        :param expressions: List of nested expressions
        """
        self._expressions = expressions

    @property
    def expressions(self) -> list[Expression]:
        """Return the list of nested expressions.

        :return: List of nested expressions
        """
        return self._expressions


class Operator(Enum):
    """Enumeration containing different types of available operators."""

    # Arithmetic operators
    NEGATION = "NEGATION"
    ADDITION = "ADDITION"
    SUBTRACTION = "SUBTRACTION"
    MULTIPLICATION = "MULTIPLICATION"
    DIVISION = "DIVISION"
    EXPONENTIATION = "EXPONENTIATION"

    # Boolean operators
    INVERSION = "INVERSION"
    CONJUNCTION = "CONJUNCTION"
    DISJUNCTION = "DISJUNCTION"

    # Comparison operators
    EQUAL = "EQUAL"
    NOT_EQUAL = "NOT_EQUAL"
    GREATER = "GREATER"
    GREATER_OR_EQUAL = "GREATER_OR_EQUAL"
    LESS = "LESS"
    LESS_OR_EQUAL = "LESS_OR_EQUAL"
    IN = "IN"


class UnaryExpression(Expression):
    """Represents an unary expression."""

    def __init__(self, operator: Operator, argument: Node):
        """Initialize a new instance of UnaryExpression class.

        :param operator: Operator

        :param argument: Argument
        """
        self._operator = operator
        self._argument = argument

    @property
    def operator(self) -> Operator:
        """Return the expression's operator.

        :return: Expression's operator
        """
        return self._operator

    @property
    def argument(self) -> Node:
        """Return the expression's argument.

        :return: Expression's argument
        """
        return self._argument


class BinaryExpression(Expression):
    """Represents a binary expression."""

    def __init__(self, operator: Operator, left_argument: Node, right_argument: Node):
        """Initialize a new instance of BinaryExpression class.

        :param operator: Operator
        :param left_argument: Left argument
        :param right_argument: Right argument
        """
        if not isinstance(operator, Operator):
            raise ValueError(
                f"Argument 'operator' must be an instance of {Operator} class"
            )

        self._operator = operator
        self._left_argument = left_argument
        self._right_argument = right_argument

    @property
    def operator(self) -> Operator:
        """Return the expression's operator.

        :return: Expression's operator
        """
        return self._operator

    @property
    def left_argument(self) -> Node:
        """Return the expression's left argument.

        :return: Expression's left argument
        """
        return self._left_argument

    @property
    def right_argument(self) -> Node:
        """Return the expression's right argument.

        :return: Expression's right argument
        """
        return self._right_argument


class UnaryArithmeticExpression(UnaryExpression):
    """Represents an unary arithmetic expression."""


class BinaryArithmeticExpression(BinaryExpression):
    """Represents a binary arithmetic expression."""


class UnaryBooleanExpression(UnaryExpression):
    """Represents an unary boolean expression."""


class BinaryBooleanExpression(BinaryExpression):
    """Represents a binary boolean expression."""


class ComparisonExpression(BinaryExpression):
    """Represents a comparison expression."""


class SliceExpression(Expression):
    """Represents a slice expression."""

    def __init__(self, array: Node, slice_expression: Expression):
        """Initialize a new instance of SliceExpression.

        :param array: Array
        :param slice_expression: Slice expression
        """
        self._array = array
        self._slice = slice_expression

    @property
    def array(self) -> Node:
        """Return the array node.

        :return: Array node
        """
        return self._array

    @property
    def slice(self) -> Expression:
        """Return the slice expression.

        :return: Slice expression
        """
        return self._slice


class FunctionCallExpression(Expression):
    """Represents a function call expression."""

    def __init__(self, function: Identifier, arguments: list[Expression]):
        """Initialize a new instance of FunctionCallExpression class.

        :param function: Function
        :param arguments: Arguments
        """
        self._function = function
        self._arguments = arguments

    @property
    def function(self) -> Identifier:
        """Return the identifier representing the function.

        :return: Identifier representing the function
        """
        return self._function

    @property
    def arguments(self) -> list[Expression]:
        """Return a list of arguments.

        :return: List of arguments
        """
        return self._arguments
