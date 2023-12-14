import operator
import types
from collections.abc import Callable
from copy import copy, deepcopy

from multipledispatch import dispatch

from core.exceptions import BaseError
from core.python_expression_dsl.ast import (
    BinaryArithmeticExpression,
    BinaryBooleanExpression,
    BinaryExpression,
    ComparisonExpression,
    DotExpression,
    FunctionCallExpression,
    Identifier,
    Number,
    Operator,
    SliceExpression,
    String,
    UnaryArithmeticExpression,
    UnaryBooleanExpression,
    UnaryExpression,
    Visitor,
)
from core.python_expression_dsl.parser import DSLParser


class DSLEvaluationError(BaseError):
    """Raised when evaluation of a DSL expression fails."""


class DSLEvaluationVisitor(Visitor):
    """Visitor traversing expression's AST and evaluating it."""

    ARITHMETIC_OPERATORS = {
        Operator.NEGATION: operator.neg,
        Operator.ADDITION: operator.add,
        Operator.SUBTRACTION: operator.sub,
        Operator.MULTIPLICATION: operator.mul,
        Operator.DIVISION: operator.truediv,
        Operator.EXPONENTIATION: operator.pow,
    }

    BOOLEAN_OPERATORS = {
        Operator.INVERSION: operator.not_,
        Operator.CONJUNCTION: operator.and_,
        Operator.DISJUNCTION: operator.or_,
    }

    COMPARISON_OPERATORS = {
        Operator.EQUAL: operator.eq,
        Operator.NOT_EQUAL: operator.ne,
        Operator.GREATER: operator.gt,
        Operator.GREATER_OR_EQUAL: operator.ge,
        Operator.LESS: operator.lt,
        Operator.LESS_OR_EQUAL: operator.le,
        Operator.IN: lambda a, b: operator.contains(b, a),
    }

    BUILTIN_FUNCTIONS = {
        "abs": abs,
        "all": all,
        "any": any,
        "len": len,
        "max": max,
        "min": min,
        "int": int,
        "float": float,
        "str": str,
    }

    BUILTIN_CLASSES = [float, int, str, types.ModuleType]

    def __init__(
        self,
        context: dict | object | None = None,
        safe_classes: list[type] | None = None,
    ):
        """Initialize a new instance of DSLEvaluationVisitor class.

        :param context: Optional evaluation context
        :param safe_classes: Optional list of classes which methods can be called.
            By default it contains only built-in classes: float, int, str
        """
        self._context: dict | object | None = {}
        self._safe_classes: list[type] | None = []
        self._current_scope = None
        self._root_dot_node = None

        if safe_classes is None:
            safe_classes = []

        self.context = context
        self.safe_classes = safe_classes

    @staticmethod
    def _get_attribute_value(obj: dict | object, attribute: str):
        """Return the attribute's value by its name.

        :param obj: Object or a dictionary containing the attribute
        :param attribute: Attribute's name

        :return: Attribute's value
        """
        if isinstance(obj, dict):
            if attribute not in obj:
                raise DSLEvaluationError(
                    f"Cannot find attribute '{attribute}' in {obj}"
                )

            return obj[attribute]
        else:
            if not hasattr(obj, attribute):
                raise DSLEvaluationError(
                    f"Cannot find attribute '{attribute}' in {obj}"
                )

            return getattr(obj, attribute)

    def _evaluate_unary_expression(
        self,
        unary_expression: UnaryExpression,
        available_operators: dict[Operator, Callable],
    ):
        """Evaluate the unary expression.

        :param unary_expression: Unary expression
        :param available_operators: Dictionary containing available operators

        :return: Evaluation result
        """
        argument = unary_expression.argument.accept(self)

        if unary_expression.operator not in available_operators:
            raise DSLEvaluationError(
                "Wrong operator {}. Was expecting one of {}".format(
                    unary_expression.operator, list(available_operators.keys())
                )
            )

        expression_operator = available_operators[unary_expression.operator]
        result = expression_operator(argument)

        return result

    def _evaluate_binary_expression(
        self,
        binary_expression: BinaryExpression,
        available_operators: dict[Operator, Callable],
    ):
        """Evaluate the binary expression.

        :param binary_expression: Binary expression
        :param available_operators: Dictionary containing available operators

        :return: Evaluation result
        """
        left_argument = binary_expression.left_argument.accept(self)
        right_argument = binary_expression.right_argument.accept(self)

        if binary_expression.operator not in available_operators:
            raise DSLEvaluationError(
                "Wrong operator {}. Was expecting one of {}".format(
                    binary_expression.operator, list(available_operators.keys())
                )
            )

        expression_operator = available_operators[binary_expression.operator]
        result = expression_operator(left_argument, right_argument)

        return result

    @property
    def context(self) -> dict | object:
        """Return the evaluation context.

        :return: Evaluation context
        """
        return self._context

    @context.setter
    def context(self, value: dict | object):
        """Set the evaluation context.

        :param value: New evaluation context
        """
        if not isinstance(value, (dict, object)):
            raise ValueError(
                "Argument 'value' must be an either a dictionary or object"
            )

        new_context = {}

        if value is not None:
            if isinstance(value, dict):
                for key, item in value.items():
                    new_context[key] = deepcopy(item)
            else:
                new_context = deepcopy(value)  # type: ignore

        self._context = new_context

    @property
    def safe_classes(self) -> list[type] | None:
        """Return a list of classes which methods can be called.

        :return: List of safe classes which methods can be called
        """
        return self._safe_classes

    @safe_classes.setter
    def safe_classes(self, value: list[type]):
        """Set safe classes which methods can be called.

        :param value: List of safe classes which methods be called
        """
        if not isinstance(value, list):
            raise ValueError("Argument 'value' must be a list")

        new_safe_classes = copy(value)
        new_safe_classes.extend(self.BUILTIN_CLASSES)
        new_safe_classes = list(set(new_safe_classes))

        self._safe_classes = new_safe_classes

    @dispatch(Identifier)
    def visit(self, node: Identifier):
        """Process the Identifier node.

        :param node: Identifier node
        """
        if self._current_scope is None and node.value in self.BUILTIN_FUNCTIONS:
            value = self.BUILTIN_FUNCTIONS[node.value]
        else:
            value = self._get_attribute_value(
                self._current_scope
                if self._current_scope is not None
                else self._context,
                node.value,
            )

        return value

    @dispatch(String)  # type: ignore
    def visit(self, node: String):
        """Process the String node.

        :param node: String node
        """
        return str(node.value)

    @dispatch(Number)  # type: ignore
    def visit(self, node: Number):
        """Process the Number node.

        :param node: Number node
        """
        try:
            return int(node.value)
        except:
            return float(node.value)

    @dispatch(DotExpression)  # type: ignore
    def visit(self, node: DotExpression):
        """Process the DotExpression node.

        :param node: DotExpression node
        """
        if self._root_dot_node is None:
            self._root_dot_node = node

        value = None

        for expression in node.expressions:
            value = expression.accept(self)

            self._current_scope = value

        if self._root_dot_node == node:
            self._root_dot_node = None
            self._current_scope = None

        return value

    @dispatch(UnaryArithmeticExpression)  # type: ignore
    def visit(self, node: UnaryArithmeticExpression):
        """Process the UnaryArithmeticExpression node.

        :param node: UnaryArithmeticExpression node
        """
        return self._evaluate_unary_expression(node, self.ARITHMETIC_OPERATORS)

    @dispatch(BinaryArithmeticExpression)  # type: ignore
    def visit(self, node: BinaryArithmeticExpression):
        """Process the BinaryArithmeticExpression node.

        :param node: BinaryArithmeticExpression node
        """
        return self._evaluate_binary_expression(node, self.ARITHMETIC_OPERATORS)

    @dispatch(UnaryBooleanExpression)  # type: ignore
    def visit(self, node: UnaryBooleanExpression):
        """Process the UnaryBooleanExpression node.

        :param node: UnaryBooleanExpression node
        """
        return self._evaluate_unary_expression(node, self.BOOLEAN_OPERATORS)

    @dispatch(BinaryBooleanExpression)  # type: ignore
    def visit(self, node: BinaryBooleanExpression):
        """Process the BinaryBooleanExpression node.

        :param node: BinaryBooleanExpression node
        """
        return self._evaluate_binary_expression(node, self.BOOLEAN_OPERATORS)

    @dispatch(ComparisonExpression)  # type: ignore
    def visit(self, node: ComparisonExpression):
        """Process the ComparisonExpression node.

        :param node: ComparisonExpression node
        """
        return self._evaluate_binary_expression(node, self.COMPARISON_OPERATORS)

    @dispatch(SliceExpression)  # type: ignore
    def visit(self, node: SliceExpression):
        """Process the SliceExpression node.

        :param node: SliceExpression node
        """
        array = node.array.accept(self)
        index = node.slice.accept(self)

        return operator.getitem(array, index)

    @dispatch(FunctionCallExpression)  # type: ignore
    def visit(self, node: FunctionCallExpression):
        """Process the FunctionCallExpression node.

        :param node: FunctionCallExpression node
        """
        function = node.function.accept(self)
        arguments = []

        if node.arguments:
            for argument in node.arguments:
                argument = argument.accept(self)

                arguments.append(argument)

        function_class = getattr(function.__self__, "__class__", None)

        if function_class and function_class not in self.safe_classes:
            raise DSLEvaluationError(
                "Function {} defined in a not-safe class {} and cannot be called".format(
                    function, function_class
                )
            )

        result = function(*arguments)

        return result


class DSLEvaluator:
    """Evaluates the expression."""

    def __init__(self, parser: DSLParser, visitor: DSLEvaluationVisitor):
        """Initialize a new instance of DSLEvaluator class.

        :param parser: DSL parser transforming the expression string into an AST object
        :param visitor: Visitor used for evaluating the expression's AST
        """
        if not isinstance(parser, DSLParser):
            raise ValueError(
                f"Argument 'parser' must be an instance of {DSLParser} class"
            )
        if not isinstance(visitor, DSLEvaluationVisitor):
            raise ValueError(
                "Argument 'visitor' must be an instance of {} class".format(
                    DSLEvaluationVisitor
                )
            )

        self._parser = parser
        self._visitor = visitor

    @property
    def parser(self) -> DSLParser:
        """Return the parser used by this evaluator.

        :return: Parser used by this evaluator
        """
        return self._parser

    def evaluate(
        self,
        expression: str,
        context: dict | object | None = None,
        safe_classes: list[type] | None = None,
    ):
        """Evaluate the expression and return the resulting value.

        :param expression: String containing the expression
        :param context: Evaluation context
        :param safe_classes: List of classes which methods can be called

        :return: Evaluation result
        """
        node = self._parser.parse(expression)

        old_context = self._visitor.context
        old_safe_classes = self._visitor.safe_classes

        self._visitor.context = context
        self._visitor.safe_classes = safe_classes

        try:
            result = self._visitor.visit(node)

            return result
        finally:
            self._visitor.context = old_context
            self._visitor.safe_classes = old_safe_classes
