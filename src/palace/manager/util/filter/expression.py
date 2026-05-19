"""Public API for the generic filter expression engine."""

from __future__ import annotations

from typing import Any

from palace.util.exceptions import BasePalaceException

from palace.manager.util.filter.evaluator import DSLEvaluationVisitor, DSLEvaluator
from palace.manager.util.filter.parser import DSLParser


class FilterExpressionError(BasePalaceException):
    """Raised when a filter expression fails to validate or evaluate."""


class FilterExpression:
    """A safe, evaluable boolean expression over a named context dictionary.

    The expression language is a Python-like DSL that supports attribute and
    subscript access, string and dict method calls, comparison and boolean
    operators, and a fixed set of builtin functions. It does not use
    ``eval()``; safety is enforced via a whitelist of callable types.

    Dot-access on a dict value resolves to key lookup, so callers can pass
    plain dicts and write ``claim.ou`` instead of ``claim["ou"]``.

    :param expression: The expression string to evaluate.
    :param extra_safe_types: Additional types whose methods may be called in
        the expression. ``str``, ``int``, ``float``, and ``dict`` are always
        safe regardless of this parameter.
    """

    def __init__(
        self,
        expression: str,
        extra_safe_types: list[type] | None = None,
    ) -> None:
        self._expression = expression
        self._extra_safe_types: list[type] = extra_safe_types or []
        self._parser = DSLParser()
        self._visitor = DSLEvaluationVisitor()
        self._evaluator = DSLEvaluator(self._parser, self._visitor)

    def validate(self) -> None:
        """Parse the expression without evaluating it.

        :raises FilterExpressionError: if the expression has a syntax error.
        """
        try:
            self._parser.parse(self._expression)
        except Exception as exc:
            raise FilterExpressionError(str(exc)) from exc

    def evaluate(self, context: dict[str, Any]) -> bool:
        """Evaluate the expression against a context dictionary.

        Context keys become top-level identifiers in the expression.

        :param context: Named values available to the expression.
        :raises FilterExpressionError: on any parse or evaluation error.
        :return: Boolean result of the expression.
        """
        try:
            result = self._evaluator.evaluate(
                self._expression,
                context=context,
                safe_classes=self._extra_safe_types,
            )
            return bool(result)
        except Exception as exc:
            raise FilterExpressionError(str(exc)) from exc
