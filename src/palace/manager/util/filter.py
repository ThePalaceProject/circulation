from __future__ import annotations

import ast
from collections.abc import Callable, Sequence
from typing import Any

from frozendict import frozendict
from simpleeval import (
    AttributeDoesNotExist,
    EvalWithCompoundTypes,
    InvalidExpression,
    NameNotDefined,
)

from palace.util.exceptions import BasePalaceException

_SAFE_BUILTINS: frozendict[str, Callable[..., Any]] = frozendict(
    {
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
)

_ALWAYS_SAFE_TYPES: frozenset[type[Any]] = frozenset(
    {float, int, str, dict, list, tuple}
)

# Named mutation methods for the mutable types in _ALWAYS_SAFE_TYPES (list and dict)
# as of Python 3.12.  Dunder variants (__setitem__, __delitem__, etc.) are blocked by
# simpleeval's DISALLOW_PREFIXES guard, which _eval_attribute delegates to via super().
# If Python adds new named mutation methods to list or dict in a future version, this
# set must be updated.
_MUTATION_METHODS: frozenset[str] = frozenset(
    {
        "append",
        "clear",
        "extend",
        "insert",
        "pop",
        "popitem",
        "remove",
        "reverse",
        "setdefault",
        "sort",
        "update",
    }
)


class FilterExpressionError(BasePalaceException):
    """Raised when a filter expression fails to parse or evaluate."""


# Note: `simpleeval` has no type annotations or stubs, thus the type ignore is needed here.
class _FilterEval(EvalWithCompoundTypes):  # type: ignore[misc]
    """EvalWithCompoundTypes extended with dict-key attribute access and safe-type enforcement."""

    def __init__(
        self,
        extra_safe_types: Sequence[type[Any]],
        missing_attribute_returns_false: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._safe_types: frozenset[type[Any]] = _ALWAYS_SAFE_TYPES | frozenset(
            extra_safe_types
        )
        self._missing_attribute_returns_false = missing_attribute_returns_false

    def _eval_attribute(self, node: ast.Attribute) -> Any:
        obj = self._eval(node.value)
        attr = node.attr
        # For dicts, key lookup takes precedence so callers can write
        # `prop.subprop` instead of prop["subprop"].  This short-circuits
        # before super() so the key shadows any same-named dict method.
        if isinstance(obj, dict) and attr in obj:
            value = obj[attr]
            if callable(value):
                raise FilterExpressionError(
                    f"Callable value at key {attr!r} is not allowed"
                )
            return value
        # Delegate to parent for prefix/method guards (DISALLOW_PREFIXES,
        # DISALLOW_METHODS), getattr, module checks, and AttributeDoesNotExist.
        # node.value is evaluated a second time inside super(); that is harmless
        # because simpleeval expressions are pure.
        try:
            value = super()._eval_attribute(node)
        except (AttributeError, AttributeDoesNotExist):
            if self._missing_attribute_returns_false:
                return False
            raise
        # Enforce safe-type whitelist for all callables.  For instance methods,
        # __self__ is the instance; for classmethods, __self__ is the class itself.
        # Static methods and free callables fall back to type(obj).
        if callable(value):
            if hasattr(value, "__self__"):
                owner = value.__self__
                cls = owner if isinstance(owner, type) else owner.__class__
            else:
                cls = type(obj)
            if cls not in self._safe_types:
                raise FilterExpressionError(
                    f"Callable attribute on unsafe type {cls.__name__!r} is not allowed"
                )
            if attr in _MUTATION_METHODS:
                raise FilterExpressionError(f"Mutating method {attr!r} is not allowed")
        return value


class FilterExpression:
    """A safe, evaluable boolean expression over a named context dictionary.

    The expression language is a Python-like DSL supporting comparisons, boolean
    and arithmetic operators, subscript and attribute access, and method calls.
    Method calls are permitted on ``str``, ``int``, ``float``, ``dict``,
    ``list``, and ``tuple``; mutating methods such as ``append`` and ``update``
    are blocked to prevent side effects. Methods on other types require the type
    to be listed in ``extra_safe_types``.

    Safety is enforced without ``eval()`` via ``simpleeval``'s restricted AST
    evaluator.

    Dot-access on a dict value resolves to key lookup first, so callers can
    pass plain dicts and write `prop.subprop` instead of `prop["subprop"]`.

    :param expression: The expression string to evaluate.
    :param extra_safe_types: Additional types whose non-mutating methods may be
        called in the expression. ``str``, ``int``, ``float``, ``dict``,
        ``list``, and ``tuple`` are always available and need not be listed.
        Types listed here should be immutable or have no mutating methods
        outside the names in ``_MUTATION_METHODS``; the blocklist only covers
        the standard Python built-in mutation API and does not protect against
        custom mutating methods on user-defined types.
    :param missing_attribute_returns_false: When ``True``, any attribute
        access that raises ``AttributeError`` — a missing dict sub-key via
        dot notation, or a missing object attribute — returns ``False``
        instead of raising :class:`FilterExpressionError`. Defaults to
        ``False`` (raise on missing attributes). Note that only the missing
        access itself is suppressed; downstream operations on the resulting
        ``False`` are evaluated normally. In particular, chained method calls
        on a missing attribute (e.g. ``claim.role.lower() == 'student'``) will
        raise because ``False`` is not callable. Use ``.get()`` with a default
        to handle such cases: ``claim.get('role', '').lower() == 'student'``.
    """

    def __init__(
        self,
        expression: str,
        extra_safe_types: Sequence[type[Any]] | None = None,
        missing_attribute_returns_false: bool = False,
    ) -> None:
        self._expression = expression
        self._extra_safe_types: tuple[type[Any], ...] = tuple(extra_safe_types or ())
        self._missing_attribute_returns_false = missing_attribute_returns_false

    def check_syntax(self) -> None:
        """Check the expression for syntax errors only.

        This performs a parse-only check; it does not verify that names used
        in the expression are present in the evaluation context, nor that the
        expression will evaluate to a :class:`bool`.

        :raises FilterExpressionError: if the expression has a syntax error.
        """
        try:
            ast.parse(self._expression, mode="eval")
        except SyntaxError as exc:
            raise FilterExpressionError(str(exc)) from exc

    def evaluate(self, context: dict[str, Any]) -> bool:
        """Evaluate the expression against a context dictionary.

        Context keys become top-level identifiers in the expression.

        :param context: Named values available to the expression.
        :raises FilterExpressionError: on any parse or evaluation error, or
            if the result is not a :class:`bool`.
        :return: Boolean result of the expression.
        """
        # A new evaluator is constructed per call rather than mutating a shared
        # instance, so FilterExpression is safe to call from multiple threads.
        # Note: `functions` parameter requires a mutable mapping.
        evaluator = _FilterEval(
            extra_safe_types=self._extra_safe_types,
            missing_attribute_returns_false=self._missing_attribute_returns_false,
            names=context,
            functions=dict(_SAFE_BUILTINS),
        )
        try:
            result = evaluator.eval(self._expression)
            if not isinstance(result, bool):
                raise FilterExpressionError(
                    f"Expression must evaluate to a bool, got {type(result).__name__!r}"
                )
            return result
        except FilterExpressionError:
            raise
        except NameNotDefined as exc:
            raise FilterExpressionError(f"Name {exc.name!r} is not defined") from exc
        except (
            InvalidExpression,
            AttributeError,
            TypeError,
            IndexError,
            KeyError,
            SyntaxError,
            ZeroDivisionError,
        ) as exc:
            raise FilterExpressionError(str(exc)) from exc
