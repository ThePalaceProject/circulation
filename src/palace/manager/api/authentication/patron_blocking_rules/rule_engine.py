from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from simpleeval import (  # type: ignore[import-untyped]
    EvalWithCompoundTypes,
    NameNotDefined,
)

from palace.manager.core.exceptions import BasePalaceException

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_RULE_LENGTH = 1000
MAX_MESSAGE_LENGTH = 1000

# Placeholder keys: alphanumeric + underscore, e.g. {dob}, {patron_type}
_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z0-9_]+)\}")

# Variable name prefix used when compiling placeholders to safe identifiers.
_VAR_PREFIX = "__v_"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class RuleValidationError(BasePalaceException):
    """Raised when a patron blocking rule fails admin-save validation."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class MissingPlaceholderError(BasePalaceException):
    """Raised when a required placeholder key is absent from the values dict."""

    def __init__(self, key: str) -> None:
        super().__init__(f"Missing placeholder value for key: {key!r}")
        self.key = key


class RuleEvaluationError(BasePalaceException):
    """Raised at runtime when a rule cannot be evaluated safely.

    All callers should treat this as a block (fail-closed).
    """

    def __init__(self, message: str, rule_name: str | None = None) -> None:
        super().__init__(message)
        self.rule_name = rule_name


# ---------------------------------------------------------------------------
# Compiled rule dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CompiledRule:
    """The result of compiling a rule expression.

    Attributes:
        original: The original rule expression string (with {key} placeholders).
        compiled: The expression with placeholders replaced by safe variable
            names (e.g. ``__v_key``), ready for simpleeval.
        var_map: Mapping from original placeholder key to its safe variable name.
    """

    original: str
    compiled: str
    var_map: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Compilation helpers
# ---------------------------------------------------------------------------


def compile_rule_expression(expr: str) -> CompiledRule:
    """Compile a rule expression by replacing ``{key}`` placeholders with safe
    variable identifiers.

    The resulting expression string is suitable for passing to simpleeval.
    Placeholder values are injected via the ``names`` dict (see
    :func:`build_names`).

    Args:
        expr: The raw rule expression, e.g. ``"age_in_years({dob}) >= 18"``.

    Returns:
        A :class:`CompiledRule` instance.
    """
    var_map: dict[str, str] = {}

    def _replace(m: re.Match[str]) -> str:
        key = m.group(1)
        var_name = f"{_VAR_PREFIX}{key}"
        var_map[key] = var_name
        return var_name

    compiled = _PLACEHOLDER_RE.sub(_replace, expr)
    return CompiledRule(original=expr, compiled=compiled, var_map=var_map)


# ---------------------------------------------------------------------------
# Names / values helpers
# ---------------------------------------------------------------------------


def build_names(compiled: CompiledRule, values: Mapping[str, Any]) -> dict[str, Any]:
    """Build the simpleeval ``names`` dict for a compiled rule.

    Raises :class:`MissingPlaceholderError` if any placeholder referenced in
    *compiled* is absent from *values*.

    Args:
        compiled: A :class:`CompiledRule` produced by
            :func:`compile_rule_expression`.
        values: Mapping of placeholder key to concrete value, e.g.
            ``{"dob": "1990-01-15"}``.

    Returns:
        Dict mapping safe variable names to their values, ready to pass as
        ``evaluator.names``.

    Raises:
        MissingPlaceholderError: If a required key is absent from *values*.
    """
    names: dict[str, Any] = {}
    for key, var_name in compiled.var_map.items():
        if key not in values:
            raise MissingPlaceholderError(key)
        names[var_name] = values[key]
    return names


# ---------------------------------------------------------------------------
# Allowed functions
# ---------------------------------------------------------------------------


def age_in_years(
    date_str: str,
    fmt: str | None = None,
    *,
    today: date | None = None,
) -> int:
    """Return the age in whole years relative to *today*.

    Args:
        date_str: A date string to parse.
        fmt: Optional :func:`~datetime.datetime.strptime` format string.  When
            omitted the function attempts ISO 8601 parsing first, then falls
            back to ``dateutil.parser`` if available.
        today: Keyword-only override for the reference date; defaults to
            :func:`datetime.date.today`.  Intended for deterministic tests.

    Returns:
        Age in whole years (floor).

    Raises:
        ValueError: If *date_str* cannot be parsed.
    """
    from datetime import datetime

    ref = today if today is not None else date.today()

    birth: date
    if fmt is not None:
        birth = datetime.strptime(date_str, fmt).date()
    else:
        # Try ISO 8601 first.
        try:
            birth = date.fromisoformat(date_str)
        except ValueError:
            # Fall back to dateutil if available.
            try:
                from dateutil import parser as _dateutil_parser

                birth = _dateutil_parser.parse(date_str).date()
            except Exception:
                raise ValueError(f"Cannot parse date string: {date_str!r}") from None

    years = ref.year - birth.year
    if (ref.month, ref.day) < (birth.month, birth.day):
        years -= 1
    return years


#: Default set of functions available in rule expressions.
DEFAULT_ALLOWED_FUNCTIONS: dict[str, Callable[..., Any]] = {
    "age_in_years": age_in_years,
}


# ---------------------------------------------------------------------------
# Evaluator factory
# ---------------------------------------------------------------------------


def make_evaluator(
    allowed_functions: dict[str, Callable[..., Any]] | None = None,
) -> EvalWithCompoundTypes:
    """Create a locked-down :class:`~simpleeval.EvalWithCompoundTypes` instance.

    Only the functions listed in *allowed_functions* (defaulting to
    :data:`DEFAULT_ALLOWED_FUNCTIONS`) are available in expressions.  No
    additional names or builtins are accessible.

    Args:
        allowed_functions: Override the function whitelist.  Pass an empty dict
            to disallow all functions.

    Returns:
        A configured :class:`~simpleeval.EvalWithCompoundTypes`.
    """
    functions = (
        allowed_functions
        if allowed_functions is not None
        else DEFAULT_ALLOWED_FUNCTIONS
    )
    return EvalWithCompoundTypes(functions=functions, names={})


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_message(message: str) -> None:
    """Validate a patron blocking rule *message* string.

    Args:
        message: The human-readable message shown when a patron is blocked.

    Raises:
        RuleValidationError: If the message is empty/whitespace or exceeds
            :data:`MAX_MESSAGE_LENGTH` characters.
    """
    if not message or not message.strip():
        raise RuleValidationError("Message must not be empty or whitespace.")
    if len(message) > MAX_MESSAGE_LENGTH:
        raise RuleValidationError(
            f"Message must not exceed {MAX_MESSAGE_LENGTH} characters "
            f"(got {len(message)})."
        )


def validate_rule_expression(
    expr: str,
    test_values: Mapping[str, Any],
    evaluator: EvalWithCompoundTypes,
) -> None:
    """Validate a rule expression at admin-save time.

    Checks performed (in order):
    1. Non-empty / non-whitespace.
    2. Length ≤ :data:`MAX_RULE_LENGTH`.
    3. All placeholders present in *test_values*.
    4. Expression parses and evaluates without error.
    5. Result is a strict :class:`bool`.

    Args:
        expr: The raw rule expression string.
        test_values: Mapping of placeholder key → test value used for the
            trial evaluation.
        evaluator: A locked-down evaluator from :func:`make_evaluator`.

    Raises:
        RuleValidationError: On any validation failure.
    """
    if not expr or not expr.strip():
        raise RuleValidationError("Rule expression must not be empty or whitespace.")
    if len(expr) > MAX_RULE_LENGTH:
        raise RuleValidationError(
            f"Rule expression must not exceed {MAX_RULE_LENGTH} characters "
            f"(got {len(expr)})."
        )

    compiled = compile_rule_expression(expr)

    try:
        names = build_names(compiled, test_values)
    except MissingPlaceholderError as exc:
        raise RuleValidationError(str(exc.message)) from exc

    evaluator.names = names
    try:
        result = evaluator.eval(compiled.compiled)
    except Exception as exc:
        raise RuleValidationError(f"Rule expression failed to evaluate: {exc}") from exc
    finally:
        evaluator.names = {}

    if not isinstance(result, bool):
        raise RuleValidationError(
            f"Rule expression must evaluate to a boolean, got {type(result).__name__!r}."
        )


# ---------------------------------------------------------------------------
# Runtime evaluation
# ---------------------------------------------------------------------------


def evaluate_rule_expression_strict_bool(
    expr: str,
    values: Mapping[str, Any],
    evaluator: EvalWithCompoundTypes,
    rule_name: str | None = None,
) -> bool:
    """Evaluate a rule expression at runtime.

    This function is **fail-closed**: any error raises
    :class:`RuleEvaluationError` rather than silently allowing access.

    Args:
        expr: The raw rule expression string (same as stored).
        values: Mapping of placeholder key → runtime value.
        evaluator: A locked-down evaluator from :func:`make_evaluator`.
        rule_name: Optional identifier for the rule, included in error messages.

    Returns:
        ``True`` if the patron should be *blocked*, ``False`` otherwise.

    Raises:
        RuleEvaluationError: On missing placeholders, parse/eval errors, or
            non-boolean result.
    """
    compiled = compile_rule_expression(expr)

    try:
        names = build_names(compiled, values)
    except MissingPlaceholderError as exc:
        raise RuleEvaluationError(
            f"Missing placeholder {exc.key!r} for rule {rule_name!r}.",
            rule_name=rule_name,
        ) from exc

    evaluator.names = names
    try:
        result = evaluator.eval(compiled.compiled)
    except NameNotDefined as exc:
        raise RuleEvaluationError(
            f"Undefined name in rule {rule_name!r}: {exc}",
            rule_name=rule_name,
        ) from exc
    except Exception as exc:
        raise RuleEvaluationError(
            f"Rule {rule_name!r} could not be evaluated.",
            rule_name=rule_name,
        ) from exc
    finally:
        evaluator.names = {}

    if not isinstance(result, bool):
        raise RuleEvaluationError(
            f"Rule {rule_name!r} did not return a boolean "
            f"(got {type(result).__name__!r}).",
            rule_name=rule_name,
        )

    return result
