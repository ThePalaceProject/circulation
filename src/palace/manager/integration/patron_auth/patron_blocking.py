"""Patron blocking rules — shared across all patron authentication protocols.

A blocking rule is a simple named predicate attached to a library's
per-protocol settings.  At authentication time, the rules are evaluated
after the remote ILS has successfully authenticated the patron.  If any
rule triggers a block, a ProblemDetail is returned instead of the Patron
object and the request is rejected.

Rule expressions are evaluated with the simpleeval-based rule engine
(see palace.manager.api.authentication.patron_blocking_rules.rule_engine).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict

from palace.manager.api.problem_details import BLOCKED_CREDENTIALS
from palace.manager.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.patron import Patron


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


class PatronBlockingRule(BaseModel):
    """A single patron blocking rule stored in a library's per-protocol settings.

    Fields
    ------
    name:
        A human-readable identifier for the rule; must be non-empty and
        unique within a library's rule list.
    rule:
        A simpleeval rule expression evaluated at authentication time.
        The expression must evaluate to a strict bool.  Placeholder
        values may be embedded as ``{key}`` and are resolved at runtime
        from the patron's profile (see :func:`build_runtime_values_from_patron`).
    message:
        Optional text shown to the patron when this rule blocks access.
        If omitted a generic default is used.
    """

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    name: str
    rule: str
    message: str | None = None


# ---------------------------------------------------------------------------
# Validation test values
# ---------------------------------------------------------------------------

#: Deterministic placeholder values used *only* during admin-save validation.
#: They provide a representative sample so that ``validate_rule_expression``
#: can do a trial evaluation and confirm that expressions return a bool.
#: Keys here define the full set of placeholder names that rules are allowed
#: to reference in validation context.
RULE_VALIDATION_TEST_VALUES: dict[str, Any] = {
    "fines": 5.00,
    "patron_type": "adult",
    # dob is validated but resolved at runtime only when the auth provider
    # supplies it; rules using {dob} will fail-closed if the patron record
    # does not carry a date-of-birth.
    "dob": "1990-01-01",
}


# ---------------------------------------------------------------------------
# Runtime values builder
# ---------------------------------------------------------------------------


def build_runtime_values_from_patron(patron: Patron) -> dict[str, Any]:
    """Build the simpleeval ``names`` dict for a patron at authentication time.

    Keys produced here correspond to the placeholder names supported at
    runtime.  Any placeholder key *not* present in the returned dict will
    cause :func:`check_patron_blocking_rules_with_evaluator` to fail closed
    (block the patron) if a rule references it.

    Args:
        patron: The authenticated :class:`~palace.manager.sqlalchemy.model.patron.Patron`.

    Returns:
        Dict mapping placeholder key to resolved value.
    """
    values: dict[str, Any] = {}

    # fines — always populated; None or unparseable → 0.0
    try:
        fines_raw = getattr(patron, "fines", None)
        values["fines"] = float(fines_raw) if fines_raw is not None else 0.0
    except (ValueError, TypeError):
        values["fines"] = 0.0

    # patron_type — always populated; None → empty string
    try:
        pt = getattr(patron, "external_type", None)
        values["patron_type"] = str(pt) if pt is not None else ""
    except Exception:
        values["patron_type"] = ""

    # NOTE: "dob" is intentionally NOT included here yet.  Rules that
    # reference {dob} will fail-closed (block) until a future version
    # populates it from the patron record or SIP2 response.

    return values


# ---------------------------------------------------------------------------
# Legacy pure-function evaluator (v1 literal BLOCK string)
# ---------------------------------------------------------------------------


def check_patron_blocking_rules(
    rules: list[PatronBlockingRule],
) -> ProblemDetail | None:
    """Evaluate a list of blocking rules using the legacy literal-BLOCK check.

    .. deprecated::
        Prefer :func:`check_patron_blocking_rules_with_evaluator` for
        simpleeval-based rule evaluation.  This function is retained for
        backward compatibility with unit tests of the pure function itself.

    :param rules: The list of PatronBlockingRule objects configured for a library.
    :return: A ProblemDetail (HTTP 403) if the patron should be blocked,
             or ``None`` if authentication should proceed normally.
    """
    for rule in rules:
        if rule.rule == "BLOCK":
            detail = rule.message or "Access blocked by library policy."
            return BLOCKED_CREDENTIALS.detailed(detail)
    return None


# ---------------------------------------------------------------------------
# simpleeval-based runtime evaluator
# ---------------------------------------------------------------------------

_DEFAULT_BLOCK_MESSAGE = "Patron is blocked by library policy."


def check_patron_blocking_rules_with_evaluator(
    rules: list[PatronBlockingRule],
    values: dict[str, Any],
    log: logging.Logger | logging.LoggerAdapter[logging.Logger] | None = None,
) -> ProblemDetail | None:
    """Evaluate blocking rules using the simpleeval rule engine.

    This function is **fail-closed**: any evaluation error (missing
    placeholder, parse error, non-bool result) is treated as a block.
    Internal error details are logged server-side but never exposed to the
    patron.

    A fresh :class:`~simpleeval.EvalWithCompoundTypes` is created for each
    call so the function is safe to call from concurrent requests.

    Args:
        rules: The list of :class:`PatronBlockingRule` objects for the library.
        values: Runtime placeholder values produced by
            :func:`build_runtime_values_from_patron`.
        log: Optional logger for server-side error diagnostics.

    Returns:
        A :class:`~palace.manager.util.problem_detail.ProblemDetail` (HTTP 403)
        if the patron should be blocked, or ``None`` if authentication should
        proceed normally.
    """
    from palace.manager.api.authentication.patron_blocking_rules.rule_engine import (
        RuleEvaluationError,
        evaluate_rule_expression_strict_bool,
        make_evaluator,
    )

    evaluator = make_evaluator()

    for rule in rules:
        try:
            blocked = evaluate_rule_expression_strict_bool(
                rule.rule, values, evaluator, rule_name=rule.name
            )
        except RuleEvaluationError as exc:
            if log:
                log.error(
                    "Patron blocking rule evaluation error "
                    "(rule=%r, reason=%s: %s). Failing closed.",
                    exc.rule_name,
                    type(exc.__cause__).__name__ if exc.__cause__ else "unknown",
                    exc,
                )
            return BLOCKED_CREDENTIALS.detailed(_DEFAULT_BLOCK_MESSAGE)

        if blocked:
            return BLOCKED_CREDENTIALS.detailed(rule.message or _DEFAULT_BLOCK_MESSAGE)

    return None
