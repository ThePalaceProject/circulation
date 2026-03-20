"""Patron blocking rules — shared across all patron authentication protocols.

A blocking rule is a simple named predicate attached to a library's
per-protocol settings.  At authentication time, the rules are evaluated
after the remote ILS has successfully authenticated the patron.  If any
rule triggers a block, a :class:`~palace.manager.util.problem_detail.ProblemDetail`
is returned instead of the :class:`~palace.manager.sqlalchemy.model.patron.Patron`
object and the request is rejected.

Rule expressions are evaluated with the simpleeval-based rule engine
(:mod:`palace.manager.api.authentication.patron_blocking_rules.rule_engine`).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict

from palace.manager.api.authentication.patron_blocking_rules.rule_engine import (
    RuleEvaluationError,
    evaluate_rule_expression_strict_bool,
    make_evaluator,
)
from palace.manager.api.problem_details import BLOCKED_BY_POLICY
from palace.manager.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.patron import Patron


class PatronBlockingRule(BaseModel):
    """A single patron blocking rule stored in a library's per-protocol settings.

    :ivar name: A human-readable identifier for the rule; must be non-empty and
        unique within a library's rule list.
    :ivar rule: A simpleeval rule expression evaluated at authentication time.
        The expression must evaluate to a strict bool.  Placeholder values may
        be embedded as ``{key}`` and are resolved at runtime from the patron's
        profile (see :func:`build_runtime_values_from_patron`).
    :ivar message: Optional text shown to the patron when this rule blocks
        access.  If omitted a generic default is used.
    """

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    name: str
    rule: str
    message: str | None = None


def build_runtime_values_from_patron(patron: Patron) -> dict[str, Any]:
    """Build the simpleeval ``names`` dict for a patron at authentication time.

    Keys produced here correspond to the placeholder names supported at
    runtime.  Any placeholder key *not* present in the returned dict will
    cause :func:`check_patron_blocking_rules_with_evaluator` to log and ignore
    that rule if it references the missing key.

    :param patron: The authenticated
        :class:`~palace.manager.sqlalchemy.model.patron.Patron`.
    :returns: Dict mapping placeholder key to resolved value.
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

    return values


_DEFAULT_BLOCK_MESSAGE = "Patron is blocked by library policy."


def check_patron_blocking_rules_with_evaluator(
    rules: list[PatronBlockingRule],
    values: dict[str, Any],
    log: logging.Logger | logging.LoggerAdapter[logging.Logger] | None = None,
) -> ProblemDetail | None:
    """Evaluate blocking rules using the simpleeval rule engine.

    This function is **fail-open**: rules that cannot be parsed or evaluated
    (missing placeholder, parse error, non-bool result) are logged and ignored;
    evaluation continues with the next rule.  Only rules that successfully
    evaluate to ``True`` cause a block.

    :param rules: The list of :class:`PatronBlockingRule` objects for the library.
    :param values: Runtime placeholder values.
    :param log: Optional logger for server-side error diagnostics.
    :returns: A :class:`~palace.manager.util.problem_detail.ProblemDetail`
        (HTTP 403) if the patron should be blocked, or ``None`` if
        authentication should proceed normally.
    """
    evaluator = make_evaluator()

    for rule in rules:
        try:
            blocked = evaluate_rule_expression_strict_bool(
                rule.rule, values, evaluator, rule_name=rule.name
            )
        except RuleEvaluationError as exc:
            if log:
                # NOTE: This particular error string can be used by Cloudwatch or
                # other monitoring tools.  Be aware that changing it may cause
                # the alarm to fail silently.
                log.error(
                    "Patron blocking rule evaluation failed (ignored): "
                    "rule=%r, reason=%s: %s",
                    exc.rule_name,
                    type(exc.__cause__).__name__ if exc.__cause__ else "unknown",
                    exc,
                )
            continue

        if blocked:
            return BLOCKED_BY_POLICY.detailed(rule.message or _DEFAULT_BLOCK_MESSAGE)

    return None
