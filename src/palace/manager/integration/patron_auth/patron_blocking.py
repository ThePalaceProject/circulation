"""Patron blocking rules — shared across all patron authentication protocols.

A blocking rule is a simple named predicate attached to a library's
per-protocol settings.  At authentication time, the rules are evaluated
after the remote ILS has successfully authenticated the patron.  If any
rule triggers a block, a ProblemDetail is returned instead of the Patron
object and the request is rejected.

v1 stub: any rule whose ``rule`` expression equals the literal string
``"BLOCK"`` triggers a block.  All other expressions are ignored.  A
full rule-language parser is a non-goal for v1.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from palace.manager.api.problem_details import BLOCKED_CREDENTIALS
from palace.manager.util.problem_detail import ProblemDetail


class PatronBlockingRule(BaseModel):
    """A single patron blocking rule stored in a library's per-protocol settings.

    Fields
    ------
    name:
        A human-readable identifier for the rule; must be non-empty and
        unique within a library's rule list.
    rule:
        An opaque rule expression evaluated at authentication time.
        In v1 the only meaningful value is the literal string ``"BLOCK"``.
    message:
        Optional text shown to the patron when this rule blocks access.
        If omitted a generic default is used.
    """

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    name: str
    rule: str
    message: str | None = None


def check_patron_blocking_rules(
    rules: list[PatronBlockingRule],
) -> ProblemDetail | None:
    """Evaluate a list of blocking rules against the current authentication attempt.

    This is a pure function with no side-effects, intentionally kept
    protocol-agnostic so it can be called from any authentication provider
    that stores patron_blocking_rules in its LibrarySettings.

    :param rules: The list of PatronBlockingRule objects configured for a library.
    :return: A ProblemDetail (HTTP 403) if the patron should be blocked,
             or ``None`` if authentication should proceed normally.
    """
    for rule in rules:
        if rule.rule == "BLOCK":
            detail = rule.message or "Access blocked by library policy."
            return BLOCKED_CREDENTIALS.detailed(detail)
    return None
