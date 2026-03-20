"""Patron blocking rules provider mixin.

Provides the :class:`HasPatronBlockingRules` abstract mixin that authentication
providers implement to support per-library patron blocking rules.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class HasPatronBlockingRules(ABC):
    """Mixin for authentication providers that support patron blocking rules.

    Implementing this interface allows the provider to evaluate patron blocking
    rules at authentication time and to supply live validation values for
    admin-side rule validation.
    """

    @classmethod
    @abstractmethod
    def fetch_live_rule_validation_values(cls, settings: Any) -> dict[str, Any]:
        """Fetch placeholder values from the ILS for rule expression validation.

        Called at admin-save time when validating patron blocking rules.
        Uses the provider's configured test identifier to fetch real patron
        information and builds a values dict suitable for
        :func:`~palace.manager.api.authentication.patron_blocking_rules
        .rule_engine.validate_rule_expression`.

        :param settings: The validated settings for this integration.
        :returns: Dict mapping placeholder key to resolved value.
        :raises ProblemDetailException: If the ILS cannot be reached or
            returns an error response.
        """
        ...
