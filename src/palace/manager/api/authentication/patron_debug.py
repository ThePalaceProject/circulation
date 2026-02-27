"""Patron debug authentication mixin.

Provides the :class:`HasPatronDebug` abstract mixin that authentication
providers implement to support the "Debug Authentication" admin tool.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from palace.manager.api.authentication.base import PatronAuthResult


class HasPatronDebug(ABC):
    """Mixin for authentication providers that support patron debug authentication.

    Implementing this interface allows the admin "Debug Authentication" tool
    to run step-by-step diagnostic checks against the provider.
    """

    @abstractmethod
    def patron_debug(
        self, username: str, password: str | None
    ) -> list[PatronAuthResult]:
        """Run diagnostic authentication checks and return step-by-step results.

        :param username: The patron identifier / barcode / username to test.
        :param password: The patron password / PIN, or None if the provider
            does not collect passwords.
        :return: An ordered list of diagnostic results, one per check step.
        """
        ...
