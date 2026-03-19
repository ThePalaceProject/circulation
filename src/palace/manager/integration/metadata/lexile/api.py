"""Client for the MetaMetrics Lexile Titles Database API."""

from __future__ import annotations

import logging
from typing import Any

from palace.manager.core.exceptions import IntegrationException
from palace.manager.integration.metadata.lexile.settings import LexileDBSettings
from palace.manager.util.http.http import HTTP
from palace.manager.util.log import LoggerMixin


class LexileDBAPI(LoggerMixin):
    """Client for fetching Lexile measures from the MetaMetrics Lexile DB API."""

    def __init__(self, settings: LexileDBSettings):
        """Initialize the API client with credentials and base URL."""
        self._settings = settings
        self._log = logging.getLogger(self.__class__.__name__)

    def fetch_lexile_for_isbn(
        self, isbn: str, *, raise_on_error: bool = False
    ) -> int | None:
        """Fetch the Lexile measure for a book by ISBN.

        :param isbn: 10 or 13 digit ISBN.
        :param raise_on_error: If True, raise IntegrationException on HTTP errors
            (e.g. 401, 403) instead of returning None. Used for self-tests.
        :return: The Lexile measure (e.g. 650) or None if not found or on error.
        """
        isbn = isbn.strip().replace("-", "")
        if not isbn:
            return None

        # Use ISBN or ISBN13 parameter based on length
        param = "ISBN13" if len(isbn) == 13 else "ISBN"
        url = f"{self._settings.base_url.rstrip('/')}/api/fab/v3/book/?format=json&{param}={isbn}"

        try:
            response = HTTP.get_with_timeout(
                url,
                timeout=30,
                auth=(self._settings.username, self._settings.password),
            )
        except Exception as e:
            self.log.warning("Lexile API request failed for ISBN %s: %s", isbn, e)
            if raise_on_error:
                raise IntegrationException(
                    "Lexile API request failed",
                    str(e),
                ) from e
            return None

        if response.status_code != 200:
            self.log.warning(
                "Lexile API returned %s for ISBN %s", response.status_code, isbn
            )
            if raise_on_error:
                if response.status_code in (401, 403):
                    raise IntegrationException(
                        "Lexile API authentication failed",
                        f"HTTP {response.status_code}. Check username and password.",
                    )
                raise IntegrationException(
                    "Lexile API request failed",
                    f"HTTP {response.status_code} for ISBN {isbn}",
                )
            return None

        try:
            data: dict[str, Any] = response.json()
        except ValueError as e:
            self.log.warning("Lexile API invalid JSON for ISBN %s: %s", isbn, e)
            return None

        meta = data.get("meta", {})
        total_count = meta.get("total_count", 0)
        if total_count == 0:
            return None

        objects = data.get("objects", [])
        if not objects:
            return None

        first = objects[0]
        lexile = first.get("lexile")
        if lexile is None:
            return None

        try:
            return int(lexile)
        except (TypeError, ValueError):
            self.log.warning(
                "Lexile API returned non-numeric lexile %r for ISBN %s", lexile, isbn
            )
            return None
