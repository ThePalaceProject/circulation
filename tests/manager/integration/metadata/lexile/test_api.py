"""Tests for the Lexile DB API client."""

from __future__ import annotations

import pytest

from palace.manager.core.exceptions import IntegrationException
from palace.manager.integration.metadata.lexile.api import LexileDBAPI
from palace.manager.integration.metadata.lexile.settings import LexileDBSettings
from tests.fixtures.http import MockHttpClientFixture


class TestLexileDBAPI:
    """Tests for LexileDBAPI."""

    def test_fetch_lexile_for_isbn_returns_lexile(
        self, http_client: MockHttpClientFixture
    ) -> None:
        """API returns Lexile measure when book is found."""
        http_client.queue_response(
            200,
            content={
                "meta": {"total_count": 1},
                "objects": [{"lexile": 650}],
            },
        )
        settings = LexileDBSettings(
            username="user",
            password="pass",
            base_url="https://api.example.com",
        )
        api = LexileDBAPI(settings)

        result = api.fetch_lexile_for_isbn("9780123456789")

        assert result == 650
        assert len(http_client.requests) == 1
        assert "ISBN13=9780123456789" in http_client.requests[0]

    def test_fetch_lexile_for_isbn_10_digit(
        self, http_client: MockHttpClientFixture
    ) -> None:
        """API uses ISBN param for 10-digit ISBNs."""
        http_client.queue_response(
            200,
            content={
                "meta": {"total_count": 1},
                "objects": [{"lexile": 720}],
            },
        )
        settings = LexileDBSettings(
            username="user",
            password="pass",
            base_url="https://api.example.com",
        )
        api = LexileDBAPI(settings)

        result = api.fetch_lexile_for_isbn("0123456789")

        assert result == 720
        assert "ISBN=0123456789" in http_client.requests[0]

    def test_fetch_lexile_for_isbn_strips_hyphens(
        self, http_client: MockHttpClientFixture
    ) -> None:
        """ISBN hyphens are stripped before request."""
        http_client.queue_response(
            200,
            content={
                "meta": {"total_count": 1},
                "objects": [{"lexile": 500}],
            },
        )
        settings = LexileDBSettings(
            username="user",
            password="pass",
            base_url="https://api.example.com",
        )
        api = LexileDBAPI(settings)

        result = api.fetch_lexile_for_isbn("978-0-12-345678-9")

        assert result == 500
        assert "ISBN13=9780123456789" in http_client.requests[0]

    def test_fetch_lexile_for_isbn_not_found(
        self, http_client: MockHttpClientFixture
    ) -> None:
        """API returns None when book has no Lexile data."""
        http_client.queue_response(
            200,
            content={
                "meta": {"total_count": 0},
                "objects": [],
            },
        )
        settings = LexileDBSettings(
            username="user",
            password="pass",
            base_url="https://api.example.com",
        )
        api = LexileDBAPI(settings)

        result = api.fetch_lexile_for_isbn("9780123456789")

        assert result is None

    def test_fetch_lexile_for_isbn_empty_objects(
        self, http_client: MockHttpClientFixture
    ) -> None:
        """API returns None when objects list is empty despite total_count."""
        http_client.queue_response(
            200,
            content={
                "meta": {"total_count": 1},
                "objects": [],
            },
        )
        settings = LexileDBSettings(
            username="user",
            password="pass",
            base_url="https://api.example.com",
        )
        api = LexileDBAPI(settings)

        result = api.fetch_lexile_for_isbn("9780123456789")

        assert result is None

    def test_fetch_lexile_for_isbn_null_lexile(
        self, http_client: MockHttpClientFixture
    ) -> None:
        """API returns None when lexile field is null."""
        http_client.queue_response(
            200,
            content={
                "meta": {"total_count": 1},
                "objects": [{"lexile": None}],
            },
        )
        settings = LexileDBSettings(
            username="user",
            password="pass",
            base_url="https://api.example.com",
        )
        api = LexileDBAPI(settings)

        result = api.fetch_lexile_for_isbn("9780123456789")

        assert result is None

    def test_fetch_lexile_for_isbn_http_error(
        self, http_client: MockHttpClientFixture
    ) -> None:
        """API returns None on HTTP error."""
        http_client.queue_response(404, content="")
        settings = LexileDBSettings(
            username="user",
            password="pass",
            base_url="https://api.example.com",
        )
        api = LexileDBAPI(settings)

        result = api.fetch_lexile_for_isbn("9780123456789")

        assert result is None

    def test_fetch_lexile_for_isbn_empty_string(
        self, http_client: MockHttpClientFixture
    ) -> None:
        """API returns None for empty ISBN."""
        settings = LexileDBSettings(
            username="user",
            password="pass",
            base_url="https://api.example.com",
        )
        api = LexileDBAPI(settings)

        result = api.fetch_lexile_for_isbn("")

        assert result is None
        assert len(http_client.requests) == 0

    def test_fetch_lexile_for_isbn_raise_on_error_403(
        self, http_client: MockHttpClientFixture
    ) -> None:
        """API raises IntegrationException on 403 when raise_on_error=True."""
        http_client.queue_response(403, content="Forbidden")
        settings = LexileDBSettings(
            username="user",
            password="pass",
            base_url="https://api.example.com",
        )
        api = LexileDBAPI(settings)

        with pytest.raises(IntegrationException) as excinfo:
            api.fetch_lexile_for_isbn("9780123456789", raise_on_error=True)

        assert "authentication" in str(excinfo.value).lower()
