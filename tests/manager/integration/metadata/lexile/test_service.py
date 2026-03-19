"""Tests for the Lexile DB service."""

from __future__ import annotations

from unittest.mock import MagicMock

from palace.manager.integration.metadata.lexile.service import LexileDBService
from palace.manager.integration.metadata.lexile.settings import (
    DEFAULT_SAMPLE_ISBN,
    LexileDBSettings,
)
from tests.fixtures.http import MockHttpClientFixture


class TestLexileDBService:
    """Tests for LexileDBService."""

    def test_run_self_tests_success_with_lexile_data(
        self, http_client: MockHttpClientFixture
    ) -> None:
        """Self-test succeeds when API returns Lexile data."""
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
        service = LexileDBService(MagicMock(), settings)

        results = list(service._run_self_tests(MagicMock()))

        assert len(results) == 1
        assert results[0].success is True
        assert "650" in str(results[0].result)
        assert DEFAULT_SAMPLE_ISBN in str(results[0].result)

    def test_run_self_tests_success_no_lexile_data(
        self, http_client: MockHttpClientFixture
    ) -> None:
        """Self-test succeeds when API returns no data (book not in DB)."""
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
        service = LexileDBService(MagicMock(), settings)

        results = list(service._run_self_tests(MagicMock()))

        assert len(results) == 1
        assert results[0].success is True
        assert "No Lexile data found" in str(results[0].result)
        assert "API connection succeeded" in str(results[0].result)

    def test_run_self_tests_uses_custom_sample_identifier(
        self, http_client: MockHttpClientFixture
    ) -> None:
        """Self-test uses sample_identifier from settings when provided."""
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
            sample_identifier="9780123456789",
        )
        service = LexileDBService(MagicMock(), settings)

        results = list(service._run_self_tests(MagicMock()))

        assert len(results) == 1
        assert results[0].success is True
        assert "9780123456789" in str(results[0].name)
        assert "720" in str(results[0].result)
        assert "9780123456789" in http_client.requests[0]

    def test_run_self_tests_fails_on_auth_error(
        self, http_client: MockHttpClientFixture
    ) -> None:
        """Self-test fails when API returns 403."""
        http_client.queue_response(403, content="Forbidden")
        settings = LexileDBSettings(
            username="user",
            password="pass",
            base_url="https://api.example.com",
        )
        service = LexileDBService(MagicMock(), settings)

        results = list(service._run_self_tests(MagicMock()))

        assert len(results) == 1
        assert results[0].success is False
        assert results[0].exception is not None
        assert "authentication" in str(results[0].exception).lower()
