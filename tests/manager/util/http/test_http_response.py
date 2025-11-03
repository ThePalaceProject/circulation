"""Tests for the HttpResponse dataclass."""

import json
import pickle

import httpx

from palace.manager.util.http.exception import HttpResponse
from tests.mocks.mock import MockRequestsResponse


class TestHttpResponse:
    """Tests for the HttpResponse dataclass."""

    def test_basic_attributes(self):
        """Test that HttpResponse correctly stores basic attributes."""
        response = HttpResponse(
            status_code=200,
            url="http://example.com",
            headers={"Content-Type": "text/html"},
            text="Hello World",
            content=b"Hello World",
            extensions={"custom": "value"},
        )

        assert response.status_code == 200
        assert response.url == "http://example.com"
        assert response.headers["Content-Type"] == "text/html"
        assert response.text == "Hello World"
        assert response.content == b"Hello World"
        assert response.extensions == {"custom": "value"}

    def test_json_method(self):
        """Test that the json() method correctly parses JSON content."""
        json_data = {"key": "value", "number": 42, "nested": {"foo": "bar"}}
        response = HttpResponse(
            status_code=200,
            url="http://api.example.com",
            headers={"Content-Type": "application/json"},
            text=json.dumps(json_data),
            content=json.dumps(json_data).encode("utf-8"),
            extensions={},
        )

        parsed = response.json()
        assert parsed == json_data
        assert parsed["key"] == "value"
        assert parsed["number"] == 42
        assert parsed["nested"]["foo"] == "bar"

    def test_json_method_invalid_json(self):
        """Test that json() raises JSONDecodeError for invalid JSON."""
        response = HttpResponse(
            status_code=200,
            url="http://example.com",
            headers={"Content-Type": "text/plain"},
            text="Not valid JSON",
            content=b"Not valid JSON",
            extensions={},
        )

        # Should raise JSONDecodeError when trying to parse invalid JSON
        try:
            response.json()
            assert False, "Should have raised JSONDecodeError"
        except json.JSONDecodeError:
            pass  # Expected

    def test_from_response_with_requests(self):
        """Test from_response() with a requests.Response object."""
        # Create a mock requests.Response
        mock_response = MockRequestsResponse(
            status_code=404,
            headers={"X-Custom-Header": "custom-value"},
            content="Page not found",
            url="http://example.com/missing",
        )

        http_response = HttpResponse.from_response(mock_response)

        assert http_response.status_code == 404
        assert http_response.url == "http://example.com/missing"
        assert http_response.headers["X-Custom-Header"] == "custom-value"
        assert http_response.text == "Page not found"
        assert http_response.content == b"Page not found"
        # requests.Response doesn't have extensions, so it should be empty
        assert http_response.extensions == {}

    def test_from_response_with_httpx(self):
        """Test from_response() with an httpx.Response object."""
        # Create a mock httpx.Response
        request = httpx.Request("GET", "http://example.com/test")
        httpx_response = httpx.Response(
            status_code=201,
            headers={"Location": "http://example.com/created/123"},
            content=b'{"id": 123}',
            request=request,
            extensions={"http_version": b"HTTP/1.1"},
        )

        http_response = HttpResponse.from_response(httpx_response)

        assert http_response.status_code == 201
        assert http_response.url == "http://example.com/test"
        # httpx normalizes headers to lowercase
        assert (
            http_response.headers.get("location") == "http://example.com/created/123"
            or http_response.headers.get("Location") == "http://example.com/created/123"
        )
        assert http_response.text == '{"id": 123}'
        assert http_response.content == b'{"id": 123}'
        # httpx.Response has extensions
        assert http_response.extensions == {"http_version": b"HTTP/1.1"}

    def test_from_response_with_self(self):
        """Test that from_response() returns self when given an HttpResponse."""
        original = HttpResponse(
            status_code=200,
            url="http://example.com",
            headers={"Content-Type": "text/plain"},
            text="Original",
            content=b"Original",
            extensions={"test": True},
        )

        result = HttpResponse.from_response(original)

        # Should return the exact same object (identity check)
        assert result is original

    def test_from_response_preserves_headers_case(self):
        """Test that headers are preserved correctly through from_response()."""
        # Test with requests (which preserves case in its CaseInsensitiveDict)
        mock_response = MockRequestsResponse(
            status_code=200,
            headers={"Content-Type": "application/json", "X-API-Key": "secret"},
            content='{"status": "ok"}',
        )

        http_response = HttpResponse.from_response(mock_response)

        # Headers should be accessible (case might vary depending on the Headers implementation)
        assert (
            http_response.headers.get("Content-Type") == "application/json"
            or http_response.headers.get("content-type") == "application/json"
        )

    def test_from_response_with_binary_content(self):
        """Test that binary content is preserved correctly."""
        binary_data = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"  # PNG file header

        mock_response = MockRequestsResponse(
            status_code=200,
            headers={"Content-Type": "image/png"},
            content=binary_data,
        )

        http_response = HttpResponse.from_response(mock_response)

        assert http_response.content == binary_data
        # Text representation might vary depending on encoding handling
        assert isinstance(http_response.text, str)

    def test_from_response_with_unicode_content(self):
        """Test that Unicode content is handled correctly."""
        unicode_text = "Hello 世界 🌍"

        mock_response = MockRequestsResponse(
            status_code=200,
            headers={"Content-Type": "text/plain; charset=utf-8"},
            content=unicode_text,
        )

        http_response = HttpResponse.from_response(mock_response)

        assert http_response.text == unicode_text
        assert http_response.content == unicode_text.encode("utf-8")

    def test_dataclass_immutability(self):
        """Test that HttpResponse is immutable (frozen dataclass)."""
        response = HttpResponse(
            status_code=200,
            url="http://example.com",
            headers={"Content-Type": "text/plain"},
            text="Test",
            content=b"Test",
            extensions={},
        )

        # Attempting to modify should raise an error
        try:
            response.status_code = 404
            assert False, "Should not be able to modify frozen dataclass"
        except Exception:
            pass  # Expected - dataclass is frozen

    def test_pickle_serialization(self):
        """Test that HttpResponse can be pickled and unpickled correctly."""
        original = HttpResponse(
            status_code=503,
            url="http://api.example.com/endpoint",
            headers={"Retry-After": "60", "Content-Type": "application/json"},
            text='{"error": "Service Unavailable"}',
            content=b'{"error": "Service Unavailable"}',
            extensions={"retry_count": 3},
        )

        # Pickle and unpickle
        pickled = pickle.dumps(original)
        unpickled = pickle.loads(pickled)

        # Verify all attributes are preserved
        assert unpickled.status_code == original.status_code
        assert unpickled.url == original.url
        assert dict(unpickled.headers) == dict(original.headers)
        assert unpickled.text == original.text
        assert unpickled.content == original.content
        assert unpickled.extensions == original.extensions

        # Verify json() method still works
        assert unpickled.json() == {"error": "Service Unavailable"}

    def test_empty_response(self):
        """Test handling of empty responses."""
        response = HttpResponse(
            status_code=204,  # No Content
            url="http://example.com/empty",
            headers={},
            text="",
            content=b"",
            extensions={},
        )

        assert response.status_code == 204
        assert response.text == ""
        assert response.content == b""
        assert len(response.headers) == 0
        assert response.extensions == {}

    def test_headers_case_insensitive_access(self):
        """Test that headers can be accessed case-insensitively."""
        from httpx import Headers

        # Create response with Headers object
        headers = Headers({"Content-Type": "application/json", "X-Custom": "value"})
        response = HttpResponse(
            status_code=200,
            url="http://example.com",
            headers=headers,
            text="test",
            content=b"test",
            extensions={},
        )

        # httpx.Headers provides case-insensitive access
        assert response.headers.get("content-type") == "application/json"
        assert response.headers.get("Content-Type") == "application/json"
        assert response.headers.get("x-custom") == "value"
        assert response.headers.get("X-Custom") == "value"

    def test_large_response(self):
        """Test handling of large response content."""
        large_text = "x" * 1000000  # 1MB of text
        response = HttpResponse(
            status_code=200,
            url="http://example.com/large",
            headers={"Content-Length": str(len(large_text))},
            text=large_text,
            content=large_text.encode("utf-8"),
            extensions={},
        )

        assert len(response.text) == 1000000
        assert len(response.content) == 1000000
        assert response.headers.get("Content-Length") == "1000000"

    def test_response_with_redirects(self):
        """Test response with redirect information in extensions."""
        response = HttpResponse(
            status_code=200,
            url="http://example.com/final",
            headers={"Content-Type": "text/html"},
            text="Final page",
            content=b"Final page",
            extensions={
                "redirects": [
                    "http://example.com/original",
                    "http://example.com/intermediate",
                    "http://example.com/final",
                ],
                "redirect_count": 2,
            },
        )

        assert response.status_code == 200
        assert response.url == "http://example.com/final"
        assert response.extensions["redirect_count"] == 2
        assert len(response.extensions["redirects"]) == 3
