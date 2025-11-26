import json
import pickle

import httpx
from httpx import Headers

from palace.manager.integration.license.opds.exception import OpdsResponseException
from palace.manager.util.http.exception import (
    BadResponseException,
    RemoteIntegrationException,
    RequestNetworkException,
    RequestTimedOut,
    ResponseData,
)
from tests.mocks.mock import MockRequestsResponse


class TestRemoteIntegrationException:
    def test_with_service_name(self):
        """You don't have to provide a URL when creating a
        RemoteIntegrationException; you can just provide the service
        name.
        """
        exc = RemoteIntegrationException(
            "Unreliable Service", "I just can't handle your request right now."
        )

        details = exc.document_detail()
        assert (
            "The server tried to access Unreliable Service but the third-party service experienced an error."
            == details
        )

        debug_details = exc.document_debug_message()
        assert (
            "Error accessing Unreliable Service: I just can't handle your request right now."
            == debug_details
        )

        assert str(exc) == debug_details

        assert exc.problem_detail.title == "Failure contacting external service"
        assert exc.problem_detail.detail == details
        assert exc.problem_detail.debug_message == debug_details

    def test_with_service_url(self):
        # If you do provide a URL, it's included in the error message.
        exc = RemoteIntegrationException(
            "http://unreliable-service/",
            "I just can't handle your request right now.",
        )

        # The url isn't included in the main details
        details = exc.document_detail()
        assert (
            "The server tried to access unreliable-service but the third-party service experienced an error."
            == details
        )

        # But it is included in the debug details.
        debug_details = exc.document_debug_message()
        assert (
            "Error accessing http://unreliable-service/: I just can't handle your request right now."
            == debug_details
        )

        assert str(exc) == debug_details

        assert exc.problem_detail.title == "Failure contacting external service"
        assert exc.problem_detail.detail == details
        assert exc.problem_detail.debug_message == debug_details

    def test_with_debug_message(self):
        # If you provide a debug message, it's included in the debug details.
        exc = RemoteIntegrationException(
            "http://unreliable-service/",
            "I just can't handle your request right now.",
            "technical details",
        )
        details = exc.document_detail()
        assert (
            "The server tried to access unreliable-service but the third-party service experienced an error."
            == details
        )

        debug_details = exc.document_debug_message()
        assert (
            "Error accessing http://unreliable-service/: I just can't handle your request right now.\n\ntechnical details"
            == debug_details
        )


class TestBadResponseException:
    def test__init__(self):
        response = MockRequestsResponse(102, content="nonsense")
        exc = BadResponseException(
            "http://url/", "Terrible response, just terrible", response
        )

        # the response gets set on the exception
        assert exc.response.status_code == response.status_code
        assert exc.response.content == response.content
        assert exc.response.headers == response.headers
        assert exc.response.url == response.url

        # Turn the exception into a problem detail document, and it's full
        # of useful information.
        problem_detail = exc.problem_detail

        assert problem_detail.title == "Bad response"
        assert (
            problem_detail.detail
            == "The server made a request to url, and got an unexpected or invalid response."
        )
        assert (
            problem_detail.debug_message
            == "Bad response from http://url/: Terrible response, just terrible\n\nStatus code: 102\nContent: nonsense"
        )
        assert problem_detail.status_code == 502

    def test_retry_count(self):
        """Test that retry_count is tracked properly."""
        response = MockRequestsResponse(500, content="Server Error")

        # Test without retry_count (should default to None)
        exc_no_retry = BadResponseException("http://url/", "Error message", response)
        assert exc_no_retry.retry_count is None

        # Test with explicit retry_count of 0
        exc_zero_retries = BadResponseException(
            "http://url/", "Error message", response, retry_count=0
        )
        assert exc_zero_retries.retry_count == 0

        # Test with retry_count > 0
        exc_with_retries = BadResponseException(
            "http://url/", "Error message", response, retry_count=3
        )
        assert exc_with_retries.retry_count == 3

    def test_bad_status_code(self):
        response = MockRequestsResponse(500, content="Internal Server Error!")
        exc = BadResponseException.bad_status_code("http://url/", response)
        doc = exc.problem_detail

        assert doc.title == "Bad response"
        assert (
            doc.detail
            == "The server made a request to url, and got an unexpected or invalid response."
        )
        assert (
            doc.debug_message
            == "Bad response from http://url/: Got status code 500 from external server, cannot continue.\n\nStatus code: 500\nContent: Internal Server Error!"
        )

    def test_problem_detail(self):
        response = MockRequestsResponse(401, content="You are not authorized!")
        exception = BadResponseException(
            "http://url/",
            "What even is this",
            debug_message="some debug info",
            response=response,
        )
        document = exception.problem_detail
        assert 502 == document.status_code
        assert "Bad response" == document.title
        assert (
            "The server made a request to url, and got an unexpected or invalid response."
            == document.detail
        )
        assert (
            "Bad response from http://url/: What even is this\n\nsome debug info"
            == document.debug_message
        )

    def test_pickle_preserves_type(self):
        """Test that BadResponseException maintains its type when pickled/unpickled.

        This test reproduces the issue seen in Celery where exceptions are
        serialized for retry handling and lose their type information, appearing
        as IntegrationException instead of BadResponseException.
        """
        response = MockRequestsResponse(
            401, headers={"Content-Type": "application/json"}, content="Unauthorized"
        )
        original_exc = BadResponseException(
            "http://url/", "Auth failed", response, debug_message="Debug info"
        )

        # Pickle and unpickle (simulates what Celery does during autoretry)
        pickled = pickle.dumps(original_exc)
        unpickled_exc = pickle.loads(pickled)

        # Verify type is preserved
        assert type(unpickled_exc).__name__ == "BadResponseException"
        assert isinstance(unpickled_exc, BadResponseException)

        # Verify attributes are preserved
        assert unpickled_exc.message == "Auth failed"
        assert unpickled_exc.debug_message == "Debug info"
        assert unpickled_exc.response.status_code == 401
        assert unpickled_exc.response.content == b"Unauthorized"
        assert unpickled_exc.response.headers.get("Content-Type") == "application/json"
        assert unpickled_exc.response.headers.get("content-type") == "application/json"

        assert str(unpickled_exc) == str(original_exc)

    def test_pickle_preserves_subclass_type(self):
        """Test that subclasses of BadResponseException also preserve their type.

        We need to import a real subclass to test this properly.
        """
        response = MockRequestsResponse(
            400, content="Bad Request", headers={"Content-Type": "application/json"}
        )
        original_exc = OpdsResponseException(
            type="http://example.com/problem",
            title="Test Error",
            status=400,
            detail="Something went wrong",
            response=response,
        )

        # Pickle and unpickle
        pickled = pickle.dumps(original_exc)
        unpickled_exc = pickle.loads(pickled)

        # Verify the subclass type is preserved
        assert type(unpickled_exc).__name__ == "OpdsResponseException"
        assert isinstance(unpickled_exc, OpdsResponseException)

        # Verify subclass-specific attributes are preserved
        assert unpickled_exc.type == "http://example.com/problem"
        assert unpickled_exc.title == "Test Error"
        assert unpickled_exc.status == 400
        assert unpickled_exc.detail == "Something went wrong"

        # Verify inherited attributes are preserved
        assert unpickled_exc.response.status_code == 400

        assert str(unpickled_exc) == str(original_exc)


class TestRequestTimedOut:
    def test_problem_detail(self):
        exception = RequestTimedOut("http://url/", "I give up")

        detail = exception.problem_detail
        assert "Timeout" == detail.title
        assert (
            "The server made a request to url, and that request timed out."
            == detail.detail
        )
        assert detail.status_code == 502
        assert detail.debug_message == "Timeout accessing http://url/: I give up"

    def test_retry_count(self):
        """Test that retry_count is tracked properly."""
        # Test without retry_count (should default to None)
        exc_no_retry = RequestTimedOut("http://url/", "Timeout occurred")
        assert exc_no_retry.retry_count is None

        # Test with explicit retry_count of 0
        exc_zero_retries = RequestTimedOut(
            "http://url/", "Timeout occurred", retry_count=0
        )
        assert exc_zero_retries.retry_count == 0

        # Test with retry_count > 0
        exc_with_retries = RequestTimedOut(
            "http://url/", "Timeout occurred", retry_count=2
        )
        assert exc_with_retries.retry_count == 2


class TestRequestNetworkException:
    def test_problem_detail(self):
        exception = RequestNetworkException("http://url/", "Colossal failure")

        detail = exception.problem_detail
        assert "Network failure contacting third-party service" == detail.title
        assert (
            "The server experienced a network error while contacting url."
            == detail.detail
        )
        assert detail.status_code == 502
        assert (
            detail.debug_message
            == "Network error contacting http://url/: Colossal failure"
        )

    def test_pickle_preserves_type(self):
        """Test that RequestNetworkException maintains its type when pickled/unpickled.

        This test ensures that when exceptions are serialized (e.g., during Celery
        retry handling), they maintain their type information and attributes.
        """
        original_exc = RequestNetworkException(
            "http://api.example.com/endpoint",
            "Connection refused",
            debug_message="Unable to establish connection",
        )

        # Pickle and unpickle (simulates what Celery does during autoretry)
        pickled = pickle.dumps(original_exc)
        unpickled_exc = pickle.loads(pickled)

        # Verify type is preserved
        assert type(unpickled_exc).__name__ == "RequestNetworkException"
        assert isinstance(unpickled_exc, RequestNetworkException)

        # Verify attributes are preserved
        assert unpickled_exc.url == "http://api.example.com/endpoint"
        assert unpickled_exc.service == "api.example.com"
        assert unpickled_exc.message == "Connection refused"
        assert unpickled_exc.debug_message == "Unable to establish connection"

        # Verify string representation is preserved
        assert str(unpickled_exc) == str(original_exc)


class TestResponseData:
    """Tests for the TestResponseData dataclass."""

    def test_basic_attributes(self):
        """Test that HttpResponse correctly stores basic attributes."""
        response = ResponseData(
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
        response = ResponseData(
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
        response = ResponseData(
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

        http_response = ResponseData.from_response(mock_response)

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

        http_response = ResponseData.from_response(httpx_response)

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
        original = ResponseData(
            status_code=200,
            url="http://example.com",
            headers={"Content-Type": "text/plain"},
            text="Original",
            content=b"Original",
            extensions={"test": True},
        )

        result = ResponseData.from_response(original)

        # Should return the exact same object (identity check)
        assert result is original

    def test_from_response_with_binary_content(self):
        """Test that binary content is preserved correctly."""
        binary_data = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"  # PNG file header

        mock_response = MockRequestsResponse(
            status_code=200,
            headers={"Content-Type": "image/png"},
            content=binary_data,
        )

        http_response = ResponseData.from_response(mock_response)

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

        http_response = ResponseData.from_response(mock_response)

        assert http_response.text == unicode_text
        assert http_response.content == unicode_text.encode("utf-8")

    def test_pickle_serialization(self):
        """Test that HttpResponse can be pickled and unpickled correctly."""
        original = ResponseData(
            status_code=503,
            url="http://api.example.com/endpoint",
            headers=Headers({"Retry-After": "60", "Content-Type": "application/json"}),
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
        assert unpickled.headers == original.headers
        assert unpickled.text == original.text
        assert unpickled.content == original.content
        assert unpickled.extensions == original.extensions

        # Verify json() method still works
        assert unpickled.json() == {"error": "Service Unavailable"}

    def test_empty_response(self):
        """Test handling of empty responses."""
        response = ResponseData(
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
        response = ResponseData(
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
