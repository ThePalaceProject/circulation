from collections.abc import AsyncGenerator
from unittest.mock import create_autospec, patch

import httpx
import pytest

from palace.manager.util.http.async_http import (
    WEB_DEFAULT_BACKOFF_FACTOR,
    WEB_DEFAULT_MAX_BACKOFF,
    WEB_DEFAULT_MAX_REDIRECTS,
    WEB_DEFAULT_MAX_RETRIES,
    WEB_DEFAULT_TIMEOUT,
    WORKER_DEFAULT_BACKOFF_FACTOR,
    WORKER_DEFAULT_MAX_BACKOFF,
    WORKER_DEFAULT_MAX_REDIRECTS,
    WORKER_DEFAULT_MAX_RETRIES,
    WORKER_DEFAULT_TIMEOUT,
    AsyncClient,
)
from palace.manager.util.http.exception import (
    BadResponseException,
    RequestNetworkException,
    RequestTimedOut,
)
from tests.fixtures.http import MockAsyncClientFixture
from tests.fixtures.webserver import MockAPIServer, MockAPIServerResponse


class AsyncClientFixture:
    def __init__(self) -> None:
        self.client = AsyncClient.for_worker()

        # Setup the client to have no backoff for testing retries
        self.client._max_retries = 4
        self.client._backoff_factor = 0

        self.mock_client = AsyncClient(client=create_autospec(httpx.AsyncClient))


@pytest.fixture
async def async_client_fixture() -> AsyncGenerator[AsyncClientFixture, None]:
    with patch("palace.manager.util.http.base.manager.__version__", "<VERSION>"):
        fixture = AsyncClientFixture()
        async with fixture.client:
            yield fixture


class TestAsyncClient:
    @pytest.mark.parametrize(
        "method", ["get", "options", "head", "post", "put", "patch", "delete"]
    )
    async def test_all_methods(
        self,
        async_client_fixture: AsyncClientFixture,
        mock_web_server: MockAPIServer,
        method: str,
    ) -> None:
        response = MockAPIServerResponse(200, "test")
        mock_web_server.enqueue_response(method, "/test", response)
        func = getattr(async_client_fixture.client, method)
        resp = await func(mock_web_server.url("/test"))
        assert resp.status_code == 200
        if method != "head":
            assert resp.text == "test"

        requests = mock_web_server.requests()
        assert len(requests) == 1
        request = requests[0]
        assert request.method.lower() == method
        assert request.path == "/test"
        assert request.headers["User-Agent"] == "Palace Manager/<VERSION>"

    async def test_custom_headers(self, mock_web_server: MockAPIServer) -> None:
        response = MockAPIServerResponse(200, "test")
        mock_web_server.enqueue_response("get", "/test", response)

        # You can override the User-Agent header
        async with AsyncClient.for_worker(headers={"User-Agent": "123"}) as client:
            await client.get(mock_web_server.url("/test"))

        assert mock_web_server.latest_request.headers["User-Agent"] == "123"

        # You can set other custom headers, and those will be merged with the default User-Agent
        mock_web_server.enqueue_response("get", "/test", response)
        async with AsyncClient.for_worker(headers={"X-Custom-Header": "456"}) as client:
            await client.get(mock_web_server.url("/test"))

        assert mock_web_server.latest_request.headers["User-Agent"].startswith(
            "Palace Manager/"
        )
        assert mock_web_server.latest_request.headers["X-Custom-Header"] == "456"

        # You can override all the headers at the request level as well
        mock_web_server.enqueue_response("get", "/test", response)
        async with AsyncClient.for_worker(headers={"X-Custom-Header": "456"}) as client:
            await client.get(
                mock_web_server.url("/test"), headers={"User-Agent": "789"}
            )
        assert mock_web_server.latest_request.headers["User-Agent"] == "789"
        assert mock_web_server.latest_request.headers["X-Custom-Header"] == "456"

    async def test_retries(
        self, async_client_fixture: AsyncClientFixture, mock_web_server: MockAPIServer
    ) -> None:
        # The client should retry on 500 errors
        mock_web_server.enqueue_response(
            "get", "/test", MockAPIServerResponse(500, "error")
        )
        mock_web_server.enqueue_response(
            "get", "/test", MockAPIServerResponse(500, "error")
        )
        mock_web_server.enqueue_response(
            "get", "/test", MockAPIServerResponse(200, "success")
        )

        resp = await async_client_fixture.client.get(mock_web_server.url("/test"))
        assert resp.status_code == 200
        assert resp.text == "success"

        requests = mock_web_server.requests()
        assert len(requests) == 3

        # The client should give up after the max retries
        mock_web_server.reset_mock()
        mock_web_server.enqueue_response(
            "get", "/test", MockAPIServerResponse(500, "error1")
        )
        mock_web_server.enqueue_response(
            "get", "/test", MockAPIServerResponse(500, "error2")
        )
        mock_web_server.enqueue_response(
            "get", "/test", MockAPIServerResponse(500, "error3")
        )
        mock_web_server.enqueue_response(
            "get", "/test", MockAPIServerResponse(500, "error4")
        )
        mock_web_server.enqueue_response(
            "get", "/test", MockAPIServerResponse(500, "error5")
        )

        with pytest.raises(BadResponseException) as excinfo:
            await async_client_fixture.client.get(mock_web_server.url("/test"))

        assert excinfo.value.response.status_code == 500
        assert excinfo.value.response.text == "error5"

        requests = mock_web_server.requests()
        assert len(requests) == 5

        # We only retry if the response code is not allowed
        mock_web_server.reset_mock()
        mock_web_server.enqueue_response(
            "get", "/test", MockAPIServerResponse(500, "error1")
        )
        response = await async_client_fixture.client.get(
            mock_web_server.url("/test"), allowed_response_codes=[500]
        )
        assert response.status_code == 500
        assert response.text == "error1"
        assert len(mock_web_server.requests()) == 1

    async def test_client_default_headers(
        self, async_http_client: MockAsyncClientFixture
    ) -> None:
        """Test that default headers are added by the client."""
        async_http_client.queue_response(200, content="success")

        async with AsyncClient.for_web() as client:
            await client.get("https://example.com/test")

        request = async_http_client.requests[0]
        assert "User-Agent" in request.headers
        assert request.headers["User-Agent"].startswith("Palace Manager/")

    async def test_client_level_headers(
        self, async_http_client: MockAsyncClientFixture
    ) -> None:
        """Test that headers set at client creation are included in requests."""
        async_http_client.queue_response(200, content="success")

        async with AsyncClient.for_web(
            headers={"X-Client-Header": "client-value"}
        ) as client:
            response = await client.get("https://example.com/test")

        assert response.status_code == 200
        assert response.text == "success"

        request = async_http_client.requests[0]
        assert request == response.request
        assert request.headers["X-Client-Header"] == "client-value"
        assert "User-Agent" in request.headers  # Default headers still present

    async def test_request_level_headers(
        self, async_http_client: MockAsyncClientFixture
    ) -> None:
        """Test that headers can be set on individual requests."""
        async_http_client.queue_response(200, content="success")

        async with AsyncClient.for_web() as client:
            await client.get(
                "https://example.com/test",
                headers={"X-Request-Header": "request-value"},
            )

        request = async_http_client.requests[0]
        assert request.headers["X-Request-Header"] == "request-value"
        assert "User-Agent" in request.headers  # Default headers still present

    async def test_request_headers_override_client_headers(
        self, async_http_client: MockAsyncClientFixture
    ) -> None:
        """Test that request-level headers override client-level headers for the same key."""
        async_http_client.queue_response(200, content="success")

        async with AsyncClient.for_web(
            headers={"X-Custom-Header": "client-value"}
        ) as client:
            await client.get(
                "https://example.com/test", headers={"X-Custom-Header": "request-value"}
            )

        request = async_http_client.requests[0]
        assert (
            request.headers["X-Custom-Header"] == "request-value"
        )  # Request overrides client

    async def test_request_headers_override_default_headers(
        self, async_http_client: MockAsyncClientFixture
    ) -> None:
        """Test that request-level headers can override default headers like User-Agent."""
        async_http_client.queue_response(200, content="success")

        async with AsyncClient.for_web() as client:
            await client.get(
                "https://example.com/test", headers={"User-Agent": "Custom Agent"}
            )

        request = async_http_client.requests[0]
        assert request.headers["User-Agent"] == "Custom Agent"

    async def test_header_precedence_all_levels(
        self, async_http_client: MockAsyncClientFixture
    ) -> None:
        """Test header precedence: request > client > default."""
        async_http_client.queue_response(200, content="success")

        # Set up headers at all levels for different keys
        async with AsyncClient.for_web(
            headers={
                "X-Client-Header": "from-client",
                "X-Override-Header": "from-client",
                "User-Agent": "from-client",
            }
        ) as client:
            await client.get(
                "https://example.com/test",
                headers={
                    "X-Request-Header": "from-request",
                    "X-Override-Header": "from-request",
                    "User-Agent": "from-request",
                },
            )

        request = async_http_client.requests[0]

        # Client-level header should be present
        assert request.headers["X-Client-Header"] == "from-client"

        # Request-level header should be present
        assert request.headers["X-Request-Header"] == "from-request"

        # Request-level should override client-level for same key
        assert request.headers["X-Override-Header"] == "from-request"

        # Request-level should override default User-Agent
        assert request.headers["User-Agent"] == "from-request"

    async def test_multiple_requests_different_headers(
        self, async_http_client: MockAsyncClientFixture
    ) -> None:
        """Test that each request can have different headers."""
        async_http_client.queue_response(200, content="first")
        async_http_client.queue_response(200, content="second")

        async with AsyncClient.for_web(headers={"X-Client-Header": "shared"}) as client:
            # First request with one set of headers
            await client.get(
                "https://example.com/first", headers={"X-Request-1": "value1"}
            )

            # Second request with different headers
            await client.get(
                "https://example.com/second", headers={"X-Request-2": "value2"}
            )

        # Check first request
        first_request = async_http_client.requests[0]
        assert first_request.headers["X-Client-Header"] == "shared"
        assert first_request.headers["X-Request-1"] == "value1"
        assert "X-Request-2" not in first_request.headers

        # Check second request
        second_request = async_http_client.requests[1]
        assert second_request.headers["X-Client-Header"] == "shared"
        assert second_request.headers["X-Request-2"] == "value2"
        assert "X-Request-1" not in second_request.headers

    async def test_for_web_defaults(
        self, async_http_client: MockAsyncClientFixture
    ) -> None:
        """Test that AsyncClient.for_web sets appropriate defaults for web requests."""
        async_http_client.queue_response(200, content="success")

        client = AsyncClient.for_web()

        # Check that the AsyncClient wrapper has the correct defaults
        assert client._max_retries == WEB_DEFAULT_MAX_RETRIES
        assert client._backoff_factor == WEB_DEFAULT_BACKOFF_FACTOR
        assert client._max_backoff == WEB_DEFAULT_MAX_BACKOFF

        # Check that the underlying httpx client has the correct configuration
        assert client._httpx_client.timeout == WEB_DEFAULT_TIMEOUT
        assert client._httpx_client.max_redirects == WEB_DEFAULT_MAX_REDIRECTS
        assert client._httpx_client.follow_redirects is True

        # Verify default headers are set
        response = await client.get("https://example.com/test")
        assert response.status_code == 200

        request = async_http_client.requests[0]
        assert "User-Agent" in request.headers
        assert request.headers["User-Agent"].startswith("Palace Manager/")

        await client.aclose()

    async def test_for_worker_defaults(
        self, async_http_client: MockAsyncClientFixture
    ) -> None:
        """Test that AsyncClient.for_worker sets appropriate defaults for background tasks."""

        async_http_client.queue_response(200, content="success")

        client = AsyncClient.for_worker()

        # Check that the AsyncClient wrapper has the correct defaults
        assert client._max_retries == WORKER_DEFAULT_MAX_RETRIES
        assert client._backoff_factor == WORKER_DEFAULT_BACKOFF_FACTOR
        assert client._max_backoff == WORKER_DEFAULT_MAX_BACKOFF

        # Check that the underlying httpx client has the correct configuration
        assert client._httpx_client.timeout == WORKER_DEFAULT_TIMEOUT
        assert client._httpx_client.max_redirects == WORKER_DEFAULT_MAX_REDIRECTS
        assert client._httpx_client.follow_redirects is True

        # Verify default headers are set
        response = await client.get("https://example.com/test")
        assert response.status_code == 200

        request = async_http_client.requests[0]
        assert "User-Agent" in request.headers
        assert request.headers["User-Agent"].startswith("Palace Manager/")

        await client.aclose()

    async def test_for_web_vs_for_worker_differences(
        self, async_http_client: MockAsyncClientFixture
    ) -> None:
        """Test the differences between web and worker client configurations."""

        async with (
            AsyncClient.for_web() as web_client,
            AsyncClient.for_worker() as worker_client,
        ):
            # Web client should have shorter timeouts and fewer retries for responsiveness
            web_connect_timeout = web_client._httpx_client.timeout.connect
            worker_connect_timeout = worker_client._httpx_client.timeout.connect
            assert web_connect_timeout is not None
            assert worker_connect_timeout is not None
            assert web_connect_timeout < worker_connect_timeout
            assert web_client._max_retries < worker_client._max_retries
            assert (
                web_client._httpx_client.max_redirects
                < worker_client._httpx_client.max_redirects
            )

            # Verify the specific values match our constants
            assert web_client._httpx_client.timeout == WEB_DEFAULT_TIMEOUT
            assert web_client._httpx_client.max_redirects == WEB_DEFAULT_MAX_REDIRECTS
            assert web_client._max_retries == WEB_DEFAULT_MAX_RETRIES

            assert worker_client._httpx_client.timeout == WORKER_DEFAULT_TIMEOUT
            assert (
                worker_client._httpx_client.max_redirects
                == WORKER_DEFAULT_MAX_REDIRECTS
            )
            assert worker_client._max_retries == WORKER_DEFAULT_MAX_RETRIES

    async def test_factory_methods_accept_overrides(
        self, async_http_client: MockAsyncClientFixture
    ) -> None:
        """Test that factory methods allow overriding default parameters."""
        async_http_client.queue_response(200, content="success")
        async_http_client.queue_response(200, content="success")

        # Test overriding httpx client parameters
        custom_timeout = httpx.Timeout(10.0)
        custom_headers = {"X-Custom": "override-test"}

        async with (
            AsyncClient.for_web(
                timeout=custom_timeout, headers=custom_headers, max_redirects=99
            ) as web_client,
            AsyncClient.for_worker(
                allowed_response_codes=[200, 201], disallowed_response_codes=[404, 500]
            ) as worker_client,
        ):
            # Verify overrides took effect for web client
            assert web_client._httpx_client.timeout == custom_timeout
            assert web_client._httpx_client.max_redirects == 99

            # Verify overrides took effect for worker client
            assert worker_client._allowed_response_codes == [200, 201]
            assert worker_client._disallowed_response_codes == [404, 500]

            # Test that custom headers are merged properly
            response = await web_client.get("https://example.com/test")
            request = async_http_client.requests[0]
            assert request.headers["X-Custom"] == "override-test"
            assert "User-Agent" in request.headers  # Default headers still present

            # Test that the worker client still works with custom response codes
            response = await worker_client.get("https://example.com/test")
            assert response.status_code == 200

    async def test_timeout_exception_wrapping(
        self, async_http_client: MockAsyncClientFixture
    ) -> None:
        """Test that httpx.TimeoutException is wrapped as RequestTimedOut."""
        # Queue a timeout exception
        timeout_exc = httpx.TimeoutException("Connection timed out")
        async_http_client.queue_exception(timeout_exc)

        async with AsyncClient.for_web() as client:
            with pytest.raises(RequestTimedOut) as exc_info:
                await client.get("https://example.com/test")

        # Verify the wrapped exception details
        wrapped_exc = exc_info.value
        assert "https://example.com/test" in str(wrapped_exc)
        assert "Connection timed out" in str(wrapped_exc)
        assert wrapped_exc.__cause__ == timeout_exc  # Original exception is preserved

        # Verify request was captured before exception
        assert len(async_http_client.requests) == 1
        assert str(async_http_client.requests[0].url) == "https://example.com/test"

    async def test_connect_error_wrapping(
        self, async_http_client: MockAsyncClientFixture
    ) -> None:
        """Test that httpx.ConnectError is wrapped as RequestNetworkException."""
        # Queue a connection error
        connect_exc = httpx.ConnectError("Connection refused")
        async_http_client.queue_exception(connect_exc)

        async with AsyncClient.for_web() as client:
            with pytest.raises(RequestNetworkException) as exc_info:
                await client.get("https://example.com/test")

        # Verify the wrapped exception details
        wrapped_exc = exc_info.value
        assert "https://example.com/test" in str(wrapped_exc)
        assert "Connection refused" in str(wrapped_exc)
        assert wrapped_exc.__cause__ == connect_exc  # Original exception is preserved

    async def test_read_error_wrapping(
        self, async_http_client: MockAsyncClientFixture
    ) -> None:
        """Test that httpx.ReadError is wrapped as RequestNetworkException."""
        # Queue a read error
        read_exc = httpx.ReadError("Connection broken: Invalid chunk encoding")
        async_http_client.queue_exception(read_exc)

        async with AsyncClient.for_web() as client:
            with pytest.raises(RequestNetworkException) as exc_info:
                await client.get("https://example.com/test")

        # Verify the wrapped exception details
        wrapped_exc = exc_info.value
        assert "https://example.com/test" in str(wrapped_exc)
        assert "Invalid chunk encoding" in str(wrapped_exc)
        assert wrapped_exc.__cause__ == read_exc

    async def test_pool_timeout_wrapping(
        self, async_http_client: MockAsyncClientFixture
    ) -> None:
        """Test that httpx.PoolTimeout is wrapped as RequestTimedOut."""
        # Queue a pool timeout (subclass of TimeoutException)
        pool_timeout_exc = httpx.PoolTimeout("Pool timeout")
        async_http_client.queue_exception(pool_timeout_exc)

        async with AsyncClient.for_web() as client:
            with pytest.raises(RequestTimedOut) as exc_info:
                await client.get("https://example.com/test")

        # Verify the wrapped exception details
        wrapped_exc = exc_info.value
        assert "https://example.com/test" in str(wrapped_exc)
        assert "Pool timeout" in str(wrapped_exc)
        assert wrapped_exc.__cause__ == pool_timeout_exc

    async def test_exception_then_success(
        self, async_http_client: MockAsyncClientFixture
    ) -> None:
        """Test that we can queue an exception followed by a successful response."""
        # Queue an exception first, then a successful response
        async_http_client.queue_exception(httpx.ConnectError("First request fails"))
        async_http_client.queue_response(200, content="Second request succeeds")

        async with AsyncClient.for_web() as client:
            # First request should raise exception
            with pytest.raises(RequestNetworkException):
                await client.get("https://example.com/first")

            # Second request should succeed
            response = await client.get("https://example.com/second")
            assert response.status_code == 200
            assert response.text == "Second request succeeds"

        # Both requests should be captured
        assert len(async_http_client.requests) == 2
        assert str(async_http_client.requests[0].url) == "https://example.com/first"
        assert str(async_http_client.requests[1].url) == "https://example.com/second"

    async def test_mixed_queue_order(
        self, async_http_client: MockAsyncClientFixture
    ) -> None:
        """Test that exceptions and responses are processed in the correct order."""
        # Queue responses and exceptions in a specific order
        async_http_client.queue_response(200, content="First success")
        async_http_client.queue_exception(httpx.TimeoutException("Second fails"))
        async_http_client.queue_response(201, content="Third success")

        async with AsyncClient.for_web() as client:
            # First request succeeds
            response1 = await client.get("https://example.com/1")
            assert response1.status_code == 200
            assert response1.text == "First success"

            # Second request times out
            with pytest.raises(RequestTimedOut):
                await client.get("https://example.com/2")

            # Third request succeeds
            response3 = await client.get("https://example.com/3")
            assert response3.status_code == 201
            assert response3.text == "Third success"

        # All requests should be captured
        assert len(async_http_client.requests) == 3
