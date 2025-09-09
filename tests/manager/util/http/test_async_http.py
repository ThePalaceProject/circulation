from collections.abc import AsyncGenerator
from unittest.mock import create_autospec, patch

import httpx
import pytest

from palace.manager.util.http.async_http import AsyncClient
from palace.manager.util.http.exception import BadResponseException
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
    def test___calculate_backoff(self) -> None:
        def assert_within_jitter(
            value: float, expected: float, jitter: float = 0.5
        ) -> None:
            min_jitter = 1 - jitter
            max_jitter = 1 + jitter
            assert expected * min_jitter <= value <= expected * max_jitter

        assert_within_jitter(AsyncClient._calculate_backoff(1, 1, 30), 2.0)
        assert_within_jitter(AsyncClient._calculate_backoff(2, 1, 30), 4.0)
        assert_within_jitter(AsyncClient._calculate_backoff(3, 1, 30), 8.0)

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
