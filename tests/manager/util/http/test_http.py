import functools
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any
from unittest import mock
from unittest.mock import MagicMock, create_autospec

import pytest
import requests
from requests import Response, Session
from requests.adapters import HTTPAdapter
from requests_mock import Mocker

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.core.problem_details import INTEGRATION_ERROR, INVALID_INPUT
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.util.http.base import raise_for_bad_response
from palace.manager.util.http.exception import (
    BadResponseException,
    RequestNetworkException,
    RequestTimedOut,
)
from palace.manager.util.http.http import (
    HTTP,
    BearerAuth,
)
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException
from tests.fixtures.webserver import MockAPIServer, MockAPIServerResponse
from tests.mocks.mock import MockRequestsResponse


@dataclass
class HttpTestFixture:
    server: MockAPIServer
    request_with_timeout: Callable[..., requests.Response]


@pytest.fixture
def test_http_fixture(mock_web_server: MockAPIServer) -> HttpTestFixture:
    # Make sure we don't wait for retries, as that will slow down the tests.
    request_with_timeout = functools.partial(
        HTTP.request_with_timeout, timeout=1, backoff_factor=0
    )
    return HttpTestFixture(
        server=mock_web_server, request_with_timeout=request_with_timeout
    )


class FakeRequest:
    def __init__(self, response: Response | None = None) -> None:
        self.agent: str | None = None
        self.args: Sequence[Any] | None = None
        self.kwargs: Mapping[str, Any] | None = None
        self.response = response or MockRequestsResponse(201)

    def fake_request(self, *args: Any, **kwargs: Any) -> Response:
        self.agent = kwargs["headers"]["User-Agent"]
        self.args = args
        self.kwargs = kwargs
        return self.response


class TestHTTP:
    def test_session(self) -> None:
        # If supplied with a max_retry_count and backoff_factor, they are used to
        # create and mount a requests HTTPAdapter.
        result = HTTP.session(max_retry_count=3, backoff_factor=2.0)
        https_adapter = result.get_adapter("https://fake.url")
        http_adapter = result.get_adapter("http://fake.url")
        # Both protocols use the same adapter.
        assert https_adapter is http_adapter
        assert isinstance(http_adapter, HTTPAdapter)
        assert http_adapter.max_retries.total == 3
        assert http_adapter.max_retries.backoff_factor == 2.0

        # If not supplied, the default values from the class are used.
        result = HTTP.session()
        adapter = result.get_adapter("https://fake.url")
        assert isinstance(adapter, HTTPAdapter)
        assert adapter.max_retries.total == HTTP.DEFAULT_REQUEST_RETRIES
        assert adapter.max_retries.backoff_factor == HTTP.DEFAULT_BACKOFF_FACTOR

    def test_request_with_timeout_defaults(self) -> None:
        with (
            mock.patch.object(HTTP, "DEFAULT_REQUEST_TIMEOUT", 10),
            mock.patch.object(HTTP, "session", autospec=True) as mock_session,
        ):
            mock_ctx = mock_session.return_value.__enter__.return_value
            mock_request = mock_ctx.request
            HTTP.request_with_timeout("GET", "url")

            # We create a session with none for parameters, which we know
            # from test_session above, uses the default values.
            mock_session.assert_called_once_with(
                max_retry_count=None, backoff_factor=None
            )

            # The request has a timeout
            mock_request.assert_called_once_with(
                "GET", "url", headers=mock.ANY, timeout=10
            )

    @mock.patch("palace.manager.util.http.base.manager.__version__", "<VERSION>")
    def test_request_with_timeout_success(self) -> None:
        request = FakeRequest(MockRequestsResponse(200, content="Success!"))
        response = HTTP._request_with_timeout(
            "GET", "http://url/", make_request_with=request.fake_request, kwarg="value"  # type: ignore[call-arg]
        )
        assert response.status_code == 200
        assert response.content == b"Success!"

        # User agent header should be set
        assert request.agent == "Palace Manager/<VERSION>"

        # The HTTP method and URL are passed in the order
        # requests.request would expect.
        assert ("GET", "http://url/") == request.args

        # Keyword arguments to _request_with_timeout are passed in
        # as-is.
        assert request.kwargs is not None
        assert request.kwargs["kwarg"] == "value"

        # A default timeout is added.
        assert request.kwargs["timeout"] == 20

    def test_request_with_timeout_with_ua(self) -> None:
        request = FakeRequest()
        assert (
            HTTP._request_with_timeout(
                "GET",
                "http://url",
                make_request_with=request.fake_request,
                headers={"User-Agent": "Fake Agent"},
            ).status_code
            == 201
        )
        assert request.agent == "Fake Agent"

    def test_request_with_timeout_session_and_retries(self) -> None:
        """
        If a session is provided, and we set the max_retry_count and backoff_factor on
        the request, we get a PalaceValueError.
        """
        session = HTTP.session()
        with pytest.raises(
            PalaceValueError,
            match="Cannot set 'max_retry_count', 'backoff_factor' when 'make_request_with' is a Session.",
        ):
            HTTP.request_with_timeout(
                "GET",
                "http://url",
                make_request_with=session,
                max_retry_count=3,
                backoff_factor=2.0,
            )

        with pytest.raises(
            PalaceValueError,
            match="Cannot set 'max_retry_count' when 'make_request_with' is a Session.",
        ):
            HTTP.request_with_timeout(
                "GET",
                "http://url",
                make_request_with=session,
                max_retry_count=3,
            )

        with pytest.raises(
            PalaceValueError,
            match="Cannot set 'backoff_factor' when 'make_request_with' is a Session.",
        ):
            HTTP.request_with_timeout(
                "GET",
                "http://url",
                make_request_with=session,
                backoff_factor=2.0,
            )

    def test_request_with_timeout_session(self) -> None:
        """
        If a session is provided, we use it to make the request.
        """
        result = MockRequestsResponse(201, content="Success!")
        mock_session = create_autospec(Session)
        mock_session.request.return_value = result

        response = HTTP.request_with_timeout(
            "GET", "http://url", make_request_with=mock_session
        )
        assert response is result

        # The session's request method was called with the correct parameters.
        mock_session.request.assert_called_once_with(
            "GET",
            "http://url",
            headers=mock.ANY,
            timeout=20,
        )

    @mock.patch("palace.manager.util.http.base.manager.__version__", None)
    def test_default_user_agent(self) -> None:
        request = FakeRequest()
        assert (
            HTTP._request_with_timeout(
                "DELETE", "/", make_request_with=request.fake_request
            ).status_code
            == 201
        )
        assert request.agent == "Palace Manager/x.x.x"

        # User agent is still set if headers are None
        assert (
            HTTP._request_with_timeout(
                "GET", "/", make_request_with=request.fake_request, headers=None
            ).status_code
            == 201
        )
        assert request.agent == "Palace Manager/x.x.x"

        # The headers are not modified if they are passed into the function
        original_headers = {"header": "value"}
        assert (
            HTTP._request_with_timeout(
                "GET",
                "/",
                make_request_with=request.fake_request,
                headers=original_headers,
            ).status_code
            == 201
        )
        assert request.agent == "Palace Manager/x.x.x"
        assert original_headers == {"header": "value"}

    def test_request_with_timeout_failure(self) -> None:
        def immediately_timeout(*args, **kwargs) -> Response:
            raise requests.exceptions.Timeout("I give up")

        with pytest.raises(
            RequestTimedOut, match="Timeout accessing http://url/: I give up"
        ):
            HTTP._request_with_timeout(
                "PUT", "http://url/", make_request_with=immediately_timeout
            )

    @mock.patch("palace.manager.util.http.base.manager.__version__", None)
    def test_request_with_timeout_verbose(self, caplog: pytest.LogCaptureFixture):
        """
        When the verbose flag is set, we log the request and response.
        """
        caplog.set_level(LogLevel.info)
        mock_raise_for_bad_response = create_autospec(raise_for_bad_response)
        make_request = MagicMock(
            return_value=MockRequestsResponse(
                204, headers={"test": "response header"}, content="Success!"
            )
        )

        response = HTTP._request_with_timeout(
            "POST",
            "http://url/",
            make_request_with=make_request,
            process_response_with=mock_raise_for_bad_response,
            verbose=True,
            headers={"header": "value"},
        )

        assert make_request.call_count == 1
        assert make_request.call_args.args == ("POST", "http://url/")

        assert mock_raise_for_bad_response.call_count == 1
        assert response == mock_raise_for_bad_response.return_value

        assert (
            "Sending POST request to http://url/: kwargs {'headers': {'User-Agent':"
            " 'Palace Manager/x.x.x', 'header': 'value'}, 'timeout': 20}"
        ) in caplog.messages
        assert (
            "Response from http://url/: 204 {'test': 'response header'} b'Success!'"
            in caplog.messages
        )

    def test_request_with_network_failure(self) -> None:
        def immediately_fail(*args, **kwargs) -> Response:
            raise requests.exceptions.ConnectionError("a disaster")

        with pytest.raises(
            RequestNetworkException,
            match="Network error contacting http://url/: a disaster",
        ):
            HTTP._request_with_timeout(
                "POST", "http://url/", make_request_with=immediately_fail
            )

    def test_request_with_response_indicative_of_failure(self) -> None:
        def fake_500_response(*args, **kwargs) -> Response:
            return MockRequestsResponse(500, content="Failure!")

        with pytest.raises(
            BadResponseException,
            match="Bad response from http://url/: Got status code 500 from external server",
        ):
            HTTP._request_with_timeout(
                "GET", "http://url/", make_request_with=fake_500_response
            )

    def test_allowed_response_codes(self) -> None:
        """Test our ability to raise BadResponseException when
        an HTTP-based integration does not behave as we'd expect.
        """

        def fake_401_response(*args, **kwargs) -> Response:
            return MockRequestsResponse(401, content="Weird")

        def fake_200_response(*args, **kwargs) -> Response:
            return MockRequestsResponse(200, content="Hurray")

        url = "http://url/"
        request = partial(HTTP._request_with_timeout, "GET", url)

        # By default, every code except for 5xx codes is allowed.
        response = request(make_request_with=fake_401_response)
        assert response.status_code == 401

        # You can say that certain codes are specifically allowed, and
        # all others are forbidden.
        with pytest.raises(
            BadResponseException,
            match="Bad response from http://url/: Got status code 401 from external server, but can only continue on: 200, 201.",
        ):
            request(
                make_request_with=fake_401_response, allowed_response_codes=[201, 200]
            )

        response = request(
            make_request_with=fake_401_response, allowed_response_codes=[401]
        )
        response = request(
            make_request_with=fake_401_response, allowed_response_codes=["4xx"]
        )

        # In this way you can even raise an exception on a 200 response code.
        with pytest.raises(
            BadResponseException,
            match="Bad response from http://url/: Got status code 200 from external server, but can only continue on: 401.",
        ):
            request(make_request_with=fake_200_response, allowed_response_codes=[401])

        # You can say that certain codes are explicitly forbidden, and
        # all others are allowed.
        with pytest.raises(
            BadResponseException,
            match="Bad response from http://url/: Got status code 401 from external server, cannot continue.",
        ) as excinfo:
            request(
                make_request_with=fake_401_response, disallowed_response_codes=[401]
            )

        with pytest.raises(
            BadResponseException,
            match="Bad response from http://url/: Got status code 200 from external server, cannot continue.",
        ):
            request(
                make_request_with=fake_200_response,
                disallowed_response_codes=["2xx", 301],
            )

        response = request(
            make_request_with=fake_401_response, disallowed_response_codes=["2xx"]
        )
        assert response.status_code == 401

        # The exception can be turned into a useful problem detail document.
        with pytest.raises(BadResponseException) as exc_info:
            request(
                make_request_with=fake_200_response, disallowed_response_codes=["2xx"]
            )

        problem_detail = exc_info.value.problem_detail

        # 502 is the status code to be returned if this integration error
        # interrupts the processing of an incoming HTTP request, not the
        # status code that caused the problem.
        #
        assert problem_detail.status_code == 502
        assert problem_detail.title == "Bad response"
        assert (
            problem_detail.detail
            == "The server made a request to url, and got an unexpected or invalid response."
        )
        assert (
            problem_detail.debug_message
            == "Bad response from http://url/: Got status code 200 from external server, cannot continue.\n\nResponse content: Hurray"
        )

    def test_debuggable_request(self) -> None:
        class Mock(HTTP):
            called_with: Sequence[Any] | None = None

            @classmethod
            def _request_with_timeout(cls, *args, **kwargs):
                cls.called_with = (args, kwargs)
                return "response"

        def mock_request(*args, **kwargs):
            response = MockRequestsResponse(200, "Success!")
            return response

        Mock.debuggable_request(
            "method", "url", make_request_with=mock_request, key="value"  # type: ignore[call-arg]
        )
        assert Mock.called_with is not None
        (args, kwargs) = Mock.called_with
        assert args == ("method", "url")
        assert kwargs["key"] == "value"
        assert kwargs["make_request_with"] == mock_request
        assert kwargs["process_response_with"] == Mock.process_debuggable_response

    def test_process_debuggable_response(self) -> None:
        """Test a method that gives more detailed information when a
        problem happens.
        """
        m = partial(
            HTTP.process_debuggable_response,
            allowed_response_codes=[],
            disallowed_response_codes=[],
        )
        success = MockRequestsResponse(200, content="Success!")
        assert success == m("url", success)

        success = MockRequestsResponse(302, content="Success!")
        assert success == m("url", success)

        # An error is turned into a ProblemError
        error = MockRequestsResponse(500, content="Error!")
        with pytest.raises(ProblemDetailException) as excinfo:
            m("url", error)
        problem = excinfo.value.problem_detail
        assert isinstance(problem, ProblemDetail)
        assert INTEGRATION_ERROR.uri == problem.uri
        assert '500 response from integration server: "Error!"' == problem.detail

        content, status_code, headers = INVALID_INPUT.response
        error = MockRequestsResponse(status_code, headers, content)
        with pytest.raises(ProblemDetailException) as excinfo:
            m("url", error)
        problem = excinfo.value.problem_detail
        assert isinstance(problem, ProblemDetail)
        assert INTEGRATION_ERROR.uri == problem.uri
        assert (
            "Remote service returned a problem detail document: %r" % content
            == problem.detail
        )
        assert content == problem.debug_message
        # You can force a response to be treated as successful by
        # passing in its response code as allowed_response_codes.
        assert error == m("url", error, allowed_response_codes=[400])
        assert error == m("url", error, allowed_response_codes=["400"])  # type: ignore[list-item]
        assert error == m("url", error, allowed_response_codes=["4xx"])

    def test_retries_unspecified(self, test_http_fixture: HttpTestFixture):
        for i in range(1, 7):
            response = MockAPIServerResponse()
            response.content = b"Ouch."
            response.status_code = 502
            test_http_fixture.server.enqueue_response("GET", "/test", response)

        with pytest.raises(BadResponseException):
            test_http_fixture.request_with_timeout(
                "GET", test_http_fixture.server.url("/test")
            )

        assert len(test_http_fixture.server.requests()) == 6
        request = test_http_fixture.server.requests().pop()
        assert request.path == "/test"
        assert request.method == "GET"

    def test_retries_none(self, test_http_fixture: HttpTestFixture):
        response = MockAPIServerResponse()
        response.content = b"Ouch."
        response.status_code = 502

        test_http_fixture.server.enqueue_response("GET", "/test", response)
        with pytest.raises(BadResponseException):
            test_http_fixture.request_with_timeout(
                "GET", test_http_fixture.server.url("/test"), max_retry_count=0
            )

        assert len(test_http_fixture.server.requests()) == 1
        request = test_http_fixture.server.requests().pop()
        assert request.path == "/test"
        assert request.method == "GET"

    def test_retries_3(self, test_http_fixture: HttpTestFixture):
        response0 = MockAPIServerResponse()
        response0.content = b"Ouch."
        response0.status_code = 502

        response1 = MockAPIServerResponse()
        response1.content = b"Ouch."
        response1.status_code = 502

        response2 = MockAPIServerResponse()
        response2.content = b"OK!"
        response2.status_code = 200

        test_http_fixture.server.enqueue_response("GET", "/test", response0)
        test_http_fixture.server.enqueue_response("GET", "/test", response1)
        test_http_fixture.server.enqueue_response("GET", "/test", response2)

        response = test_http_fixture.request_with_timeout(
            "GET", test_http_fixture.server.url("/test"), max_retry_count=3
        )
        assert response.status_code == 200

        assert len(test_http_fixture.server.requests()) == 3
        request = test_http_fixture.server.requests().pop()
        assert request.path == "/test"
        assert request.method == "GET"

        request = test_http_fixture.server.requests().pop()
        assert request.path == "/test"
        assert request.method == "GET"

        request = test_http_fixture.server.requests().pop()
        assert request.path == "/test"
        assert request.method == "GET"


class TestBearerAuth:
    def test___call__(self, requests_mock: Mocker):
        # An auth token is set on the request when using BearerAuth
        auth = BearerAuth("token")

        requests_mock.get("http://example.com", text="success")
        response = requests.get("http://example.com", auth=auth)
        assert response.status_code == 200
        assert response.text == "success"
        assert requests_mock.last_request is not None
        assert requests_mock.last_request.headers["Authorization"] == "Bearer token"

    def test___repr__(self):
        auth = BearerAuth("token")
        assert repr(auth) == "BearerAuth(token)"

    def test___eq__(self):
        assert BearerAuth("token") == BearerAuth("token")
        assert BearerAuth("token") != BearerAuth("different token")
        assert BearerAuth("token") != "token"
