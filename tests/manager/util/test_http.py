from collections.abc import Mapping, Sequence
from functools import partial
from typing import Any
from unittest import mock
from unittest.mock import MagicMock, create_autospec

import pytest
import requests
from requests import Response
from requests_mock import Mocker

from palace.manager.core.problem_details import INTEGRATION_ERROR, INVALID_INPUT
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.util.http import (
    HTTP,
    BadResponseException,
    BearerAuth,
    RemoteIntegrationException,
    RequestNetworkException,
    RequestTimedOut,
)
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException
from tests.mocks.mock import MockRequestsResponse


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
    def test_series(self) -> None:
        m = HTTP.series
        assert m(201) == "2xx"
        assert m(399) == "3xx"
        assert m(500) == "5xx"

    @mock.patch("palace.manager.util.http.sessions.Session")
    def test_request_with_timeout_defaults(self, mock_session: MagicMock) -> None:
        with (
            mock.patch.object(HTTP, "DEFAULT_REQUEST_TIMEOUT", 10),
            mock.patch.object(HTTP, "DEFAULT_REQUEST_RETRIES", 2),
        ):
            mock_ctx = mock_session().__enter__()
            mock_request = mock_ctx.request
            HTTP.request_with_timeout("GET", "url")
            # The session adapter has a retry attached
            assert mock_ctx.mount.call_args[0][1].max_retries.total == 2
            mock_request.assert_called_once()
            # The request has a timeout
            assert mock_request.call_args[1]["timeout"] == 10

    @mock.patch("palace.manager.util.http.manager.__version__", "<VERSION>")
    def test_request_with_timeout_success(self) -> None:
        request = FakeRequest(MockRequestsResponse(200, content="Success!"))
        response = HTTP._request_with_timeout(
            "GET", "http://url/", request.fake_request, kwarg="value"  # type: ignore[call-arg]
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
                request.fake_request,
                headers={"User-Agent": "Fake Agent"},
            ).status_code
            == 201
        )
        assert request.agent == "Fake Agent"

    @mock.patch("palace.manager.util.http.manager.__version__", None)
    def test_default_user_agent(self) -> None:
        request = FakeRequest()
        assert (
            HTTP._request_with_timeout("DELETE", "/", request.fake_request).status_code
            == 201
        )
        assert request.agent == "Palace Manager/1.x.x"

        # User agent is still set if headers are None
        assert (
            HTTP._request_with_timeout(
                "GET", "/", request.fake_request, headers=None
            ).status_code
            == 201
        )
        assert request.agent == "Palace Manager/1.x.x"

        # The headers are not modified if they are passed into the function
        original_headers = {"header": "value"}
        assert (
            HTTP._request_with_timeout(
                "GET", "/", request.fake_request, headers=original_headers
            ).status_code
            == 201
        )
        assert request.agent == "Palace Manager/1.x.x"
        assert original_headers == {"header": "value"}

    def test_request_with_timeout_failure(self) -> None:
        def immediately_timeout(*args, **kwargs) -> Response:
            raise requests.exceptions.Timeout("I give up")

        with pytest.raises(
            RequestTimedOut, match="Timeout accessing http://url/: I give up"
        ):
            HTTP._request_with_timeout("PUT", "http://url/", immediately_timeout)

    @mock.patch("palace.manager.util.http.manager.__version__", None)
    def test_request_with_timeout_verbose(self, caplog: pytest.LogCaptureFixture):
        """
        When the verbose flag is set, we log the request and response.
        """
        caplog.set_level(LogLevel.info)
        mock_process_response = create_autospec(HTTP._process_response)
        make_request = MagicMock(
            return_value=MockRequestsResponse(
                204, headers={"test": "response header"}, content="Success!"
            )
        )

        response = HTTP._request_with_timeout(
            "POST",
            "http://url/",
            make_request,
            process_response_with=mock_process_response,
            verbose=True,
            headers={"header": "value"},
        )

        assert make_request.call_count == 1
        assert make_request.call_args.args == ("POST", "http://url/")

        assert mock_process_response.call_count == 1
        assert response == mock_process_response.return_value

        assert (
            "Sending POST request to http://url/: kwargs {'headers': {'User-Agent':"
            " 'Palace Manager/1.x.x', 'header': 'value'}, 'timeout': 20}"
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
            HTTP._request_with_timeout("POST", "http://url/", immediately_fail)

    def test_request_with_response_indicative_of_failure(self) -> None:
        def fake_500_response(*args, **kwargs) -> Response:
            return MockRequestsResponse(500, content="Failure!")

        with pytest.raises(
            BadResponseException,
            match="Bad response from http://url/: Got status code 500 from external server",
        ):
            HTTP._request_with_timeout("GET", "http://url/", fake_500_response)

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
        response = request(fake_401_response)
        assert response.status_code == 401

        # You can say that certain codes are specifically allowed, and
        # all others are forbidden.
        with pytest.raises(
            BadResponseException,
            match="Bad response from http://url/: Got status code 401 from external server, but can only continue on: 200, 201.",
        ):
            request(fake_401_response, allowed_response_codes=[201, 200])

        response = request(fake_401_response, allowed_response_codes=[401])
        response = request(fake_401_response, allowed_response_codes=["4xx"])

        # In this way you can even raise an exception on a 200 response code.
        with pytest.raises(
            BadResponseException,
            match="Bad response from http://url/: Got status code 200 from external server, but can only continue on: 401.",
        ):
            request(fake_200_response, allowed_response_codes=[401])

        # You can say that certain codes are explicitly forbidden, and
        # all others are allowed.
        with pytest.raises(
            BadResponseException,
            match="Bad response from http://url/: Got status code 401 from external server, cannot continue.",
        ) as excinfo:
            request(fake_401_response, disallowed_response_codes=[401])

        with pytest.raises(
            BadResponseException,
            match="Bad response from http://url/: Got status code 200 from external server, cannot continue.",
        ):
            request(fake_200_response, disallowed_response_codes=["2xx", 301])

        response = request(fake_401_response, disallowed_response_codes=["2xx"])
        assert response.status_code == 401

        # The exception can be turned into a useful problem detail document.
        with pytest.raises(BadResponseException) as exc_info:
            request(fake_200_response, disallowed_response_codes=["2xx"])

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
        m = HTTP.process_debuggable_response
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
        assert exc.response is response

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
        assert exception.response is response


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
