from unittest import mock

import pytest
import requests

from core.problem_details import INVALID_INPUT
from core.util.http import (
    HTTP,
    INTEGRATION_ERROR,
    BadResponseException,
    RemoteIntegrationException,
    RequestNetworkException,
    RequestTimedOut,
)
from core.util.problem_detail import ProblemDetail, ProblemDetailException
from tests.core.mock import MockRequestsResponse


class TestHTTP:
    @pytest.fixture
    def mock_request(self):
        class FakeRequest:
            def __init__(self, response=None):
                self.agent = None
                self.args = None
                self.kwargs = None
                self.response = response or MockRequestsResponse(201)

            def fake_request(self, *args, **kwargs):
                self.agent = kwargs["headers"][b"User-Agent"]
                self.args = args
                self.kwargs = kwargs
                return self.response

        return FakeRequest

    def test_series(self):
        m = HTTP.series
        assert "2xx" == m(201)
        assert "3xx" == m(399)
        assert "5xx" == m(500)

    @mock.patch("core.util.http.sessions.Session")
    def test_request_with_timeout_defaults(self, mock_session):
        with mock.patch.object(HTTP, "DEFAULT_REQUEST_TIMEOUT", 10), mock.patch.object(
            HTTP, "DEFAULT_REQUEST_RETRIES", 2
        ):
            mock_ctx = mock_session().__enter__()
            mock_request = mock_ctx.request
            HTTP.request_with_timeout("GET", "url")
            # The session adapter has a retry attached
            assert mock_ctx.mount.call_args[0][1].max_retries.total == 2
            mock_request.assert_called_once()
            # The request has a timeout
            assert mock_request.call_args[1]["timeout"] == 10

    @mock.patch("core.util.http.core.__version__", "<VERSION>")
    def test_request_with_timeout_success(self, mock_request):
        request = mock_request(MockRequestsResponse(200, content="Success!"))
        response = HTTP._request_with_timeout(
            "http://url/", request.fake_request, "GET", kwarg="value"
        )
        assert 200 == response.status_code
        assert b"Success!" == response.content

        # User agent header should be set
        assert b"Palace Manager/<VERSION>" == request.agent

        # The HTTP method and URL are passed in the order
        # requests.request would expect.
        assert ("GET", "http://url/") == request.args

        # Keyword arguments to _request_with_timeout are passed in
        # as-is.
        assert "value" == request.kwargs["kwarg"]

        # A default timeout is added.
        assert 20 == request.kwargs["timeout"]

    def test_request_with_timeout_with_ua(self, mock_request):
        request = mock_request()
        assert (
            HTTP._request_with_timeout(
                "http://url",
                request.fake_request,
                "GET",
                headers={"User-Agent": "Fake Agent"},
            ).status_code
            == 201
        )
        assert request.agent == b"Fake Agent"

    @mock.patch("core.util.http.core.__version__", None)
    def test_default_user_agent(self, mock_request):
        request = mock_request()
        assert HTTP._request_with_timeout("/", request.fake_request).status_code == 201
        assert request.agent == b"Palace Manager/1.x.x"

    def test_request_with_timeout_failure(self):
        def immediately_timeout(*args, **kwargs):
            raise requests.exceptions.Timeout("I give up")

        with pytest.raises(RequestTimedOut) as excinfo:
            HTTP._request_with_timeout("http://url/", immediately_timeout, "a", "b")
        assert "Timeout accessing http://url/: I give up" in str(excinfo.value)

    def test_request_with_network_failure(self):
        def immediately_fail(*args, **kwargs):
            raise requests.exceptions.ConnectionError("a disaster")

        with pytest.raises(RequestNetworkException) as excinfo:
            HTTP._request_with_timeout("http://url/", immediately_fail, "a", "b")
        assert "Network error contacting http://url/: a disaster" in str(excinfo.value)

    def test_request_with_response_indicative_of_failure(self):
        def fake_500_response(*args, **kwargs):
            return MockRequestsResponse(500, content="Failure!")

        with pytest.raises(BadResponseException) as excinfo:
            HTTP._request_with_timeout("http://url/", fake_500_response, "a", "b")
        assert (
            "Bad response from http://url/: Got status code 500 from external server"
            in str(excinfo.value)
        )

    def test_allowed_response_codes(self):
        """Test our ability to raise BadResponseException when
        an HTTP-based integration does not behave as we'd expect.
        """

        def fake_401_response(*args, **kwargs):
            return MockRequestsResponse(401, content="Weird")

        def fake_200_response(*args, **kwargs):
            return MockRequestsResponse(200, content="Hurray")

        url = "http://url/"
        m = HTTP._request_with_timeout

        # By default, every code except for 5xx codes is allowed.
        response = m(url, fake_401_response)
        assert 401 == response.status_code

        # You can say that certain codes are specifically allowed, and
        # all others are forbidden.
        with pytest.raises(BadResponseException) as excinfo:
            m(url, fake_401_response, allowed_response_codes=[201, 200])
        assert (
            "Bad response from http://url/: Got status code 401 from external server, but can only continue on: 200, 201."
            in str(excinfo.value)
        )

        response = m(url, fake_401_response, allowed_response_codes=[401])
        response = m(url, fake_401_response, allowed_response_codes=["4xx"])

        # In this way you can even raise an exception on a 200 response code.
        with pytest.raises(BadResponseException) as excinfo:
            m(url, fake_200_response, allowed_response_codes=[401])
        assert (
            "Bad response from http://url/: Got status code 200 from external server, but can only continue on: 401."
            in str(excinfo.value)
        )

        # You can say that certain codes are explicitly forbidden, and
        # all others are allowed.
        with pytest.raises(BadResponseException) as excinfo:
            m(url, fake_401_response, disallowed_response_codes=[401])
        assert (
            "Bad response from http://url/: Got status code 401 from external server, cannot continue."
            in str(excinfo.value)
        )

        with pytest.raises(BadResponseException) as excinfo:
            m(url, fake_200_response, disallowed_response_codes=["2xx", 301])
        assert (
            "Bad response from http://url/: Got status code 200 from external server, cannot continue."
            in str(excinfo.value)
        )

        response = m(url, fake_401_response, disallowed_response_codes=["2xx"])
        assert 401 == response.status_code

        # The exception can be turned into a useful problem detail document.
        exc = None
        try:
            m(url, fake_200_response, disallowed_response_codes=["2xx"])
        except Exception as e:
            exc = e
        assert exc is not None

        problem_detail = exc.problem_detail

        # 502 is the status code to be returned if this integration error
        # interrupts the processing of an incoming HTTP request, not the
        # status code that caused the problem.
        #
        assert 502 == problem_detail.status_code
        assert "Bad response" == problem_detail.title
        assert (
            "The server made a request to url, and got an unexpected or invalid response."
            == problem_detail.detail
        )
        assert (
            "Bad response from http://url/: Got status code 200 from external server, cannot continue.\n\nResponse content: Hurray"
            == problem_detail.debug_message
        )

    def test_unicode_converted_to_utf8(self):
        """Any Unicode that sneaks into the URL, headers or body is
        converted to UTF-8.
        """

        class ResponseGenerator:
            def __init__(self):
                self.requests = []

            def response(self, *args, **kwargs):
                self.requests.append((args, kwargs))
                return MockRequestsResponse(200, content="Success!")

        generator = ResponseGenerator()
        url = "http://foo"
        response = HTTP._request_with_timeout(
            url,
            generator.response,
            "POST",
            headers={"unicode header": "unicode value"},
            data="unicode data",
        )
        [(args, kwargs)] = generator.requests
        url, method = args
        headers = kwargs["headers"]
        data = kwargs["data"]

        # All the Unicode data was converted to bytes before being sent
        # "over the wire".
        for k, v in list(headers.items()):
            assert isinstance(k, bytes)
            assert isinstance(v, bytes)
        assert isinstance(data, bytes)

    def test_debuggable_request(self):
        class Mock(HTTP):
            @classmethod
            def _request_with_timeout(cls, *args, **kwargs):
                cls.called_with = (args, kwargs)
                return "response"

        def mock_request(*args, **kwargs):
            response = MockRequestsResponse(200, "Success!")
            return response

        Mock.debuggable_request(
            "method", "url", make_request_with=mock_request, key="value"
        )
        (args, kwargs) = Mock.called_with
        assert args == ("url", mock_request, "method")
        assert kwargs["key"] == "value"
        assert kwargs["process_response_with"] == Mock.process_debuggable_response

    def test_process_debuggable_response(self):
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
        assert error == m("url", error, allowed_response_codes=["400"])
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
    def test_from_response(self):
        response = MockRequestsResponse(102, content="nonsense")
        exc = BadResponseException.from_response(
            "http://url/", "Terrible response, just terrible", response
        )

        # the status code gets set on the exception
        assert exc.status_code == 102

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

    def test_bad_status_code(object):
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
        exception = BadResponseException(
            "http://url/",
            "What even is this",
            debug_message="some debug info",
            status_code=401,
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
        assert exception.status_code == 401


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
