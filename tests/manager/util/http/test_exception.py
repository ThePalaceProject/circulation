import pickle

from palace.manager.integration.license.opds.exception import OpdsResponseException
from palace.manager.util.http.exception import (
    BadResponseException,
    RemoteIntegrationException,
    RequestNetworkException,
    RequestTimedOut,
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
