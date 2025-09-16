import pytest
from flask import Response

from palace.manager.api.circulation.fulfillment import (
    DirectFulfillment,
    FetchFulfillment,
    RedirectFulfillment,
)
from palace.manager.util.http.exception import BadResponseException
from tests.fixtures.http import MockHttpClientFixture


class TestDirectFulfillment:
    def test_response(self) -> None:
        fulfillment = DirectFulfillment("This is some content.", "text/plain")
        response = fulfillment.response()
        assert isinstance(response, Response)
        assert response.status_code == 200
        assert response.get_data(as_text=True) == "This is some content."
        assert response.content_type == "text/plain"

    def test__repr__(self) -> None:
        fulfillment = DirectFulfillment("test", "foo/bar")
        assert (
            fulfillment.__repr__()
            == "<DirectFulfillment: content_type: foo/bar, content: 4 bytes>"
        )


class TestRedirectFulfillment:
    def test_response(self) -> None:
        fulfillment = RedirectFulfillment("http://some.location", "foo/bar")
        response = fulfillment.response()
        assert isinstance(response, Response)
        assert response.status_code == 302
        assert response.headers["Location"] == "http://some.location"
        assert response.content_type != "foo/bar"
        assert response.content_type == "text/plain"

    def test__repr__(self) -> None:
        fulfillment = RedirectFulfillment("http://some.location")
        assert (
            fulfillment.__repr__()
            == "<RedirectFulfillment: content_link: http://some.location>"
        )

        fulfillment = RedirectFulfillment("http://some.location", "foo/bar")
        assert (
            fulfillment.__repr__()
            == "<RedirectFulfillment: content_link: http://some.location, content_type: foo/bar>"
        )


class TestFetchFulfillment:
    def test_fetch_fulfillment(self, http_client: MockHttpClientFixture) -> None:
        http_client.queue_response(
            204,
            content="This is some content.",
            media_type="application/xyz",
            headers={"X-Test": "test"},
        )
        fulfillment = FetchFulfillment("http://some.location", "foo/bar")
        response = fulfillment.response()
        assert isinstance(response, Response)
        # The external requests status code is passed through.
        assert response.status_code == 204
        # As is its content.
        assert response.get_data(as_text=True) == "This is some content."
        # Any content type set on the fulfillment, overrides the content type from the request.
        assert response.content_type == "foo/bar"
        assert http_client.requests == ["http://some.location"]
        assert "X-Test" not in response.headers

        # If no content type is set on the fulfillment, the content type from the request is used.
        http_client.reset_mock()
        http_client.queue_response(
            200, content="Other content.", media_type="application/xyz"
        )
        fulfillment = FetchFulfillment("http://some.other.location")
        response = fulfillment.response()
        assert isinstance(response, Response)
        assert response.status_code == 200
        assert response.get_data(as_text=True) == "Other content."
        assert response.content_type == "application/xyz"
        assert http_client.requests == ["http://some.other.location"]
        [kwargs] = http_client.requests_args
        assert kwargs["allow_redirects"] is True

        # If the content type is not set on the fulfillment, and the response does not have a content type,
        # we fall back to no content type.
        http_client.reset_mock()
        http_client.queue_response(200, content="Other content.")
        fulfillment = FetchFulfillment("http://some.other.location")
        response = fulfillment.response()
        assert isinstance(response, Response)
        assert response.content_type is None

    def test_fetch_fulfillment_include_headers(
        self, http_client: MockHttpClientFixture
    ) -> None:
        # If include_headers is set, the headers are set when the fetch is made, but
        # not included in the response.
        http_client.queue_response(
            204, content="This is some content.", media_type="application/xyz"
        )
        fulfillment = FetchFulfillment(
            "http://some.location", "foo/bar", include_headers={"X-Test": "test"}
        )
        response = fulfillment.response()
        assert isinstance(response, Response)
        assert response.status_code == 204
        assert response.get_data(as_text=True) == "This is some content."
        assert response.content_type == "foo/bar"
        assert "X-Test" not in response.headers
        assert http_client.requests == ["http://some.location"]
        [kwargs] = http_client.requests_args
        assert kwargs["headers"] is not None
        assert kwargs["headers"]["X-Test"] == "test"

    def test_fetch_fulfillment_allowed_response_codes(
        self, caplog: pytest.LogCaptureFixture, http_client: MockHttpClientFixture
    ) -> None:
        http_client.queue_response(
            403,
            content='{"type":"http://opds-spec.org/odl/error/checkout/expired",'
            '"title":"the license has expired","detail":"["loan_term_limit_reached"]","status":403}',
            media_type="application/api-problem+json",
        )
        fulfillment = FetchFulfillment(
            "http://some.location", allowed_response_codes=["2xx"]
        )
        with pytest.raises(BadResponseException) as excinfo:
            fulfillment.response()

        assert (
            excinfo.value.problem_detail.detail
            == "The server made a request to some.location, and got an unexpected or invalid response."
        )
        assert (
            "Error fulfilling loan. Bad response from: http://some.location."
            in caplog.text
        )

    def test__repr__(self) -> None:
        fulfillment = FetchFulfillment("http://some.location")
        assert (
            fulfillment.__repr__()
            == "<FetchFulfillment: content_link: http://some.location>"
        )

        fulfillment = FetchFulfillment("http://some.location", "foo/bar")
        assert (
            fulfillment.__repr__()
            == "<FetchFulfillment: content_link: http://some.location, content_type: foo/bar>"
        )
