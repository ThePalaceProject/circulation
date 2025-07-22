from __future__ import annotations

import socket
import ssl
import urllib
from contextlib import contextmanager
from functools import partial
from unittest.mock import MagicMock, Mock, PropertyMock, patch

import pytest

from palace.manager.integration.license.boundless.fulfillment import (
    BoundlessAcsFulfillment,
)
from palace.manager.util.flask_util import Response
from palace.manager.util.problem_detail import BaseProblemDetailException


class BoundlessAcsFulfillmentFixture:
    def __init__(self, mock_urlopen: MagicMock):
        self.fulfillment_info = partial(
            BoundlessAcsFulfillment,
            content_link="https://fake.url",
            verify=False,
        )
        self.mock_request = self.create_mock_request()
        self.mock_urlopen = mock_urlopen
        self.mock_urlopen.return_value = self.mock_request

    @staticmethod
    def create_mock_request() -> MagicMock:
        # Create a mock request object that we can use in the tests
        response = MagicMock(return_value="")
        type(response).headers = PropertyMock(return_value=[])
        type(response).status = PropertyMock(return_value=200)
        mock_request = MagicMock()
        mock_request.__enter__.return_value = response
        mock_request.__exit__.return_value = None
        return mock_request

    @classmethod
    @contextmanager
    def fixture(self):
        with patch("urllib.request.urlopen") as mock_urlopen:
            yield BoundlessAcsFulfillmentFixture(mock_urlopen)


@pytest.fixture
def boundless_acs_fulfillment_fixture():
    with BoundlessAcsFulfillmentFixture.fixture() as fixture:
        yield fixture


class TestBoundlessAcsFulfillment:
    def test_url_encoding_not_capitalized(
        self, boundless_acs_fulfillment_fixture: BoundlessAcsFulfillmentFixture
    ):
        # Mock the urllopen function to make sure that the URL is not actually requested
        # then make sure that when the request is built the %3a character encoded in the
        # string is not uppercased to be %3A.

        fulfillment = boundless_acs_fulfillment_fixture.fulfillment_info(
            content_link="https://test.com/?param=%3atest123"
        )
        response = fulfillment.response()
        boundless_acs_fulfillment_fixture.mock_urlopen.assert_called()
        called_url = boundless_acs_fulfillment_fixture.mock_urlopen.call_args[0][0]
        assert called_url is not None
        assert called_url.selector == "/?param=%3atest123"
        assert called_url.host == "test.com"
        assert type(response) == Response
        mock_request = boundless_acs_fulfillment_fixture.mock_request
        mock_request.__enter__.assert_called()
        mock_request.__enter__.return_value.read.assert_called()
        assert "status" in dir(mock_request.__enter__.return_value)
        assert "headers" in dir(mock_request.__enter__.return_value)
        mock_request.__exit__.assert_called()

    @pytest.mark.parametrize(
        "exception",
        [
            urllib.error.HTTPError(url="", code=301, msg="", hdrs={}, fp=Mock()),  # type: ignore
            socket.timeout(),
            urllib.error.URLError(reason=""),
            ssl.SSLError(),
        ],
        ids=lambda val: val.__class__.__name__,
    )
    def test_exception_raises_problem_detail_exception(
        self,
        boundless_acs_fulfillment_fixture: BoundlessAcsFulfillmentFixture,
        exception: Exception,
    ):
        # Check that when the urlopen function throws an exception, we catch the exception and
        # we turn it into a problem detail to be returned to the client. This mimics the behavior
        # of the http utils function that we are bypassing with this fulfillment method.
        boundless_acs_fulfillment_fixture.mock_urlopen.side_effect = exception
        fulfillment = boundless_acs_fulfillment_fixture.fulfillment_info()
        with pytest.raises(BaseProblemDetailException):
            fulfillment.response()

    @pytest.mark.parametrize(
        ("verify", "verify_mode", "check_hostname"),
        [(True, ssl.CERT_REQUIRED, True), (False, ssl.CERT_NONE, False)],
    )
    def test_verify_ssl(
        self,
        boundless_acs_fulfillment_fixture: BoundlessAcsFulfillmentFixture,
        verify: bool,
        verify_mode: ssl.VerifyMode,
        check_hostname: bool,
    ):
        # Make sure that when the verify parameter of the fulfillment method is set we use the
        # correct SSL context to either verify or not verify the ssl certificate for the
        # URL we are fetching.
        fulfillment = boundless_acs_fulfillment_fixture.fulfillment_info(verify=verify)
        fulfillment.response()
        boundless_acs_fulfillment_fixture.mock_urlopen.assert_called()
        assert "context" in boundless_acs_fulfillment_fixture.mock_urlopen.call_args[1]
        context = boundless_acs_fulfillment_fixture.mock_urlopen.call_args[1]["context"]
        assert context.verify_mode == verify_mode
        assert context.check_hostname == check_hostname
