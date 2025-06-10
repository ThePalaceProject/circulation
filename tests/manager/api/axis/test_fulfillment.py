from __future__ import annotations

import socket
import ssl
import urllib
from contextlib import contextmanager
from functools import partial
from unittest.mock import MagicMock, Mock, PropertyMock, patch

import pytest

from palace.manager.api.axis.fulfillment import (
    Axis360AcsFulfillment,
    Axis360Fulfillment,
)
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism
from palace.manager.util.flask_util import Response
from palace.manager.util.problem_detail import BaseProblemDetailException
from tests.manager.api.axis.conftest import Axis360Fixture


class TestAxis360Fulfillment:
    """An Axis360Fulfillment can fulfill a title whether it's an ebook
    (fulfilled through AxisNow) or an audiobook (fulfilled through
    Findaway).
    """

    def test_fetch_audiobook(self, axis360: Axis360Fixture):
        # When Findaway information is present in the response from
        # the fulfillment API, a second request is made to get
        # spine-item metadata. Information from both requests is
        # combined into a Findaway fulfillment document.
        fulfillment_info = axis360.sample_data("audiobook_fulfillment_info.json")
        axis360.api.queue_response(200, {}, fulfillment_info)

        metadata = axis360.sample_data("audiobook_metadata.json")
        axis360.api.queue_response(200, {}, metadata)

        # Setup.
        edition, pool = axis360.db.edition(with_license_pool=True)
        identifier = pool.identifier
        fulfillment = Axis360Fulfillment(
            axis360.api,
            pool.data_source.name,
            identifier.type,
            identifier.identifier,
            "transaction_id",
        )
        assert fulfillment.content_type is None
        assert fulfillment.content is None

        # Turn the crank.
        fulfillment.response()

        # The Axis360Fulfillment now contains a Findaway manifest
        # document.
        assert fulfillment.content_type == DeliveryMechanism.FINDAWAY_DRM
        assert fulfillment.content is not None
        assert isinstance(fulfillment.content, str)

        # The manifest document combines information from the
        # fulfillment document and the metadata document.
        for required in (
            '"findaway:sessionKey": "0f547af1-38c1-4b1c-8a1a-169d353065d0"',
            '"duration": 8150.87',
        ):
            assert required in fulfillment.content

    def test_fetch_ebook(self, axis360: Axis360Fixture):
        # When no Findaway information is present in the response from
        # the fulfillment API, information from the request is
        # used to make an AxisNow fulfillment document.

        fulfillment_info = axis360.sample_data("ebook_fulfillment_info.json")
        axis360.api.queue_response(200, {}, fulfillment_info)

        # Setup.
        edition, pool = axis360.db.edition(with_license_pool=True)
        identifier = pool.identifier
        fulfillment = Axis360Fulfillment(
            axis360.api,
            pool.data_source.name,
            identifier.type,
            identifier.identifier,
            "transaction_id",
        )
        assert fulfillment.content_type is None
        assert fulfillment.content is None

        # Turn the crank.
        fulfillment.response()

        # The Axis360Fulfillment now contains an AxisNow manifest
        # document derived from the fulfillment document.
        assert fulfillment.content_type == DeliveryMechanism.AXISNOW_DRM
        assert (
            fulfillment.content
            == '{"book_vault_uuid": "1c11c31f-81c2-41bb-9179-491114c3f121", "isbn": "9780547351551"}'
        )


class Axis360AcsFulfillmentFixture:
    def __init__(self, mock_urlopen: MagicMock):
        self.fulfillment_info = partial(
            Axis360AcsFulfillment,
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
            yield Axis360AcsFulfillmentFixture(mock_urlopen)


@pytest.fixture
def axis360_acs_fulfillment_fixture():
    with Axis360AcsFulfillmentFixture.fixture() as fixture:
        yield fixture


class TestAxis360AcsFulfillment:
    def test_url_encoding_not_capitalized(
        self, axis360_acs_fulfillment_fixture: Axis360AcsFulfillmentFixture
    ):
        # Mock the urllopen function to make sure that the URL is not actually requested
        # then make sure that when the request is built the %3a character encoded in the
        # string is not uppercased to be %3A.

        fulfillment = axis360_acs_fulfillment_fixture.fulfillment_info(
            content_link="https://test.com/?param=%3atest123"
        )
        response = fulfillment.response()
        axis360_acs_fulfillment_fixture.mock_urlopen.assert_called()
        called_url = axis360_acs_fulfillment_fixture.mock_urlopen.call_args[0][0]
        assert called_url is not None
        assert called_url.selector == "/?param=%3atest123"
        assert called_url.host == "test.com"
        assert type(response) == Response
        mock_request = axis360_acs_fulfillment_fixture.mock_request
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
        axis360_acs_fulfillment_fixture: Axis360AcsFulfillmentFixture,
        exception: Exception,
    ):
        # Check that when the urlopen function throws an exception, we catch the exception and
        # we turn it into a problem detail to be returned to the client. This mimics the behavior
        # of the http utils function that we are bypassing with this fulfillment method.
        axis360_acs_fulfillment_fixture.mock_urlopen.side_effect = exception
        fulfillment = axis360_acs_fulfillment_fixture.fulfillment_info()
        with pytest.raises(BaseProblemDetailException):
            fulfillment.response()

    @pytest.mark.parametrize(
        ("verify", "verify_mode", "check_hostname"),
        [(True, ssl.CERT_REQUIRED, True), (False, ssl.CERT_NONE, False)],
    )
    def test_verify_ssl(
        self,
        axis360_acs_fulfillment_fixture: Axis360AcsFulfillmentFixture,
        verify: bool,
        verify_mode: ssl.VerifyMode,
        check_hostname: bool,
    ):
        # Make sure that when the verify parameter of the fulfillment method is set we use the
        # correct SSL context to either verify or not verify the ssl certificate for the
        # URL we are fetching.
        fulfillment = axis360_acs_fulfillment_fixture.fulfillment_info(verify=verify)
        fulfillment.response()
        axis360_acs_fulfillment_fixture.mock_urlopen.assert_called()
        assert "context" in axis360_acs_fulfillment_fixture.mock_urlopen.call_args[1]
        context = axis360_acs_fulfillment_fixture.mock_urlopen.call_args[1]["context"]
        assert context.verify_mode == verify_mode
        assert context.check_hostname == check_hostname
