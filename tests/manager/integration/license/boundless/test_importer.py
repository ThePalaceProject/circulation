from datetime import datetime
from unittest.mock import MagicMock, create_autospec

import pytest

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.integration.license.boundless.importer import BoundlessImporter
from palace.manager.integration.license.boundless.requests import BoundlessRequests
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.util.datetime_helpers import datetime_utc
from palace.manager.util.http.exception import BadResponseException
from tests.fixtures.database import DatabaseTransactionFixture
from tests.manager.integration.license.boundless.conftest import BoundlessFixture
from tests.mocks.mock import MockRequestsResponse


class TestBoundlessImporter:
    def test__recent_activity(self, boundless: BoundlessFixture):
        # Test the recent_activity method, which returns a list of
        # recent activity for the collection.
        data = boundless.files.sample_data("tiny_collection.xml")
        boundless.http_client.queue_response(200, content=data)
        importer = boundless.create_importer()

        # Get the activity for the last 5 minutes.
        since = datetime_utc(2012, 10, 1, 15, 45, 25, 4456)
        activity = list(importer._recent_activity(since))

        # We made a request to the correct URL.
        assert "/availability/v2" in boundless.http_client.requests[1]
        args = boundless.http_client.requests_args[1]
        assert args["params"] == {
            "updatedDate": "10-01-2012 15:45:25",
        }

        # We made the request with a long timeout.
        assert args["timeout"] == 600

        assert len(activity) == 2

    def test_incorrect_collection_protocol(
        self, db: DatabaseTransactionFixture, boundless: BoundlessFixture
    ):
        # Test that an error is raised if the collection protocol is not BoundlessApi
        collection = db.collection(protocol="SomeOtherProtocol")
        with pytest.raises(PalaceValueError, match="is not a Boundless collection"):
            boundless.create_importer(collection=collection)

    def test_import_configuration_error(
        self,
        db: DatabaseTransactionFixture,
        boundless: BoundlessFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        mock_requests = create_autospec(BoundlessRequests)
        mock_requests.refresh_bearer_token.side_effect = BadResponseException(
            "service", "uh oh", MockRequestsResponse(401)
        )
        importer = boundless.create_importer(api_requests=mock_requests)
        assert importer.import_collection(apply_bibliographic=MagicMock()) is None
        assert "Failed to authenticate with Boundless API" in caplog.text

    def test__get_timestamp(
        self, db: DatabaseTransactionFixture, boundless: BoundlessFixture
    ):
        importer = boundless.create_importer()

        # If no timestamp exists, we create one
        ts = importer._get_timestamp()
        assert ts.service == "Boundless Import"
        assert ts.service_type == ts.TASK_TYPE
        assert ts.collection == boundless.collection
        assert ts.start is None
        assert ts.finish is None

        # If one does exist, we return the existing one
        ts.start = datetime_utc(2023, 1, 1)
        ts.finish = datetime_utc(2023, 1, 2)
        ts2 = importer._get_timestamp()
        assert ts == ts2
        assert ts2.start == datetime_utc(2023, 1, 1)
        assert ts2.finish == datetime_utc(2023, 1, 2)

    @pytest.mark.parametrize(
        "import_all, identifier_set, existing_timestamp_start, expected_start_time",
        [
            pytest.param(
                True,
                False,
                datetime_utc(2023, 1, 1),
                BoundlessImporter._DEFAULT_START_TIME,
                id="import_all",
            ),
            pytest.param(
                False,
                True,
                datetime_utc(2023, 1, 1),
                BoundlessImporter._DEFAULT_START_TIME,
                id="identifier_set",
            ),
            pytest.param(
                False,
                False,
                None,
                BoundlessImporter._DEFAULT_START_TIME,
                id="no_existing_start",
            ),
            pytest.param(
                False,
                False,
                datetime_utc(2023, 1, 1),
                datetime_utc(2023, 1, 1),
                id="use_existing_start",
            ),
        ],
    )
    def test__get_start_time(
        self,
        db: DatabaseTransactionFixture,
        boundless: BoundlessFixture,
        import_all: bool,
        identifier_set: bool,
        existing_timestamp_start: datetime | None,
        expected_start_time: datetime,
    ):
        mock_identifier_set = create_autospec(IdentifierSet) if identifier_set else None
        importer = boundless.create_importer(
            import_all=import_all, identifier_set=mock_identifier_set
        )
        ts = importer._get_timestamp()
        ts.start = existing_timestamp_start
        assert importer._get_start_time(ts) == expected_start_time

    def test__check_api_credentials(self, boundless: BoundlessFixture):
        mock_api_requests = create_autospec(BoundlessRequests)
        importer = boundless.create_importer(api_requests=mock_api_requests)

        # If api.bearer_token() runs successfully, the function should return True
        assert importer._check_api_credentials() is True
        mock_api_requests.refresh_bearer_token.assert_called_once()

        # If a BadResponseException is raised with a 401 status code, the function should return False
        mock_api_requests.refresh_bearer_token.side_effect = BadResponseException(
            "service", "uh oh", MockRequestsResponse(401)
        )
        assert importer._check_api_credentials() is False

        # If a BadResponseException is raised with a status code other than 401, the function should raise the exception
        mock_api_requests.refresh_bearer_token.side_effect = BadResponseException(
            "service", "uh oh", MockRequestsResponse(500)
        )
        with pytest.raises(BadResponseException):
            importer._check_api_credentials()

        # Any other exception should be raised
        mock_api_requests.refresh_bearer_token.side_effect = ValueError
        with pytest.raises(ValueError):
            importer._check_api_credentials()
