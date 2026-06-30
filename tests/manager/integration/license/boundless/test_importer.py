from functools import partial
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from palace.util.datetime_helpers import datetime_utc
from palace.util.exceptions import PalaceValueError

from palace.manager.celery.tasks.apply import (
    ApplyBibliographicCallable,
    ApplyCirculationCallable,
)
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.integration.license.boundless.api import BoundlessApi
from palace.manager.integration.license.boundless.importer import BoundlessImporter
from palace.manager.sqlalchemy.model.licensing import LicensePoolStatus
from tests.fixtures.database import DatabaseTransactionFixture
from tests.manager.integration.license.boundless.conftest import BoundlessFixture


class TestBoundlessImporter:
    def test__import_active_titles(
        self, boundless: BoundlessFixture, db: DatabaseTransactionFixture
    ):
        # Test the _import_active_titles method, which fetches availability data
        # for active titles and imports them.
        data = boundless.files.sample_data("tiny_collection.xml")
        boundless.http_client.queue_response(200, content=data)
        importer = boundless.create_importer()

        # Mock the apply callables
        apply_bibliographic = create_autospec(ApplyBibliographicCallable)
        apply_circulation = create_autospec(ApplyCirculationCallable)

        # Import two active titles
        active_title_ids = ["0003642860", "0012164897"]
        importer._import_active_titles(
            active_title_ids, apply_bibliographic, apply_circulation
        )

        # We made a request to the correct URL.
        assert "/availability/v2" in boundless.http_client.requests[1]
        args = boundless.http_client.requests_args[1]
        assert args["params"] == {
            "titleIds": "0003642860,0012164897",
        }

        # Both titles are new, so both go through the bibliographic apply path
        # (which carries their embedded circulation); no circulation-only applies.
        assert apply_bibliographic.call_count == 2
        apply_circulation.assert_not_called()

        # Make sure the circulation data is correct
        calls = apply_bibliographic.call_args_list
        bib_data_1 = calls[0].args[0]  # First call
        bib_data_2 = calls[1].args[0]  # Second call

        circ_data_1 = bib_data_1.circulation
        circ_data_2 = bib_data_2.circulation

        assert circ_data_1.primary_identifier_data.identifier == "0003642860"
        assert circ_data_1.licenses_owned == 9
        assert circ_data_1.licenses_available == 9
        assert circ_data_1.status == LicensePoolStatus.ACTIVE

        assert circ_data_2.primary_identifier_data.identifier == "0012164897"
        assert circ_data_2.licenses_owned == 10
        assert circ_data_2.licenses_available == 10
        assert circ_data_2.status == LicensePoolStatus.ACTIVE

    def test__import_active_titles_chunking(
        self, boundless: BoundlessFixture, db: DatabaseTransactionFixture
    ):
        # Test that _import_active_titles chunks title IDs when there are more than
        # _AVAILABILITY_CALL_MAXIMUM_IDENTIFIERS titles.
        data = boundless.files.sample_data("tiny_collection.xml")

        # Queue responses for multiple chunks
        boundless.http_client.queue_response(200, content=data)
        boundless.http_client.queue_response(200, content=data)

        importer = boundless.create_importer()
        apply_bibliographic = MagicMock()
        apply_circulation = MagicMock()

        # Create a list of title IDs that exceeds the chunk size
        chunk_size = importer._AVAILABILITY_CALL_MAXIMUM_IDENTIFIERS
        active_title_ids = [f"{i:010d}" for i in range(chunk_size + 10)]

        importer._import_active_titles(
            active_title_ids, apply_bibliographic, apply_circulation
        )

        # We should have made two requests (one for each chunk)
        # Request 0 is the token request, requests 1 and 2 are the availability calls
        assert "/availability/v2" in boundless.http_client.requests[1]
        assert "/availability/v2" in boundless.http_client.requests[2]

        # First chunk should have chunk_size titles
        args1 = boundless.http_client.requests_args[1]
        params1 = args1["params"]
        assert params1 is not None
        title_ids_1 = params1["titleIds"]
        assert isinstance(title_ids_1, str)
        assert len(title_ids_1.split(",")) == chunk_size

        # Second chunk should have 10 titles
        args2 = boundless.http_client.requests_args[2]
        params2 = args2["params"]
        assert params2 is not None
        title_ids_2 = params2["titleIds"]
        assert isinstance(title_ids_2, str)
        assert len(title_ids_2.split(",")) == 10

    def test__import_active_titles_applies_circulation_only_change(
        self, boundless: BoundlessFixture, db: DatabaseTransactionFixture
    ):
        """Regression: an availability-only change is applied on its own.

        ``BibliographicData.needs_apply()`` excludes circulation from its hash,
        so when a title's metadata is unchanged but its availability changed,
        ``needs_apply()`` is ``False``.  The importer must fall back to
        ``CirculationData.needs_apply()`` and dispatch the circulation update --
        otherwise the change that put the title in the modified-since feed in the
        first place is silently dropped.
        """
        data = boundless.files.sample_data("tiny_collection.xml")
        boundless.http_client.queue_response(200, content=data)
        importer = boundless.create_importer()

        apply_bibliographic = create_autospec(ApplyBibliographicCallable)
        apply_circulation = create_autospec(ApplyCirculationCallable)

        active_title_ids = ["0003642860", "0012164897"]
        # Metadata unchanged (needs_apply False) but availability changed
        # (circulation needs_apply True).
        with (
            patch.object(BibliographicData, "needs_apply", return_value=False),
            patch.object(CirculationData, "needs_apply", return_value=True),
        ):
            importer._import_active_titles(
                active_title_ids, apply_bibliographic, apply_circulation
            )

        # No bibliographic apply, but a circulation apply per title.
        apply_bibliographic.assert_not_called()
        assert apply_circulation.call_count == 2
        applied_ids = {
            call.args[0].primary_identifier_data.identifier
            for call in apply_circulation.call_args_list
        }
        assert applied_ids == {"0003642860", "0012164897"}
        for call in apply_circulation.call_args_list:
            assert call.kwargs["collection_id"] == importer._collection.id

    def test__import_active_titles_skips_when_nothing_changed(
        self, boundless: BoundlessFixture, db: DatabaseTransactionFixture
    ):
        """Neither callable fires when both metadata and circulation are unchanged."""
        data = boundless.files.sample_data("tiny_collection.xml")
        boundless.http_client.queue_response(200, content=data)
        importer = boundless.create_importer()

        apply_bibliographic = create_autospec(ApplyBibliographicCallable)
        apply_circulation = create_autospec(ApplyCirculationCallable)

        with (
            patch.object(BibliographicData, "needs_apply", return_value=False),
            patch.object(CirculationData, "needs_apply", return_value=False),
        ):
            importer._import_active_titles(
                ["0003642860", "0012164897"], apply_bibliographic, apply_circulation
            )

        apply_bibliographic.assert_not_called()
        apply_circulation.assert_not_called()

    def test_incorrect_collection_protocol(
        self, db: DatabaseTransactionFixture, boundless: BoundlessFixture
    ):
        # Test that an error is raised if the collection protocol is not BoundlessApi
        collection = db.collection(protocol="SomeOtherProtocol")
        with pytest.raises(PalaceValueError, match="is not a Boundless collection"):
            boundless.create_importer(collection=collection)

    def test_get_timestamp(self, db: DatabaseTransactionFixture):
        collection = db.collection(name="test_collection", protocol=BoundlessApi)
        get_timestamp = partial(BoundlessImporter.get_timestamp, db.session, collection)

        # If no timestamp exists, we create one
        ts = get_timestamp()
        assert ts.service == "Boundless Import"
        assert ts.service_type == ts.TASK_TYPE
        assert ts.collection == collection
        assert ts.start is None
        assert ts.finish is None

        # If one does exist, we return the existing one
        ts.start = datetime_utc(2023, 1, 1)
        ts.finish = datetime_utc(2023, 1, 2)
        ts2 = get_timestamp()
        assert ts == ts2
        assert ts2.start == datetime_utc(2023, 1, 1)
        assert ts2.finish == datetime_utc(2023, 1, 2)

    def test_mark_inactive_titles_status(
        self, boundless: BoundlessFixture, db: DatabaseTransactionFixture
    ):
        """Test that inactive titles are marked with EXHAUSTED status."""
        importer = boundless.create_importer()

        # Mock the apply callable to capture the CirculationData objects
        apply_circulation = create_autospec(ApplyCirculationCallable)

        # Mark two titles as inactive
        inactive_title_ids = ["1234567890", "0987654321"]
        importer._mark_inactive_titles(inactive_title_ids, apply_circulation)

        # We should have created CirculationData for both inactive titles
        assert apply_circulation.call_count == 2

        # Each should have 0 licenses and EXHAUSTED status
        for circ_data in [c.args[0] for c in apply_circulation.call_args_list]:
            assert circ_data.licenses_owned == 0
            assert circ_data.licenses_available == 0
            assert circ_data.status == LicensePoolStatus.EXHAUSTED
