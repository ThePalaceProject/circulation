import datetime

import pytest
from sqlalchemy.exc import IntegrityError

from core.model import create
from core.model.time_tracking import PlaytimeEntry, PlaytimeSummary
from core.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class TestPlaytimeEntries:
    def test_create(self, db: DatabaseTransactionFixture):
        identifier = db.identifier()
        collection = db.default_collection()
        library = db.default_library()
        now = utc_now()
        entry, _ = create(
            db.session,
            PlaytimeEntry,
            identifier_id=identifier.id,
            collection_id=collection.id,
            library_id=library.id,
            timestamp=now,
            total_seconds_played=30,
            tracking_id="tracking-id",
        )

        assert entry.identifier == identifier
        assert entry.collection == collection
        assert entry.library == library
        assert entry.total_seconds_played == 30
        assert entry.timestamp == now
        assert entry.tracking_id == "tracking-id"

    def test_constraints(self, db: DatabaseTransactionFixture):
        identifier = db.identifier()
        collection = db.default_collection()
        library = db.default_library()
        now = utc_now()

        # > 60 second playtime (per minute)
        with pytest.raises(IntegrityError) as raised:
            create(
                db.session,
                PlaytimeEntry,
                identifier_id=identifier.id,
                collection_id=collection.id,
                library_id=library.id,
                timestamp=now,
                total_seconds_played=61,
                tracking_id="tracking-id",
            )
        assert "max_total_seconds_played_constraint" in raised.exconly()
        db.session.rollback()

        # rollback means we need the data again
        collection = db.collection()
        library = db.library()
        identifier = db.identifier()
        identifier_2 = db.identifier()

        create(
            db.session,
            PlaytimeEntry,
            identifier_id=identifier.id,
            collection_id=collection.id,
            library_id=library.id,
            timestamp=now,
            total_seconds_played=60,
            tracking_id="tracking-id-0",
        )
        # Different identifier same tracking id is ok
        create(
            db.session,
            PlaytimeEntry,
            identifier_id=identifier_2.id,
            collection_id=collection.id,
            library_id=library.id,
            timestamp=now,
            total_seconds_played=60,
            tracking_id="tracking-id-0",
        )
        # Same identifier different tracking id is ok
        create(
            db.session,
            PlaytimeEntry,
            identifier_id=identifier.id,
            collection_id=collection.id,
            library_id=library.id,
            timestamp=now,
            total_seconds_played=60,
            tracking_id="tracking-id-1",
        )
        with pytest.raises(IntegrityError) as raised:
            # Same identifier same tracking id is not ok
            create(
                db.session,
                PlaytimeEntry,
                identifier_id=identifier.id,
                collection_id=collection.id,
                library_id=library.id,
                timestamp=now,
                total_seconds_played=60,
                tracking_id="tracking-id-0",
            )
        assert (
            f"Key (identifier_id, collection_id, library_id, tracking_id)=({identifier.id}, {collection.id}, {library.id}, tracking-id-0) already exists"
            in raised.exconly()
        )


class TestPlaytimeSummaries:
    def test_contraints(self, db: DatabaseTransactionFixture):
        identifier = db.identifier()
        collection = db.default_collection()
        library = db.default_library()

        create(
            db.session,
            PlaytimeSummary,
            identifier_id=identifier.id,
            collection_id=collection.id,
            library_id=library.id,
            identifier_str=identifier.urn,
            collection_name=collection.name,
            library_name=library.name,
            timestamp=datetime.datetime(2000, 1, 1, 12, 00, 00),
            total_seconds_played=600,
        )

        # Same identifier string with same timestamp
        with pytest.raises(IntegrityError) as raised:
            create(
                db.session,
                PlaytimeSummary,
                identifier_id=identifier.id,
                collection_id=collection.id,
                library_id=library.id,
                identifier_str=identifier.urn,
                collection_name=collection.name,
                library_name=library.name,
                timestamp=datetime.datetime(2000, 1, 1, 12, 00, 00),
                total_seconds_played=600,
            )
        assert (
            f'Key (identifier_str, collection_name, library_name, "timestamp")=({identifier.urn}, {collection.name}, {library.name}, 2000-01-01 12:00:00+00) already exists'
            in raised.exconly()
        )

        db.session.rollback()

        # timestamp not at the minute boundary
        with pytest.raises(IntegrityError) as raised:
            create(
                db.session,
                PlaytimeSummary,
                identifier_id=identifier.id,
                collection_id=collection.id,
                library_id=library.id,
                identifier_str=identifier.urn,
                collection_name=collection.name,
                library_name=library.name,
                timestamp=datetime.datetime(2000, 1, 1, 12, 00, 1),
                total_seconds_played=600,
            )
        assert "timestamp_minute_boundary_constraint" in raised.exconly()

    def test_cascades(self, db: DatabaseTransactionFixture):
        identifier = db.identifier()
        collection = db.default_collection()
        library = db.default_library()

        urn = identifier.urn
        entry, _ = create(
            db.session,
            PlaytimeSummary,
            identifier_id=identifier.id,
            collection_id=collection.id,
            library_id=library.id,
            identifier_str=identifier.urn,
            collection_name=collection.name,
            library_name=library.name,
            timestamp=datetime.datetime(2000, 1, 1, 12, 00, 00),
            total_seconds_played=600,
        )

        assert entry.identifier == identifier
        assert entry.identifier_str == urn

        db.session.delete(identifier)
        db.session.refresh(entry)

        assert entry.identifier == None
        assert entry.identifier_str == urn
