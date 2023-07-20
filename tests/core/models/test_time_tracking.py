import datetime

import pytest
from sqlalchemy.exc import IntegrityError

from core.model import create
from core.model.time_tracking import IdentifierPlaytime, IdentifierPlaytimeEntry
from core.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class TestIdentifierPlaytimeEntries:
    def test_create(self, db: DatabaseTransactionFixture):
        identifier = db.identifier()
        now = utc_now()
        entry, _ = create(
            db.session,
            IdentifierPlaytimeEntry,
            identifier_id=identifier.id,
            timestamp=now,
            total_seconds_played=30,
            tracking_id="tracking-id",
        )

        assert entry.identifier == identifier
        assert entry.total_seconds_played == 30
        assert entry.timestamp == now
        assert entry.tracking_id == "tracking-id"

    def test_constraints(self, db: DatabaseTransactionFixture):
        identifier = db.identifier()
        now = utc_now()

        # > 60 second playtime (per minute)
        with pytest.raises(IntegrityError) as raised:
            create(
                db.session,
                IdentifierPlaytimeEntry,
                identifier_id=identifier.id,
                timestamp=now,
                total_seconds_played=61,
                tracking_id="tracking-id",
            )
        assert "max_total_seconds_played_constraint" in raised.exconly()
        db.session.rollback()

        # rollback means we need the data again
        identifier = db.identifier()
        identifier_2 = db.identifier()

        create(
            db.session,
            IdentifierPlaytimeEntry,
            identifier_id=identifier.id,
            timestamp=now,
            total_seconds_played=60,
            tracking_id="tracking-id-0",
        )
        # Different identifier same tracking id is ok
        create(
            db.session,
            IdentifierPlaytimeEntry,
            identifier_id=identifier_2.id,
            timestamp=now,
            total_seconds_played=60,
            tracking_id="tracking-id-0",
        )
        # Same identifier different tracking id is ok
        create(
            db.session,
            IdentifierPlaytimeEntry,
            identifier_id=identifier.id,
            timestamp=now,
            total_seconds_played=60,
            tracking_id="tracking-id-1",
        )
        with pytest.raises(IntegrityError) as raised:
            # Same identifier same tracking id is not ok
            create(
                db.session,
                IdentifierPlaytimeEntry,
                identifier_id=identifier.id,
                timestamp=now,
                total_seconds_played=60,
                tracking_id="tracking-id-0",
            )
        assert (
            f"Key (identifier_id, tracking_id)=({identifier.id}, tracking-id-0) already exists"
            in raised.exconly()
        )


class TestIdentifierPlaytime:
    def test_contraints(self, db: DatabaseTransactionFixture):
        identifier = db.identifier()

        create(
            db.session,
            IdentifierPlaytime,
            identifier_id=identifier.id,
            identifier_str=identifier.urn,
            timestamp=datetime.datetime(2000, 1, 1, 12, 00, 00),
            total_seconds_played=600,
        )

        # Same identifier string with same timestamp
        with pytest.raises(IntegrityError) as raised:
            create(
                db.session,
                IdentifierPlaytime,
                identifier_id=identifier.id,
                identifier_str=identifier.urn,
                timestamp=datetime.datetime(2000, 1, 1, 12, 00, 00),
                total_seconds_played=600,
            )
        assert (
            f'Key (identifier_str, "timestamp")=({identifier.urn}, 2000-01-01 12:00:00) already exists'
            in raised.exconly()
        )

        db.session.rollback()

        # timestamp not at the minute boundary
        with pytest.raises(IntegrityError) as raised:
            create(
                db.session,
                IdentifierPlaytime,
                identifier_id=identifier.id,
                identifier_str=identifier.urn,
                timestamp=datetime.datetime(2000, 1, 1, 12, 00, 1),
                total_seconds_played=600,
            )
        assert "timestamp_minute_boundary_constraint" in raised.exconly()

    def test_cascades(self, db: DatabaseTransactionFixture):
        identifier = db.identifier()
        urn = identifier.urn
        entry, _ = create(
            db.session,
            IdentifierPlaytime,
            identifier_id=identifier.id,
            identifier_str=identifier.urn,
            timestamp=datetime.datetime(2000, 1, 1, 12, 00, 00),
            total_seconds_played=600,
        )

        assert entry.identifier == identifier
        assert entry.identifier_str == urn

        db.session.delete(identifier)
        db.session.refresh(entry)

        assert entry.identifier == None
        assert entry.identifier_str == urn
