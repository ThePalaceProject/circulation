from unittest.mock import patch

import flask
from sqlalchemy.exc import IntegrityError

from core.model import get_one
from core.model.time_tracking import PlaytimeEntry
from tests.fixtures.api_controller import CirculationControllerFixture


class TestPlaytimeEntriesController:
    def test_track_playtime(self, circulation_fixture: CirculationControllerFixture):
        db = circulation_fixture.db
        identifier = db.identifier()
        collection = db.default_collection()
        patron = db.patron()

        data = dict(
            timeEntries=[
                {
                    "id": "tracking-id-0",
                    "during_minute": "2000-01-01T12:00Z",
                    "seconds_played": 12,
                },
                {
                    "id": "tracking-id-1",
                    "during_minute": "2000-01-01T12:01Z",
                    "seconds_played": 17,
                },
            ]
        )
        with circulation_fixture.request_context_with_library(
            "/", method="POST", json=data
        ):
            flask.request.patron = patron  # type: ignore
            response = circulation_fixture.manager.playtime_entries.track_playtimes(
                collection.id, identifier.type, identifier.identifier
            )

            assert response.status_code == 207
            data = response.json
            assert data["summary"] == dict(total=2, successes=2, failures=0)
            assert len(data["responses"]) == 2
            assert data["responses"][0] == dict(
                id="tracking-id-0", status=201, message="Created"
            )
            assert data["responses"][1] == dict(
                id="tracking-id-1", status=201, message="Created"
            )

            entry = get_one(db.session, PlaytimeEntry, tracking_id="tracking-id-0")
            assert entry is not None
            assert entry.identifier == identifier
            assert entry.collection == collection
            assert entry.library == db.default_library()
            assert entry.total_seconds_played == 12
            assert entry.timestamp.isoformat() == "2000-01-01T12:00:00+00:00"

            entry = get_one(db.session, PlaytimeEntry, tracking_id="tracking-id-1")
            assert entry is not None
            assert entry.identifier == identifier
            assert entry.collection == collection
            assert entry.library == db.default_library()
            assert entry.total_seconds_played == 17
            assert entry.timestamp.isoformat() == "2000-01-01T12:01:00+00:00"

    def test_track_playtime_duplicate_id_ok(
        self, circulation_fixture: CirculationControllerFixture
    ):
        db = circulation_fixture.db
        identifier = db.identifier()
        collection = db.default_collection()
        patron = db.patron()

        db.session.add(
            PlaytimeEntry(
                tracking_id="tracking-id-0",
                timestamp="2000-01-01T12:00Z",  # type: ignore
                total_seconds_played=12,
                identifier_id=identifier.id,
                collection_id=collection.id,
                library_id=db.default_library().id,
            )
        )

        data = dict(
            timeEntries=[
                {
                    "id": "tracking-id-0",
                    "during_minute": "2000-01-01T12:00Z",
                    "seconds_played": 12,
                },
                {
                    "id": "tracking-id-1",
                    "during_minute": "2000-01-01T12:01Z",
                    "seconds_played": 12,
                },
            ]
        )
        with circulation_fixture.request_context_with_library(
            "/", method="POST", json=data
        ):
            flask.request.patron = patron  # type: ignore
            response = circulation_fixture.manager.playtime_entries.track_playtimes(
                collection.id, identifier.type, identifier.identifier
            )

            assert response.status_code == 207
            data = response.json

            # A duplicate tracking id is considered OK
            assert data["summary"] == dict(failures=0, successes=2, total=2)
            assert data["responses"][0] == dict(
                status=200, message="OK", id="tracking-id-0"
            )
            assert data["responses"][1] == dict(
                status=201, message="Created", id="tracking-id-1"
            )

    def test_track_playtime_failure(
        self, circulation_fixture: CirculationControllerFixture
    ):
        db = circulation_fixture.db
        identifier = db.identifier()
        collection = db.default_collection()
        patron = db.patron()

        data = dict(
            timeEntries=[
                {
                    "id": "tracking-id-1",
                    "during_minute": "2000-01-01T12:01Z",
                    "seconds_played": 12,
                }
            ]
        )
        with circulation_fixture.request_context_with_library(
            "/", method="POST", json=data
        ):
            flask.request.patron = patron  # type: ignore
            with patch("api.controller.create") as mock_create:
                mock_create.side_effect = IntegrityError(
                    "STATEMENT", {}, Exception("Fake Exception")
                )
                response = circulation_fixture.manager.playtime_entries.track_playtimes(
                    collection.id, identifier.type, identifier.identifier
                )

            assert response.status_code == 207
            data = response.json

            assert data["summary"] == dict(failures=1, successes=0, total=1)
            assert data["responses"] == [
                dict(status=400, message="Fake Exception", id="tracking-id-1")
            ]
