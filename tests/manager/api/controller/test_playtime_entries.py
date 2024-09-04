import datetime
import hashlib
from unittest.mock import patch

import flask
from sqlalchemy.exc import IntegrityError

from palace.manager.api.controller.playtime_entries import (
    MISSING_LOAN_IDENTIFIER,
    resolve_loan_identifier,
)
from palace.manager.sqlalchemy.model.patron import Loan
from palace.manager.sqlalchemy.model.time_tracking import PlaytimeEntry
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.problem_detail import ProblemDetail
from tests.fixtures.api_controller import CirculationControllerFixture


def date_string(hour=None, minute=None):
    now = utc_now()
    if hour is not None:
        now = now.replace(hour=hour)
    if minute is not None:
        now = now.replace(minute=minute)
    return now.strftime("%Y-%m-%dT%H:%M:00+00:00")


class TestPlaytimeEntriesController:
    def test_track_playtime(self, circulation_fixture: CirculationControllerFixture):
        db = circulation_fixture.db
        identifier = db.identifier()
        collection = db.default_collection()
        # Attach the identifier to the collection
        pool = db.licensepool(
            db.edition(
                identifier_type=identifier.type, identifier_id=identifier.identifier
            ),
            collection=collection,
        )
        patron = db.patron()

        loan_exists_date_str = date_string(hour=12, minute=0)
        inscope_loan_start = datetime.datetime.fromisoformat(loan_exists_date_str)
        inscope_loan_end = inscope_loan_start + datetime.timedelta(days=14)

        loan, _ = pool.loan_to(
            patron,
            inscope_loan_start,
            inscope_loan_end,
        )

        expected_loan_identifier = resolve_loan_identifier(loan=loan)

        data = dict(
            timeEntries=[
                {
                    "id": "tracking-id-0",
                    "during_minute": loan_exists_date_str,
                    "seconds_played": 12,
                },
                {
                    "id": "tracking-id-1",
                    "during_minute": date_string(hour=12, minute=1),
                    "seconds_played": 17,
                },
                {
                    "id": "tracking-id-2",
                    "during_minute": "2000-01-01T12:00Z",  # A very old entry, 410
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
            assert data["summary"] == dict(total=3, successes=2, failures=1)
            assert len(data["responses"]) == 3
            assert data["responses"][0] == dict(
                id="tracking-id-0", status=201, message="Created"
            )
            assert data["responses"][1] == dict(
                id="tracking-id-1", status=201, message="Created"
            )
            assert data["responses"][2] == dict(
                id="tracking-id-2",
                status=410,
                message="Time entry too old and can no longer be processed",
            )

            entry = get_one(db.session, PlaytimeEntry, tracking_id="tracking-id-0")
            assert entry is not None
            assert entry.identifier == identifier
            assert entry.collection == collection
            assert entry.library == db.default_library()
            assert entry.total_seconds_played == 12
            assert entry.timestamp.isoformat() == date_string(hour=12, minute=0)
            assert entry.loan_identifier == expected_loan_identifier

            entry = get_one(db.session, PlaytimeEntry, tracking_id="tracking-id-1")
            assert entry is not None
            assert entry.identifier == identifier
            assert entry.collection == collection
            assert entry.library == db.default_library()
            assert entry.total_seconds_played == 17
            assert entry.timestamp.isoformat() == date_string(hour=12, minute=1)
            assert entry.loan_identifier == expected_loan_identifier

            # The very old entry does not get recorded
            assert None == get_one(
                db.session, PlaytimeEntry, tracking_id="tracking-id-2"
            )

    def test_resolve_loan_identifier(self):
        no_loan = resolve_loan_identifier(loan=None)
        test_id = 1
        test_loan_identifier = resolve_loan_identifier(Loan(id=test_id))
        assert no_loan == MISSING_LOAN_IDENTIFIER
        assert (
            test_loan_identifier
            == hashlib.sha1(f"loan: {test_id}".encode()).hexdigest()
        )

    def test_track_playtime_duplicate_id_ok(
        self, circulation_fixture: CirculationControllerFixture
    ):
        db = circulation_fixture.db
        identifier = db.identifier()
        collection = db.default_collection()
        library = db.default_library()
        patron = db.patron()
        # Attach the identifier to the collection
        pool = db.licensepool(
            db.edition(
                identifier_type=identifier.type, identifier_id=identifier.identifier
            ),
            collection=collection,
        )

        loan_identifier = resolve_loan_identifier(loan=None)

        db.session.add(
            PlaytimeEntry(
                tracking_id="tracking-id-0",
                timestamp=date_string(hour=12, minute=0),
                total_seconds_played=12,
                identifier_id=identifier.id,
                collection_id=collection.id,
                library_id=library.id,
                identifier_str=identifier.urn,
                collection_name=collection.name,
                library_name=library.name,
                loan_identifier=loan_identifier,
            )
        )

        data = dict(
            timeEntries=[
                {
                    "id": "tracking-id-0",
                    "during_minute": date_string(hour=12, minute=0),
                    "seconds_played": 12,
                },
                {
                    "id": "tracking-id-1",
                    "during_minute": date_string(hour=12, minute=1),
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
        # Attach the identifier to the collection
        pool = db.licensepool(
            db.edition(
                identifier_type=identifier.type, identifier_id=identifier.identifier
            ),
            collection=collection,
        )
        patron = db.patron()

        data = dict(
            timeEntries=[
                {
                    "id": "tracking-id-1",
                    "during_minute": date_string(hour=12, minute=1),
                    "seconds_played": 12,
                }
            ]
        )
        with circulation_fixture.request_context_with_library(
            "/", method="POST", json=data
        ):
            flask.request.patron = patron  # type: ignore
            with patch(
                "palace.manager.core.query.playtime_entries.create"
            ) as mock_create:
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

    def test_api_validation(self, circulation_fixture: CirculationControllerFixture):
        db = circulation_fixture.db
        identifier = db.identifier()
        collection = db.collection()
        library = db.default_library()
        patron = db.patron()

        with circulation_fixture.request_context_with_library(
            "/", method="POST", json={}
        ):
            flask.request.patron = patron  # type: ignore

            # Bad identifier
            response = circulation_fixture.manager.playtime_entries.track_playtimes(
                collection.id, identifier.type, "not-an-identifier"
            )
            assert isinstance(response, ProblemDetail)
            assert response.status_code == 404
            assert (
                response.detail
                == "The identifier Gutenberg ID/not-an-identifier was not found."
            )

            # Bad collection
            response = circulation_fixture.manager.playtime_entries.track_playtimes(
                9088765, identifier.type, identifier.identifier
            )
            assert isinstance(response, ProblemDetail)
            assert response.status_code == 404
            assert response.detail == f"The collection 9088765 was not found."

            # Collection not in library
            response = circulation_fixture.manager.playtime_entries.track_playtimes(
                collection.id, identifier.type, identifier.identifier
            )
            assert isinstance(response, ProblemDetail)
            assert response.status_code == 400
            assert response.detail == "Collection was not found in the Library."

            # Identifier not part of collection
            collection.libraries.append(library)
            response = circulation_fixture.manager.playtime_entries.track_playtimes(
                collection.id, identifier.type, identifier.identifier
            )
            assert isinstance(response, ProblemDetail)
            assert response.status_code == 400
            assert response.detail == "This Identifier was not found in the Collection."

            # Attach the identifier to the collection
            pool = db.licensepool(
                db.edition(
                    identifier_type=identifier.type, identifier_id=identifier.identifier
                ),
                collection=collection,
            )

            # Incorrect JSON format
            response = circulation_fixture.manager.playtime_entries.track_playtimes(
                collection.id, identifier.type, identifier.identifier
            )
            assert isinstance(response, ProblemDetail)
            assert response.status_code == 400
            assert response.detail is not None
            assert "timeEntries" in response.detail
            assert "field required" in response.detail
