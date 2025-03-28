import datetime
import hashlib
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import IntegrityError

from palace.manager.api.controller.playtime_entries import (
    MISSING_LOAN_IDENTIFIER,
    PlaytimeEntriesController,
    resolve_loan_identifier,
)
from palace.manager.sqlalchemy.model.patron import Loan
from palace.manager.sqlalchemy.model.time_tracking import PlaytimeEntry
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.problem_detail import ProblemDetail
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


class PlaytimeEntriesControllerFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        mock_manager = MagicMock()
        mock_manager._db = db.session
        self.controller = PlaytimeEntriesController(mock_manager)

    @staticmethod
    def date_string(hour=None, minute=None):
        now = utc_now()
        if hour is not None:
            now = now.replace(hour=hour)
        if minute is not None:
            now = now.replace(minute=minute)
        return now.strftime("%Y-%m-%dT%H:%M:00+00:00")


@pytest.fixture()
def playtime_entries_controller_fixture(
    db: DatabaseTransactionFixture,
) -> PlaytimeEntriesControllerFixture:
    return PlaytimeEntriesControllerFixture(db)


class TestPlaytimeEntriesController:
    @pytest.mark.parametrize(
        "no_loan_end_date",
        [
            pytest.param(False),
            pytest.param(True),
        ],
    )
    def test_track_playtime(
        self,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
        playtime_entries_controller_fixture: PlaytimeEntriesControllerFixture,
        no_loan_end_date: bool,
    ) -> None:
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

        loan_exists_date_str = playtime_entries_controller_fixture.date_string(
            hour=12, minute=0
        )
        inscope_loan_start = datetime.datetime.fromisoformat(loan_exists_date_str)
        inscope_loan_end = (
            None
            if no_loan_end_date
            else inscope_loan_start + datetime.timedelta(days=14)
        )

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
                    "during_minute": playtime_entries_controller_fixture.date_string(
                        hour=12, minute=1
                    ),
                    "seconds_played": 17,
                },
                {
                    "id": "tracking-id-2",
                    "during_minute": "2000-01-01T12:00Z",  # A very old entry, 410
                    "seconds_played": 17,
                },
            ]
        )
        with flask_app_fixture.test_request_context(
            "/", method="POST", json=data, patron=patron, library=db.default_library()
        ):
            response = playtime_entries_controller_fixture.controller.track_playtimes(
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
            assert (
                entry.timestamp.isoformat()
                == playtime_entries_controller_fixture.date_string(hour=12, minute=0)
            )
            assert entry.loan_identifier == expected_loan_identifier

            entry = get_one(db.session, PlaytimeEntry, tracking_id="tracking-id-1")
            assert entry is not None
            assert entry.identifier == identifier
            assert entry.collection == collection
            assert entry.library == db.default_library()
            assert entry.total_seconds_played == 17
            assert (
                entry.timestamp.isoformat()
                == playtime_entries_controller_fixture.date_string(hour=12, minute=1)
            )
            assert entry.loan_identifier == expected_loan_identifier

            # The very old entry does not get recorded
            assert None == get_one(
                db.session, PlaytimeEntry, tracking_id="tracking-id-2"
            )

    def test_resolve_loan_identifier(self) -> None:
        no_loan = resolve_loan_identifier(loan=None)
        test_id = 1
        test_loan_identifier = resolve_loan_identifier(Loan(id=test_id))
        assert no_loan == MISSING_LOAN_IDENTIFIER
        assert (
            test_loan_identifier
            == hashlib.sha1(f"loan: {test_id}".encode()).hexdigest()
        )

    def test_track_playtime_duplicate_id_ok(
        self,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
        playtime_entries_controller_fixture: PlaytimeEntriesControllerFixture,
    ):
        identifier = db.identifier()
        collection = db.default_collection()
        data_source_name = "ds1"
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
                timestamp=playtime_entries_controller_fixture.date_string(
                    hour=12, minute=0
                ),
                total_seconds_played=12,
                identifier_id=identifier.id,
                collection_id=collection.id,
                library_id=library.id,
                identifier_str=identifier.urn,
                collection_name=collection.name,
                library_name=library.name or "",
                loan_identifier=loan_identifier,
                data_source_name=data_source_name,
            )
        )

        data = dict(
            timeEntries=[
                {
                    "id": "tracking-id-0",
                    "during_minute": playtime_entries_controller_fixture.date_string(
                        hour=12, minute=0
                    ),
                    "seconds_played": 12,
                },
                {
                    "id": "tracking-id-1",
                    "during_minute": playtime_entries_controller_fixture.date_string(
                        hour=12, minute=1
                    ),
                    "seconds_played": 12,
                },
            ]
        )
        with flask_app_fixture.test_request_context(
            "/", method="POST", json=data, library=library, patron=patron
        ):
            response = playtime_entries_controller_fixture.controller.track_playtimes(
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
        self,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
        playtime_entries_controller_fixture: PlaytimeEntriesControllerFixture,
    ):
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
                    "during_minute": playtime_entries_controller_fixture.date_string(
                        hour=12, minute=1
                    ),
                    "seconds_played": 12,
                }
            ]
        )
        with flask_app_fixture.test_request_context(
            "/", method="POST", json=data, patron=patron, library=db.default_library()
        ):
            with patch(
                "palace.manager.core.query.playtime_entries.create"
            ) as mock_create:
                mock_create.side_effect = IntegrityError(
                    "STATEMENT", {}, Exception("Fake Exception")
                )
                response = (
                    playtime_entries_controller_fixture.controller.track_playtimes(
                        collection.id, identifier.type, identifier.identifier
                    )
                )

            assert response.status_code == 207
            data = response.json

            assert data["summary"] == dict(failures=1, successes=0, total=1)
            assert data["responses"] == [
                dict(status=400, message="Fake Exception", id="tracking-id-1")
            ]

    def test_api_validation(
        self,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
        playtime_entries_controller_fixture: PlaytimeEntriesControllerFixture,
    ):
        identifier = db.identifier()
        collection = db.collection()
        library = db.default_library()
        patron = db.patron()

        tracking_request_data = dict(
            timeEntries=[
                {
                    "id": "tracking-id-1",
                    "during_minute": playtime_entries_controller_fixture.date_string(
                        hour=12, minute=1
                    ),
                    "seconds_played": 12,
                },
                {
                    "id": "tracking-id-2",
                    "during_minute": playtime_entries_controller_fixture.date_string(
                        hour=12, minute=2
                    ),
                    "seconds_played": 60,
                },
                {
                    "id": "tracking-id-1",
                    "during_minute": playtime_entries_controller_fixture.date_string(
                        hour=12, minute=3
                    ),
                    "seconds_played": 60,
                },
            ]
        )

        def expected_207_response_body(expected_message, expected_status_code):
            count = len(tracking_request_data["timeEntries"])
            entry_responses = [
                dict(
                    id=entry["id"],
                    status=expected_status_code,
                    message=expected_message,
                )
                for entry in tracking_request_data["timeEntries"]
            ]
            response_body = {
                "responses": entry_responses,
                "summary": {"total": count, "successes": 0, "failures": count},
            }
            return response_body

        with flask_app_fixture.test_request_context(
            "/",
            method="POST",
            json=tracking_request_data,
            library=None,
            patron=patron,
        ):
            # Bad library
            response = playtime_entries_controller_fixture.controller.track_playtimes(
                collection.id, identifier.type, "not-an-identifier"
            )
            assert response.status_code == 207
            assert response.json == expected_207_response_body(
                "The library was not found.", 410
            )

        with flask_app_fixture.test_request_context(
            "/",
            method="POST",
            json=tracking_request_data,
            library=library,
            patron=patron,
        ):
            # Bad identifier
            response = playtime_entries_controller_fixture.controller.track_playtimes(
                collection.id, identifier.type, "not-an-identifier"
            )
            assert response.status_code == 207
            assert response.json == expected_207_response_body(
                "The identifier Gutenberg ID/not-an-identifier was not found.", 410
            )

            # Bad collection
            response = playtime_entries_controller_fixture.controller.track_playtimes(
                9088765, identifier.type, identifier.identifier
            )
            assert response.status_code == 207
            assert response.json == expected_207_response_body(
                "The collection 9088765 was not found.", 410
            )

            # Collection not in library
            response = playtime_entries_controller_fixture.controller.track_playtimes(
                collection.id, identifier.type, identifier.identifier
            )
            assert response.status_code == 207
            assert response.json == expected_207_response_body(
                "Collection was not found in the Library.", 410
            )

            # Add the collection to the library.
            collection.associated_libraries.append(library)

            # Identifier not part of collection
            response = playtime_entries_controller_fixture.controller.track_playtimes(
                collection.id, identifier.type, identifier.identifier
            )
            assert response.status_code == 207
            assert response.json == expected_207_response_body(
                "This Identifier was not found in the Collection.", 410
            )

        # Attach the identifier to the collection.
        _ = db.licensepool(
            db.edition(
                identifier_type=identifier.type, identifier_id=identifier.identifier
            ),
            collection=collection,
        )

        with flask_app_fixture.test_request_context(
            "/", method="POST", json={}, library=library, patron=patron
        ):
            # Incorrect JSON format
            response = playtime_entries_controller_fixture.controller.track_playtimes(
                collection.id, identifier.type, identifier.identifier
            )
            assert isinstance(response, ProblemDetail)
            assert response.status_code == 400
            assert response.detail is not None
            assert "timeEntries" in response.detail
            assert "Field required" in response.detail
