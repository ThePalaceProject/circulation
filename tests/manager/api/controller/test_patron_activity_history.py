from __future__ import annotations

import pytest

from palace.manager.api.controller.patron_activity_history import (
    PatronActivityHistoryController,
)
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


class PatronActivityHistoryControllerFixture:
    def __init__(
        self,
        db: DatabaseTransactionFixture,
    ):
        self.controller = PatronActivityHistoryController()
        self.db = db


@pytest.fixture
def controller_fixture(
    db: DatabaseTransactionFixture,
) -> PatronActivityHistoryControllerFixture:
    return PatronActivityHistoryControllerFixture(db)


class TestPatronActivityHistoryController:
    """Test that a client can interact with the Patron Activity History."""

    def test_reset_statistics_uuid(
        self,
        controller_fixture: PatronActivityHistoryControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        patron = db.patron()
        with flask_app_fixture.test_request_context("/", method="PUT", patron=patron):
            uuid1 = patron.uuid
            assert uuid1
            response = controller_fixture.controller.reset_statistics_uuid()
            uuid2 = patron.uuid
            assert uuid1 != uuid2
            assert response.status_code == 200
