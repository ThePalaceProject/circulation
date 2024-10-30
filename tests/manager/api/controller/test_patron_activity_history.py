from __future__ import annotations

import pytest

from palace.manager.sqlalchemy.model.patron import Patron
from tests.fixtures.api_controller import ControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


class PatronActivityHistoryFixture(ControllerFixture):
    def __init__(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ):
        super().__init__(db, services_fixture)
        self.default_patron = db.patron()
        self.auth = dict(Authorization=self.valid_auth)


@pytest.fixture(scope="function")
def patron_activity_history_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
):
    return PatronActivityHistoryFixture(db, services_fixture)


class TestPatronActivityHistoryController:
    """Test that a client can interact with the Patron Activity History."""

    def test_erase(self, patron_activity_history_fixture: PatronActivityHistoryFixture):
        with patron_activity_history_fixture.request_context_with_library(
            "/", method="PUT", headers=patron_activity_history_fixture.auth
        ):
            patron = (
                patron_activity_history_fixture.controller.authenticated_patron_from_request()
            )
            assert isinstance(patron, Patron)
            uuid1 = patron.uuid
            assert uuid1
            response = (
                patron_activity_history_fixture.manager.patron_activity_history.erase()
            )
            uuid2 = patron.uuid
            assert uuid1 != uuid2
            assert 200 == response.status_code
