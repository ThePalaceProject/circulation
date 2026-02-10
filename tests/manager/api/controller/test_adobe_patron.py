from __future__ import annotations

import pytest

from palace.manager.api.adobe_vendor_id import AuthdataUtility
from palace.manager.sqlalchemy.model.patron import Patron
from tests.fixtures.api_controller import ControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


class AdobePatronFixture(ControllerFixture):
    pass


@pytest.fixture(scope="function")
def adobe_patron_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
):
    with AdobePatronFixture.fixture(db, services_fixture) as fixture:
        yield fixture


class TestAdobePatronController:
    """Tests for the patron-facing Adobe ID deletion endpoint."""

    def test_delete_adobe_id_removes_credentials(
        self, adobe_patron_fixture: AdobePatronFixture
    ):
        """Deletion removes Adobe-relevant credentials and returns 200."""
        fixture = adobe_patron_fixture
        patron = fixture.default_patron
        fixture.db.credential(
            patron=patron,
            type=AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
        )
        assert len(patron.credentials) == 1

        with fixture.request_context_with_library(
            "/", method="DELETE", headers=dict(Authorization=fixture.valid_auth)
        ):
            fixture.controller.authenticated_patron_from_request()
            response = fixture.manager.adobe_patron.delete_adobe_id()

        assert response.status_code == 200
        fixture.db.session.expire_all()  # refresh from db
        assert patron.credentials == []

    def test_delete_adobe_id_no_credentials_succeeds(
        self, adobe_patron_fixture: AdobePatronFixture
    ):
        """Deletion with no Adobe credentials returns 200 and does not error."""
        fixture = adobe_patron_fixture
        patron = fixture.default_patron
        assert len(patron.credentials) == 0

        with fixture.request_context_with_library(
            "/", method="DELETE", headers=dict(Authorization=fixture.valid_auth)
        ):
            fixture.controller.authenticated_patron_from_request()
            response = fixture.manager.adobe_patron.delete_adobe_id()

        assert response.status_code == 200

    def test_delete_adobe_id_uses_authenticated_patron_only(
        self, adobe_patron_fixture: AdobePatronFixture
    ):
        """Only the authenticated patron's credentials are deleted."""
        fixture = adobe_patron_fixture
        other_patron = fixture.db.patron()
        fixture.db.credential(
            patron=other_patron,
            type=AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
        )
        fixture.db.session.commit()
        assert len(other_patron.credentials) == 1

        with fixture.request_context_with_library(
            "/", method="DELETE", headers=dict(Authorization=fixture.valid_auth)
        ):
            authenticated = fixture.controller.authenticated_patron_from_request()
            assert isinstance(authenticated, Patron)
            assert authenticated.id == fixture.default_patron.id
            response = fixture.manager.adobe_patron.delete_adobe_id()

        assert response.status_code == 200
        fixture.db.session.refresh(other_patron)
        assert len(other_patron.credentials) == 1
