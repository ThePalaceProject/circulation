import pytest

from palace.manager.api.admin.controller.base import (
    AdminController,
    AdminPermissionsControllerMixin,
)
from palace.manager.api.admin.exceptions import AdminNotAuthorized
from palace.manager.sqlalchemy.model.admin import AdminRole
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


@pytest.fixture()
def controller() -> AdminPermissionsControllerMixin:
    return AdminPermissionsControllerMixin()


class TestAdminPermissionsControllerMixin:
    def test_require_system_admin(
        self,
        controller: AdminPermissionsControllerMixin,
        flask_app_fixture: FlaskAppFixture,
    ):
        with flask_app_fixture.test_request_context("/admin"):
            pytest.raises(
                AdminNotAuthorized,
                controller.require_system_admin,
            )

        with flask_app_fixture.test_request_context_system_admin("/admin"):
            controller.require_system_admin()

    def test_require_sitewide_library_manager(
        self,
        controller: AdminPermissionsControllerMixin,
        flask_app_fixture: FlaskAppFixture,
    ):
        with flask_app_fixture.test_request_context("/admin"):
            pytest.raises(
                AdminNotAuthorized,
                controller.require_sitewide_library_manager,
            )

        library_manager = flask_app_fixture.admin_user(
            role=AdminRole.SITEWIDE_LIBRARY_MANAGER
        )
        with flask_app_fixture.test_request_context("/admin", admin=library_manager):
            controller.require_sitewide_library_manager()

    def test_require_library_manager(
        self,
        controller: AdminPermissionsControllerMixin,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        with flask_app_fixture.test_request_context("/admin"):
            pytest.raises(
                AdminNotAuthorized,
                controller.require_library_manager,
                db.default_library(),
            )

        library_manager = flask_app_fixture.admin_user(
            role=AdminRole.LIBRARY_MANAGER, library=db.default_library()
        )
        with flask_app_fixture.test_request_context("/admin", admin=library_manager):
            controller.require_library_manager(db.default_library())

    def test_require_librarian(
        self,
        controller: AdminPermissionsControllerMixin,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        with flask_app_fixture.test_request_context("/admin"):
            pytest.raises(
                AdminNotAuthorized,
                controller.require_librarian,
                db.default_library(),
            )

        librarian = flask_app_fixture.admin_user(
            role=AdminRole.LIBRARIAN, library=db.default_library()
        )
        with flask_app_fixture.test_request_context("/admin", admin=librarian):
            controller.require_librarian(db.default_library())


class TestAdminController:
    @pytest.mark.parametrize(
        "token",
        [
            pytest.param("", id="empty-string"),
            pytest.param("short", id="too-short"),
            pytest.param("a" * 100, id="too-long"),
            pytest.param(
                "YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXo=",
                id="valid-base64-wrong-decoded-length",
            ),
            pytest.param(
                "!!!!invalid-base64-chars-here!!!",
                id="32-chars-invalid-base64-exception",
            ),
            pytest.param(
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
                id="32-chars-incorrect-padding-exception",
            ),
        ],
    )
    def test_validate_csrf_token_invalid(self, token: str) -> None:
        """Test that invalid CSRF tokens are rejected."""
        assert AdminController.validate_csrf_token(token) is False

    def test_validate_csrf_token_valid(self) -> None:
        """Test that valid CSRF tokens are accepted."""
        valid_token = AdminController.generate_csrf_token()
        assert AdminController.validate_csrf_token(valid_token)
        assert len(valid_token) == 32
