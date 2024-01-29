import pytest

from api.admin.controller.base import AdminPermissionsControllerMixin
from api.admin.exceptions import AdminNotAuthorized
from core.model import AdminRole
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
