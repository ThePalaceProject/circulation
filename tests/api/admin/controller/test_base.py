import pytest

from api.admin.exceptions import AdminNotAuthorized
from core.model import AdminRole
from tests.fixtures.api_admin import AdminControllerFixture


class TestAdminPermissionsControllerMixin:
    def test_require_system_admin(self, admin_ctrl_fixture: AdminControllerFixture):
        with admin_ctrl_fixture.request_context_with_admin("/admin"):
            pytest.raises(
                AdminNotAuthorized,
                admin_ctrl_fixture.manager.admin_work_controller.require_system_admin,
            )

            admin_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
            admin_ctrl_fixture.manager.admin_work_controller.require_system_admin()

    def test_require_sitewide_library_manager(
        self, admin_ctrl_fixture: AdminControllerFixture
    ):
        with admin_ctrl_fixture.request_context_with_admin("/admin"):
            pytest.raises(
                AdminNotAuthorized,
                admin_ctrl_fixture.manager.admin_work_controller.require_sitewide_library_manager,
            )

            admin_ctrl_fixture.admin.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
            admin_ctrl_fixture.manager.admin_work_controller.require_sitewide_library_manager()

    def test_require_library_manager(self, admin_ctrl_fixture: AdminControllerFixture):
        with admin_ctrl_fixture.request_context_with_admin("/admin"):
            pytest.raises(
                AdminNotAuthorized,
                admin_ctrl_fixture.manager.admin_work_controller.require_library_manager,
                admin_ctrl_fixture.ctrl.db.default_library(),
            )

            admin_ctrl_fixture.admin.add_role(
                AdminRole.LIBRARY_MANAGER, admin_ctrl_fixture.ctrl.db.default_library()
            )
            admin_ctrl_fixture.manager.admin_work_controller.require_library_manager(
                admin_ctrl_fixture.ctrl.db.default_library()
            )

    def test_require_librarian(self, admin_ctrl_fixture: AdminControllerFixture):
        with admin_ctrl_fixture.request_context_with_admin("/admin"):
            pytest.raises(
                AdminNotAuthorized,
                admin_ctrl_fixture.manager.admin_work_controller.require_librarian,
                admin_ctrl_fixture.ctrl.db.default_library(),
            )

            admin_ctrl_fixture.admin.add_role(
                AdminRole.LIBRARIAN, admin_ctrl_fixture.ctrl.db.default_library()
            )
            admin_ctrl_fixture.manager.admin_work_controller.require_librarian(
                admin_ctrl_fixture.ctrl.db.default_library()
            )
