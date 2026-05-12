from __future__ import annotations

from typing import TYPE_CHECKING

from palace.manager.api.admin.controller.quicksight import QuickSightController
from palace.manager.api.admin.controller.report import ReportController

if TYPE_CHECKING:
    from palace.manager.api.circulation_manager import CirculationManager


def setup_admin_controllers(manager: CirculationManager) -> None:
    """Set up all the controllers that will be used by the admin parts of the web app."""
    from palace.manager.api.admin.controller.admin_search import AdminSearchController
    from palace.manager.api.admin.controller.announcement_service import (
        AnnouncementSettings,
    )
    from palace.manager.api.admin.controller.catalog_services import (
        CatalogServicesController,
    )
    from palace.manager.api.admin.controller.collection_settings import (
        CollectionSettingsController,
    )
    from palace.manager.api.admin.controller.custom_lists import CustomListsController
    from palace.manager.api.admin.controller.dashboard import DashboardController
    from palace.manager.api.admin.controller.discovery_service_library_registrations import (
        DiscoveryServiceLibraryRegistrationsController,
    )
    from palace.manager.api.admin.controller.discovery_services import (
        DiscoveryServicesController,
    )
    from palace.manager.api.admin.controller.feed import FeedController
    from palace.manager.api.admin.controller.individual_admin_settings import (
        IndividualAdminSettingsController,
    )
    from palace.manager.api.admin.controller.lanes import LanesController
    from palace.manager.api.admin.controller.library_settings import (
        LibrarySettingsController,
    )
    from palace.manager.api.admin.controller.metadata_services import (
        MetadataServicesController,
    )
    from palace.manager.api.admin.controller.patron import PatronController
    from palace.manager.api.admin.controller.patron_auth_services import (
        PatronAuthServicesController,
    )
    from palace.manager.api.admin.controller.reset_password import (
        ResetPasswordController,
    )
    from palace.manager.api.admin.controller.sign_in import SignInController
    from palace.manager.api.admin.controller.timestamps import TimestampsController
    from palace.manager.api.admin.controller.view import ViewController
    from palace.manager.api.admin.controller.work_editor import WorkController

    manager.admin_view_controller = ViewController(manager)
    manager.admin_sign_in_controller = SignInController(manager)
    manager.admin_reset_password_controller = ResetPasswordController(manager)
    manager.timestamps_controller = TimestampsController(manager)
    manager.admin_work_controller = WorkController(manager)
    manager.admin_feed_controller = FeedController(manager)
    manager.admin_custom_lists_controller = CustomListsController(manager)
    manager.admin_lanes_controller = LanesController(manager)
    manager.admin_dashboard_controller = DashboardController(manager)
    manager.admin_patron_controller = PatronController(manager)
    manager.admin_discovery_services_controller = DiscoveryServicesController(
        manager._db, manager.services.integration_registry.discovery()
    )
    manager.admin_discovery_service_library_registrations_controller = (
        DiscoveryServiceLibraryRegistrationsController(
            manager._db, manager.services.integration_registry.discovery()
        )
    )
    manager.admin_metadata_services_controller = MetadataServicesController(
        manager._db, manager.services.integration_registry.metadata()
    )
    manager.admin_patron_auth_services_controller = PatronAuthServicesController(
        manager._db, manager.services.integration_registry.patron_auth()
    )

    manager.admin_collection_settings_controller = CollectionSettingsController(
        manager._db, manager.services.integration_registry.license_providers()
    )
    manager.admin_library_settings_controller = LibrarySettingsController(manager)
    manager.admin_individual_admin_settings_controller = (
        IndividualAdminSettingsController(manager._db)
    )
    manager.admin_catalog_services_controller = CatalogServicesController(
        manager._db, manager.services.integration_registry.catalog_services()
    )
    manager.admin_announcement_service = AnnouncementSettings(manager._db)
    manager.admin_search_controller = AdminSearchController(manager)
    manager.admin_quicksight_controller = QuickSightController(
        manager._db, manager.services.config.sitewide.quicksight_authorized_arns()
    )
    manager.admin_report_controller = ReportController(
        manager._db, manager.services.integration_registry.license_providers()
    )
