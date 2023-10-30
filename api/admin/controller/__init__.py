from __future__ import annotations

from typing import TYPE_CHECKING

from api.admin.controller.quicksight import QuickSightController

if TYPE_CHECKING:
    from api.controller import CirculationManager


def setup_admin_controllers(manager: CirculationManager):
    """Set up all the controllers that will be used by the admin parts of the web app."""
    from api.admin.controller.admin_search import AdminSearchController
    from api.admin.controller.announcement_service import AnnouncementSettings
    from api.admin.controller.catalog_services import CatalogServicesController
    from api.admin.controller.collection_self_tests import CollectionSelfTestsController
    from api.admin.controller.collection_settings import CollectionSettingsController
    from api.admin.controller.custom_lists import CustomListsController
    from api.admin.controller.dashboard import DashboardController
    from api.admin.controller.discovery_service_library_registrations import (
        DiscoveryServiceLibraryRegistrationsController,
    )
    from api.admin.controller.discovery_services import DiscoveryServicesController
    from api.admin.controller.feed import FeedController
    from api.admin.controller.individual_admin_settings import (
        IndividualAdminSettingsController,
    )
    from api.admin.controller.lanes import LanesController
    from api.admin.controller.library_settings import LibrarySettingsController
    from api.admin.controller.metadata_service_self_tests import (
        MetadataServiceSelfTestsController,
    )
    from api.admin.controller.metadata_services import MetadataServicesController
    from api.admin.controller.patron import PatronController
    from api.admin.controller.patron_auth_service_self_tests import (
        PatronAuthServiceSelfTestsController,
    )
    from api.admin.controller.patron_auth_services import PatronAuthServicesController
    from api.admin.controller.reset_password import ResetPasswordController
    from api.admin.controller.search_service_self_tests import (
        SearchServiceSelfTestsController,
    )
    from api.admin.controller.self_tests import SelfTestsController
    from api.admin.controller.settings import SettingsController
    from api.admin.controller.sign_in import SignInController
    from api.admin.controller.sitewide_services import (
        SearchServicesController,
        SitewideServicesController,
    )
    from api.admin.controller.sitewide_settings import (
        SitewideConfigurationSettingsController,
    )
    from api.admin.controller.timestamps import TimestampsController
    from api.admin.controller.view import ViewController
    from api.admin.controller.work_editor import WorkController

    manager.admin_view_controller = ViewController(manager)
    manager.admin_sign_in_controller = SignInController(manager)
    manager.admin_reset_password_controller = ResetPasswordController(manager)
    manager.timestamps_controller = TimestampsController(manager)
    manager.admin_work_controller = WorkController(manager)
    manager.admin_feed_controller = FeedController(manager)
    manager.admin_custom_lists_controller = CustomListsController(manager)
    manager.admin_lanes_controller = LanesController(manager)
    manager.admin_dashboard_controller = DashboardController(manager)
    manager.admin_settings_controller = SettingsController(manager)
    manager.admin_patron_controller = PatronController(manager)
    manager.admin_self_tests_controller = SelfTestsController(manager)
    manager.admin_discovery_services_controller = DiscoveryServicesController(manager)
    manager.admin_discovery_service_library_registrations_controller = (
        DiscoveryServiceLibraryRegistrationsController(manager)
    )
    manager.admin_metadata_services_controller = MetadataServicesController(manager)
    manager.admin_metadata_service_self_tests_controller = (
        MetadataServiceSelfTestsController(manager)
    )
    manager.admin_patron_auth_services_controller = PatronAuthServicesController(
        manager
    )

    manager.admin_patron_auth_service_self_tests_controller = (
        PatronAuthServiceSelfTestsController(manager._db)
    )
    manager.admin_collection_settings_controller = CollectionSettingsController(manager)
    manager.admin_collection_self_tests_controller = CollectionSelfTestsController(
        manager
    )
    manager.admin_sitewide_configuration_settings_controller = (
        SitewideConfigurationSettingsController(manager)
    )
    manager.admin_library_settings_controller = LibrarySettingsController(manager)
    manager.admin_individual_admin_settings_controller = (
        IndividualAdminSettingsController(manager)
    )
    manager.admin_sitewide_services_controller = SitewideServicesController(manager)
    manager.admin_search_service_self_tests_controller = (
        SearchServiceSelfTestsController(manager)
    )
    manager.admin_search_services_controller = SearchServicesController(manager)
    manager.admin_catalog_services_controller = CatalogServicesController(manager)
    manager.admin_announcement_service = AnnouncementSettings(manager)
    manager.admin_search_controller = AdminSearchController(manager)
    manager.admin_quicksight_controller = QuickSightController(manager)
