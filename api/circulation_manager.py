from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import flask
from dependency_injector.wiring import Provide, inject
from expiringdict import ExpiringDict
from flask_babel import lazy_gettext as _
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.authenticator import Authenticator
from api.circulation import CirculationAPI, CirculationApiType
from api.config import Configuration
from api.controller.analytics import AnalyticsController
from api.controller.annotation import AnnotationController
from api.controller.device_tokens import DeviceTokensController
from api.controller.index import IndexController
from api.controller.loan import LoanController
from api.controller.marc import MARCRecordController
from api.controller.odl_notification import ODLNotificationController
from api.controller.opds_feed import OPDSFeedController
from api.controller.patron_auth_token import PatronAuthTokenController
from api.controller.playtime_entries import PlaytimeEntriesController
from api.controller.profile import ProfileController
from api.controller.urn_lookup import URNLookupController
from api.controller.work import WorkController
from api.integration.registry.license_providers import LicenseProvidersRegistry
from api.lanes import load_lanes
from api.problem_details import NO_SUCH_LANE
from api.saml.controller import SAMLController
from core.analytics import Analytics
from core.app_server import ApplicationVersionController, load_facets_from_request
from core.config import CannotLoadConfiguration
from core.feed.annotator.circulation import (
    CirculationManagerAnnotator,
    LibraryAnnotator,
)
from core.lane import Lane, WorkList
from core.model import Collection, Library
from core.model.discovery_service_registration import DiscoveryServiceRegistration
from core.service.container import Services
from core.service.logging.configuration import LogLevel
from core.util.log import LoggerMixin, elapsed_time_logging, log_elapsed_time

if TYPE_CHECKING:
    from api.admin.controller.admin_search import AdminSearchController
    from api.admin.controller.announcement_service import AnnouncementSettings
    from api.admin.controller.catalog_services import CatalogServicesController
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
    from api.admin.controller.metadata_services import MetadataServicesController
    from api.admin.controller.patron import PatronController
    from api.admin.controller.patron_auth_services import PatronAuthServicesController
    from api.admin.controller.quicksight import QuickSightController
    from api.admin.controller.report import ReportController
    from api.admin.controller.reset_password import ResetPasswordController
    from api.admin.controller.sign_in import SignInController
    from api.admin.controller.timestamps import TimestampsController
    from api.admin.controller.view import ViewController
    from api.admin.controller.work_editor import WorkController as AdminWorkController


class CirculationManager(LoggerMixin):
    # API Controllers
    index_controller: IndexController
    opds_feeds: OPDSFeedController
    marc_records: MARCRecordController
    loans: LoanController
    annotations: AnnotationController
    urn_lookup: URNLookupController
    work_controller: WorkController
    analytics_controller: AnalyticsController
    profiles: ProfileController
    patron_devices: DeviceTokensController
    version: ApplicationVersionController
    odl_notification_controller: ODLNotificationController
    playtime_entries: PlaytimeEntriesController

    # Admin controllers
    admin_sign_in_controller: SignInController
    admin_reset_password_controller: ResetPasswordController
    timestamps_controller: TimestampsController
    admin_work_controller: AdminWorkController
    admin_feed_controller: FeedController
    admin_custom_lists_controller: CustomListsController
    admin_lanes_controller: LanesController
    admin_dashboard_controller: DashboardController
    admin_patron_controller: PatronController
    admin_discovery_services_controller: DiscoveryServicesController
    admin_discovery_service_library_registrations_controller: DiscoveryServiceLibraryRegistrationsController
    admin_metadata_services_controller: MetadataServicesController
    admin_patron_auth_services_controller: PatronAuthServicesController
    admin_collection_settings_controller: CollectionSettingsController
    admin_library_settings_controller: LibrarySettingsController
    admin_individual_admin_settings_controller: IndividualAdminSettingsController
    admin_catalog_services_controller: CatalogServicesController
    admin_announcement_service: AnnouncementSettings
    admin_search_controller: AdminSearchController
    admin_view_controller: ViewController
    admin_quicksight_controller: QuickSightController
    admin_report_controller: ReportController

    @inject
    def __init__(
        self,
        _db,
        services: Services = Provide[Services],
    ):
        self._db = _db
        self.services = services
        self.analytics = services.analytics.analytics()
        self.external_search = services.search.index()
        self.site_configuration_last_update = (
            Configuration.site_configuration_last_update(self._db, timeout=0)
        )
        self.setup_one_time_controllers()
        self.patron_web_domains: set[str] = set()
        authentication_document_cache_time = (
            self.services.config.sitewide.authentication_document_cache_time()
        )
        self.authentication_for_opds_documents = ExpiringDict(
            max_len=1000, max_age_seconds=authentication_document_cache_time
        )
        self.load_settings()

    def load_facets_from_request(self, *args, **kwargs):
        """Load a faceting object from the incoming request, but also apply some
        application-specific access restrictions:

        * You can't use nonstandard caching rules unless you're an authenticated administrator.
        * You can't access a WorkList that's not accessible to you.
        """

        facets = load_facets_from_request(*args, **kwargs)

        worklist = kwargs.get("worklist")
        if worklist is not None:
            # Try to get the index controller. If it's not initialized
            # for any reason, don't run this check -- we have bigger
            # problems.
            index_controller = getattr(self, "index_controller", None)
            if index_controller and not worklist.accessible_to(
                index_controller.request_patron
            ):
                return NO_SUCH_LANE.detailed(_("Lane does not exist"))

        return facets

    def reload_settings_if_changed(self):
        """If the site configuration has been updated, reload the
        CirculationManager's configuration from the database.
        """
        last_update = Configuration.site_configuration_last_update(self._db)
        if last_update > self.site_configuration_last_update:
            self.load_settings()
            self.site_configuration_last_update = last_update

    def get_patron_web_domains(self) -> set[str]:
        """Return the set of patron web client domains."""
        # Assemble the list of patron web client domains from individual
        # library registration settings as well as a sitewide setting.
        patron_web_domains: set[str] = set()
        sitewide_patron_web_domains = (
            self.services.config.sitewide.patron_web_hostnames()
        )
        if not isinstance(sitewide_patron_web_domains, list):
            sitewide_patron_web_domains = [sitewide_patron_web_domains]

        patron_web_domains.update(sitewide_patron_web_domains)

        registry_patron_web_domains = [
            row.web_client
            for row in self._db.execute(
                select(DiscoveryServiceRegistration.web_client).where(
                    DiscoveryServiceRegistration.web_client != None
                )
            )
        ]
        patron_web_domains.update(registry_patron_web_domains)

        return patron_web_domains

    @log_elapsed_time(log_level=LogLevel.info, message_prefix="load_settings")
    def load_settings(self):
        """Load all necessary configuration settings and external
        integrations from the database.

        This is called once when the CirculationManager is
        initialized.  It may also be called later to reload the site
        configuration after changes are made in the administrative
        interface.
        """
        with elapsed_time_logging(
            log_method=self.log.debug,
            skip_start=True,
            message_prefix="load_settings - load libraries",
        ):
            libraries = self._db.query(Library).all()

        with elapsed_time_logging(
            log_method=self.log.debug,
            skip_start=True,
            message_prefix="load_settings - populate caches",
        ):
            # Populate caches
            Library.cache_warm(self._db, lambda: libraries)

        with elapsed_time_logging(
            log_method=self.log.debug,
            skip_start=True,
            message_prefix="load_settings - populate collection info",
        ):
            collections: set[Collection] = set()
            libraries_collections: dict[int | None, list[Collection]] = {}
            for library in libraries:
                library_collections = library.collections
                collections.update(library_collections)
                libraries_collections[library.id] = library_collections

        with elapsed_time_logging(
            log_method=self.log.debug,
            skip_start=True,
            message_prefix="load_settings - create collection apis",
        ):
            collection_apis = {}
            registry = LicenseProvidersRegistry()
            for collection in collections:
                try:
                    api = registry[collection.protocol](self._db, collection)
                    collection_apis[collection.id] = api
                except CannotLoadConfiguration as exception:
                    self.log.exception(
                        "Error loading configuration for {}: {}".format(
                            collection.name, str(exception)
                        )
                    )
                except KeyError:
                    self.log.exception(
                        f"Unable to load protocol {collection.protocol} for collection {collection.name}"
                    )

        self.auth = Authenticator(self._db, libraries, self.analytics)

        # Track the Lane configuration for each library by mapping its
        # short name to the top-level lane.
        new_top_level_lanes = {}
        # Create a CirculationAPI for each library.
        new_circulation_apis = {}

        with elapsed_time_logging(
            log_method=self.log.debug,
            message_prefix="load_settings - per-library lanes",
        ):
            for library in libraries:
                new_top_level_lanes[library.id] = load_lanes(
                    self._db, library, [c.id for c in libraries_collections[library.id]]
                )

        with elapsed_time_logging(
            log_method=self.log.debug,
            message_prefix="load_settings - api",
        ):
            for library in libraries:
                library_collection_apis = {
                    collection.id: collection_apis[collection.id]
                    for collection in libraries_collections[library.id]
                }
                new_circulation_apis[library.id] = self.setup_circulation_api(
                    self._db, library, library_collection_apis, self.analytics
                )

        self.top_level_lanes = new_top_level_lanes
        self.circulation_apis = new_circulation_apis

        self.patron_web_domains = self.get_patron_web_domains()
        self.setup_configuration_dependent_controllers()
        self.authentication_for_opds_documents.clear()

    def log_lanes(self, lanelist=None, level=0):
        """Output information about the lane layout."""
        lanelist = lanelist or self.top_level_lane.sublanes
        for lane in lanelist:
            self.log.debug("%s%r", "-" * level, lane)
            if lane.sublanes:
                self.log_lanes(lane.sublanes, level + 1)

    def setup_circulation_api(
        self,
        db: Session,
        library: Library,
        library_collection_apis: Mapping[int | None, CirculationApiType],
        analytics: Analytics | None = None,
    ) -> CirculationAPI:
        """Set up the Circulation API object."""
        return CirculationAPI(db, library, library_collection_apis, analytics=analytics)

    def setup_one_time_controllers(self):
        """Set up all the controllers that will be used by the web app.

        This method will be called only once, no matter how many times the
        site configuration changes.
        """
        self.index_controller = IndexController(self)
        self.opds_feeds = OPDSFeedController(self)
        self.marc_records = MARCRecordController(self.services.storage.public())
        self.loans = LoanController(self)
        self.annotations = AnnotationController(self)
        self.urn_lookup = URNLookupController(self)
        self.work_controller = WorkController(self)
        self.analytics_controller = AnalyticsController(self)
        self.profiles = ProfileController(self)
        self.patron_devices = DeviceTokensController(self)
        self.version = ApplicationVersionController()
        self.odl_notification_controller = ODLNotificationController(self)
        self.patron_auth_token = PatronAuthTokenController(self)
        self.playtime_entries = PlaytimeEntriesController(self)

    def setup_configuration_dependent_controllers(self):
        """Set up all the controllers that depend on the
        current site configuration.

        This method will be called fresh every time the site
        configuration changes.
        """
        self.saml_controller = SAMLController(self, self.auth)

    def annotator(self, lane, facets=None, *args, **kwargs):
        """Create an appropriate OPDS annotator for the given lane.

        :param lane: A Lane or WorkList.
        :param facets: A faceting object.
        :param annotator_class: Instantiate this annotator class if possible.
           Intended for use in unit tests.
        """
        library = None
        if lane and isinstance(lane, Lane):
            library = lane.library
        elif lane and isinstance(lane, WorkList):
            library = lane.get_library(self._db)
        if not library and hasattr(flask.request, "library"):
            library = flask.request.library

        # If no library is provided, the best we can do is a generic
        # annotator for this application.
        if not library:
            return CirculationManagerAnnotator(lane)

        # At this point we know the request is in a library context, so we
        # can create a LibraryAnnotator customized for that library.

        # Some features are only available if a patron authentication
        # mechanism is set up for this library.
        authenticator = self.auth.library_authenticators.get(library.short_name)
        library_identifies_patrons = (
            authenticator is not None and authenticator.identifies_individuals
        )
        annotator_class = kwargs.pop("annotator_class", LibraryAnnotator)
        return annotator_class(
            self.circulation_apis[library.id],
            lane,
            library,
            top_level_title="All Books",
            library_identifies_patrons=library_identifies_patrons,
            facets=facets,
            *args,
            **kwargs,
        )

    @property
    def authentication_for_opds_document(self):
        """Make sure the current request's library has an Authentication For
        OPDS document in the cache, then return the cached version.

        If the cache is disabled, a fresh document is created every time.

        If the query argument `debug` is provided and the
        WSGI_DEBUG_KEY site-wide setting is set to True, the
        authentication document is annotated with a '_debug' section
        describing the current WSGI environment. Since this can reveal
        internal details of deployment, it should only be enabled when
        diagnosing deployment problems.
        """
        name = flask.request.library.short_name
        value = self.authentication_for_opds_documents.get(name, None)
        if value is None:
            # The document was not in the cache, either because it's
            # expired or because the cache itself has been disabled.
            # Create a new one and stick it in the cache for next
            # time.
            value = self.auth.create_authentication_document()
            self.authentication_for_opds_documents[name] = value
        return value
