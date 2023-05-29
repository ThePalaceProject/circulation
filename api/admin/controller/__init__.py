import base64
import copy
import json
import logging
import os
import urllib.parse
from datetime import date, datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import flask
from flask import Request, Response, redirect, url_for
from flask_babel import lazy_gettext as _
from flask_pydantic_spec.flask_backend import Context
from pydantic import BaseModel
from sqlalchemy import or_
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import desc, nullslast
from werkzeug.urls import BaseURL, url_parse, url_quote_plus
from werkzeug.wrappers import Response as WerkzeugResponse

from api.admin.config import Configuration as AdminClientConfig
from api.admin.exceptions import *
from api.admin.model.dashboard_statistics import StatisticsResponse
from api.admin.opds import AdminAnnotator, AdminFeed
from api.admin.password_admin_authentication_provider import (
    PasswordAdminAuthenticationProvider,
)
from api.admin.template_styles import (
    body_style,
    error_style,
    hr_style,
    logo_style,
    section_style,
    small_link_style,
)
from api.admin.templates import admin as admin_template
from api.admin.templates import response_template_with_message_and_redirect_button
from api.admin.validator import Validator
from api.adobe_vendor_id import AuthdataUtility
from api.authentication.base import CannotCreateLocalPatron, PatronData
from api.authenticator import LibraryAuthenticator
from api.axis import Axis360API
from api.bibliotheca import BibliothecaAPI
from api.config import Configuration
from api.controller import CirculationManager, CirculationManagerController
from api.enki import EnkiAPI
from api.lanes import create_default_lanes
from api.lcp.collection import LCPAPI
from api.local_analytics_exporter import LocalAnalyticsExporter
from api.odilo import OdiloAPI
from api.odl import ODLAPI, SharedODLAPI
from api.odl2 import ODL2API
from api.opds_for_distributors import OPDSForDistributorsAPI
from api.overdrive import OverdriveAPI
from core.app_server import load_pagination_from_request
from core.classifier import genres
from core.external_search import ExternalSearchIndex
from core.lane import Lane, WorkList
from core.local_analytics_provider import LocalAnalyticsProvider
from core.model import (
    Admin,
    AdminRole,
    CirculationEvent,
    Collection,
    ConfigurationSetting,
    CustomList,
    DataSource,
    ExternalIntegration,
    Identifier,
    Library,
    LicensePool,
    Timestamp,
    Work,
    create,
    get_one,
    get_one_or_create,
)
from core.model.classification import Classification, Genre, Subject
from core.model.configuration import ExternalIntegrationLink
from core.model.edition import Edition
from core.opds import AcquisitionFeed
from core.opds2_import import OPDS2Importer
from core.opds_import import OPDSImporter, OPDSImportMonitor
from core.query.customlist import CustomListQueries
from core.s3 import S3UploaderConfiguration
from core.selftest import HasSelfTests
from core.util.cache import memoize
from core.util.flask_util import OPDSFeedResponse
from core.util.languages import LanguageCodes
from core.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from api.admin.problem_details import (
        AUTO_UPDATE_CUSTOM_LIST_CANNOT_HAVE_ENTRIES,
        CANNOT_CHANGE_LIBRARY_FOR_CUSTOM_LIST,
        CANNOT_DELETE_SHARED_LIST,
        COLLECTION_NOT_ASSOCIATED_WITH_LIBRARY,
        CUSTOM_LIST_NAME_ALREADY_IN_USE,
        INVALID_INPUT,
        MISSING_COLLECTION,
        MISSING_CUSTOM_LIST,
        MISSING_INTEGRATION,
    )
    from api.admin.template_styles import (
        body_style,
        error_style,
        hr_style,
        logo_style,
        small_link_style,
    )


def setup_admin_controllers(manager):
    """Set up all the controllers that will be used by the admin parts of the web app."""
    manager.admin_view_controller = ViewController(manager)
    manager.admin_sign_in_controller = SignInController(manager)
    manager.admin_reset_password_controller = ResetPasswordController(manager)
    manager.timestamps_controller = TimestampsController(manager)
    from api.admin.controller.work_editor import WorkController

    manager.admin_work_controller = WorkController(manager)
    manager.admin_feed_controller = FeedController(manager)
    manager.admin_custom_lists_controller = CustomListsController(manager)
    manager.admin_lanes_controller = LanesController(manager)
    manager.admin_dashboard_controller = DashboardController(manager)
    manager.admin_settings_controller = SettingsController(manager)
    manager.admin_patron_controller = PatronController(manager)
    from api.admin.controller.self_tests import SelfTestsController

    manager.admin_self_tests_controller = SelfTestsController(manager)
    from api.admin.controller.discovery_services import DiscoveryServicesController

    manager.admin_discovery_services_controller = DiscoveryServicesController(manager)
    from api.admin.controller.discovery_service_library_registrations import (
        DiscoveryServiceLibraryRegistrationsController,
    )

    manager.admin_discovery_service_library_registrations_controller = (
        DiscoveryServiceLibraryRegistrationsController(manager)
    )
    from api.admin.controller.analytics_services import AnalyticsServicesController

    manager.admin_analytics_services_controller = AnalyticsServicesController(manager)
    from api.admin.controller.metadata_services import MetadataServicesController

    manager.admin_metadata_services_controller = MetadataServicesController(manager)
    from api.admin.controller.metadata_service_self_tests import (
        MetadataServiceSelfTestsController,
    )
    from api.admin.controller.patron_auth_services import PatronAuthServicesController

    manager.admin_metadata_service_self_tests_controller = (
        MetadataServiceSelfTestsController(manager)
    )
    manager.admin_patron_auth_services_controller = PatronAuthServicesController(
        manager
    )
    from api.admin.controller.patron_auth_service_self_tests import (
        PatronAuthServiceSelfTestsController,
    )

    manager.admin_patron_auth_service_self_tests_controller = (
        PatronAuthServiceSelfTestsController(manager._db)
    )

    from api.admin.controller.collection_settings import CollectionSettingsController

    manager.admin_collection_settings_controller = CollectionSettingsController(manager)
    from api.admin.controller.collection_self_tests import CollectionSelfTestsController

    manager.admin_collection_self_tests_controller = CollectionSelfTestsController(
        manager
    )
    from api.admin.controller.collection_library_registrations import (
        CollectionLibraryRegistrationsController,
    )

    manager.admin_collection_library_registrations_controller = (
        CollectionLibraryRegistrationsController(manager)
    )
    from api.admin.controller.sitewide_settings import (
        SitewideConfigurationSettingsController,
    )

    manager.admin_sitewide_configuration_settings_controller = (
        SitewideConfigurationSettingsController(manager)
    )
    from api.admin.controller.library_settings import LibrarySettingsController

    manager.admin_library_settings_controller = LibrarySettingsController(manager)
    from api.admin.controller.individual_admin_settings import (
        IndividualAdminSettingsController,
    )

    manager.admin_individual_admin_settings_controller = (
        IndividualAdminSettingsController(manager)
    )
    from api.admin.controller.sitewide_services import (
        LoggingServicesController,
        SearchServicesController,
        SitewideServicesController,
    )

    manager.admin_sitewide_services_controller = SitewideServicesController(manager)
    manager.admin_logging_services_controller = LoggingServicesController(manager)
    from api.admin.controller.search_service_self_tests import (
        SearchServiceSelfTestsController,
    )

    manager.admin_search_service_self_tests_controller = (
        SearchServiceSelfTestsController(manager)
    )
    manager.admin_search_services_controller = SearchServicesController(manager)
    from api.admin.controller.storage_services import StorageServicesController

    manager.admin_storage_services_controller = StorageServicesController(manager)
    from api.admin.controller.catalog_services import CatalogServicesController

    manager.admin_catalog_services_controller = CatalogServicesController(manager)

    from api.admin.controller.announcement_service import AnnouncementSettings

    manager.admin_announcement_service = AnnouncementSettings(manager)

    manager.admin_search_controller = AdminSearchController(manager)


class SanitizedRedirections:
    """Functions to sanitize redirects."""

    @staticmethod
    def _check_redirect(target: str) -> Tuple[bool, str]:
        """Check that a redirect is allowed.
        Because the URL redirect is assumed to be untrusted user input,
        we extract the URL path and forbid redirecting to external
        hosts.
        """
        redirect_url: BaseURL = url_parse(target)

        # If the redirect isn't asking for a particular host, then it's safe.
        if redirect_url.netloc in (None, ""):
            return True, ""

        # Otherwise, if the redirect is asking for a different host, it's unsafe.
        if redirect_url.netloc != flask.request.host:
            logging.warning(f"Redirecting to {redirect_url.netloc} is not permitted")
            return False, _("Redirecting to an external domain is not allowed.")

        return True, ""

    @staticmethod
    def redirect(target: str) -> WerkzeugResponse:
        """Check that a redirect is allowed before performing it."""
        ok, message = SanitizedRedirections._check_redirect(target)
        if ok:
            return redirect(target, Response=Response)
        else:
            return Response(message, 400)


class AdminController:
    def __init__(self, manager):
        self.manager = manager
        self._db = self.manager._db

    @property
    def admin_auth_providers(self):
        if Admin.with_password(self._db).count() != 0:
            return [PasswordAdminAuthenticationProvider()]

        return []

    def admin_auth_provider(self, type):
        # Return an auth provider with the given type.
        # If no auth provider has this type, return None.
        for provider in self.admin_auth_providers:
            if provider.NAME == type:
                return provider
        return None

    def authenticated_admin_from_request(self):
        """Returns an authenticated admin or a problem detail."""
        if not self.admin_auth_providers:
            return ADMIN_AUTH_NOT_CONFIGURED

        email = flask.session.get("admin_email")
        type = flask.session.get("auth_type")

        if email and type:
            admin = get_one(self._db, Admin, email=email)
            auth = self.admin_auth_provider(type)
            if not auth:
                return ADMIN_AUTH_MECHANISM_NOT_CONFIGURED
            if admin:
                flask.request.admin = admin
                return admin
        flask.request.admin = None
        return INVALID_ADMIN_CREDENTIALS

    def authenticated_admin(self, admin_details):
        """Creates or updates an admin with the given details"""

        admin, is_new = get_one_or_create(self._db, Admin, email=admin_details["email"])

        if is_new and admin_details.get("roles"):
            for role in admin_details.get("roles"):
                if role.get("role") in AdminRole.ROLES:
                    library = Library.lookup(self._db, role.get("library"))
                    if role.get("library") and not library:
                        self.log.warn(
                            "%s authentication provider specified an unknown library for a new admin: %s"
                            % (admin_details.get("type"), role.get("library"))
                        )
                    else:
                        admin.add_role(role.get("role"), library)
                else:
                    self.log.warn(
                        "%s authentication provider specified an unknown role for a new admin: %s"
                        % (admin_details.get("type"), role.get("role"))
                    )

        # Set up the admin's flask session.
        flask.session["admin_email"] = admin_details.get("email")
        flask.session["auth_type"] = admin_details.get("type")

        # A permanent session expires after a fixed time, rather than
        # when the user closes the browser.
        flask.session.permanent = True

        # If this is the first time an admin has been authenticated,
        # make sure there is a value set for the sitewide BASE_URL_KEY
        # setting. If it's not set, set it to the hostname of the
        # current request. This assumes the first authenticated admin
        # is accessing the admin interface through the hostname they
        # want to be used for the site itself.
        base_url = ConfigurationSetting.sitewide(self._db, Configuration.BASE_URL_KEY)
        if not base_url.value:
            base_url.value = urllib.parse.urljoin(flask.request.url, "/")

        return admin

    def check_csrf_token(self):
        """Verifies that the CSRF token in the form data or X-CSRF-Token header
        matches the one in the session cookie.
        """
        cookie_token = self.get_csrf_token()
        header_token = flask.request.headers.get("X-CSRF-Token")
        if not cookie_token or cookie_token != header_token:
            return INVALID_CSRF_TOKEN
        return cookie_token

    def get_csrf_token(self):
        """Returns the CSRF token for the current session."""
        return flask.request.cookies.get("csrf_token")

    def generate_csrf_token(self):
        """Generate a random CSRF token."""
        return base64.b64encode(os.urandom(24)).decode("utf-8")


class AdminCirculationManagerController(CirculationManagerController):
    """Parent class that provides methods for verifying an admin's roles."""

    def require_system_admin(self):
        admin = getattr(flask.request, "admin", None)
        if not admin or not admin.is_system_admin():
            raise AdminNotAuthorized()

    def require_sitewide_library_manager(self):
        admin = getattr(flask.request, "admin", None)
        if not admin or not admin.is_sitewide_library_manager():
            raise AdminNotAuthorized()

    def require_library_manager(self, library):
        admin = getattr(flask.request, "admin", None)
        if not admin or not admin.is_library_manager(library):
            raise AdminNotAuthorized()

    def require_librarian(self, library):
        admin = getattr(flask.request, "admin", None)
        if not admin or not admin.is_librarian(library):
            raise AdminNotAuthorized()

    def require_higher_than_librarian(self):
        # A quick way to check the admin's permissions level without needing to already know the library;
        # used as a fail-safe in AnalyticsServicesController.process_post in case a librarian somehow manages
        # to submit a Local Analytics form despite the checks on the front end.
        admin = getattr(flask.request, "admin", None)
        if not admin or not admin.roles or admin.roles[0].role == "librarian":
            raise AdminNotAuthorized()


class ViewController(AdminController):
    def __call__(self, collection, book, path=None):
        setting_up = self.admin_auth_providers == []
        email = None
        roles = []
        if not setting_up:
            admin = self.authenticated_admin_from_request()
            if isinstance(admin, ProblemDetail):
                redirect_url = flask.request.url
                if collection:
                    redirect_url = redirect_url.replace(
                        collection, url_quote_plus(collection)
                    )
                if book:
                    redirect_url = redirect_url.replace(book, url_quote_plus(book))
                return redirect(
                    url_for("admin_sign_in", redirect=redirect_url, _external=True)
                )

            if not collection and not book and not path:
                if self._db.query(Library).count() > 0:
                    # Find the first library the admin is a librarian of.
                    library_name = None
                    for library in self._db.query(Library).order_by(Library.id.asc()):
                        if admin.is_librarian(library):
                            library_name = library.short_name
                            break
                    if not library_name:
                        return Response(
                            _(
                                "Your admin account doesn't have access to any libraries. Contact your library manager for assistance."
                            ),
                            200,
                        )
                    return redirect(
                        url_for("admin_view", collection=library_name, _external=True)
                    )

            email = admin.email
            for role in admin.roles:
                if role.library:
                    roles.append({"role": role.role, "library": role.library})
                else:
                    roles.append({"role": role.role})

        csrf_token = (
            flask.request.cookies.get("csrf_token") or self.generate_csrf_token()
        )
        admin_js = AdminClientConfig.lookup_asset_url(key="admin_js")
        admin_css = AdminClientConfig.lookup_asset_url(key="admin_css")

        # Find the URL and text to use when rendering the Terms of
        # Service link in the footer.
        sitewide_tos_href = (
            ConfigurationSetting.sitewide(self._db, Configuration.CUSTOM_TOS_HREF).value
            or Configuration.DEFAULT_TOS_HREF
        )

        sitewide_tos_text = (
            ConfigurationSetting.sitewide(self._db, Configuration.CUSTOM_TOS_TEXT).value
            or Configuration.DEFAULT_TOS_TEXT
        )

        local_analytics = get_one(
            self._db,
            ExternalIntegration,
            protocol=LocalAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )
        show_circ_events_download = local_analytics != None

        response = Response(
            flask.render_template_string(
                admin_template,
                app_name=AdminClientConfig.APP_NAME,
                csrf_token=csrf_token,
                sitewide_tos_href=sitewide_tos_href,
                sitewide_tos_text=sitewide_tos_text,
                show_circ_events_download=show_circ_events_download,
                setting_up=setting_up,
                email=email,
                roles=roles,
                admin_js=admin_js,
                admin_css=admin_css,
            )
        )

        # The CSRF token is in its own cookie instead of the session cookie,
        # because if your session expires and you log in again, you should
        # be able to submit a form you already had open. The CSRF token lasts
        # until the user closes the browser window.
        response.set_cookie("csrf_token", csrf_token, httponly=True)
        return response


class TimestampsController(AdminCirculationManagerController):
    """Returns a dict: each key is a type of service (script, monitor, or coverage provider);
    each value is a nested dict in which timestamps are organized by service name and then by collection ID.
    """

    def diagnostics(self):
        self.require_system_admin()
        timestamps = self._db.query(Timestamp).order_by(Timestamp.start)
        sorted = self._sort_by_type(timestamps)
        for type, services in list(sorted.items()):
            for service in services:
                by_collection = self._sort_by_collection(sorted[type][service])
                sorted[type][service] = by_collection
        return sorted

    def _sort_by_type(self, timestamps):
        """Takes a list of Timestamp objects.  Returns a dict: each key is a type of service
        (script, monitor, or coverage provider); each value is a dict in which the keys are the names
        of services and the values are lists of timestamps."""

        result = {}
        for ts in timestamps:
            info = self._extract_info(ts)
            result.setdefault((ts.service_type or "other"), []).append(info)

        for type, data in list(result.items()):
            result[type] = self._sort_by_service(data)

        return result

    def _sort_by_service(self, timestamps):
        """Returns a dict: each key is the name of a service; each value is a list of timestamps."""

        result = {}
        for timestamp in timestamps:
            result.setdefault(timestamp.get("service"), []).append(timestamp)
        return result

    def _sort_by_collection(self, timestamps):
        """Takes a list of timestamps; turns it into a dict in which each key is a
        collection ID and each value is a list of the timestamps associated with that collection.
        """

        result = {}
        for timestamp in timestamps:
            result.setdefault(timestamp.get("collection_name"), []).append(timestamp)
        return result

    def _extract_info(self, timestamp):
        """Takes a Timestamp object and returns a dict"""

        duration = None
        if timestamp.start and timestamp.finish:
            duration = (timestamp.finish - timestamp.start).total_seconds()

        collection_name = "No associated collection"
        if timestamp.collection:
            collection_name = timestamp.collection.name

        return dict(
            id=timestamp.id,
            start=timestamp.start,
            duration=duration,
            exception=timestamp.exception,
            service=timestamp.service,
            collection_name=collection_name,
            achievements=timestamp.achievements,
        )


class SignInController(AdminController):
    HEAD_TEMPLATE = """<head>
<meta charset="utf8">
<title>{app_name}</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;700&display=swap');
</style>
</head>
""".format(
        app_name=AdminClientConfig.APP_NAME
    )

    ERROR_RESPONSE_TEMPLATE = """<!DOCTYPE HTML>
<html lang="en">
{head_html}
<body style="{error}">
<p><strong>%(status_code)d ERROR:</strong> %(message)s</p>
<hr style="{hr}">
<a href="/admin/sign_in" style="{link}">Try again</a>
</body>
</html>""".format(
        head_html=HEAD_TEMPLATE, error=error_style, hr=hr_style, link=small_link_style
    )

    SIGN_IN_TEMPLATE = """<!DOCTYPE HTML>
<html lang="en">
{head_html}
<body style="{body}">
<img src="%(logo_url)s" alt="{app_name}" style="{logo}">
%(auth_provider_html)s
</body>
</html>""".format(
        head_html=HEAD_TEMPLATE,
        body=body_style,
        app_name=AdminClientConfig.APP_NAME,
        logo=logo_style,
    )

    def sign_in(self):
        """Redirects admin if they're signed in, or shows the sign in page."""
        if not self.admin_auth_providers:
            return ADMIN_AUTH_NOT_CONFIGURED

        admin = self.authenticated_admin_from_request()

        if isinstance(admin, ProblemDetail):
            redirect_url = flask.request.args.get("redirect")
            auth_provider_html = [
                auth.sign_in_template(redirect_url)
                for auth in self.admin_auth_providers
            ]
            auth_provider_html = """
                <section style="{section}">
                <hr style="{hr}">or<hr style="{hr}">
                </section>
            """.format(
                section=section_style, hr=hr_style
            ).join(
                auth_provider_html
            )

            html = self.SIGN_IN_TEMPLATE % dict(
                auth_provider_html=auth_provider_html,
                logo_url=AdminClientConfig.lookup_asset_url(key="admin_logo"),
            )
            headers = dict()
            headers["Content-Type"] = "text/html"
            return Response(html, 200, headers)
        elif admin:
            return SanitizedRedirections.redirect(flask.request.args.get("redirect"))

    def password_sign_in(self):
        if not self.admin_auth_providers:
            return ADMIN_AUTH_NOT_CONFIGURED

        auth = self.admin_auth_provider(PasswordAdminAuthenticationProvider.NAME)
        if not auth:
            return ADMIN_AUTH_MECHANISM_NOT_CONFIGURED

        admin_details, redirect_url = auth.sign_in(self._db, flask.request.form)
        if isinstance(admin_details, ProblemDetail):
            return self.error_response(INVALID_ADMIN_CREDENTIALS)

        admin = self.authenticated_admin(admin_details)
        return SanitizedRedirections.redirect(redirect_url)

    def change_password(self):
        admin = flask.request.admin
        new_password = flask.request.form.get("password")
        if new_password:
            admin.password = new_password
        return Response(_("Success"), 200)

    def sign_out(self):
        # Clear out the admin's flask session.
        flask.session.pop("admin_email", None)
        flask.session.pop("auth_type", None)

        redirect_url = url_for(
            "admin_sign_in",
            redirect=url_for("admin_view", _external=True),
            _external=True,
        )
        return SanitizedRedirections.redirect(redirect_url)

    def error_response(self, problem_detail):
        """Returns a problem detail as an HTML response"""
        html = self.ERROR_RESPONSE_TEMPLATE % dict(
            status_code=problem_detail.status_code, message=problem_detail.detail
        )
        return Response(html, problem_detail.status_code)


class ResetPasswordController(AdminController):
    FORGOT_PASSWORD_TEMPLATE = SignInController.SIGN_IN_TEMPLATE
    RESET_PASSWORD_TEMPLATE = SignInController.SIGN_IN_TEMPLATE

    HEAD_TEMPLATE = SignInController.HEAD_TEMPLATE

    RESPONSE_TEMPLATE_WITH_MESSAGE = (
        response_template_with_message_and_redirect_button.format(
            head_html=HEAD_TEMPLATE, hr=hr_style, link=small_link_style
        )
    )

    def forgot_password(self) -> Union[ProblemDetail, WerkzeugResponse]:
        """Shows forgot password page or starts off forgot password workflow"""

        if not self.admin_auth_providers:
            return ADMIN_AUTH_NOT_CONFIGURED

        auth = self.admin_auth_provider(PasswordAdminAuthenticationProvider.NAME)
        if not auth:
            return ADMIN_AUTH_MECHANISM_NOT_CONFIGURED

        admin = self.authenticated_admin_from_request()

        admin_view_redirect = redirect(url_for("admin_view"))

        if isinstance(admin, Admin):
            return admin_view_redirect

        if flask.request.method == "GET":
            auth_provider_html = auth.forgot_password_template(admin_view_redirect)

            html = self.FORGOT_PASSWORD_TEMPLATE % dict(
                auth_provider_html=auth_provider_html,
                logo_url=AdminClientConfig.lookup_asset_url(key="admin_logo"),
            )
            headers = dict()
            headers["Content-Type"] = "text/html"

            return Response(html, 200, headers)

        admin = self._extract_admin_from_request(flask.request)

        if not admin:
            return self._response_with_message_and_redirect_button(
                INVALID_ADMIN_CREDENTIALS.detail,
                url_for("admin_forgot_password"),
                "Try again",
                is_error=True,
                status_code=INVALID_ADMIN_CREDENTIALS.status_code,
            )

        reset_password_url = self._generate_reset_password_url(admin, auth)

        auth.send_reset_password_email(admin, reset_password_url)

        return self._response_with_message_and_redirect_button(
            "Email successfully sent! Please check your inbox.",
            url_for("admin_sign_in"),
            "Sign in",
        )

    def _extract_admin_from_request(self, request: Request) -> Optional[Admin]:
        email = request.form.get("email")

        admin = get_one(self._db, Admin, email=email)

        return admin

    def _generate_reset_password_url(
        self, admin: Admin, auth: PasswordAdminAuthenticationProvider
    ) -> str:
        reset_password_token = auth.generate_reset_password_token(admin, self._db)

        reset_password_url = url_for(
            "admin_reset_password",
            reset_password_token=reset_password_token,
            _external=True,
        )

        return reset_password_url

    def reset_password(self, reset_password_token: str) -> Optional[WerkzeugResponse]:
        """Shows reset password page or process the reset password request"""
        auth = self.admin_auth_provider(PasswordAdminAuthenticationProvider.NAME)
        if not auth:
            return self._response_with_message_and_redirect_button(
                ADMIN_AUTH_MECHANISM_NOT_CONFIGURED.detail,
                url_for("admin_sign_in"),
                "Sign in",
                is_error=True,
                status_code=ADMIN_AUTH_MECHANISM_NOT_CONFIGURED.status_code,
            )

        logged_in_admin = self.authenticated_admin_from_request()

        admin_view_redirect = redirect(url_for("admin_view"))

        # If the admin is logged in we redirect it since in that case the logged in change password option can be used
        if isinstance(logged_in_admin, Admin):
            return admin_view_redirect

        admin_from_token = auth.validate_token_and_extract_admin(
            reset_password_token, self._db
        )

        if isinstance(admin_from_token, ProblemDetail):
            return self._response_with_message_and_redirect_button(
                admin_from_token.detail,
                url_for("admin_forgot_password"),
                "Try again",
                is_error=True,
                status_code=admin_from_token.status_code,
            )

        if flask.request.method == "GET":
            auth_provider_html = auth.reset_password_template(
                reset_password_token, admin_view_redirect
            )

            html = self.RESET_PASSWORD_TEMPLATE % dict(
                auth_provider_html=auth_provider_html,
                logo_url=AdminClientConfig.lookup_asset_url(key="admin_logo"),
            )
            headers = dict()
            headers["Content-Type"] = "text/html"

            return Response(html, 200, headers)

        if flask.request.method == "POST":
            new_password = flask.request.form.get("password")
            confirm_password = flask.request.form.get("confirm_password")

            if new_password and confirm_password and new_password == confirm_password:
                admin_from_token.password = new_password

            else:
                problem_detail = INVALID_ADMIN_CREDENTIALS.detailed(
                    _("Passwords do not match.")
                )

                return self._response_with_message_and_redirect_button(
                    problem_detail.detail,
                    url_for(
                        "admin_reset_password",
                        reset_password_token=reset_password_token,
                    ),
                    "Try again",
                    is_error=True,
                    status_code=problem_detail.status_code,
                )

            return self._response_with_message_and_redirect_button(
                "Password successfully changed!",
                url_for("admin_sign_in"),
                "Sign in",
            )

        return None

    def _response_with_message_and_redirect_button(
        self,
        message: str,
        redirect_button_link: str,
        redirect_button_text: str,
        is_error: bool = False,
        status_code: int = 200,
    ) -> Response:
        style = error_style if is_error else body_style

        html = self.RESPONSE_TEMPLATE_WITH_MESSAGE % dict(
            body_style=style,
            message=message,
            redirect_link=redirect_button_link,
            button_text=redirect_button_text,
        )

        return Response(html, status_code)


class PatronController(AdminCirculationManagerController):
    def _load_patrondata(self, authenticator=None):
        """Extract a patron identifier from an incoming form submission,
        and ask the library's LibraryAuthenticator to turn it into a
        PatronData by doing a remote lookup in the ILS.

        :param authenticator: A LibraryAuthenticator. This is for mocking
        during tests; it's not necessary to provide it normally.
        """
        self.require_librarian(flask.request.library)

        identifier = flask.request.form.get("identifier")
        if not identifier:
            return NO_SUCH_PATRON.detailed(_("Please enter a patron identifier"))

        if not authenticator:
            authenticator = LibraryAuthenticator.from_config(
                self._db, flask.request.library
            )

        patron_data = PatronData(authorization_identifier=identifier)
        complete_patron_data = None

        if not authenticator.providers:
            return NO_SUCH_PATRON.detailed(
                _("This library has no authentication providers, so it has no patrons.")
            )

        for provider in authenticator.providers:
            complete_patron_data = provider.remote_patron_lookup(patron_data)
            if complete_patron_data:
                return complete_patron_data

        # If we get here, none of the providers succeeded.
        if not complete_patron_data:
            return NO_SUCH_PATRON.detailed(
                _(
                    "No patron with identifier %(patron_identifier)s was found at your library",
                    patron_identifier=identifier,
                ),
            )

    def lookup_patron(self, authenticator=None):
        """Look up personal information about a patron via the ILS.

        :param authenticator: A LibraryAuthenticator. This is for mocking
            during tests; it's not necessary to provide it normally.
        """
        patrondata = self._load_patrondata(authenticator)
        if isinstance(patrondata, ProblemDetail):
            return patrondata
        return patrondata.to_dict

    def reset_adobe_id(self, authenticator=None):
        """Delete all Credentials for a patron that are relevant
        to the patron's Adobe Account ID.

        :param authenticator: A LibraryAuthenticator. This is for mocking
            during tests; it's not necessary to provide it normal
        """
        patrondata = self._load_patrondata(authenticator)
        if isinstance(patrondata, ProblemDetail):
            return patrondata
        # Turn the Identifier into a Patron object.
        try:
            patron, is_new = patrondata.get_or_create_patron(
                self._db, flask.request.library.id
            )
        except CannotCreateLocalPatron as e:
            return NO_SUCH_PATRON.detailed(
                _(
                    "Could not create local patron object for %(patron_identifier)s",
                    patron_identifier=patrondata.authorization_identifier,
                )
            )

        # Wipe the Patron's 'identifier for Adobe ID purposes'.
        for credential in AuthdataUtility.adobe_relevant_credentials(patron):
            self._db.delete(credential)
        if patron.username:
            identifier = patron.username
        else:
            identifier = "with identifier " + patron.authorization_identifier
        return Response(
            str(
                _(
                    "Adobe ID for patron %(name_or_auth_id)s has been reset.",
                    name_or_auth_id=identifier,
                )
            ),
            200,
        )


class FeedController(AdminCirculationManagerController):
    def suppressed(self):
        self.require_librarian(flask.request.library)

        this_url = url_for("suppressed", _external=True)
        annotator = AdminAnnotator(self.circulation, flask.request.library)
        pagination = load_pagination_from_request()
        if isinstance(pagination, ProblemDetail):
            return pagination
        opds_feed = AdminFeed.suppressed(
            _db=self._db,
            title="Hidden Books",
            url=this_url,
            annotator=annotator,
            pagination=pagination,
        )
        return OPDSFeedResponse(opds_feed, max_age=0)

    def genres(self):
        data = dict({"Fiction": dict({}), "Nonfiction": dict({})})
        for name in genres:
            top = "Fiction" if genres[name].is_fiction else "Nonfiction"
            data[top][name] = dict(
                {
                    "name": name,
                    "parents": [parent.name for parent in genres[name].parents],
                    "subgenres": [subgenre.name for subgenre in genres[name].subgenres],
                }
            )
        return data


class CustomListsController(AdminCirculationManagerController):
    class CustomListSharePostResponse(BaseModel):
        successes: int = 0
        failures: int = 0

    class CustomListPostRequest(BaseModel):
        name: str
        id: Optional[int] = None
        entries: List[dict] = []
        collections: List[int] = []
        deletedEntries: List[dict] = []
        # For auto updating lists
        auto_update: bool = False
        auto_update_query: Optional[dict] = None
        auto_update_facets: Optional[dict] = None

    def _list_as_json(self, list: CustomList, is_owner=True) -> Dict:
        """Transform a CustomList object into a response ready dict"""
        collections = []
        for collection in list.collections:
            collections.append(
                dict(
                    id=collection.id,
                    name=collection.name,
                    protocol=collection.protocol,
                )
            )
        return dict(
            id=list.id,
            name=list.name,
            collections=collections,
            entry_count=list.size,
            auto_update=list.auto_update_enabled,
            auto_update_query=list.auto_update_query,
            auto_update_facets=list.auto_update_facets,
            auto_update_status=list.auto_update_status,
            is_owner=is_owner,
            is_shared=len(list.shared_locally_with_libraries) > 0,
        )

    def custom_lists(self) -> Union[Dict, ProblemDetail, Response, None]:
        library: Library = flask.request.library  # type: ignore  # "Request" has no attribute "library"
        self.require_librarian(library)

        if flask.request.method == "GET":
            custom_lists = []
            for list in library.custom_lists:
                custom_lists.append(self._list_as_json(list))

            for list in library.shared_custom_lists:
                custom_lists.append(self._list_as_json(list, is_owner=False))

            return dict(custom_lists=custom_lists)

        if flask.request.method == "POST":
            ctx: Context = flask.request.context.body  # type: ignore
            return self._create_or_update_list(
                library,
                ctx.name,
                ctx.entries,
                ctx.collections,
                id=ctx.id,
                auto_update=ctx.auto_update,
                auto_update_facets=ctx.auto_update_facets,
                auto_update_query=ctx.auto_update_query,
            )

        return None

    def _getJSONFromRequest(self, values: Optional[str]) -> list:
        if values:
            return_values = json.loads(values)
        else:
            return_values = []

        return return_values

    def _get_work_from_urn(
        self, library: Library, urn: Optional[str]
    ) -> Optional[Work]:
        identifier, ignore = Identifier.parse_urn(self._db, urn)

        if identifier is None:
            return None

        query = (
            self._db.query(Work)
            .join(LicensePool, LicensePool.work_id == Work.id)
            .join(Collection, LicensePool.collection_id == Collection.id)
            .filter(LicensePool.identifier_id == identifier.id)
            .filter(Collection.id.in_([c.id for c in library.all_collections]))
        )
        work = query.one()
        return work

    def _create_or_update_list(
        self,
        library: Library,
        name: str,
        entries: List[Dict],
        collections: List[int],
        deleted_entries: Optional[List[Dict]] = None,
        id: Optional[int] = None,
        auto_update: Optional[bool] = None,
        auto_update_query: Optional[str] = None,
        auto_update_facets: Optional[str] = None,
    ) -> Union[ProblemDetail, Response]:
        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)

        old_list_with_name = CustomList.find(self._db, name, library=library)

        list: CustomList
        if id:
            is_new = False
            list = get_one(self._db, CustomList, id=int(id), data_source=data_source)
            if not list:
                return MISSING_CUSTOM_LIST
            if list.library != library:
                return CANNOT_CHANGE_LIBRARY_FOR_CUSTOM_LIST
            if old_list_with_name and old_list_with_name != list:
                return CUSTOM_LIST_NAME_ALREADY_IN_USE
        elif old_list_with_name:
            return CUSTOM_LIST_NAME_ALREADY_IN_USE
        else:
            list, is_new = create(
                self._db, CustomList, name=name, data_source=data_source
            )
            list.created = datetime.now()
            list.library = library

        # Test JSON viability of auto update data
        try:
            auto_update_query_str = None
            auto_update_facets_str = None
            if auto_update_query is not None:
                try:
                    auto_update_query_str = json.dumps(auto_update_query)
                except json.JSONDecodeError:
                    raise Exception(
                        INVALID_INPUT.detailed(
                            "auto_update_query is not JSON serializable"
                        )
                    )

                if entries and len(entries) > 0:
                    raise Exception(AUTO_UPDATE_CUSTOM_LIST_CANNOT_HAVE_ENTRIES)
                if deleted_entries and len(deleted_entries) > 0:
                    raise Exception(AUTO_UPDATE_CUSTOM_LIST_CANNOT_HAVE_ENTRIES)

            if auto_update_facets is not None:
                try:
                    auto_update_facets_str = json.dumps(auto_update_facets)
                except json.JSONDecodeError:
                    raise Exception(
                        INVALID_INPUT.detailed(
                            "auto_update_facets is not JSON serializable"
                        )
                    )
            if auto_update is True and auto_update_query is None:
                raise Exception(
                    INVALID_INPUT.detailed(
                        "auto_update_query must be present when auto_update is enabled"
                    )
                )
        except Exception as e:
            auto_update_error = e.args[0] if len(e.args) else None

            if not auto_update_error or type(auto_update_error) != ProblemDetail:
                raise

            # Rollback if this was a deliberate error
            self._db.rollback()
            return auto_update_error

        list.updated = datetime.now()
        list.name = name
        previous_auto_update_query = list.auto_update_query
        # Record the time the auto_update was toggled "on"
        if auto_update is True and list.auto_update_enabled is False:
            list.auto_update_last_update = datetime.now()
        if auto_update is not None:
            list.auto_update_enabled = auto_update
        if auto_update_query is not None:
            list.auto_update_query = auto_update_query_str
        if auto_update_facets is not None:
            list.auto_update_facets = auto_update_facets_str

        # In case this is a new list with no entries, populate the first page
        if (
            is_new
            and list.auto_update_enabled
            and list.auto_update_status == CustomList.INIT
        ):
            CustomListQueries.populate_query_pages(self._db, list, max_pages=1)
        elif (
            not is_new
            and list.auto_update_enabled
            and auto_update_query
            and previous_auto_update_query
        ):
            # In case this is a previous auto update list, we must check if the
            # query has been updated
            # JSON maps are unordered by definition, so we must deserialize and compare dicts
            try:
                prev_query_dict = json.loads(previous_auto_update_query)
                if prev_query_dict != auto_update_query:
                    list.auto_update_status = CustomList.REPOPULATE
            except json.JSONDecodeError:
                # Do nothing if the previous query was not valid
                pass

        membership_change = False

        works_to_update_in_search = set()

        for entry in entries:
            urn = entry.get("id")
            work = self._get_work_from_urn(library, urn)

            if work:
                entry, entry_is_new = list.add_entry(work, featured=True)
                if entry_is_new:
                    works_to_update_in_search.add(work)
                    membership_change = True

        if deleted_entries:
            for entry in deleted_entries:
                urn = entry.get("id")
                work = self._get_work_from_urn(library, urn)

                if work:
                    list.remove_entry(work)
                    works_to_update_in_search.add(work)
                    membership_change = True

        if membership_change:
            # We need to update the search index entries for works that caused a membership change,
            # so the upstream counts can be calculated correctly.
            self.search_engine.bulk_update(works_to_update_in_search)

            # If this list was used to populate any lanes, those lanes need to have their counts updated.
            for lane in Lane.affected_by_customlist(list):
                lane.update_size(self._db, self.search_engine)

        new_collections = []
        for collection_id in collections:
            collection = get_one(self._db, Collection, id=collection_id)
            if not collection:
                self._db.rollback()
                return MISSING_COLLECTION
            if list.library not in collection.libraries:
                self._db.rollback()
                return COLLECTION_NOT_ASSOCIATED_WITH_LIBRARY
            new_collections.append(collection)
        list.collections = new_collections

        if is_new:
            return Response(str(list.id), 201)
        else:
            return Response(str(list.id), 200)

    def url_for_custom_list(
        self, library: Library, list: CustomList
    ) -> Callable[[int], str]:
        def url_fn(after):
            return url_for(
                "custom_list_get",
                after=after,
                library_short_name=library.short_name,
                list_id=list.id,
                _external=True,
            )

        return url_fn

    def custom_list(
        self, list_id: int
    ) -> Optional[Union[Response, Dict, ProblemDetail]]:
        library: Library = flask.request.library  # type: ignore
        self.require_librarian(library)
        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)

        list: CustomList = get_one(
            self._db, CustomList, id=list_id, data_source=data_source
        )
        if not list:
            return MISSING_CUSTOM_LIST

        if flask.request.method == "GET":
            pagination = load_pagination_from_request()
            if isinstance(pagination, ProblemDetail):
                return pagination

            query = CustomList.entries_having_works(self._db, list_id)
            url = url_for(
                "custom_list_get",
                list_name=list.name,
                library_short_name=library.short_name,
                list_id=list_id,
                _external=True,
            )

            worklist = WorkList()
            worklist.initialize(library, customlists=[list])

            annotator = self.manager.annotator(worklist)
            url_fn = self.url_for_custom_list(library, list)
            feed = AcquisitionFeed.from_query(
                query, self._db, list.name, url, pagination, url_fn, annotator
            )
            annotator.annotate_feed(feed, worklist)

            return OPDSFeedResponse(str(feed), max_age=0)

        elif flask.request.method == "POST":
            ctx: Context = flask.request.context.body  # type: ignore
            return self._create_or_update_list(
                library,
                ctx.name,
                ctx.entries,
                ctx.collections,
                deleted_entries=ctx.deletedEntries,
                id=list_id,
                auto_update=ctx.auto_update,
                auto_update_query=ctx.auto_update_query,
                auto_update_facets=ctx.auto_update_facets,
            )

        elif flask.request.method == "DELETE":
            # Deleting requires a library manager.
            self.require_library_manager(flask.request.library)  # type: ignore

            if len(list.shared_locally_with_libraries) > 0:
                return CANNOT_DELETE_SHARED_LIST

            # Build the list of affected lanes before modifying the
            # CustomList.
            affected_lanes = Lane.affected_by_customlist(list)
            surviving_lanes = []
            for lane in affected_lanes:
                if lane.list_datasource == None and len(lane.customlist_ids) == 1:
                    # This Lane is based solely upon this custom list,
                    # which is about to be deleted. Delete the Lane
                    # itself.
                    self._db.delete(lane)
                else:
                    surviving_lanes.append(lane)
            for entry in list.entries:
                self._db.delete(entry)
            self._db.delete(list)
            self._db.flush()
            # Update the size for any lanes affected by this
            # CustomList which _weren't_ deleted.
            for lane in surviving_lanes:
                lane.update_size(self._db, self.search_engine)
            return Response(str(_("Deleted")), 200)

        return None

    def share_locally(
        self, customlist_id: int
    ) -> Union[ProblemDetail, Dict[str, int], Response]:
        """Share this customlist with all libraries on this local CM"""
        if not customlist_id:
            return INVALID_INPUT
        customlist: CustomList = get_one(self._db, CustomList, id=customlist_id)
        if customlist.library != flask.request.library:  # type: ignore
            return ADMIN_NOT_AUTHORIZED.detailed(
                _("This library does not have permissions on this customlist.")
            )

        if flask.request.method == "POST":
            return self.share_locally_POST(customlist)
        elif flask.request.method == "DELETE":
            return self.share_locally_DELETE(customlist)
        else:
            return METHOD_NOT_ALLOWED

    def share_locally_POST(
        self, customlist: CustomList
    ) -> Union[ProblemDetail, Dict[str, int]]:
        successes = []
        failures = []
        for library in self._db.query(Library).all():
            # Do not share with self
            if library == customlist.library:
                continue

            # Do not attempt to re-share
            if library in customlist.shared_locally_with_libraries:
                continue

            # Attempt to share the list
            response = CustomListQueries.share_locally_with_library(
                self._db, customlist, library
            )

            if response is not True:
                failures.append(library)
            else:
                successes.append(library)

        self._db.commit()
        return self.CustomListSharePostResponse(
            successes=len(successes), failures=len(failures)
        ).dict()

    def share_locally_DELETE(
        self, customlist: CustomList
    ) -> Union[ProblemDetail, Response]:
        """Delete the shared status of a custom list
        If a customlist is actively in use by another library, then disallow the unshare
        """
        if not customlist.shared_locally_with_libraries:
            return Response("", 204)

        shared_list_lanes = (
            self._db.query(Lane)
            .filter(
                Lane.customlists.contains(customlist),
                Lane.library_id != customlist.library_id,
            )
            .count()
        )

        if shared_list_lanes > 0:
            return CUSTOMLIST_CANNOT_DELETE_SHARE.detailed(
                _(
                    "This list cannot be unshared because it is currently being used by one or more libraries on this Palace Manager."
                )
            )

        # This list is not in use by any other libraries, we can delete the share
        # by simply emptying the list of shared libraries
        customlist.shared_locally_with_libraries = []

        return Response("", status=204)


class LanesController(AdminCirculationManagerController):
    def lanes(self):
        library = flask.request.library
        self.require_librarian(library)

        if flask.request.method == "GET":

            def lanes_for_parent(parent):
                lanes = (
                    self._db.query(Lane)
                    .filter(Lane.library == library)
                    .filter(Lane.parent == parent)
                    .order_by(Lane.priority)
                )
                return [
                    {
                        "id": lane.id,
                        "display_name": lane.display_name,
                        "visible": lane.visible,
                        "count": lane.size,
                        "sublanes": lanes_for_parent(lane),
                        "custom_list_ids": [list.id for list in lane.customlists],
                        "inherit_parent_restrictions": lane.inherit_parent_restrictions,
                    }
                    for lane in lanes
                ]

            return dict(lanes=lanes_for_parent(None))

        if flask.request.method == "POST":
            self.require_library_manager(flask.request.library)

            id = flask.request.form.get("id")
            parent_id = flask.request.form.get("parent_id")
            display_name = flask.request.form.get("display_name")
            custom_list_ids = json.loads(
                flask.request.form.get("custom_list_ids", "[]")
            )
            inherit_parent_restrictions = flask.request.form.get(
                "inherit_parent_restrictions"
            )
            if inherit_parent_restrictions == "true":
                inherit_parent_restrictions = True
            else:
                inherit_parent_restrictions = False

            if not display_name:
                return NO_DISPLAY_NAME_FOR_LANE

            if id:
                is_new = False
                lane = get_one(self._db, Lane, id=id, library=library)
                if not lane:
                    return MISSING_LANE

                if not lane.customlists:
                    # just update what is allowed for default lane, and exit out
                    lane.display_name = display_name
                    return Response(str(lane.id), 200)
                else:
                    # In case we are not a default lane, the lane MUST have custom lists
                    if not custom_list_ids or len(custom_list_ids) == 0:
                        return NO_CUSTOM_LISTS_FOR_LANE

                if display_name != lane.display_name:
                    old_lane = get_one(
                        self._db, Lane, display_name=display_name, parent=lane.parent
                    )
                    if old_lane:
                        return LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS
                lane.display_name = display_name
            else:
                if not custom_list_ids or len(custom_list_ids) == 0:
                    return NO_CUSTOM_LISTS_FOR_LANE

                parent = None
                if parent_id:
                    parent = get_one(self._db, Lane, id=parent_id, library=library)
                    if not parent:
                        return MISSING_LANE.detailed(
                            _(
                                "The specified parent lane does not exist, or is associated with a different library."
                            )
                        )
                old_lane = get_one(
                    self._db,
                    Lane,
                    display_name=display_name,
                    parent=parent,
                    library=library,
                )
                if old_lane:
                    return LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS

                lane, is_new = create(
                    self._db,
                    Lane,
                    display_name=display_name,
                    parent=parent,
                    library=library,
                )

                # Make a new lane the first child of its parent and bump all the siblings down in priority.
                siblings = (
                    self._db.query(Lane)
                    .filter(Lane.library == library)
                    .filter(Lane.parent == lane.parent)
                    .filter(Lane.id != lane.id)
                )
                for sibling in siblings:
                    sibling.priority += 1
                lane.priority = 0

            lane.inherit_parent_restrictions = inherit_parent_restrictions

            for list_id in custom_list_ids:
                list = get_one(self._db, CustomList, library=library, id=list_id)
                if not list:
                    # We did not find a list, is this a shared list?
                    list = (
                        self._db.query(CustomList)
                        .join(CustomList.shared_locally_with_libraries)
                        .filter(CustomList.id == list_id, Library.id == library.id)
                        .first()
                    )
                if not list:
                    self._db.rollback()
                    return MISSING_CUSTOM_LIST.detailed(
                        _(
                            "The list with id %(list_id)s does not exist or is associated with a different library.",
                            list_id=list_id,
                        )
                    )
                lane.customlists.append(list)

            for list in lane.customlists:
                if list.id not in custom_list_ids:
                    lane.customlists.remove(list)
            lane.update_size(self._db, self.search_engine)

            if is_new:
                return Response(str(lane.id), 201)
            else:
                return Response(str(lane.id), 200)

    def lane(self, lane_identifier):
        if flask.request.method == "DELETE":
            library = flask.request.library
            self.require_library_manager(library)

            lane = get_one(self._db, Lane, id=lane_identifier, library=library)
            if not lane:
                return MISSING_LANE
            if not lane.customlists:
                return CANNOT_EDIT_DEFAULT_LANE

            # Recursively delete all the lane's sublanes.
            def delete_lane_and_sublanes(lane):
                for sublane in lane.sublanes:
                    delete_lane_and_sublanes(sublane)
                self._db.delete(lane)

            delete_lane_and_sublanes(lane)
            return Response(str(_("Deleted")), 200)

    def show_lane(self, lane_identifier):
        library = flask.request.library
        self.require_library_manager(library)

        lane = get_one(self._db, Lane, id=lane_identifier, library=library)
        if not lane:
            return MISSING_LANE
        if lane.parent and not lane.parent.visible:
            return CANNOT_SHOW_LANE_WITH_HIDDEN_PARENT
        lane.visible = True
        return Response(str(_("Success")), 200)

    def hide_lane(self, lane_identifier):
        library = flask.request.library
        self.require_library_manager(library)

        lane = get_one(self._db, Lane, id=lane_identifier, library=library)
        if not lane:
            return MISSING_LANE
        lane.visible = False
        return Response(str(_("Success")), 200)

    def reset(self):
        self.require_library_manager(flask.request.library)

        create_default_lanes(self._db, flask.request.library)
        return Response(str(_("Success")), 200)

    def change_order(self):
        self.require_library_manager(flask.request.library)

        submitted_lanes = json.loads(flask.request.data)

        def update_lane_order(lanes):
            for index, lane_data in enumerate(lanes):
                lane_id = lane_data.get("id")
                lane = self._db.query(Lane).filter(Lane.id == lane_id).one()
                lane.priority = index
                update_lane_order(lane_data.get("sublanes", []))

        update_lane_order(submitted_lanes)

        return Response(str(_("Success")), 200)


class DashboardController(AdminCirculationManagerController):
    def stats(
        self, stats_function: Callable[[Admin, Session], StatisticsResponse]
    ) -> StatisticsResponse:
        admin: Admin = getattr(flask.request, "admin")
        return stats_function(admin, self._db)

    def circulation_events(self):
        annotator = AdminAnnotator(self.circulation, flask.request.library)
        num = min(int(flask.request.args.get("num", "100")), 500)

        results = (
            self._db.query(CirculationEvent)
            .join(LicensePool)
            .join(Work)
            .join(DataSource)
            .join(Identifier)
            .order_by(nullslast(desc(CirculationEvent.start)))
            .limit(num)
            .all()
        )

        events = [
            {
                "id": result.id,
                "type": result.type,
                "time": result.start,
                "book": {
                    "title": result.license_pool.work.title,
                    "url": annotator.permalink_for(
                        result.license_pool.work,
                        result.license_pool,
                        result.license_pool.identifier,
                    ),
                },
            }
            for result in results
        ]

        return dict({"circulation_events": events})

    def bulk_circulation_events(self, analytics_exporter=None):
        date_format = "%Y-%m-%d"

        def get_date(field):
            # Return a date or datetime object representing the
            # _beginning_ of the asked-for day, local time.
            #
            # Unlike most places in this application we do not
            # use UTC since the sime was selected by a human user.
            today = date.today()
            value = flask.request.args.get(field, None)
            if not value:
                return today
            try:
                return datetime.strptime(value, date_format).date()
            except ValueError as e:
                # This won't happen in real life since the format is
                # controlled by the calendar widget. There's no need
                # to send an error message -- just use the default
                # date.
                return today

        # For the start date we should use the _beginning_ of the day,
        # which is what get_date returns.
        date_start = get_date("date")

        # When running the search, the cutoff is the first moment of
        # the day _after_ the end date. When generating the filename,
        # though, we should use the date provided by the user.
        date_end_label = get_date("dateEnd")
        date_end = date_end_label + timedelta(days=1)
        locations = flask.request.args.get("locations", None)
        library = getattr(flask.request, "library", None)
        library_short_name = library.short_name if library else None

        analytics_exporter = analytics_exporter or LocalAnalyticsExporter()
        data = analytics_exporter.export(
            self._db, date_start, date_end, locations, library
        )
        return (
            data,
            date_start.strftime(date_format),
            date_end_label.strftime(date_format),
            library_short_name,
        )


class SettingsController(AdminCirculationManagerController):
    METADATA_SERVICE_URI_TYPE = "application/opds+json;profile=https://librarysimplified.org/rel/profile/metadata-service"

    NO_MIRROR_INTEGRATION = "NO_MIRROR"

    PROVIDER_APIS = [
        OPDSImporter,
        OPDSForDistributorsAPI,
        OPDS2Importer,
        OverdriveAPI,
        OdiloAPI,
        BibliothecaAPI,
        Axis360API,
        EnkiAPI,
        ODLAPI,
        ODL2API,
        SharedODLAPI,
        LCPAPI,
    ]

    def _set_storage_external_integration_link(
        self, service: ExternalIntegration, purpose: str, setting_key: str
    ) -> Optional[ProblemDetail]:
        """Either set or delete the external integration link between the
        service and the storage integration.

        :param service: Service's ExternalIntegration object

        :param purpose: Service's purpose

        :param setting_key: Key of the configuration setting that must be set in the storage integration.
            For example, a specific bucket (MARC, Analytics, etc.).

        :return: ProblemDetail object if the operation failed
        """
        mirror_integration_id = flask.request.form.get("mirror_integration_id")

        if not mirror_integration_id:
            return None

        # If no storage integration was selected, then delete the existing
        # external integration link.
        if mirror_integration_id == self.NO_MIRROR_INTEGRATION:
            current_integration_link = get_one(
                self._db,
                ExternalIntegrationLink,
                library_id=None,
                external_integration_id=service.id,
                purpose=purpose,
            )

            if current_integration_link:
                self._db.delete(current_integration_link)
        else:
            storage_integration = get_one(
                self._db, ExternalIntegration, id=mirror_integration_id
            )

            # Only get storage integrations that have a specific configuration setting set.
            # For example: a specific bucket.
            if (
                not storage_integration
                or not storage_integration.setting(setting_key).value
            ):
                return MISSING_INTEGRATION

            current_integration_link, ignore = get_one_or_create(
                self._db,
                ExternalIntegrationLink,
                library_id=None,
                external_integration_id=service.id,
                purpose=purpose,
            )

            current_integration_link.other_integration_id = storage_integration.id

        return None

    @classmethod
    def _get_integration_protocols(cls, provider_apis, protocol_name_attr="__module__"):
        protocols = []
        for api in provider_apis:
            protocol = dict()
            name = getattr(api, protocol_name_attr)
            protocol["name"] = name

            label = getattr(api, "NAME", name)
            protocol["label"] = label

            description = getattr(api, "DESCRIPTION", None)
            if description != None:
                protocol["description"] = description

            instructions = getattr(api, "INSTRUCTIONS", None)
            if instructions != None:
                protocol["instructions"] = instructions

            sitewide = getattr(api, "SITEWIDE", None)
            if sitewide != None:
                protocol["sitewide"] = sitewide

            settings = getattr(api, "SETTINGS", [])
            protocol["settings"] = list(settings)

            child_settings = getattr(api, "CHILD_SETTINGS", None)
            if child_settings != None:
                protocol["child_settings"] = list(child_settings)

            library_settings = getattr(api, "LIBRARY_SETTINGS", None)
            if library_settings != None:
                protocol["library_settings"] = list(library_settings)

            cardinality = getattr(api, "CARDINALITY", None)
            if cardinality != None:
                protocol["cardinality"] = cardinality

            supports_registration = getattr(api, "SUPPORTS_REGISTRATION", None)
            if supports_registration != None:
                protocol["supports_registration"] = supports_registration
            supports_staging = getattr(api, "SUPPORTS_STAGING", None)
            if supports_staging != None:
                protocol["supports_staging"] = supports_staging

            protocols.append(protocol)
        return protocols

    def _get_integration_library_info(self, integration, library, protocol):
        library_info = dict(short_name=library.short_name)
        for setting in protocol.get("library_settings", []):
            key = setting.get("key")
            if setting.get("type") == "list":
                value = ConfigurationSetting.for_library_and_externalintegration(
                    self._db, key, library, integration
                ).json_value
            else:
                value = ConfigurationSetting.for_library_and_externalintegration(
                    self._db, key, library, integration
                ).value
            if value:
                library_info[key] = value
        return library_info

    def _get_integration_info(self, goal, protocols):
        services = []
        settings_query = (
            self._db.query(ConfigurationSetting)
            .join(ExternalIntegration)
            .filter(ExternalIntegration.goal == goal)
        )
        ConfigurationSetting.cache_warm(self._db, settings_query.all)
        for service in (
            self._db.query(ExternalIntegration)
            .filter(ExternalIntegration.goal == goal)
            .order_by(ExternalIntegration.name)
        ):
            candidates = [p for p in protocols if p.get("name") == service.protocol]
            if not candidates:
                continue
            protocol = candidates[0]
            libraries = []
            if not protocol.get("sitewide") or protocol.get("library_settings"):
                for library in service.libraries:
                    libraries.append(
                        self._get_integration_library_info(service, library, protocol)
                    )

            settings = dict()
            for setting in protocol.get("settings", []):
                key = setting.get("key")

                # If the setting is a covers or books mirror, we need to get
                # the value from ExternalIntegrationLink and
                # not from a ConfigurationSetting.
                if key.endswith("mirror_integration_id"):
                    storage_integration = get_one(
                        self._db,
                        ExternalIntegrationLink,
                        external_integration_id=service.id,
                    )
                    if storage_integration:
                        value = str(storage_integration.other_integration_id)
                    else:
                        value = self.NO_MIRROR_INTEGRATION
                else:
                    if setting.get("type") in ("list", "menu"):
                        value = ConfigurationSetting.for_externalintegration(
                            key, service
                        ).json_value
                    else:
                        value = ConfigurationSetting.for_externalintegration(
                            key, service
                        ).value
                settings[key] = value

            service_info = dict(
                id=service.id,
                name=service.name,
                protocol=service.protocol,
                settings=settings,
                libraries=libraries,
            )

            if "test_search_term" in [x.get("key") for x in protocol.get("settings")]:
                service_info["self_test_results"] = self._get_prior_test_results(
                    service
                )

            services.append(service_info)
        return services

    @staticmethod
    def _get_menu_values(setting_key, form):
        """circulation-admin returns "menu" values in a different format not compatible with werkzeug.MultiDict semantics:
            {setting_key}_{menu} = {value_in_the_dropdown_box}
            {setting_key}_{setting_value1} = {setting_label1}
            {setting_key}_{setting_value2} = {setting_label2}
            ...
            {setting_key}_{setting_valueN} = {setting_labelN}

        It means we can't use werkzeug.MultiDict.getlist method and have to extract them manually.

        :param setting_key: Setting's key
        :type setting_key: str

        :param form: Multi-dictionary containing input values submitted by the user
            and sent back to CM by circulation-admin
        :type form: werkzeug.MultiDict

        :return: List of "menu" values
        :rtype: List[str]
        """
        values = []

        for form_item_key in list(form.keys()):
            if setting_key in form_item_key:
                value = form_item_key.replace(setting_key, "").lstrip("_")

                if value != "menu":
                    values.append(value)

        return values

    def _set_integration_setting(self, integration, setting):
        setting_key = setting.get("key")
        setting_type = setting.get("type")

        if setting_type == "list" and not setting.get("options"):
            value = [item for item in flask.request.form.getlist(setting_key) if item]
            if value:
                value = json.dumps(value)
        elif setting_type == "menu":
            value = self._get_menu_values(setting_key, flask.request.form)
        else:
            value = flask.request.form.get(setting_key)

        if value and setting.get("options"):
            # This setting can only take on values that are in its
            # list of options.
            allowed_values = [option.get("key") for option in setting.get("options")]
            submitted_values = value

            if not isinstance(submitted_values, list):
                submitted_values = [submitted_values]

            for submitted_value in submitted_values:
                if submitted_value not in allowed_values:
                    return INVALID_CONFIGURATION_OPTION.detailed(
                        _(
                            "The configuration value for %(setting)s is invalid.",
                            setting=setting.get("label"),
                        )
                    )

        value_missing = value is None
        value_required = setting.get("required")

        if value_missing and value_required:
            value_default = setting.get("default")
            if not value_default:
                return INCOMPLETE_CONFIGURATION.detailed(
                    _(
                        "The configuration is missing a required setting: %(setting)s",
                        setting=setting.get("label"),
                    )
                )

        if isinstance(value, list):
            value = json.dumps(value)

        integration.setting(setting_key).value = value

    def _set_integration_library(self, integration, library_info, protocol):
        library = get_one(self._db, Library, short_name=library_info.get("short_name"))
        if not library:
            return NO_SUCH_LIBRARY.detailed(
                _(
                    "You attempted to add the integration to %(library_short_name)s, but it does not exist.",
                    library_short_name=library_info.get("short_name"),
                )
            )

        integration.libraries += [library]
        for setting in protocol.get("library_settings", []):
            key = setting.get("key")
            value = library_info.get(key)
            if value and setting.get("type") == "list" and not setting.get("options"):
                value = json.dumps(value)
            if setting.get("options") and value not in [
                option.get("key") for option in setting.get("options")
            ]:
                return INVALID_CONFIGURATION_OPTION.detailed(
                    _(
                        "The configuration value for %(setting)s is invalid.",
                        setting=setting.get("label"),
                    )
                )
            if not value and setting.get("required"):
                return INCOMPLETE_CONFIGURATION.detailed(
                    _(
                        "The configuration is missing a required setting: %(setting)s for library %(library)s",
                        setting=setting.get("label"),
                        library=library.short_name,
                    )
                )
            ConfigurationSetting.for_library_and_externalintegration(
                self._db, key, library, integration
            ).value = value

    def _set_integration_settings_and_libraries(self, integration, protocol):
        settings = protocol.get("settings")
        for setting in settings:
            if not setting.get("key").endswith("mirror_integration_id"):
                result = self._set_integration_setting(integration, setting)
                if isinstance(result, ProblemDetail):
                    return result

        if not protocol.get("sitewide") or protocol.get("library_settings"):
            integration.libraries = []

            libraries = []
            if flask.request.form.get("libraries"):
                libraries = json.loads(flask.request.form.get("libraries"))

            for library_info in libraries:
                result = self._set_integration_library(
                    integration, library_info, protocol
                )
                if isinstance(result, ProblemDetail):
                    return result
        return True

    def _delete_integration(self, integration_id, goal):
        if flask.request.method != "DELETE":
            return
        self.require_system_admin()

        integration = get_one(
            self._db, ExternalIntegration, id=integration_id, goal=goal
        )
        if not integration:
            return MISSING_SERVICE
        self._db.delete(integration)
        return Response(str(_("Deleted")), 200)

    def _get_collection_protocols(self, provider_apis):
        protocols = self._get_integration_protocols(
            provider_apis, protocol_name_attr="NAME"
        )
        protocols.append(
            {
                "name": ExternalIntegration.MANUAL,
                "label": _("Manual import"),
                "description": _(
                    "Books will be manually added to the circulation manager, "
                    "not imported automatically through a protocol."
                ),
                "settings": [],
            }
        )

        return protocols

    def _get_prior_test_results(self, item, protocol_class=None, *extra_args):
        # :param item: An ExternalSearchIndex, an ExternalIntegration for patron authentication, or a Collection
        if not protocol_class and hasattr(self, "protocol_class"):
            protocol_class = self.protocol_class

        if not item:
            return None

        self_test_results = None

        try:
            if self.type == "collection":
                if not item.protocol or not len(item.protocol):
                    return None
                provider_apis = list(self.PROVIDER_APIS)
                provider_apis.append(OPDSImportMonitor)

                if item.protocol == OPDSImportMonitor.PROTOCOL:
                    protocol_class = OPDSImportMonitor

                if protocol_class in provider_apis and issubclass(
                    protocol_class, HasSelfTests
                ):
                    if item.protocol == OPDSImportMonitor.PROTOCOL:
                        extra_args = (OPDSImporter,)
                    else:
                        extra_args = ()

                    self_test_results = protocol_class.prior_test_results(
                        self._db, protocol_class, self._db, item, *extra_args
                    )

            elif self.type == "search service":
                self_test_results = ExternalSearchIndex.prior_test_results(
                    self._db, None, self._db, item
                )
            elif self.type == "metadata service" and protocol_class:
                self_test_results = protocol_class.prior_test_results(
                    self._db, *extra_args
                )
            elif self.type == "patron authentication service":
                library = None
                if len(item.libraries):
                    library = item.libraries[0]
                    self_test_results = protocol_class.prior_test_results(
                        self._db, None, library, item
                    )
                else:
                    self_test_results = dict(
                        exception=_(
                            "You must associate this service with at least one library before you can run self tests for it."
                        ),
                        disabled=True,
                    )

        except Exception as e:
            # This is bad, but not so bad that we should short-circuit
            # this whole process -- that might prevent an admin from
            # making the configuration changes necessary to fix
            # this problem.
            message = _("Exception getting self-test results for %s %s: %s")
            error_message = str(e)
            args = (self.type, item.name, error_message)
            logging.warning(message, *args, exc_info=error_message)
            self_test_results = dict(exception=message % args)

        return self_test_results

    def _mirror_integration_settings(self):
        """Create a setting interface for selecting a storage integration to
        be used when mirroring items from a collection.
        """
        integrations = (
            self._db.query(ExternalIntegration)
            .filter(ExternalIntegration.goal == ExternalIntegration.STORAGE_GOAL)
            .order_by(ExternalIntegration.name)
        )

        if not integrations.all():
            return

        mirror_integration_settings = copy.deepcopy(
            ExternalIntegrationLink.COLLECTION_MIRROR_SETTINGS
        )
        for integration in integrations:
            book_covers_bucket = integration.setting(
                S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY
            ).value
            open_access_bucket = integration.setting(
                S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY
            ).value
            protected_access_bucket = integration.setting(
                S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY
            ).value

            analytics_bucket = integration.setting(
                S3UploaderConfiguration.ANALYTICS_BUCKET_KEY
            ).value

            for setting in mirror_integration_settings:
                if (
                    setting["key"] == ExternalIntegrationLink.COVERS_KEY
                    and book_covers_bucket
                ):
                    setting["options"].append(
                        {"key": str(integration.id), "label": integration.name}
                    )
                elif setting["key"] == ExternalIntegrationLink.OPEN_ACCESS_BOOKS_KEY:
                    if open_access_bucket:
                        setting["options"].append(
                            {"key": str(integration.id), "label": integration.name}
                        )
                elif (
                    setting["key"] == ExternalIntegrationLink.PROTECTED_ACCESS_BOOKS_KEY
                ):
                    if protected_access_bucket:
                        setting["options"].append(
                            {"key": str(integration.id), "label": integration.name}
                        )
                elif setting["key"] == ExternalIntegrationLink.ANALYTICS_KEY:
                    if protected_access_bucket:
                        setting["options"].append(
                            {"key": str(integration.id), "label": integration.name}
                        )

        return mirror_integration_settings

    def _create_integration(self, protocol_definitions, protocol, goal):
        """Create a new ExternalIntegration for the given protocol and
        goal, assuming that doing so is compatible with the protocol's
        definition.

        :return: A 2-tuple (result, is_new). `result` will be an
            ExternalIntegration if one could be created, and a
            ProblemDetail otherwise.
        """
        if not protocol:
            return NO_PROTOCOL_FOR_NEW_SERVICE, False
        matches = [x for x in protocol_definitions if x.get("name") == protocol]
        if not matches:
            return UNKNOWN_PROTOCOL, False
        definition = matches[0]

        # Most of the time there can be multiple ExternalIntegrations with
        # the same protocol and goal...
        allow_multiple = True
        m = create
        args = (self._db, ExternalIntegration)
        kwargs = dict(protocol=protocol, goal=goal)
        if definition.get("cardinality") == 1:
            # ...but not all the time.
            allow_multiple = False
            existing = get_one(*args, **kwargs)
            if existing is not None:
                # We were asked to create a new ExternalIntegration
                # but there's already one for this protocol, which is not
                # allowed.
                return DUPLICATE_INTEGRATION, False
            m = get_one_or_create

        integration, is_new = m(*args, **kwargs)
        if not is_new and not allow_multiple:
            # This can happen, despite our check above, in a race
            # condition where two clients try simultaneously to create
            # two integrations of the same type.
            return DUPLICATE_INTEGRATION, False
        return integration, is_new

        [protocol] = [p for p in protocols if p.get("name") == protocol]
        result = self._set_integration_settings_and_libraries(auth_service, protocol)
        if isinstance(result, ProblemDetail):
            return result

    def check_name_unique(self, new_service, name):
        """A service cannot be created with, or edited to have, the same name
        as a service that already exists.
        This method is used by analytics_services, cdn_services, discovery_services,
        metadata_services, and sitewide_services.
        """

        existing_service = get_one(self._db, ExternalIntegration, name=name)
        if existing_service and not existing_service.id == new_service.id:
            # Without checking that the IDs are different, you can't save
            # changes to an existing service unless you've also changed its name.
            return INTEGRATION_NAME_ALREADY_IN_USE

    @classmethod
    def url_variants(cls, url, check_protocol_variant=True):
        """Generate minor variants of a URL -- HTTP vs HTTPS, trailing slash
        vs not, etc.

        Technically these are all distinct URLs, but in real life they
        generally mean someone typed the same URL slightly
        differently. Since this isn't an exact science, this doesn't
        need to catch all variant URLs, only the most common ones.
        """
        if not Validator()._is_url(url, []):
            # An invalid URL has no variants.
            return

        # A URL is a 'variant' of itself.
        yield url

        # Adding or removing a slash creates a variant.
        if url.endswith("/"):
            yield url[:-1]
        else:
            yield url + "/"

        # Changing protocols may create one or more variants.
        https = "https://"
        http = "http://"
        if check_protocol_variant:
            protocol_variant = None
            if url.startswith(https):
                protocol_variant = url.replace(https, http, 1)
            elif url.startswith(http):
                protocol_variant = url.replace(http, https, 1)
            if protocol_variant:
                yield from cls.url_variants(protocol_variant, False)

    def check_url_unique(self, new_service, url, protocol, goal):
        """Enforce a rule that a given circulation manager can only have
        one integration that uses a given URL for a certain purpose.

        Whether to enforce this rule for a given type of integration
        is up to you -- it's a good general rule but there are
        conceivable exceptions.

        This method is used by discovery_services.
        """
        if not url:
            return

        # Look for the given URL as well as minor variations.
        #
        # We can't use urlparse to ignore minor differences in URLs
        # because we're doing the comparison in the database.
        urls = list(self.url_variants(url))

        qu = (
            self._db.query(ExternalIntegration)
            .join(ExternalIntegration.settings)
            .filter(
                # Protocol must match.
                ExternalIntegration.protocol
                == protocol
            )
            .filter(
                # Goal must match.
                ExternalIntegration.goal
                == goal
            )
            .filter(ConfigurationSetting.key == ExternalIntegration.URL)
            .filter(
                # URL must be one of the URLs we're concerned about.
                ConfigurationSetting.value.in_(urls)
            )
            .filter(
                # But don't count the service we're trying to edit.
                ExternalIntegration.id
                != new_service.id
            )
        )
        if qu.count() > 0:
            return INTEGRATION_URL_ALREADY_IN_USE

    def look_up_service_by_id(self, id, protocol, goal=None):
        """Find an existing service, and make sure that the user is not trying to edit
        its protocol.
        This method is used by analytics_services, cdn_services, metadata_services,
        and sitewide_services.
        """

        if not goal:
            goal = self.goal

        service = get_one(self._db, ExternalIntegration, id=id, goal=goal)
        if not service:
            return MISSING_SERVICE
        if protocol and (protocol != service.protocol):
            return CANNOT_CHANGE_PROTOCOL
        return service

    def set_protocols(self, service, protocol, protocols=None):
        """Validate the protocol that the user has submitted; depending on whether
        the validations pass, either save it to this metadata service or
        return an error message.
        This method is used by analytics_services, cdn_services, discovery_services,
        metadata_services, and sitewide_services.
        """

        if not protocols:
            protocols = self.protocols

        [protocol] = [p for p in protocols if p.get("name") == protocol]
        result = self._set_integration_settings_and_libraries(service, protocol)
        if isinstance(result, ProblemDetail):
            return result

    def validate_protocol(self, protocols=None):
        protocols = protocols or self.protocols
        if flask.request.form.get("protocol") not in [p.get("name") for p in protocols]:
            return UNKNOWN_PROTOCOL

    def _get_settings(self):
        if hasattr(self, "protocols"):
            [protocol] = [
                p
                for p in self.protocols
                if p.get("name") == flask.request.form.get("protocol")
            ]
            return protocol.get("settings")
        return []

    def validate_formats(self, settings=None, validator=None):
        # If the service has self.protocols set, we can extract the list of settings here;
        # otherwise, the settings have to be passed in as an argument--either a list or
        # a string.
        validator = validator or Validator()
        settings = settings or self._get_settings()
        form = flask.request.form or None
        try:
            files = flask.request.files
        except:
            files = None
        error = validator.validate(settings, dict(form=form, files=files))
        if error:
            return error


class AdminSearchController(AdminController):
    """APIs for the admin search pages
    Eg. Lists Creation
    """

    def search_field_values(self) -> dict:
        """Enumerate the possible values for the search fields with counts
        - Audience
        - Distributor
        - Genre
        - Language
        - Publisher
        - Subject
        """
        library: Library = flask.request.library  # type: ignore
        collection_ids = [coll.id for coll in library.collections if coll.id]
        return self._search_field_values_cached(collection_ids)

    @classmethod
    def _unzip(cls, values: List[Tuple[str, int]]) -> dict:
        """Covert a list of tuples to a {value0: value1} dictionary"""
        return {a[0]: a[1] for a in values if type(a[0]) is str}

    # 1 hour in-memory cache
    @memoize(ttls=3600)
    def _search_field_values_cached(self, collection_ids: List[int]) -> dict:
        licenses_filter = or_(
            LicensePool.open_access == True,
            LicensePool.self_hosted == True,
            LicensePool.licenses_owned != 0,
        )

        # Reusable queries
        classification_query = (
            self._db.query(Classification)
            .join(Classification.subject)
            .join(
                LicensePool, LicensePool.identifier_id == Classification.identifier_id
            )
            .filter(LicensePool.collection_id.in_(collection_ids), licenses_filter)
        )

        editions_query = (
            self._db.query(LicensePool)
            .join(LicensePool.presentation_edition)
            .filter(LicensePool.collection_id.in_(collection_ids), licenses_filter)
        )

        # Concrete values
        subjects_list = list(
            classification_query.group_by(Subject.name).values(
                func.distinct(Subject.name), func.count(Subject.name)
            )
        )
        subjects = self._unzip(subjects_list)

        audiences_list = list(
            classification_query.group_by(Subject.audience).values(
                func.distinct(Subject.audience), func.count(Subject.audience)
            )
        )
        audiences = self._unzip(audiences_list)

        genres_list = list(
            classification_query.join(Subject.genre)
            .group_by(Genre.name)
            .values(func.distinct(Genre.name), func.count(Genre.name))
        )
        genres = self._unzip(genres_list)

        distributors_list = list(
            editions_query.join(Edition.data_source)
            .group_by(DataSource.name)
            .values(func.distinct(DataSource.name), func.count(DataSource.name))
        )
        distributors = self._unzip(distributors_list)

        languages_list = list(
            editions_query.group_by(Edition.language).values(
                func.distinct(Edition.language), func.count(Edition.language)
            )
        )
        converted_languages_list = []
        # We want full english names, not codes
        for name, num in languages_list:
            full_name_set = LanguageCodes.english_names.get(name, [name])
            # Language codes are an array of multiple choices, we only want one
            full_name = full_name_set[0] if len(full_name_set) > 0 else name
            converted_languages_list.append((full_name, num))
        languages = self._unzip(converted_languages_list)

        publishers_list = list(
            editions_query.group_by(Edition.publisher).values(
                func.distinct(Edition.publisher), func.count(Edition.publisher)
            )
        )
        publishers = self._unzip(publishers_list)

        return {
            "subjects": subjects,
            "audiences": audiences,
            "genres": genres,
            "distributors": distributors,
            "languages": languages,
            "publishers": publishers,
        }
