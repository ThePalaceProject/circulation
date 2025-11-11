from collections.abc import Callable
from datetime import timedelta
from functools import wraps
from typing import ParamSpec, TypeVar

import flask
from flask import Response, make_response, redirect, request, url_for

from palace.manager.api.admin.config import (
    Configuration as AdminClientConfig,
    OperationalMode,
)
from palace.manager.api.admin.dashboard_stats import generate_statistics
from palace.manager.api.admin.model.dashboard_statistics import StatisticsResponse
from palace.manager.api.admin.problem_details import (
    ADMIN_NOT_AUTHORIZED,
    INVALID_ADMIN_CREDENTIALS,
)
from palace.manager.api.app import app
from palace.manager.api.controller.static_file import StaticFileController
from palace.manager.api.routes import allows_library, has_library, library_route
from palace.manager.core.app_server import returns_problem_detail
from palace.manager.sqlalchemy.model.admin import Admin
from palace.manager.util.problem_detail import BaseProblemDetailException, ProblemDetail

# An admin's session will expire after this amount of time and
# the admin will have to log in again.
app.permanent_session_lifetime = timedelta(hours=9)

P = ParamSpec("P")
T = TypeVar("T")


def allows_admin_auth_setup(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        setting_up = app.manager.admin_sign_in_controller.admin_auth_providers == []
        return f(*args, setting_up=setting_up, **kwargs)

    return decorated


def requires_basic_auth[**P, T](func: Callable[P, T]) -> Callable[P, T | ProblemDetail]:
    """Basic auth for stateless system admin only API calls."""

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T | ProblemDetail:
        auth = request.authorization
        if not auth or auth.username is None or auth.password is None:
            return INVALID_ADMIN_CREDENTIALS

        admin = Admin.authenticate(app.manager._db, auth.username, auth.password)

        if not admin:
            return INVALID_ADMIN_CREDENTIALS

        if not admin.is_system_admin():
            return ADMIN_NOT_AUTHORIZED
        return func(*args, **kwargs)

    return wrapper


def requires_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "setting_up" in kwargs:
            # If the function also requires a CSRF token,
            # setting_up needs to stay in the arguments for
            # the next decorator. Otherwise, it should be
            # removed before the route function.
            if f.__dict__.get("requires_csrf_token"):
                setting_up = kwargs.get("setting_up")
            else:
                setting_up = kwargs.pop("setting_up")
        else:
            setting_up = False
        if not setting_up:
            admin = (
                app.manager.admin_sign_in_controller.authenticated_admin_from_request()
            )
            if isinstance(admin, ProblemDetail):
                return app.manager.admin_sign_in_controller.error_response(admin)
            elif isinstance(admin, Response):
                return admin

        return f(*args, **kwargs)

    return decorated


def requires_csrf_token(f):
    f.__dict__["requires_csrf_token"] = True

    @wraps(f)
    def decorated(*args, **kwargs):
        if "setting_up" in kwargs:
            setting_up = kwargs.pop("setting_up")
        else:
            setting_up = False
        if not setting_up and flask.request.method in ["POST", "PUT", "DELETE"]:
            token = app.manager.admin_sign_in_controller.check_csrf_token()
            if isinstance(token, ProblemDetail):
                return token
        return f(*args, **kwargs)

    return decorated


def returns_json_or_response_or_problem_detail(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            v = f(*args, **kwargs)
        except BaseProblemDetailException as ex:
            # A ProblemDetailException just needs to be converted to a ProblemDetail.
            v = ex.problem_detail
        if isinstance(v, ProblemDetail):
            return v.response
        if isinstance(v, Response):
            return v
        return flask.jsonify(**v)

    return decorated


@app.route("/admin/sign_in_with_password", methods=["POST"])
@returns_problem_detail
def password_auth():
    return app.manager.admin_sign_in_controller.password_sign_in()


@app.route("/admin/sign_in")
@returns_problem_detail
def admin_sign_in():
    return app.manager.admin_sign_in_controller.sign_in()


@app.route("/admin/sign_out")
@returns_problem_detail
@requires_admin
def admin_sign_out():
    return app.manager.admin_sign_in_controller.sign_out()


@app.route("/admin/change_password", methods=["POST"])
@returns_problem_detail
@requires_admin
def admin_change_password():
    return app.manager.admin_sign_in_controller.change_password()


@app.route("/admin/forgot_password", methods=["GET", "POST"])
@returns_problem_detail
def admin_forgot_password():
    return app.manager.admin_reset_password_controller.forgot_password()


@app.route(
    "/admin/reset_password/<reset_password_token>/<admin_id>", methods=["GET", "POST"]
)
@returns_problem_detail
def admin_reset_password(reset_password_token, admin_id):
    return app.manager.admin_reset_password_controller.reset_password(
        reset_password_token, admin_id
    )


@library_route("/admin/works/<identifier_type>/<path:identifier>", methods=["GET"])
@has_library
@returns_problem_detail
@requires_admin
def work_details(identifier_type, identifier):
    return app.manager.admin_work_controller.details(identifier_type, identifier)


@library_route(
    "/admin/works/<identifier_type>/<path:identifier>/classifications", methods=["GET"]
)
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
def work_classifications(identifier_type, identifier):
    return app.manager.admin_work_controller.classifications(
        identifier_type, identifier
    )


@library_route(
    "/admin/works/<identifier_type>/<path:identifier>/lists", methods=["GET", "POST"]
)
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def work_custom_lists(identifier_type, identifier):
    return app.manager.admin_work_controller.custom_lists(identifier_type, identifier)


@library_route(
    "/admin/works/<identifier_type>/<path:identifier>/edit", methods=["POST"]
)
@has_library
@returns_problem_detail
@requires_admin
@requires_csrf_token
def edit(identifier_type, identifier):
    return app.manager.admin_work_controller.edit(identifier_type, identifier)


@library_route(
    "/admin/works/<identifier_type>/<path:identifier>/suppression", methods=["POST"]
)
@allows_library
@returns_problem_detail
@requires_admin
@requires_csrf_token
def suppress_for_library(identifier_type, identifier):
    return app.manager.admin_work_controller.suppress(identifier_type, identifier)


@library_route(
    "/admin/works/<identifier_type>/<path:identifier>/suppression", methods=["DELETE"]
)
@allows_library
@returns_problem_detail
@requires_admin
@requires_csrf_token
def unsuppress_for_library(identifier_type, identifier):
    return app.manager.admin_work_controller.unsuppress(identifier_type, identifier)


@DeprecationWarning
@library_route(
    "/admin/works/<identifier_type>/<path:identifier>/suppress", methods=["POST"]
)
@allows_library
@returns_problem_detail
@requires_admin
@requires_csrf_token
def suppress_deprecated(identifier_type, identifier):
    return app.manager.admin_work_controller.suppress(identifier_type, identifier)


@DeprecationWarning
@library_route(
    "/admin/works/<identifier_type>/<path:identifier>/unsuppress", methods=["POST"]
)
@allows_library
@returns_problem_detail
@requires_admin
@requires_csrf_token
def unsuppress_deprecated(identifier_type, identifier):
    return app.manager.admin_work_controller.unsuppress(identifier_type, identifier)


@library_route("/works/<identifier_type>/<path:identifier>/refresh", methods=["POST"])
@has_library
@returns_problem_detail
@requires_admin
@requires_csrf_token
def refresh(identifier_type, identifier):
    return app.manager.admin_work_controller.refresh_metadata(
        identifier_type, identifier
    )


@library_route(
    "/admin/works/<identifier_type>/<path:identifier>/edit_classifications",
    methods=["POST"],
)
@has_library
@returns_problem_detail
@requires_admin
@requires_csrf_token
def edit_classifications(identifier_type, identifier):
    return app.manager.admin_work_controller.edit_classifications(
        identifier_type, identifier
    )


@app.route("/admin/roles")
@returns_json_or_response_or_problem_detail
def roles():
    return app.manager.admin_work_controller.roles()


@app.route("/admin/languages")
@returns_json_or_response_or_problem_detail
def languages():
    return app.manager.admin_work_controller.languages()


@app.route("/admin/media")
@returns_json_or_response_or_problem_detail
def media():
    return app.manager.admin_work_controller.media()


@app.route("/admin/rights_status")
@returns_json_or_response_or_problem_detail
def rights_status():
    return app.manager.admin_work_controller.rights_status()


@library_route("/admin/suppressed")
@has_library
@returns_problem_detail
@requires_admin
def suppressed():
    """Returns a feed of suppressed works."""
    return app.manager.admin_feed_controller.suppressed()


@app.route("/admin/genres")
@returns_json_or_response_or_problem_detail
@requires_admin
def genres():
    """Returns a JSON representation of complete genre tree."""
    return app.manager.admin_feed_controller.genres()


@library_route("/admin/bulk_circulation_events")
@returns_problem_detail
@allows_library
@requires_admin
def bulk_circulation_events():
    """Returns a CSV representation of all circulation events with optional
    start and end times."""
    (
        data,
        date,
        date_end,
        library,
    ) = app.manager.admin_dashboard_controller.bulk_circulation_events()
    if isinstance(data, ProblemDetail):
        return data

    response = make_response(data)

    # If gathering events per library, include the library name in the file
    # for convenience. The start and end dates will always be included.
    filename = library + "-" if library else ""
    filename += date + "-to-" + date_end if date_end and date != date_end else date
    response.headers["Content-Disposition"] = (
        "attachment; filename=circulation_events_" + filename + ".csv"
    )
    response.headers["Content-type"] = "text/csv"
    return response


@app.route("/admin/stats")
@returns_json_or_response_or_problem_detail
@requires_admin
def stats():
    statistics_response: StatisticsResponse = (
        app.manager.admin_dashboard_controller.stats(stats_function=generate_statistics)
    )
    return statistics_response.api_dict()


@app.route("/admin/quicksight_embed/<dashboard_name>")
@returns_json_or_response_or_problem_detail
@requires_admin
def generate_quicksight_url(dashboard_name: str):
    return app.manager.admin_quicksight_controller.generate_quicksight_url(
        dashboard_name
    )


@app.route("/admin/quicksight_embed/names")
@returns_json_or_response_or_problem_detail
@requires_admin
def get_quicksight_names():
    return app.manager.admin_quicksight_controller.get_dashboard_names()


@app.route("/admin/libraries", methods=["GET", "POST"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def libraries():
    return app.manager.admin_library_settings_controller.process_libraries()


@app.route("/admin/library/<library_uuid>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def library(library_uuid):
    return app.manager.admin_library_settings_controller.process_delete(library_uuid)


@app.route("/admin/collections", methods=["GET", "POST"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def collections():
    return app.manager.admin_collection_settings_controller.process_collections()


@app.route("/admin/collection/<collection_id>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def collection(collection_id):
    return app.manager.admin_collection_settings_controller.process_delete(
        collection_id
    )


@app.route("/admin/collection_self_tests/<identifier>", methods=["GET", "POST"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def collection_self_tests(identifier):
    return (
        app.manager.admin_collection_settings_controller.process_collection_self_tests(
            identifier
        )
    )


@app.route("/admin/individual_admins", methods=["GET", "POST"])
@returns_json_or_response_or_problem_detail
@allows_admin_auth_setup
@requires_admin
@requires_csrf_token
def individual_admins():
    return (
        app.manager.admin_individual_admin_settings_controller.process_individual_admins()
    )


@app.route("/admin/individual_admin/<email>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def individual_admin(email):
    return app.manager.admin_individual_admin_settings_controller.process_delete(email)


@app.route("/admin/patron_auth_services", methods=["GET", "POST"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def patron_auth_services():
    return (
        app.manager.admin_patron_auth_services_controller.process_patron_auth_services()
    )


@app.route("/admin/patron_auth_service/<service_id>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def patron_auth_service(service_id):
    return app.manager.admin_patron_auth_services_controller.process_delete(service_id)


@app.route(
    "/admin/patron_auth_service_self_tests/<identifier>", methods=["GET", "POST"]
)
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def patron_auth_self_tests(identifier):
    return app.manager.admin_patron_auth_services_controller.process_patron_auth_service_self_tests(
        identifier
    )


@library_route("/admin/manage_patrons", methods=["POST"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def lookup_patron():
    return app.manager.admin_patron_controller.lookup_patron()


@library_route("/admin/manage_patrons/reset_adobe_id", methods=["POST"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def reset_adobe_id():
    return app.manager.admin_patron_controller.reset_adobe_id()


@app.route("/admin/metadata_services", methods=["GET", "POST"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def metadata_services():
    return app.manager.admin_metadata_services_controller.process_metadata_services()


@app.route("/admin/metadata_service/<service_id>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def metadata_service(service_id):
    return app.manager.admin_metadata_services_controller.process_delete(service_id)


@app.route("/admin/metadata_service_self_tests/<identifier>", methods=["GET", "POST"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def metadata_service_self_tests(identifier):
    return app.manager.admin_metadata_services_controller.process_metadata_service_self_tests(
        identifier
    )


@app.route("/admin/catalog_services", methods=["GET", "POST"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def catalog_services():
    return app.manager.admin_catalog_services_controller.process_catalog_services()


@app.route("/admin/catalog_service/<service_id>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def catalog_service(service_id):
    return app.manager.admin_catalog_services_controller.process_delete(service_id)


@app.route("/admin/discovery_services", methods=["GET", "POST"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def discovery_services():
    return app.manager.admin_discovery_services_controller.process_discovery_services()


@app.route("/admin/discovery_service/<service_id>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def discovery_service(service_id):
    return app.manager.admin_discovery_services_controller.process_delete(service_id)


@app.route("/admin/announcements", methods=["GET", "POST"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def announcements_for_all():
    return app.manager.admin_announcement_service.process_many()


@app.route("/admin/discovery_service_library_registrations", methods=["GET", "POST"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def discovery_service_library_registrations():
    return (
        app.manager.admin_discovery_service_library_registrations_controller.process_discovery_service_library_registrations()
    )


@library_route("/admin/custom_lists", methods=["POST"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def custom_lists_post():
    return app.manager.admin_custom_lists_controller.custom_lists()


@library_route("/admin/custom_lists", methods=["GET"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def custom_lists_get():
    return app.manager.admin_custom_lists_controller.custom_lists()


@library_route("/admin/custom_list/<list_id>", methods=["GET"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def custom_list_get(list_id: int):
    return app.manager.admin_custom_lists_controller.custom_list(list_id)


@library_route("/admin/custom_list/<list_id>", methods=["POST"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def custom_list_post(list_id):
    return app.manager.admin_custom_lists_controller.custom_list(list_id)


@library_route("/admin/custom_list/<list_id>", methods=["DELETE"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def custom_list_delete(list_id):
    return app.manager.admin_custom_lists_controller.custom_list(list_id)


@library_route("/admin/custom_list/<list_id>/share", methods=["POST"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def custom_list_share(list_id: int):
    """Share a custom list with all libraries in the CM that share the collections of this library and works of this list"""
    return app.manager.admin_custom_lists_controller.share_locally(list_id)


@library_route("/admin/custom_list/<list_id>/share", methods=["DELETE"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def custom_list_unshare(list_id: int):
    """Unshare the list from all libraries, as long as no other library is using the list in its lanes"""
    return app.manager.admin_custom_lists_controller.share_locally(list_id)


@library_route("/admin/lanes", methods=["GET", "POST"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def lanes():
    return app.manager.admin_lanes_controller.lanes()


@library_route("/admin/lane/<lane_identifier>", methods=["DELETE"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def lane(lane_identifier):
    return app.manager.admin_lanes_controller.lane(lane_identifier)


@library_route("/admin/lane/<lane_identifier>/show", methods=["POST"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def lane_show(lane_identifier):
    return app.manager.admin_lanes_controller.show_lane(lane_identifier)


@library_route("/admin/lane/<lane_identifier>/hide", methods=["POST"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def lane_hide(lane_identifier):
    return app.manager.admin_lanes_controller.hide_lane(lane_identifier)


@library_route("/admin/lanes/reset", methods=["POST"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def reset_lanes():
    return app.manager.admin_lanes_controller.reset()


@library_route("/admin/lanes/change_order", methods=["POST"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def change_lane_order():
    return app.manager.admin_lanes_controller.change_order()


@library_route("/admin/search_field_values", methods=["GET"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
def search_field_values():
    return app.manager.admin_search_controller.search_field_values()


@app.route("/admin/diagnostics")
@requires_admin
@returns_json_or_response_or_problem_detail
def diagnostics():
    return app.manager.timestamps_controller.diagnostics()


@app.route(
    "/admin/reports/inventory_report/<path:library_short_name>",
    methods=["GET"],
)
@allows_library
@returns_json_or_response_or_problem_detail
@requires_admin
def inventory_report_info():
    return app.manager.admin_report_controller.inventory_report_info()


@app.route(
    "/admin/reports/inventory_report/<path:library_short_name>",
    methods=["POST"],
)
@allows_library
@returns_json_or_response_or_problem_detail
@requires_admin
def generate_inventory_report():
    return app.manager.admin_report_controller.generate_inventory_report()


@library_route("/admin/reports/<report_key>", methods=["POST"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
def generate_report(report_key: str):
    return app.manager.admin_report_controller.generate_report(report_key=report_key)


@app.route("/admin/sign_in_again")
def admin_sign_in_again():
    """Allows an  admin with expired credentials to sign back in
    from a new browser tab so they won't lose changes.
    """
    admin = app.manager.admin_sign_in_controller.authenticated_admin_from_request()
    csrf_token = app.manager.admin_sign_in_controller.get_csrf_token()
    if (
        isinstance(admin, ProblemDetail)
        or csrf_token is None
        or isinstance(csrf_token, ProblemDetail)
    ):
        redirect_url = flask.request.url
        return redirect(url_for("admin_sign_in", redirect=redirect_url, _external=True))
    return flask.render_template(
        "admin/signed-back-in.html.jinja2", csrf_token=csrf_token
    )


@app.route("/admin/web/", strict_slashes=False)
@app.route("/admin/web/collection/<path:collection>/book/<path:book>")
@app.route("/admin/web/collection/<path:collection>")
@app.route("/admin/web/book/<path:book>")
@app.route("/admin/web/<path:etc>")  # catchall for single-page URLs
def admin_view(collection=None, book=None, etc=None, **kwargs):
    return app.manager.admin_view_controller(collection, book, path=etc)


@app.route("/admin/", strict_slashes=False)
def admin_base(**kwargs):
    return redirect(url_for("admin_view", _external=True))


@app.route("/admin/libraries/import", strict_slashes=False, methods=["POST"])
@returns_json_or_response_or_problem_detail
@requires_basic_auth
def import_libraries():
    """Import multiple libraries from a list of library configurations."""
    return app.manager.admin_library_settings_controller.import_libraries()


# This path is used only in debug mode to serve frontend assets.
if AdminClientConfig.operational_mode() == OperationalMode.development:

    @app.route("/admin/static/<filename>")
    @returns_problem_detail
    def admin_static_file(filename):
        return StaticFileController.static_file(
            AdminClientConfig.static_files_directory(), filename
        )
