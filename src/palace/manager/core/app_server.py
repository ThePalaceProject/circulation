"""Implement logic common to more than one of the Simplified applications."""

from __future__ import annotations

import gzip
from collections.abc import Callable
from functools import wraps
from io import BytesIO
from typing import TYPE_CHECKING

import flask
from flask import Response, make_response, url_for
from psycopg2 import OperationalError
from werkzeug.exceptions import HTTPException

from palace import manager
from palace.manager.api.admin.config import Configuration as AdminUiConfig
from palace.manager.api.util.flask import PalaceFlask, get_request_library
from palace.manager.core.problem_details import INVALID_URN
from palace.manager.feed.acquisition import LookupAcquisitionFeed, OPDSAcquisitionFeed
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.lane import Facets, Pagination
from palace.manager.util.log import LoggerMixin
from palace.manager.util.opds_writer import OPDSMessage
from palace.manager.util.problem_detail import BaseProblemDetailException, ProblemDetail

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.library import Library
    from palace.manager.sqlalchemy.model.work import Work
    from palace.manager.util.flask_util import OPDSEntryResponse


def load_facets_from_request(
    facet_config=None,
    worklist=None,
    base_class=Facets,
    base_class_constructor_kwargs=None,
    default_entrypoint=None,
):
    """Figure out which faceting object this request is asking for.

    The active request must have the `library` member set to a Library
    object.

    :param worklist: The WorkList, if any, associated with the request.
    :param facet_config: An object containing the currently configured
        facet groups, if different from the request library.
    :param base_class: The faceting class to instantiate.
    :param base_class_constructor_kwargs: Keyword arguments to pass into
        the faceting class constructor, other than those obtained from
        the request.
    :return: A faceting object if possible; otherwise a ProblemDetail.
    """
    kwargs = base_class_constructor_kwargs or dict()
    get_arg = flask.request.args.get
    get_header = flask.request.headers.get
    library = get_request_library()
    facet_config = facet_config or library
    return base_class.from_request(
        library,
        facet_config,
        get_arg,
        get_header,
        worklist,
        default_entrypoint,
        **kwargs,
    )


def load_pagination_from_request(
    base_class=Pagination, base_class_constructor_kwargs=None, default_size=None
):
    """Figure out which Pagination object this request is asking for.

    :param base_class: A subclass of Pagination to instantiate.
    :param base_class_constructor_kwargs: Extra keyword arguments to use
        when instantiating the Pagination subclass.
    :param default_size: The default page size.
    :return: An instance of `base_class`.
    """
    kwargs = base_class_constructor_kwargs or dict()

    get_arg = flask.request.args.get
    return base_class.from_request(get_arg, default_size, **kwargs)


def returns_problem_detail(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        v = f(*args, **kwargs)
        if isinstance(v, ProblemDetail):
            return v.response
        return v

    return decorated


def raises_problem_detail[T, **P](f: Callable[P, T]) -> Callable[P, T | Response]:
    @wraps(f)
    def decorated(*args: P.args, **kwargs: P.kwargs) -> T | Response:
        try:
            return f(*args, **kwargs)
        except BaseProblemDetailException as e:
            return make_response(e.problem_detail.response)

    return decorated


def _parse_cache_control(cache_control_header: str | None) -> dict[str, int | None]:
    """
    Parse the Cache-Control header into a dictionary of directives.

    https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control
    """
    cache_control_header = cache_control_header or ""

    directives: dict[str, int | None] = {}
    for directive in cache_control_header.split(","):
        directive = directive.strip().lower()
        if not directive:
            continue

        if "=" in directive:
            key, value = directive.split("=", 1)
            try:
                int_val = int(value)
            except ValueError:
                continue
            directives[key] = int_val
        else:
            directives[directive] = None

    return directives


def cache_control_headers[**P](
    default_max_age: int | None = None,
) -> Callable[[Callable[P, Response]], Callable[P, Response]]:
    """
    Decorator that manages Cache-Control headers on Flask responses based on request and configuration.

    This decorator processes incoming request Cache-Control headers and adds appropriate Cache-Control
    headers to responses.

    Note: This decorator allows incoming requests to override cache settings, potentially bypassing
          the cache. This could enable malicious users to cause excessive load on the server. Use
          with caution.
    Todo: Consider restricting this functionality to authenticated users in the future.

    :param default_max_age: Optional integer specifying the default max-age in seconds
            to set on responses that don't already have Cache-Control headers
    """

    def decorator(f: Callable[P, Response]) -> Callable[P, Response]:
        @wraps(f)
        def decorated(*args: P.args, **kwargs: P.kwargs) -> Response:
            """Set cache control headers on the response."""
            response = f(*args, **kwargs)

            # Check if the incoming request has a Cache-Control header
            directives = _parse_cache_control(
                flask.request.headers.get("Cache-Control")
            )

            if "no-cache" in directives or "no-store" in directives:
                # Set the response to be non-cacheable.
                response.headers["Cache-Control"] = "no-store"
            elif "max-age" in directives and directives["max-age"]:
                # Set the response to be cacheable for the specified max-age.
                response.headers["Cache-Control"] = f"max-age={directives['max-age']}"
            elif (
                "Cache-Control" not in response.headers and default_max_age is not None
            ):
                # If no Cache-Control header is set, use the default max-age.
                response.headers["Cache-Control"] = f"max-age={default_max_age}"
            return response

        return decorated

    return decorator


def compressible(f):
    """Decorate a function to make it transparently handle whatever
    compression the client has announced it supports.

    Currently the only form of compression supported is
    representation-level gzip compression requested through the
    Accept-Encoding header.

    This code was modified from
    http://kb.sites.apiit.edu.my/knowledge-base/how-to-gzip-response-in-flask/,
    though I don't know if that's the original source; it shows up in
    a lot of places.
    """

    @wraps(f)
    def compressor(*args, **kwargs):
        @flask.after_this_request
        def compress(response):
            if (
                response.status_code < 200
                or response.status_code >= 300
                or "Content-Encoding" in response.headers
            ):
                # Don't encode anything other than a 2xx response
                # code. Don't encode a response that's
                # already been encoded.
                return response

            accept_encoding = flask.request.headers.get("Accept-Encoding", "")
            if not "gzip" in accept_encoding.lower():
                return response

            # At this point we know we're going to be changing the
            # outgoing response.

            # TODO: I understand what direct_passthrough does, but am
            # not sure what it has to do with this, and commenting it
            # out doesn't change the results or cause tests to
            # fail. This is pure copy-and-paste magic.
            response.direct_passthrough = False

            buffer = BytesIO()
            gzipped = gzip.GzipFile(mode="wb", fileobj=buffer)
            gzipped.write(response.data)
            gzipped.close()
            response.data = buffer.getvalue()

            response.headers["Content-Encoding"] = "gzip"
            response.vary.add("Accept-Encoding")
            response.headers["Content-Length"] = len(response.data)

            return response

        return f(*args, **kwargs)

    return compressor


class ErrorHandler(LoggerMixin):
    def __init__(self, app: PalaceFlask) -> None:
        """Constructor.

        :param app: The Flask application object.
        :param log_level: The log level set for this application.
        """
        self.app = app

    def handle(self, exception: Exception) -> Response | HTTPException:
        """Something very bad has happened. Notify the client."""
        if isinstance(exception, HTTPException):
            # This isn't an exception we need to handle, it's werkzeug's way
            # of interrupting normal control flow with a specific HTTP response.
            # Return the exception and it will be used as the response.
            return exception

        if hasattr(self.app, "manager") and hasattr(self.app.manager, "_db"):
            # If there is an active database session, then roll the session back.
            self.app.manager._db.rollback()

        # By default, the error will be logged at log level ERROR.
        log_method = self.log.error

        # If we can, we will turn the exception into a problem detail
        if isinstance(exception, BaseProblemDetailException):
            document = exception.problem_detail
            document.debug_message = None
            if document.status_code == 502:
                # This is an error in integrating with some upstream
                # service. It's a serious problem, but probably not
                # indicative of a bug in our software. Log it at log level
                # WARN.
                log_method = self.log.warning
            response = make_response(document.response)
        elif isinstance(exception, OperationalError):
            # This is an error, but it is probably unavoidable. Likely it was caused by
            # the database dropping our connection which can happen then the database is
            # restarted for maintenance. We'll log it at log level WARN.
            log_method = self.log.warning
            body = "Service temporarily unavailable. Please try again later."
            response = make_response(body, 503, {"Content-Type": "text/plain"})
        else:
            # There's no way to turn this exception into a problem
            # document. This is probably indicative of a bug in our
            # software.
            body = "An internal error occurred"
            response = make_response(body, 500, {"Content-Type": "text/plain"})

        log_method("Exception in web app: %s", exception, exc_info=exception)
        return response


class ApplicationVersionController:
    @staticmethod
    def version():
        response = {
            "version": manager.__version__,
            "commit": manager.__commit__,
            "branch": manager.__branch__,
            "admin_ui": {
                "package": AdminUiConfig.package_name(),
                "version": AdminUiConfig.package_version(),
            },
        }
        return response


class URNLookupController:
    """A controller for looking up OPDS entries for specific books,
    identified in terms of their Identifier URNs.
    """

    def __init__(self, _db):
        """Constructor.

        :param _db: A database connection.
        """
        self._db = _db

    def work_lookup(
        self,
        annotator,
        route_name="lookup",
        library: Library | None = None,
        **process_urn_kwargs,
    ):
        """Generate an OPDS feed describing works identified by identifier.

        :param annotator: The annotator to use for generating OPDS entries.
        :param route_name: The name of the route for generating URLs.
        :param library: Optional Library to filter works against.
        """
        urns = flask.request.args.getlist("urn")

        this_url = url_for(route_name, _external=True, urn=urns)
        handler = self.process_urns(urns, library=library, **process_urn_kwargs)

        if isinstance(handler, ProblemDetail):
            # In a subclass, self.process_urns may return a ProblemDetail
            return handler
        opds_feed = LookupAcquisitionFeed(
            "Lookup results",
            this_url,
            handler.works,
            annotator,
            precomposed_entries=handler.precomposed_entries,
        )
        opds_feed.generate_feed(annotate=False)
        return opds_feed.as_response(mime_types=flask.request.accept_mimetypes)

    def process_urns(self, urns, library: Library | None = None, **process_urn_kwargs):
        """Process a number of URNs by instantiating a URNLookupHandler
        and having it do the work.

        The information gathered by the URNLookupHandler can be used
        by the caller to generate an OPDS feed.

        :param urns: List of URNs to look up.
        :param library: Optional Library to filter works against.
        :return: A URNLookupHandler, or a ProblemDetail if
            there's a problem with the request.
        """
        handler = URNLookupHandler(self._db, library=library)
        handler.process_urns(urns, **process_urn_kwargs)
        return handler

    def permalink(self, urn, annotator, route_name="work"):
        """Look up a single identifier and generate an OPDS feed.

        TODO: This method is tested, but it seems unused and it
        should be possible to remove it.
        """
        handler = URNLookupHandler(self._db)
        this_url = url_for(route_name, _external=True, urn=urn)
        handler.process_urns([urn])

        # A LookupAcquisitionFeed's .works is a list of (identifier,
        # work) tuples, but an AcquisitionFeed's .works is just a
        # list of works.
        works = [work for (identifier, work) in handler.works]
        opds_feed = OPDSAcquisitionFeed(
            urn,
            this_url,
            works,
            annotator,
            precomposed_entries=handler.precomposed_entries,
        )
        return opds_feed.as_response()


class URNLookupHandler:
    """A helper for URNLookupController that takes URNs as input and looks
    up their OPDS entries.

    This is a separate class from URNLookupController because
    URNLookupController is designed to not keep state.
    """

    UNRECOGNIZED_IDENTIFIER = "This work is not in the collection."
    WORK_NOT_PRESENTATION_READY = "Work created but not yet presentation-ready."
    WORK_NOT_CREATED = "Identifier resolved but work not yet created."

    def __init__(self, _db, library: Library | None = None):
        """
        :param _db: A database session.
        :param library: Optional Library to filter works against. If provided,
            works matching the library's filtered_audiences or filtered_genres
            will be excluded from results.
        """
        self._db = _db
        self.library = library
        self.works: list[tuple[Identifier, Work]] = []
        self.precomposed_entries: list[OPDSMessage | OPDSEntryResponse] = []

    def process_urns(self, urns, **process_urn_kwargs):
        """Processes a list of URNs for a lookup request.

        :return: None or, to override default feed behavior, a ProblemDetail
            or Response.

        """
        identifiers_by_urn, failures = Identifier.parse_urns(self._db, urns)
        self.add_urn_failure_messages(failures)

        for urn, identifier in list(identifiers_by_urn.items()):
            self.process_identifier(identifier, urn, **process_urn_kwargs)
        self.post_lookup_hook()

    def add_urn_failure_messages(self, failures):
        for urn in failures:
            self.add_message(urn, 400, INVALID_URN.detail)

    def process_identifier(self, identifier, urn, **kwargs):
        """Turn a URN into a Work suitable for use in an OPDS feed."""
        if not identifier.licensed_through:
            # The default URNLookupHandler cannot look up an
            # Identifier that has no associated LicensePool.
            return self.add_message(urn, 404, self.UNRECOGNIZED_IDENTIFIER)

        # If we get to this point, there is at least one LicensePool
        # for this identifier.
        work = identifier.work
        if not work:
            # There are LicensePools but no Work.
            return self.add_message(urn, 202, self.WORK_NOT_CREATED)
        if not work.presentation_ready:
            # There is a work but it's not presentation ready.
            return self.add_message(urn, 202, self.WORK_NOT_PRESENTATION_READY)

        # Check library content filtering
        if self.library and work.is_filtered_for_library(self.library):
            # This work is filtered by the library's content settings.
            # Treat it as if it doesn't exist.
            return self.add_message(urn, 404, self.UNRECOGNIZED_IDENTIFIER)

        # The work is ready for use in an OPDS feed!
        return self.add_work(identifier, work)

    def add_work(self, identifier, work):
        """An identifier lookup succeeded in finding a Work."""
        self.works.append((identifier, work))

    def add_entry(self, entry):
        """An identifier lookup succeeded in creating an OPDS entry."""
        self.precomposed_entries.append(entry)

    def add_message(self, urn, status_code, message):
        """An identifier lookup resulted in the creation of a message."""
        self.precomposed_entries.append(OPDSMessage(urn, status_code, message))

    def post_lookup_hook(self):
        """Run after looking up a number of Identifiers.

        By default, does nothing.
        """
