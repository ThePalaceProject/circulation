import gzip
import json
from collections.abc import Callable, Iterable
from functools import partial
from io import BytesIO
from unittest.mock import MagicMock, PropertyMock

import flask
import pytest
from flask import Flask, Response, make_response
from flask_babel import Babel
from flask_babel import lazy_gettext as _

import core
from api.admin.config import Configuration as AdminUiConfig
from api.util.flask import PalaceFlask
from core.app_server import (
    ApplicationVersionController,
    ErrorHandler,
    URNLookupController,
    URNLookupHandler,
    compressible,
    load_facets_from_request,
    load_pagination_from_request,
)
from core.entrypoint import AudiobooksEntryPoint, EbooksEntryPoint
from core.feed.annotator.base import Annotator
from core.lane import Facets, Pagination, SearchFacets, WorkList
from core.model import Identifier
from core.problem_details import INVALID_INPUT, INVALID_URN
from core.service.logging.configuration import LogLevel
from core.util.opds_writer import OPDSFeed, OPDSMessage
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture


class TestApplicationVersionController:
    @pytest.mark.parametrize(
        "version,commit,branch,ui_version,ui_package",
        [("123", "xyz", "abc", "def", "ghi"), (None, None, None, None, None)],
    )
    def test_version(
        self, version, commit, branch, ui_version, ui_package, monkeypatch
    ):
        app = Flask(__name__)

        # Mock the cm version strings
        monkeypatch.setattr(core, "__version__", version)
        monkeypatch.setattr(core, "__commit__", commit)
        monkeypatch.setattr(core, "__branch__", branch)

        # Mock the admin ui version strings
        if ui_package is not None:
            monkeypatch.setenv(AdminUiConfig.ENV_ADMIN_UI_PACKAGE_NAME, ui_package)
        else:
            monkeypatch.delenv(AdminUiConfig.ENV_ADMIN_UI_PACKAGE_NAME, raising=False)

        if ui_version is not None:
            monkeypatch.setenv(AdminUiConfig.ENV_ADMIN_UI_PACKAGE_VERSION, ui_version)
        else:
            monkeypatch.delenv(
                AdminUiConfig.ENV_ADMIN_UI_PACKAGE_VERSION, raising=False
            )

        controller = ApplicationVersionController()
        with app.test_request_context("/"):
            response = make_response(controller.version())

        assert response.status_code == 200
        assert response.headers.get("Content-Type") == "application/json"

        assert response.json["version"] == version
        assert response.json["commit"] == commit
        assert response.json["branch"] == branch

        # When the env are not set (None) we use defaults
        assert (
            response.json["admin_ui"]["package"] == ui_package
            if ui_package
            else AdminUiConfig.PACKAGE_NAME
        )
        assert (
            response.json["admin_ui"]["version"] == ui_version
            if ui_version
            else AdminUiConfig.PACKAGE_VERSION
        )


class URNLookupHandlerFixture:
    transaction: DatabaseTransactionFixture
    handler: URNLookupHandler


@pytest.fixture()
def urn_lookup_handler_fixture(
    db: DatabaseTransactionFixture,
) -> URNLookupHandlerFixture:
    data = URNLookupHandlerFixture()
    data.transaction = db
    data.handler = URNLookupHandler(db.session)
    return data


class TestURNLookupHandler:
    @staticmethod
    def assert_one_message(urn, code, message, fix: URNLookupHandlerFixture):
        """Assert that the given message is the only thing
        in the feed.
        """
        [obj] = fix.handler.precomposed_entries
        expect = OPDSMessage(urn, code, message)
        assert isinstance(obj, OPDSMessage)
        assert urn == obj.urn
        assert code == obj.status_code
        assert message == obj.message
        assert [] == fix.handler.works

    def test_process_urns_hook_method(
        self, urn_lookup_handler_fixture: URNLookupHandlerFixture
    ):
        data, session = (
            urn_lookup_handler_fixture,
            urn_lookup_handler_fixture.transaction.session,
        )

        # Verify that process_urns() calls post_lookup_hook() once
        # it's done.
        class Mock(URNLookupHandler):
            def post_lookup_hook(self):
                self.called = True

        handler = Mock(session)
        handler.process_urns([])
        assert True == handler.called

    def test_process_urns_invalid_urn(
        self, urn_lookup_handler_fixture: URNLookupHandlerFixture
    ):
        data, session = (
            urn_lookup_handler_fixture,
            urn_lookup_handler_fixture.transaction.session,
        )

        urn = "not even a URN"
        data.handler.process_urns([urn])
        self.assert_one_message(urn, 400, INVALID_URN.detail, data)

    def test_process_urns_unrecognized_identifier(
        self, urn_lookup_handler_fixture: URNLookupHandlerFixture
    ):
        data, session = (
            urn_lookup_handler_fixture,
            urn_lookup_handler_fixture.transaction.session,
        )

        # Give the handler a URN that, although valid, doesn't
        # correspond to any Identifier in the database.
        urn = Identifier.GUTENBERG_URN_SCHEME_PREFIX + "Gutenberg%20ID/000"
        data.handler.process_urns([urn])

        # The result is a 404 message.
        self.assert_one_message(urn, 404, data.handler.UNRECOGNIZED_IDENTIFIER, data)

    def test_process_identifier_no_license_pool(
        self, urn_lookup_handler_fixture: URNLookupHandlerFixture
    ):
        data, session = (
            urn_lookup_handler_fixture,
            urn_lookup_handler_fixture.transaction.session,
        )

        # Give the handler a URN that corresponds to an Identifier
        # which has no LicensePool.
        identifier = data.transaction.identifier()
        data.handler.process_identifier(identifier, identifier.urn)

        # The result is a 404 message.
        self.assert_one_message(
            identifier.urn, 404, data.handler.UNRECOGNIZED_IDENTIFIER, data
        )

    def test_process_identifier_license_pool_but_no_work(
        self, urn_lookup_handler_fixture: URNLookupHandlerFixture
    ):
        data, session = (
            urn_lookup_handler_fixture,
            urn_lookup_handler_fixture.transaction.session,
        )

        edition, pool = data.transaction.edition(with_license_pool=True)
        identifier = edition.primary_identifier
        data.handler.process_identifier(identifier, identifier.urn)
        self.assert_one_message(
            identifier.urn, 202, data.handler.WORK_NOT_CREATED, data
        )

    def test_process_identifier_work_not_presentation_ready(
        self, urn_lookup_handler_fixture: URNLookupHandlerFixture
    ):
        data, session = (
            urn_lookup_handler_fixture,
            urn_lookup_handler_fixture.transaction.session,
        )

        work = data.transaction.work(with_license_pool=True)
        work.presentation_ready = False
        identifier = work.license_pools[0].identifier
        data.handler.process_identifier(identifier, identifier.urn)

        self.assert_one_message(
            identifier.urn, 202, data.handler.WORK_NOT_PRESENTATION_READY, data
        )

    def test_process_identifier_work_is_presentation_ready(
        self, urn_lookup_handler_fixture: URNLookupHandlerFixture
    ):
        data, session = (
            urn_lookup_handler_fixture,
            urn_lookup_handler_fixture.transaction.session,
        )

        work = data.transaction.work(with_license_pool=True)
        identifier = work.license_pools[0].identifier
        data.handler.process_identifier(identifier, identifier.urn)
        assert [] == data.handler.precomposed_entries
        assert [
            (work.presentation_edition.primary_identifier, work)
        ] == data.handler.works


class URNLookupControllerFixture:
    transaction: DatabaseTransactionFixture
    controller: URNLookupController
    app: Flask

    def lookup(self, urn):
        pass

    def work(self, urn):
        pass


@pytest.fixture()
def urn_lookup_controller_fixture(
    db,
) -> Iterable[URNLookupControllerFixture]:
    data = URNLookupControllerFixture()
    data.transaction = db
    data.controller = URNLookupController(db.session)
    data.app = Flask(URNLookupControllerFixture.__name__)

    # Register endpoints manually, because using decorators seems to
    # have scope-related issues when used in fixtures.
    data.app.add_url_rule(rule="/lookup", endpoint="lookup", view_func=data.lookup)
    data.app.add_url_rule(rule="/work", endpoint="work", view_func=data.work)
    yield data


class TestURNLookupController:
    def test_work_lookup(
        self, urn_lookup_controller_fixture: URNLookupControllerFixture
    ):
        data, session = (
            urn_lookup_controller_fixture,
            urn_lookup_controller_fixture.transaction.session,
        )

        work = data.transaction.work(with_license_pool=True)
        identifier = work.license_pools[0].identifier
        annotator = Annotator()
        # NOTE: We run this test twice to verify that the controller
        # doesn't keep any state between requests. At one point there
        # was a bug which would have caused a book to show up twice on
        # the second request.
        for i in range(2):
            with data.app.test_request_context("/?urn=%s" % identifier.urn):
                response = data.controller.work_lookup(annotator=annotator)

                # We got an OPDS feed that includes an entry for the work.
                assert 200 == response.status_code
                assert (
                    OPDSFeed.ACQUISITION_FEED_TYPE == response.headers["Content-Type"]
                )
                response_data = response.data.decode("utf8")
                assert identifier.urn in response_data
                assert 1 == response_data.count(work.title)

    def test_process_urns_problem_detail(
        self, urn_lookup_controller_fixture: URNLookupControllerFixture
    ):
        data, session = (
            urn_lookup_controller_fixture,
            urn_lookup_controller_fixture.transaction.session,
        )

        # Verify the behavior of work_lookup in the case where
        # process_urns returns a problem detail.
        class Mock(URNLookupController):
            def process_urns(self, urns, **kwargs):
                return INVALID_INPUT

        controller = Mock(session)
        with data.app.test_request_context("/?urn=foobar"):
            response = controller.work_lookup(annotator=object())
            assert INVALID_INPUT == response

    def test_permalink(self, urn_lookup_controller_fixture: URNLookupControllerFixture):
        data, session = (
            urn_lookup_controller_fixture,
            urn_lookup_controller_fixture.transaction.session,
        )

        work = data.transaction.work(with_license_pool=True)
        work.license_pools[0].open_access = False
        identifier = work.license_pools[0].identifier
        annotator = Annotator()
        with data.app.test_request_context("/?urn=%s" % identifier.urn):
            response = data.controller.permalink(identifier.urn, annotator)

            # We got an OPDS feed that includes an entry for the work.
            assert 200 == response.status_code
            assert OPDSFeed.ACQUISITION_FEED_TYPE == response.headers["Content-Type"]
            response_data = response.data.decode("utf8")
            assert identifier.urn in response_data
            assert work.title in response_data


class LoadMethodsFixture:
    transaction: DatabaseTransactionFixture
    app: Flask


@pytest.fixture()
def load_methods_fixture(
    db,
) -> LoadMethodsFixture:
    data = LoadMethodsFixture()
    data.transaction = db
    data.app = Flask(LoadMethodsFixture.__name__)
    Babel(data.app)
    return data


class TestLoadMethods:
    def test_load_facets_from_request(
        self, load_methods_fixture: LoadMethodsFixture, library_fixture: LibraryFixture
    ):
        fixture, data = load_methods_fixture, load_methods_fixture.transaction

        # The library has two EntryPoints enabled.
        settings = library_fixture.mock_settings()
        settings.enabled_entry_points = [
            EbooksEntryPoint.INTERNAL_NAME,
            AudiobooksEntryPoint.INTERNAL_NAME,
        ]
        library = data.library(settings=settings)

        with fixture.app.test_request_context("/?order=%s" % Facets.ORDER_TITLE):
            flask.request.library = library  # type: ignore[attr-defined]
            facets = load_facets_from_request()
            assert Facets.ORDER_TITLE == facets.order
            # Enabled facets are passed in to the newly created Facets,
            # in case the load method received a custom config.
            assert facets.facets_enabled_at_init is not None

        with fixture.app.test_request_context("/?order=bad_facet"):
            flask.request.library = library  # type: ignore[attr-defined]
            problemdetail = load_facets_from_request()
            assert INVALID_INPUT.uri == problemdetail.uri

        # An EntryPoint will be picked up from the request and passed
        # into the Facets object, assuming the EntryPoint is
        # configured on the present library.
        worklist = WorkList()
        worklist.initialize(library)
        with fixture.app.test_request_context("/?entrypoint=Audio"):
            flask.request.library = library  # type: ignore[attr-defined]
            facets = load_facets_from_request(worklist=worklist)
            assert AudiobooksEntryPoint == facets.entrypoint
            assert facets.entrypoint_is_default is False

        # If the requested EntryPoint not configured, the default
        # EntryPoint is used.
        with fixture.app.test_request_context("/?entrypoint=NoSuchEntryPoint"):
            flask.request.library = library  # type: ignore[attr-defined]
            default_entrypoint = object()
            facets = load_facets_from_request(
                worklist=worklist, default_entrypoint=default_entrypoint
            )
            assert default_entrypoint == facets.entrypoint
            assert facets.entrypoint_is_default is True

        # Load a SearchFacets object that pulls information from an
        # HTTP header.
        with fixture.app.test_request_context("/", headers={"Accept-Language": "ja"}):
            flask.request.library = data.default_library()  # type: ignore[attr-defined]
            facets = load_facets_from_request(base_class=SearchFacets)
            assert ["jpn"] == facets.languages

    def test_load_facets_from_request_class_instantiation(
        self, load_methods_fixture: LoadMethodsFixture
    ):
        """The caller of load_facets_from_request() can specify a class other
        than Facets to call from_request() on.
        """

        fixture, data = load_methods_fixture, load_methods_fixture.transaction

        class MockFacets:
            called_with: dict

            @classmethod
            def from_request(*args, **kwargs):
                facets = MockFacets()
                facets.called_with = kwargs
                return facets

        kwargs = dict(some_arg="some value")
        with fixture.app.test_request_context(""):
            flask.request.library = data.default_library()  # type: ignore[attr-defined]
            facets = load_facets_from_request(
                None, None, base_class=MockFacets, base_class_constructor_kwargs=kwargs
            )
        assert isinstance(facets, MockFacets)
        assert "some value" == facets.called_with["some_arg"]

    def test_load_pagination_from_request(
        self, load_methods_fixture: LoadMethodsFixture
    ):
        fixture = load_methods_fixture

        # Verify that load_pagination_from_request instantiates a
        # pagination object of the specified class (Pagination, by
        # default.)
        class Mock:
            DEFAULT_SIZE = 22
            called_with: tuple

            @classmethod
            def from_request(cls, get_arg, default_size, **kwargs):
                cls.called_with = (get_arg, default_size, kwargs)
                return "I'm a pagination object!"

        with fixture.app.test_request_context("/"):
            # Call load_pagination_from_request and verify that
            # Mock.from_request was called with the arguments we expect.
            extra_kwargs = dict(extra="kwarg")
            pagination = load_pagination_from_request(
                base_class=Mock,
                base_class_constructor_kwargs=extra_kwargs,
                default_size=44,
            )
            assert "I'm a pagination object!" == pagination
            assert (flask.request.args.get, 44, extra_kwargs) == Mock.called_with

        # If no default size is specified, we trust from_request to
        # use the class default.
        with fixture.app.test_request_context("/"):
            pagination = load_pagination_from_request(base_class=Mock)
            assert (flask.request.args.get, None, {}) == Mock.called_with

        # Now try a real case using the default pagination class,
        # Pagination
        with fixture.app.test_request_context("/?size=50&after=10"):
            pagination = load_pagination_from_request()
            assert isinstance(pagination, Pagination)
            assert 50 == pagination.size
            assert 10 == pagination.offset

        # Tests of from_request() are found in the tests of the various
        # pagination classes.


class CanBeProblemDetailDocument(Exception):
    """A fake exception that can be represented as a problem
    detail document.
    """

    def as_problem_detail_document(self, debug):
        return INVALID_URN.detailed(
            _("detail info"),
            debug_message="A debug_message which should only appear in debug mode.",
        )


class ErrorHandlerFixture:
    transaction: DatabaseTransactionFixture
    app: PalaceFlask
    handler: Callable[..., ErrorHandler]


@pytest.fixture()
def error_handler_fixture(
    db,
) -> ErrorHandlerFixture:
    session = db.session

    mock_manager = MagicMock()
    type(mock_manager)._db = PropertyMock(return_value=session)

    data = ErrorHandlerFixture()
    data.transaction = db
    data.app = PalaceFlask(ErrorHandlerFixture.__name__)
    Babel(data.app)
    data.app.manager = mock_manager
    data.handler = partial(ErrorHandler, app=data.app, log_level=LogLevel.error)
    return data


class TestErrorHandler:
    def raise_exception(self, cls=Exception):
        """Simulate an exception that happens deep within the stack."""
        raise cls()

    def test_unhandled_error(self, error_handler_fixture: ErrorHandlerFixture):
        handler = error_handler_fixture.handler()
        with error_handler_fixture.app.test_request_context("/"):
            response = None
            try:
                self.raise_exception()
            except Exception as exception:
                response = handler.handle(exception)
            assert isinstance(response, Response)
            assert 500 == response.status_code
            assert "An internal error occurred" == response.data.decode("utf8")

    def test_unhandled_error_debug(self, error_handler_fixture: ErrorHandlerFixture):
        # Set the sitewide log level to DEBUG to get a stack trace
        # instead of a generic error message.
        handler = error_handler_fixture.handler(log_level=LogLevel.debug)

        with error_handler_fixture.app.test_request_context("/"):
            response = None
            try:
                self.raise_exception()
            except Exception as exception:
                response = handler.handle(exception)
            assert isinstance(response, Response)
            assert 500 == response.status_code
            assert response.data.startswith(b"Traceback (most recent call last)")

    def test_handle_error_as_problem_detail_document(
        self, error_handler_fixture: ErrorHandlerFixture
    ):
        handler = error_handler_fixture.handler()
        with error_handler_fixture.app.test_request_context("/"):
            try:
                self.raise_exception(CanBeProblemDetailDocument)
            except Exception as exception:
                response = handler.handle(exception)

            assert isinstance(response, Response)
            assert 400 == response.status_code
            data = json.loads(response.data.decode("utf8"))
            assert INVALID_URN.title == data["title"]

            # Since we are not in debug mode, the debug_message is
            # destroyed.
            assert "debug_message" not in data

    def test_handle_error_as_problem_detail_document_debug(
        self, error_handler_fixture: ErrorHandlerFixture
    ):
        # When in debug mode, the debug_message is preserved and a
        # stack trace is appended to it.
        handler = error_handler_fixture.handler(log_level=LogLevel.debug)
        with error_handler_fixture.app.test_request_context("/"):
            try:
                self.raise_exception(CanBeProblemDetailDocument)
            except Exception as exception:
                response = handler.handle(exception)

            assert isinstance(response, Response)
            assert 400 == response.status_code
            data = json.loads(response.data.decode("utf8"))
            assert INVALID_URN.title == data["title"]
            assert data["debug_message"].startswith(
                "A debug_message which should only appear in debug mode.\n\n"
                "Traceback (most recent call last)"
            )


class TestCompressibleAnnotator:
    """Test the @compressible annotator."""

    def test_compressible(self):
        # Test the @compressible annotator.
        app = Flask(__name__)

        # Prepare a value and a gzipped version of the value.
        value = b"Compress me! (Or not.)"

        buffer = BytesIO()
        gzipped = gzip.GzipFile(mode="wb", fileobj=buffer)
        gzipped.write(value)
        gzipped.close()
        compressed = buffer.getvalue()

        # Spot-check the compressed value
        assert b"-(J-.V" in compressed

        # This compressible controller function always returns the
        # same value.
        @compressible
        def function():
            return value

        def ask_for_compression(compression, header="Accept-Encoding"):
            """This context manager simulates the entire Flask
            request-response cycle, including a call to
            process_response(), which triggers the @after_this_request
            hooks.

            :return: The Response object.
            """
            headers = {}
            if compression:
                headers[header] = compression
            with app.test_request_context(headers=headers):
                response = flask.Response(function())
                app.process_response(response)
                return response

        # If the client asks for gzip through Accept-Encoding, the
        # representation is compressed.
        response = ask_for_compression("gzip")
        assert compressed == response.data
        assert "gzip" == response.headers["Content-Encoding"]

        # If the client doesn't ask for compression, the value is
        # passed through unchanged.
        response = ask_for_compression(None)
        assert value == response.data
        assert "Content-Encoding" not in response.headers

        # Similarly if the client asks for an unsupported compression
        # mechanism.
        response = ask_for_compression("compress")
        assert value == response.data
        assert "Content-Encoding" not in response.headers

        # Or if the client asks for a compression mechanism through
        # Accept-Transfer-Encoding, which is currently unsupported.
        response = ask_for_compression("gzip", "Accept-Transfer-Encoding")
        assert value == response.data
        assert "Content-Encoding" not in response.headers
