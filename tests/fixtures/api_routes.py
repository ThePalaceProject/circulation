import logging
from collections.abc import Generator
from typing import Any

import flask
import pytest
from werkzeug.exceptions import MethodNotAllowed

from api import routes
from api.controller.circulation_manager import CirculationManagerController
from tests.api.mockapi.circulation import MockCirculationManager
from tests.fixtures.api_controller import ControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture


class MockApp:
    """Pretends to be a Flask application with a configured
    CirculationManager.
    """

    def __init__(self):
        self.manager = MockManager()


class MockManager:
    """Pretends to be a CirculationManager with configured controllers."""

    def __init__(self):
        self._cache = {}

        # This is used by the allows_patron_web annotator.
        self.patron_web_domains = {"http://patron/web"}

    def __getattr__(self, controller_name):
        return self._cache.setdefault(controller_name, MockController(controller_name))


class MockControllerMethod:
    """Pretends to be one of the methods of a controller class."""

    def __init__(self, controller, name):
        """Constructor.

        :param controller: A MockController.
        :param name: The name of this method.
        """
        self.controller = controller
        self.name = name
        self.callable_name = name

    def __call__(self, *args, **kwargs):
        """Simulate a successful method call.

        :return: A Response object, as required by Flask, with this
        method smuggled out as the 'method' attribute.
        """
        self.args = args
        self.kwargs = kwargs
        response = flask.Response("I called %s" % repr(self), 200)
        response.method = self
        return response

    def __repr__(self):
        return f"<MockControllerMethod {self.controller.name}.{self.name}>"


class MockController(MockControllerMethod):
    """Pretends to be a controller.

    A controller has methods, but it may also be called _as_ a method,
    so this class subclasses MockControllerMethod.
    """

    AUTHENTICATED_PATRON = "i am a mock patron"

    def __init__(self, name):
        """Constructor.

        :param name: The name of the controller.
        """
        self.name = name

        # If this controller were to be called as a method, the method
        # name would be __call__, not the name of the controller.
        self.callable_name = "__call__"

        self._cache = {}
        self.authenticated = False
        self.csrf_token = False
        self.authenticated_problem_detail = False

    def authenticated_patron_from_request(self):
        if self.authenticated:
            patron = object()
            flask.request.patron = self.AUTHENTICATED_PATRON
            return self.AUTHENTICATED_PATRON
        else:
            return flask.Response(
                "authenticated_patron_from_request called without authorizing", 401
            )

    def __getattr__(self, method_name):
        """Locate a method of this controller as a MockControllerMethod."""
        return self._cache.setdefault(
            method_name, MockControllerMethod(self, method_name)
        )

    def __repr__(self):
        return "<MockControllerMethod %s>" % self.name


class RouteTestFixture:
    # The first time __init__() is called, it will instantiate a real
    # CirculationManager object and store it in REAL_CIRCULATION_MANAGER.
    # We only do this once because it takes about a second to instantiate
    # this object. Calling any of this object's methods could be problematic,
    # since it's probably left over from a previous test, but we won't be
    # calling any methods -- we just want to verify the _existence_,
    # in a real CirculationManager, of the methods called in
    # routes.py.

    REAL_CIRCULATION_MANAGER = None

    def __init__(
        self, db: DatabaseTransactionFixture, controller_fixture: ControllerFixture
    ):
        self.db = db
        self.controller_fixture = controller_fixture
        self.setup_circulation_manager = False
        if not RouteTestFixture.REAL_CIRCULATION_MANAGER:
            manager = MockCirculationManager(self.db.session)
            RouteTestFixture.REAL_CIRCULATION_MANAGER = manager

        app = MockApp()
        self.routes = routes
        self.manager = app.manager
        self.original_app = self.routes.app
        self.resolver = self.original_app.url_map.bind("", "/")

        self.controller: CirculationManagerController | None = None
        self.real_controller: CirculationManagerController | None = None
        self.routes.app = app  # type: ignore

    def set_controller_name(self, name: str):
        self.controller = getattr(self.manager, name)
        # Make sure there's a controller by this name in the real
        # CirculationManager.
        self.real_controller = getattr(self.REAL_CIRCULATION_MANAGER, name)

    def close(self):
        self.routes.app = self.original_app

    def request(self, url, method="GET"):
        """Simulate a request to a URL without triggering any code outside
        routes.py.
        """
        # Map an incoming URL to the name of a function within routes.py
        # and a set of arguments to the function.
        function_name, kwargs = self.resolver.match(url, method)
        # Locate the corresponding function in our mock app.
        mock_function = getattr(self.routes, function_name)

        # Call it in the context of the mock app.
        with self.controller_fixture.app.test_request_context():
            return mock_function(**kwargs)

    def assert_request_calls(self, url, method, *args, **kwargs):
        """Make a request to the given `url` and assert that
        the given controller `method` was called with the
        given `args` and `kwargs`.
        """
        http_method = kwargs.pop("http_method", "GET")
        response = self.request(url, http_method)
        assert response.method == method
        assert response.method.args == args
        assert response.method.kwargs == kwargs

        # Make sure the real controller has a method by the name of
        # the mock method that was called. We won't call it, because
        # it would slow down these tests dramatically, but we can make
        # sure it exists.
        if self.real_controller:
            real_method = getattr(self.real_controller, method.callable_name)

            # TODO: We could use inspect.getarcspec to verify that the
            # argument names line up with the variables passed in to
            # the mock method. This might remove the need to call the
            # mock method at all.

    def assert_request_calls_method_using_identifier(
        self, url, method, *args, **kwargs
    ):
        # Call an assertion method several times, using different
        # types of identifier in the URL, to make sure the identifier
        # is always passed through correctly.
        #
        # The url must contain the string '<identifier>' standing in
        # for the place where an identifier should be plugged in, and
        # the *args list must include the string '<identifier>'.
        authenticated = kwargs.pop("authenticated", False)
        if authenticated:
            assertion_method = self.assert_authenticated_request_calls
        else:
            assertion_method = self.assert_request_calls
        assert "<identifier>" in url
        args = list(args)
        identifier_index = args.index("<identifier>")
        for identifier in (
            "<identifier>",
            "an/identifier/",
            "http://an-identifier/",
            "http://an-identifier",
        ):
            modified_url = url.replace("<identifier>", identifier)
            args[identifier_index] = identifier
            assertion_method(modified_url, method, *args, **kwargs)

    def assert_authenticated_request_calls(self, url, method, *args, **kwargs):
        """First verify that an unauthenticated request fails. Then make an
        authenticated request to `url` and verify the results, as with
        assert_request_calls
        """
        authentication_required = kwargs.pop("authentication_required", True)

        http_method = kwargs.pop("http_method", "GET")
        response = self.request(url, http_method)
        if authentication_required:
            assert 401 == response.status_code
            assert (
                "authenticated_patron_from_request called without authorizing"
                == response.get_data(as_text=True)
            )
        else:
            assert 200 == response.status_code

        # Set a variable so that authenticated_patron_from_request
        # will succeed, and try again.
        self.manager.index_controller.authenticated = True
        try:
            kwargs["http_method"] = http_method
            self.assert_request_calls(url, method, *args, **kwargs)
        finally:
            # Un-set authentication for the benefit of future
            # assertions in this test function.
            self.manager.index_controller.authenticated = False

    def assert_supported_methods(self, url, *methods):
        """Verify that the given HTTP `methods` are the only ones supported
        on the given `url`.
        """
        # The simplest way to do this seems to be to try each of the
        # other potential methods and verify that MethodNotAllowed is
        # raised each time.
        check = {"GET", "POST", "PUT", "DELETE"} - set(methods)
        # Treat HEAD specially. Any controller that supports GET
        # automatically supports HEAD. So we only assert that HEAD
        # fails if the method supports neither GET nor HEAD.
        if "GET" not in methods and "HEAD" not in methods:
            check.add("HEAD")
        for method in check:
            logging.debug("MethodNotAllowed should be raised on %s", method)
            pytest.raises(MethodNotAllowed, self.request, url, method)
            logging.debug("And it was.")


@pytest.fixture(scope="function")
def route_test(
    db: DatabaseTransactionFixture, controller_fixture: ControllerFixture
) -> Generator[RouteTestFixture, Any, None]:
    fix = RouteTestFixture(db, controller_fixture)
    yield fix
    fix.close()
