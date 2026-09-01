from collections.abc import Callable

import pytest

from palace.util.exceptions import PalaceValueError

from palace.manager.api.util.flask import PalaceFlask, get_request_var
from tests.fixtures.flask import FlaskAppFixture


class TestGetRequestVar:
    def test_no_request_context(self) -> None:
        # If we supply a default, we get the default if there is no request context.
        assert get_request_var("foo", str, default="bar") == "bar"

        # If we don't supply a default, we get the normal RuntimeError.
        with pytest.raises(RuntimeError, match="Working outside of request context"):
            get_request_var("foo", str)

    def test_no_var_set(self, flask_app_fixture: FlaskAppFixture) -> None:
        with flask_app_fixture.test_request_context():
            assert get_request_var("foo", str, default=None) is None

            with pytest.raises(
                PalaceValueError, match="No 'foo' set on 'flask.request'"
            ):
                get_request_var("foo", str)

    def test_var_set_to_wrong_type(self, flask_app_fixture: FlaskAppFixture) -> None:
        with flask_app_fixture.test_request_context() as ctx:
            setattr(ctx.request, "foo", 123)

            assert get_request_var("foo", str, default=None) is None

            with pytest.raises(
                PalaceValueError, match="incorrect type 'int' expected 'str'"
            ):
                get_request_var("foo", str)


class TestPalaceFlask:
    # add_url_rule reads the marker attribute that allows_public_cors
    # stamps on its wrapper, so a bare function with the marker is enough
    # to exercise the check without importing the routes module.
    @staticmethod
    def _marked_view() -> Callable[[], str]:
        def view() -> str:
            return "view"

        setattr(view, "allows_public_cors", True)
        return view

    @pytest.mark.parametrize(
        "methods",
        [
            pytest.param(None, id="default-get"),
            pytest.param(["GET"], id="get"),
            pytest.param(["GET", "HEAD", "OPTIONS"], id="all-read-methods"),
        ],
    )
    def test_add_url_rule_accepts_read_only_public_cors(
        self, methods: list[str] | None
    ) -> None:
        app = PalaceFlask(__name__)
        app.add_url_rule("/read", view_func=self._marked_view(), methods=methods)

    @pytest.mark.parametrize(
        "methods",
        [
            pytest.param(["GET", "POST"], id="post"),
            pytest.param(["PUT"], id="put"),
            pytest.param(["get", "delete"], id="lowercase-delete"),
        ],
    )
    def test_add_url_rule_rejects_write_methods_with_public_cors(
        self, methods: list[str]
    ) -> None:
        app = PalaceFlask(__name__)
        with pytest.raises(PalaceValueError, match="must not use allows_public_cors"):
            app.add_url_rule("/write", view_func=self._marked_view(), methods=methods)

    def test_add_url_rule_ignores_unmarked_views(self) -> None:
        app = PalaceFlask(__name__)

        def view() -> str:
            return "view"

        app.add_url_rule("/write", view_func=view, methods=["GET", "POST"])

    def test_add_url_rule_string_methods_raise_flask_type_error(self) -> None:
        # A bare string is invalid; the check defers to Flask's clear error
        # instead of raising a confusing per-character PalaceValueError.
        app = PalaceFlask(__name__)
        with pytest.raises(TypeError):
            app.add_url_rule("/write", view_func=self._marked_view(), methods="POST")

    def test_add_url_rule_empty_methods_pass_through(self) -> None:
        # An explicit empty list contains no unsafe methods, so the check
        # lets Flask decide what to do with it.
        app = PalaceFlask(__name__)
        app.add_url_rule("/read", view_func=self._marked_view(), methods=[])
