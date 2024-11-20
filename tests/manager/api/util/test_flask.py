import pytest

from palace.manager.api.util.flask import get_request_var
from palace.manager.core.exceptions import PalaceValueError
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
