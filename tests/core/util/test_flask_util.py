"""Test functionality of util/flask_util.py."""

import datetime
import time
from wsgiref.handlers import format_date_time

import pytest
from flask import Response as FlaskResponse
from flask_pydantic_spec.flask_backend import Context
from flask_pydantic_spec.utils import parse_multi_dict

from core.util.datetime_helpers import utc_now
from core.util.flask_util import (
    OPDSEntryResponse,
    OPDSFeedResponse,
    Response,
    _snake_to_camel_case,
    boolean_value,
    str_comma_list_validator,
)
from core.util.opds_writer import OPDSFeed


class TestResponse:
    def test_constructor(self):
        response = Response(
            "content",
            401,
            dict(Header="value"),
            "mime/type",
            "content/type",
            True,
            1002,
        )
        assert 1002 == response.max_age
        assert isinstance(response, FlaskResponse)
        assert 401 == response.status_code
        assert "content" == str(response)
        assert True == response.direct_passthrough

        # Response.headers is tested in more detail below.
        headers = response.headers
        assert "value" == headers["Header"]
        assert "Cache-Control" in headers
        assert "Expires" in headers

    def test_headers(self):
        # First, test cases where the response should be private and
        # not cached. These are the kinds of settings used for error
        # messages.
        def assert_not_cached(max_age):
            headers = Response(max_age=max_age).headers
            assert "private, no-cache" == headers["Cache-Control"]
            assert "Authorization" == headers["Vary"]
            assert "Expires" not in headers

        assert_not_cached(max_age=None)
        assert_not_cached(max_age=0)
        assert_not_cached(max_age="Not a number")

        # Test the case where the response is public but should not be cached.
        headers = Response(max_age=0, private=False).headers
        assert "public, no-cache" == headers["Cache-Control"]
        assert "Vary" not in headers

        # Test the case where the response is private but may be
        # cached privately.
        headers = Response(max_age=300, private=True).headers
        assert "private, no-transform, max-age=300" == headers["Cache-Control"]
        assert "Authorization" == headers["Vary"]

        # Test the case where the response is public and may be cached,
        # including by intermediaries.
        max_age = 60 * 60 * 24 * 12
        obj = Response(max_age=max_age)

        headers = obj.headers
        cc = headers["Cache-Control"]
        assert cc == "public, no-transform, max-age=1036800, s-maxage=518400"

        # We expect the Expires header to look basically like this.
        expect_expires = utc_now() + datetime.timedelta(seconds=max_age)
        expect_expires_string = format_date_time(
            time.mktime(expect_expires.timetuple())
        )

        # We'll only check the date part of the Expires header, to
        # minimize the changes of spurious failures based on
        # unfortunate timing.
        expires = headers["Expires"]
        assert expires[:17] == expect_expires_string[:17]

        # It's possible to have a response that is private but should
        # be cached. The feed of a patron's current loans is a good
        # example.
        response = Response(max_age=30, private=True)
        cache_control = response.headers["Cache-Control"]
        assert "private" in cache_control
        assert "max-age=30" in cache_control
        assert "Authorization" == response.headers["Vary"]

    def test_unicode(self):
        # You can easily convert a Response object to Unicode
        # for use in a test.
        obj = Response("some data")
        assert "some data" == str(obj)


class TestOPDSFeedResponse:
    """Test the OPDS feed-specific specialization of Response."""

    def test_defaults(self):
        # OPDSFeedResponse provides reasonable defaults for
        # `mimetype` and `max_age`.
        c = OPDSFeedResponse

        use_defaults = c("a feed")
        assert OPDSFeed.ACQUISITION_FEED_TYPE == use_defaults.content_type
        assert OPDSFeed.DEFAULT_MAX_AGE == use_defaults.max_age

        # Flask Response.mimetype is the same as content_type but
        # with parameters removed.
        assert OPDSFeed.ATOM_TYPE == use_defaults.mimetype

        # These defaults can be overridden.
        override_defaults = c(
            "a feed", 200, dict(Header="value"), "mime/type", "content/type", True, 1002
        )
        assert 1002 == override_defaults.max_age

        # In Flask code, if mimetype and content_type conflict,
        # content_type takes precedence.
        assert "content/type" == override_defaults.content_type
        assert "content/type" == override_defaults.mimetype

        # A max_age of zero is retained, not replaced by the default.
        do_not_cache = c(max_age=0)
        assert 0 == do_not_cache.max_age


class TestOPDSEntryResponse:
    """Test the OPDS entry-specific specialization of Response."""

    def test_defaults(self):
        # OPDSEntryResponse provides a reasonable defaults for
        # `mimetype`.
        c = OPDSEntryResponse

        use_defaults = c("an entry")
        assert OPDSFeed.ENTRY_TYPE == use_defaults.content_type

        # Flask Response.mimetype is the same as content_type but
        # with parameters removed.
        assert OPDSFeed.ATOM_TYPE == use_defaults.mimetype

        # These defaults can be overridden.
        override_defaults = c("an entry", content_type="content/type")
        assert "content/type" == override_defaults.content_type
        assert "content/type" == override_defaults.mimetype


class TestMethods:
    @pytest.mark.parametrize(
        "value,result",
        [
            ("true", True),
            ("True", True),
            (True, True),
            ("1", True),
            ("false", False),
            ("False", False),
            ("0", False),
            ("t", False),
            (None, False),
        ],
    )
    def test_boolean_value(self, value, result):
        assert boolean_value(value) == result


def add_request_context(request, model, form=None) -> None:
    """Add a flask pydantic model into the request context
    :param model: The pydantic model
    :param form: A form multidict
    TODO:
    - query params
    - json post requests
    """
    body = None
    query = None
    if form is not None:
        request.form = form
        body = model.parse_obj(parse_multi_dict(form))

    request.context = Context(query, body, None, None)


def test_snake_to_camel_case():
    assert _snake_to_camel_case("a_snake_case_word") == "aSnakeCaseWord"  # liar
    assert _snake_to_camel_case("double__scores") == "doubleScores"
    assert _snake_to_camel_case("__magic") == "magic"
    assert (
        _snake_to_camel_case("SnakesAreInnocent_snokes_are_not")
        == "snakesareinnocentSnokesAreNot"
    )

    # Error case
    with pytest.raises(ValueError):
        _snake_to_camel_case("_")


def test_str_comma_list_validator():
    assert str_comma_list_validator(5) == ["5"]
    assert str_comma_list_validator(1.2) == ["1.2"]
    assert str_comma_list_validator("1,2,3") == ["1", "2", "3"]
    assert str_comma_list_validator("") == [""]

    # Unsupported types
    assert pytest.raises(TypeError, str_comma_list_validator, None)
    assert pytest.raises(TypeError, str_comma_list_validator, [])
