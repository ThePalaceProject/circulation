"""Utilities for Flask applications."""

import datetime
import json
import time
from json import JSONDecodeError
from typing import Any
from wsgiref.handlers import format_date_time

from flask import Response as FlaskResponse
from lxml import etree
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel
from werkzeug.datastructures import MultiDict

from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.opds_writer import OPDSFeed


class Response(FlaskResponse):
    """A Flask Response object with some conveniences added.

    The conveniences:

       * It's easy to calculate header values such as Cache-Control.
       * A response can be easily converted into a string for use in
         tests.
    """

    def __init__(
        self,
        response: Any = None,
        status: int | None = None,
        headers: dict[str, Any] | None = None,
        mimetype: str | None = None,
        content_type: str | None = None,
        direct_passthrough: bool = False,
        max_age: int | str = 0,
        private: bool | None = None,
    ) -> None:
        """Constructor.

        All parameters are the same as for the Flask/Werkzeug Response class,
        with these additions:

        :param max_age: The number of seconds for which clients should
            cache this response. Used to set a value for the
            Cache-Control header.
        :param private: If this is True, then the response contains
            information from an authenticated client and should not be stored
            in intermediate caches.
        """
        max_age = max_age or 0
        try:
            max_age = int(max_age)
        except ValueError:
            max_age = 0
        self.max_age = max_age
        if private is None:
            if self.max_age == 0:
                # The most common reason for max_age to be set to 0 is that a resource
                # is _also_ private.
                private = True
            else:
                private = False
        self.private = private

        body = response
        if isinstance(body, etree._Element):
            body = etree.tostring(body)
        elif not isinstance(body, (bytes, str)):
            body = str(body)

        super().__init__(
            response=body,
            status=status,
            headers=self._headers(headers or {}),
            mimetype=mimetype,
            content_type=content_type,
            direct_passthrough=direct_passthrough,
        )

    def __str__(self) -> str:
        """This object can be treated as a string, e.g. in tests.

        :return: The entity-body portion of the response.
        """
        return self.get_data(as_text=True)

    def _headers(self, headers: dict[str, Any] | None = None) -> dict[str, str]:
        """Build an appropriate set of HTTP response headers."""
        if headers is None:
            headers = {}
        # Don't modify the underlying dictionary; it came from somewhere else.
        headers = dict(headers)

        # Set headers based on privacy settings and maximum age.
        if self.private:
            private = "private"

            # A private resource should be re-requested, rather than
            # retrieved from cache, if the authorization credentials
            # change from those originally used to retrieve it.
            headers["Vary"] = "Authorization"
        else:
            private = "public"
        if self.max_age and isinstance(self.max_age, int):
            client_cache = self.max_age
            if self.private:
                # A private resource should not be cached by
                # intermediaries at all.
                s_maxage = ""
            else:
                # A public resource can be cached by intermediaries
                # for half as long as the end-user can cache it.
                s_maxage = ", s-maxage=%d" % (self.max_age / 2)
            cache_control = "%s, no-transform, max-age=%d%s" % (
                private,
                client_cache,
                s_maxage,
            )

            # Explicitly set Expires based on max-age; some clients need this.
            expires_at = utc_now() + datetime.timedelta(seconds=self.max_age)
            headers["Expires"] = format_date_time(time.mktime(expires_at.timetuple()))
        else:
            # Missing, invalid or zero max-age means don't cache at all.
            cache_control = "%s, no-cache" % private
        headers["Cache-Control"] = cache_control

        return headers


class OPDSFeedResponse(Response):
    """A convenience specialization of Response for typical OPDS feeds."""

    def __init__(
        self,
        response: Any = None,
        status: int | None = None,
        headers: dict[str, Any] | None = None,
        mimetype: str | None = None,
        content_type: str | None = None,
        direct_passthrough: bool = False,
        max_age: int | str | None = None,
        private: bool | None = None,
    ) -> None:
        mimetype = mimetype or OPDSFeed.ACQUISITION_FEED_TYPE
        status = status or 200
        if max_age is None:
            max_age = OPDSFeed.DEFAULT_MAX_AGE
        super().__init__(
            response=response,
            status=status,
            headers=headers,
            mimetype=mimetype,
            content_type=content_type,
            direct_passthrough=direct_passthrough,
            max_age=max_age,
            private=private,
        )


class OPDSEntryResponse(Response):
    """A convenience specialization of Response for typical OPDS entries."""

    def __init__(self, response: Any = None, **kwargs: Any) -> None:
        kwargs.setdefault("mimetype", OPDSFeed.ENTRY_TYPE)
        super().__init__(response, **kwargs)


def boolean_value(value: str) -> bool:
    """Convert a string request value into a boolean, used for form encoded requests
    JSON encoded requests will get automatically converted"""
    return True if value in ["true", "True", True, "1"] else False


class CustomBaseModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="forbid",
        coerce_numbers_to_str=True,
    )

    def api_dict(self, **kwargs: Any) -> dict[str, Any]:
        """Return the instance in a form suitable for a web response.

        By default, the properties use their lower camel case aliases,
        rather than their Python class member names.
        """
        kwargs.setdefault("by_alias", True)
        return self.model_dump(**kwargs)


def str_comma_list_validator(value: int | float | str) -> list[str]:
    """Validate a comma separated string and parse it into a list, generally used for query parameters"""
    if isinstance(value, (int, float)):
        # A single number shows up as an int
        value = str(value)
    elif not isinstance(value, str):
        raise TypeError("string required")

    return value.split(",")


# This code is borrowed from `flask-pydantic-spec
# - https://github.com/turner-townsend/flask-pydantic-spec/blob/2d29e45f428b7e7bee60c1bc3657e95ee1f3a866/flask_pydantic_spec/utils.py#L200-L211
def parse_multi_dict(input: MultiDict[str, Any]) -> dict[str, Any]:
    result = {}
    for key, value in input.to_dict(flat=False).items():
        if len(value) == 1:
            try:
                value_to_use = json.loads(value[0])
            except (TypeError, JSONDecodeError):
                value_to_use = value[0]
        else:
            value_to_use = value
        result[key] = value_to_use
    return result
