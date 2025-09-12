from __future__ import annotations

from collections.abc import Collection
from typing import Literal, TypeVar

import httpx
import requests

from palace import manager
from palace.manager.util.http.exception import BadResponseException

# In case an app version is not present, we can use this version as a fallback
# for all outgoing http requests without a custom user-agent
DEFAULT_USER_AGENT_VERSION = "x.x.x"


def get_user_agent() -> str:
    """
    Generate a User-Agent string for HTTP requests.
    """

    version = manager.__version__ if manager.__version__ else DEFAULT_USER_AGENT_VERSION

    return f"Palace Manager/{version}"


def get_default_headers() -> dict[str, str]:
    """
    Generate default headers for HTTP requests.
    """
    return {
        "User-Agent": get_user_agent(),
    }


ResponseCodesStringLiterals = Literal["2xx", "3xx", "4xx", "5xx"]
ResponseCodesTypes = Collection[ResponseCodesStringLiterals | int]


def get_series(status_code: int) -> ResponseCodesStringLiterals:
    """Return the HTTP series for the given status code."""
    return f"{int(status_code) // 100}xx"  # type: ignore[return-value]


T = TypeVar("T", requests.Response, httpx.Response)


def raise_for_bad_response(
    url: str | httpx.URL,
    response: T,
    allowed_response_codes: ResponseCodesTypes,
    disallowed_response_codes: ResponseCodesTypes,
) -> T:
    """
    Raise a BadResponseException if the response code indicates a
    server-side failure, or behavior so unpredictable that we can't
    continue.

    :param allowed_response_codes If passed, then only the responses with
        http status codes in this list are processed.  The rest generate
        BadResponseExceptions. If both allowed_response_codes and
        disallowed_response_codes are passed, then the allowed_response_codes
        list is used.
    :param disallowed_response_codes The values passed are added to 5xx, as
        http status codes that would generate BadResponseExceptions.
    """
    allowed_response_codes_str = list(map(str, allowed_response_codes))
    disallowed_response_codes_str = list(map(str, disallowed_response_codes))

    series = get_series(response.status_code)
    code = str(response.status_code)

    if code in allowed_response_codes_str or series in allowed_response_codes_str:
        # The code or series has been explicitly allowed. Allow
        # the request to be processed.
        return response

    error_message = None
    if (
        series == "5xx"
        or code in disallowed_response_codes_str
        or series in disallowed_response_codes_str
    ):
        # Unless explicitly allowed, the 5xx series always results in an exception.
        error_message = BadResponseException.BAD_STATUS_CODE_MESSAGE
    elif allowed_response_codes_str and not (
        code in allowed_response_codes_str or series in allowed_response_codes_str
    ):
        error_message = (
            "Got status code %%s from external server, but can only continue on: %s."
            % (", ".join(sorted(allowed_response_codes_str)),)
        )

    if error_message:
        raise BadResponseException(
            str(url),
            error_message % code,
            debug_message=f"Response content: {response.text}",
            response=response,
        )
    return response
