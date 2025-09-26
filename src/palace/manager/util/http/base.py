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


def status_code_matches(status_code: int, code_collection: ResponseCodesTypes) -> bool:
    """Check if a status code matches any value in a collection.

    :param status_code: The HTTP status code to check
    :param code_collection: Collection of status codes or series (e.g., "4xx")
    :return: True if the status code matches any value in the collection
    """
    code_str = str(status_code)
    series = get_series(status_code)
    collection_str = list(map(str, code_collection))

    return code_str in collection_str or series in collection_str


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
    if status_code_matches(response.status_code, allowed_response_codes):
        # The code or series has been explicitly allowed. Allow
        # the request to be processed.
        return response

    error_message = None
    series = get_series(response.status_code)

    if series == "5xx" or status_code_matches(
        response.status_code, disallowed_response_codes
    ):
        # Unless explicitly allowed, the 5xx series always results in an exception.
        error_message = BadResponseException.BAD_STATUS_CODE_MESSAGE
    elif allowed_response_codes and not status_code_matches(
        response.status_code, allowed_response_codes
    ):
        error_message = (
            "Got status code %%s from external server, but can only continue on: %s."
            % (", ".join(sorted(list(map(str, allowed_response_codes)))),)
        )

    if error_message:
        raise BadResponseException(
            str(url),
            error_message % response.status_code,
            debug_message=f"Response content: {response.text}",
            response=response,
        )
    return response
