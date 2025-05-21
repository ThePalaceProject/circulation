import json
import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import patch

from requests import Request, Response
from typing_extensions import Unpack

from palace.manager.core.coverage import (
    BibliographicCoverageProvider,
    CollectionCoverageProvider,
    IdentifierCoverageProvider,
)
from palace.manager.core.opds_import import OPDSAPI
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.resource import HttpResponseTuple
from palace.manager.util.http import HTTP, GetRequestKwargs, RequestKwargs


def _normalize_level(level):
    return level.lower()


class LogCaptureHandler(logging.Handler):
    """A `logging.Handler` context manager that captures the messages
    of emitted log records in the context of the specified `logger`.
    """

    _level_names = logging._levelToName.values()

    LEVEL_NAMES = list(map(_normalize_level, _level_names))

    def __init__(self, logger, *args, **kwargs):
        """Constructor.

        :param logger: `logger` to which this handler will be added.
        :param args: positional arguments to `logging.Handler.__init__`.
        :param kwargs: keyword arguments to `logging.Handler.__init__`.
        """
        self.logger = logger
        self._records = {}
        logging.Handler.__init__(self, *args, **kwargs)

    def __enter__(self):
        self.reset()
        self.logger.addHandler(self)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.logger.removeHandler(self)

    def emit(self, record):
        level = _normalize_level(record.levelname)
        if level not in self.LEVEL_NAMES:
            message = "Unexpected log level: '%s'." % record.levelname
            raise ValueError(message)
        self._records[level].append(record.getMessage())

    def reset(self):
        """Empty the message accumulators."""
        self._records = {level: [] for level in self.LEVEL_NAMES}

    def __getitem__(self, item):
        if item in self.LEVEL_NAMES:
            return self._records[item]
        else:
            message = "'{}' object has no attribute '{}'".format(
                self.__class__.__name__,
                item,
            )
            raise AttributeError(message)

    def __getattr__(self, item):
        return self.__getitem__(item)


class MockCoverageProvider:
    """Mixin class for mock CoverageProviders that defines common constants."""

    SERVICE_NAME: str | None = "Generic mock CoverageProvider"

    # Whenever a CoverageRecord is created, the data_source of that
    # record will be Project Gutenberg.
    DATA_SOURCE_NAME = DataSource.GUTENBERG

    # For testing purposes, this CoverageProvider will try to cover
    # every identifier in the database.
    INPUT_IDENTIFIER_TYPES: None | str | object = None

    # This CoverageProvider can work with any Collection that supports
    # the OPDS import protocol (e.g. DatabaseTest._default_collection).
    PROTOCOL: str | None = OPDSAPI.label()


class InstrumentedCoverageProvider(MockCoverageProvider, IdentifierCoverageProvider):
    """A CoverageProvider that keeps track of every item it tried
    to cover.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.attempts = []
        self.finalize_batch_called = False

    def process_item(self, item):
        self.attempts.append(item)
        return item

    def finalize_batch(self):
        self.finalize_batch_called = True


class AlwaysSuccessfulCollectionCoverageProvider(
    MockCoverageProvider, CollectionCoverageProvider
):
    """A CollectionCoverageProvider that does nothing and always succeeds."""

    SERVICE_NAME = "Always successful (collection)"

    def process_item(self, item):
        return item


class AlwaysSuccessfulCoverageProvider(InstrumentedCoverageProvider):
    """A CoverageProvider that does nothing and always succeeds."""

    SERVICE_NAME = "Always successful"


class AlwaysSuccessfulBibliographicCoverageProvider(
    MockCoverageProvider, BibliographicCoverageProvider
):
    """A BibliographicCoverageProvider that does nothing and is always
    successful.

    Note that this only works if you've put a working Edition and
    LicensePool in place beforehand. Otherwise the process will fail
    during handle_success().
    """

    SERVICE_NAME = "Always successful (bibliographic)"

    def process_item(self, identifier):
        return identifier


class NeverSuccessfulCoverageProvider(InstrumentedCoverageProvider):
    """A CoverageProvider that does nothing and always fails."""

    SERVICE_NAME = "Never successful"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.transient = kwargs.get("transient") or False

    def process_item(self, item):
        self.attempts.append(item)
        return self.failure(item, "What did you expect?", self.transient)


class NeverSuccessfulBibliographicCoverageProvider(
    MockCoverageProvider, BibliographicCoverageProvider
):
    """Simulates a BibliographicCoverageProvider that's never successful."""

    SERVICE_NAME = "Never successful (bibliographic)"

    def process_item(self, identifier):
        return self.failure(identifier, "Bitter failure", transient=True)


class TransientFailureCoverageProvider(InstrumentedCoverageProvider):
    SERVICE_NAME = "Never successful (transient)"

    def process_item(self, item):
        self.attempts.append(item)
        return self.failure(item, "Oops!", True)


class TaskIgnoringCoverageProvider(InstrumentedCoverageProvider):
    """A coverage provider that ignores all work given to it."""

    SERVICE_NAME = "I ignore all work."

    def process_batch(self, batch):
        return []


class MockHTTPClient:
    def __init__(self) -> None:
        self.responses: list[Response] = []
        self.requests: list[str] = []
        self.requests_args: list[RequestKwargs] = []
        self.requests_methods: list[str] = []

    def reset_mock(self) -> None:
        self.responses = []
        self.requests = []
        self.requests_args = []
        self.requests_methods = []

    def queue_response(
        self,
        response_code: int,
        media_type: str | None = None,
        other_headers: dict[str, str] | None = None,
        content: str | bytes | dict[str, Any] = "",
    ):
        """Queue a response of the type produced by HTTP.get_with_timeout."""
        headers = dict(other_headers or {})
        if media_type:
            headers["Content-Type"] = media_type

        self.responses.append(MockRequestsResponse(response_code, headers, content))

    def _request(self, *args: Any, **kwargs: Any) -> Response:
        return self.responses.pop(0)

    def do_request(
        self, http_method: str, url: str, **kwargs: Unpack[RequestKwargs]
    ) -> Response:
        self.requests.append(url)
        self.requests_methods.append(http_method)
        self.requests_args.append(kwargs)
        return HTTP._request_with_timeout(http_method, url, self._request, **kwargs)

    def do_get(self, url: str, **kwargs: Unpack[GetRequestKwargs]) -> Response:
        return self.do_request("GET", url, **kwargs)

    @contextmanager
    def patch(self) -> Generator[None, None, None]:
        with patch.object(HTTP, "request_with_timeout", self.do_request):
            yield


class MockRepresentationHTTPClient:
    def __init__(self) -> None:
        self.responses: list[HttpResponseTuple] = []
        self.requests: list[str | tuple[str, str]] = []

    def queue_response(
        self,
        response_code: int,
        media_type: str | None = "text/html",
        other_headers: dict[str, str] | None = None,
        content: str | bytes = "",
    ) -> None:
        """Queue a response of the type produced by
        Representation.simple_http_get.
        """
        headers = {}
        # We want to enforce that the mocked content is a bytestring
        # just like a real response.
        if not isinstance(content, bytes):
            content = content.encode("utf-8")
        if media_type:
            headers["content-type"] = media_type
        if other_headers:
            for k, v in list(other_headers.items()):
                headers[k.lower()] = v
        self.responses.append((response_code, headers, content))

    def do_get(self, url: str, *args: Any, **kwargs: Any) -> HttpResponseTuple:
        self.requests.append(url)
        return self.responses.pop(0)

    def do_post(
        self, url: str, data: str, *wargs: Any, **kwargs: Any
    ) -> HttpResponseTuple:
        self.requests.append((url, data))
        return self.responses.pop(0)


class MockRequestsRequest:
    """A mock object that simulates an HTTP request from the
    `requests` library.
    """

    def __init__(self, url, method="GET", headers=None):
        self.url = url
        self.method = method
        self.headers = headers or dict()


class MockRequestsResponse(Response):
    """A mock object that simulates an HTTP response from the
    `requests` library.
    """

    def __init__(
        self,
        status_code: int,
        headers: dict[str, str] | None = None,
        content: Any = None,
        url: str | None = None,
        request: Request | None = None,
    ):
        super().__init__()

        self.status_code = status_code
        if headers is not None:
            for k, v in headers.items():
                self.headers[k] = v

        # We want to enforce that the mocked content is a bytestring
        # just like a real response.
        if content is not None:
            if isinstance(content, str):
                content_bytes = content.encode("utf-8")
            elif isinstance(content, bytes):
                content_bytes = content
            else:
                content_bytes = json.dumps(content).encode("utf-8")
            self._content = content_bytes

        if request and not url:
            url = request.url
        self.url = url or "http://url/"
        self.encoding = "utf-8"
        if request:
            self.request = request.prepare()
