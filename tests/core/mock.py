import json
import logging

from core.coverage import (
    BibliographicCoverageProvider,
    CollectionCoverageProvider,
    IdentifierCoverageProvider,
    WorkCoverageProvider,
)
from core.model import DataSource, ExternalIntegration


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
    PROTOCOL: str | None = ExternalIntegration.OPDS_IMPORT


class InstrumentedCoverageProvider(MockCoverageProvider, IdentifierCoverageProvider):
    """A CoverageProvider that keeps track of every item it tried
    to cover.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.attempts = []

    def process_item(self, item):
        self.attempts.append(item)
        return item


class InstrumentedWorkCoverageProvider(MockCoverageProvider, WorkCoverageProvider):
    """A WorkCoverageProvider that keeps track of every item it tried
    to cover.
    """

    def __init__(self, _db, *args, **kwargs):
        super().__init__(_db, *args, **kwargs)
        self.attempts = []

    def process_item(self, item):
        self.attempts.append(item)
        return item


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


class AlwaysSuccessfulWorkCoverageProvider(InstrumentedWorkCoverageProvider):
    """A WorkCoverageProvider that does nothing and always succeeds."""

    SERVICE_NAME = "Always successful (works)"


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


class NeverSuccessfulWorkCoverageProvider(InstrumentedWorkCoverageProvider):
    SERVICE_NAME = "Never successful (works)"

    def process_item(self, item):
        self.attempts.append(item)
        return self.failure(item, "What did you expect?", False)


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


class TransientFailureWorkCoverageProvider(InstrumentedWorkCoverageProvider):
    SERVICE_NAME = "Never successful (transient, works)"

    def process_item(self, item):
        self.attempts.append(item)
        return self.failure(item, "Oops!", True)


class TaskIgnoringCoverageProvider(InstrumentedCoverageProvider):
    """A coverage provider that ignores all work given to it."""

    SERVICE_NAME = "I ignore all work."

    def process_batch(self, batch):
        return []


class DummyHTTPClient:
    def __init__(self):
        self.responses = []
        self.requests = []

    def queue_response(
        self, response_code, media_type="text/html", other_headers=None, content=""
    ):
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

    def queue_requests_response(
        self, response_code, media_type="text/html", other_headers=None, content=""
    ):
        """Queue a response of the type produced by HTTP.get_with_timeout."""
        headers = dict(other_headers or {})
        if media_type:
            headers["Content-Type"] = media_type
        response = MockRequestsResponse(response_code, headers, content)
        self.responses.append(response)

    def do_get(self, url, *args, **kwargs):
        self.requests.append(url)
        return self.responses.pop(0)

    def do_post(self, url, data, *wargs, **kwargs):
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


class MockRequestsResponse:
    """A mock object that simulates an HTTP response from the
    `requests` library.
    """

    def __init__(self, status_code, headers={}, content=None, url=None, request=None):
        self.status_code = status_code
        self.headers = headers
        # We want to enforce that the mocked content is a bytestring
        # just like a real response.
        if content and isinstance(content, str):
            self.content = content.encode("utf-8")
        else:
            self.content = content
        if request and not url:
            url = request.url
        self.url = url or "http://url/"
        self.encoding = "utf-8"
        self.request = request

    def json(self):
        content = self.content
        # The queued content might be a JSON string or it might
        # just be the object you'd get from loading a JSON string.
        if isinstance(content, (str, bytes)):
            content = json.loads(self.content)
        return content

    @property
    def text(self):
        if isinstance(self.content, bytes):
            return self.content.decode("utf8")
        return self.content

    def raise_for_status(self):
        """Null implementation of raise_for_status, a method
        implemented by real requests Response objects.
        """
