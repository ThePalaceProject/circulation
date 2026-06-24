import json
import logging
from typing import Any

from requests import Request, Response

from palace.manager.sqlalchemy.model.resource import HttpResponseTuple


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
