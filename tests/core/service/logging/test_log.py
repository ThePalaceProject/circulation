from __future__ import annotations

import functools
import json
import logging
import sys
from collections.abc import Callable, Mapping
from functools import partial
from unittest.mock import MagicMock, patch

import pytest
from _pytest.monkeypatch import MonkeyPatch
from freezegun import freeze_time
from watchtower import CloudWatchLogHandler

from core.service.logging.configuration import LogLevel
from core.service.logging.log import (
    JSONFormatter,
    LogLoopPreventionFilter,
    create_cloudwatch_handler,
    create_stream_handler,
    setup_logging,
)
from tests.fixtures.flask import FlaskAppFixture


class TestJSONFormatter:
    LogRecordCallable = Callable[..., logging.LogRecord]

    @pytest.fixture()
    def log_record(self) -> LogRecordCallable:
        return functools.partial(
            logging.LogRecord,
            name="some logger",
            level=logging.DEBUG,
            pathname="pathname",
            lineno=104,
            msg="A message",
            args={},
            exc_info=None,
            func=None,
        )

    @freeze_time("1990-05-05")
    def test_format_exception(self, log_record: LogRecordCallable) -> None:
        formatter = JSONFormatter()

        exc_info = None
        # Cause an exception so we can capture its exc_info()
        try:
            raise ValueError("fake exception")
        except ValueError as e:
            exc_info = sys.exc_info()

        record = log_record(exc_info=exc_info)
        data = json.loads(formatter.format(record))
        assert data["name"] == "some logger"
        assert data["timestamp"] == "1990-05-05T00:00:00+00:00"
        assert data["level"] == "DEBUG"
        assert data["message"] == "A message"
        assert data["filename"] == "pathname"
        assert "ValueError: fake exception" in data["traceback"]

    @pytest.mark.parametrize(
        "msg, args, expected",
        [
            ("An important snowman: %s", ("☃",), "An important snowman: ☃"),
            ("An important snowman: %s", ("☃".encode(),), "An important snowman: ☃"),
            (b"An important snowman: %s", ("☃",), "An important snowman: ☃"),
            (
                b"An important snowman: %s",
                [
                    "☃".encode(),
                ],
                "An important snowman: ☃",
            ),
            (
                "abc %(test1)s %(test2)s",
                {"test1": "🚀".encode(), "test2": "🪄"},
                "abc 🚀 🪄",
            ),
            (
                b"cba %(test1)s %(test2)s",
                {b"test1": "🎸", "test2": "🦃".encode()},
                "cba 🎸 🦃",
            ),
            (
                b"Not a string: %s %s %s %s",
                ({}, [], "c", b"d"),
                "Not a string: {} [] c d",
            ),
            (
                "Foo Bar Baz",
                ("a", "b", "c"),
                "Log message could not be formatted. Exception: TypeError('not all arguments "
                "converted during string formatting'). Original message: message='Foo Bar Baz'"
                " args=('a', 'b', 'c')",
            ),
            (
                "Another test %s",
                MagicMock(),
                "Another test %s",
            ),
        ],
    )
    def test_format_args(
        self,
        msg: str | bytes,
        args: tuple[str | bytes, ...] | Mapping[str | bytes, str | bytes],
        expected: str,
        log_record: LogRecordCallable,
    ) -> None:
        # As long as all data is either Unicode or UTF-8, any combination
        # of Unicode and bytestrings can be combined in log messages.
        formatter = JSONFormatter()
        record = log_record(msg=msg, args=args)
        data = json.loads(formatter.format(record))
        # The resulting data is always a Unicode string.
        assert data["message"] == expected

    def test_flask_request(
        self,
        log_record: LogRecordCallable,
        flask_app_fixture: FlaskAppFixture,
        monkeypatch: MonkeyPatch,
    ) -> None:
        # Outside a Flask request context, the request data is not included in the log.
        formatter = JSONFormatter()
        record = log_record()
        data = json.loads(formatter.format(record))
        assert "request" not in data

        # Inside a Flask request context, the request data is included in the log.
        with flask_app_fixture.test_request_context("/"):
            data = json.loads(formatter.format(record))
            assert "request" in data
            request = data["request"]
            assert request["path"] == "/"
            assert request["method"] == "GET"
            assert "host" in request
            assert "query" not in request

        with flask_app_fixture.test_request_context(
            "/test?query=string&foo=bar", method="POST"
        ):
            data = json.loads(formatter.format(record))
            assert "request" in data
            request = data["request"]
            assert request["path"] == "/test"
            assert request["method"] == "POST"
            assert request["query"] == "query=string&foo=bar"

        # If flask is not installed, the request data is not included in the log.
        monkeypatch.delitem(sys.modules, "flask", raising=False)
        data = json.loads(formatter.format(record))
        assert "request" not in data

    def test_uwsgi_worker(
        self, log_record: LogRecordCallable, monkeypatch: MonkeyPatch
    ) -> None:
        # Outside a uwsgi context, the worker id is not included in the log.
        formatter = JSONFormatter()
        record = log_record()
        data = json.loads(formatter.format(record))
        assert "uwsgi" not in data

        # Inside a uwsgi context, the worker id is included in the log.
        mock_uwsgi = MagicMock()
        monkeypatch.setitem(sys.modules, "uwsgi", mock_uwsgi)
        mock_uwsgi.worker_id.return_value = 42

        data = json.loads(formatter.format(record))
        assert "uwsgi" in data
        assert data["uwsgi"]["worker"] == 42


class TestLogLoopPreventionFilter:
    @pytest.mark.parametrize(
        "name, expected",
        [
            ("requests.request", True),
            ("palace.app", True),
            ("palace.app.submodule", True),
            ("botocore", False),
            ("urllib3.connectionpool", False),
        ],
    )
    def test_filter(self, name: str, expected: bool) -> None:
        filter = LogLoopPreventionFilter()
        record = logging.LogRecord(
            name, logging.DEBUG, "pathname", 104, "A message", {}, None, None
        )
        assert expected == filter.filter(record)


def test_create_cloudwatch_handler() -> None:
    mock_formatter = MagicMock()
    mock_client = MagicMock()

    handler = create_cloudwatch_handler(
        formatter=mock_formatter,
        level=LogLevel.info,
        client=mock_client,
        group="test_group",
        stream="test_stream",
        interval=13,
        create_group=True,
    )

    assert isinstance(handler, CloudWatchLogHandler)
    assert handler.log_group_name == "test_group"
    assert handler.log_stream_name == "test_stream"
    assert handler.send_interval == 13
    assert any(isinstance(f, LogLoopPreventionFilter) for f in handler.filters)
    assert handler.formatter == mock_formatter
    assert handler.level == logging.INFO


def test_create_stream_handler() -> None:
    mock_formatter = MagicMock()

    handler = create_stream_handler(formatter=mock_formatter, level=LogLevel.debug)

    assert isinstance(handler, logging.StreamHandler)
    assert not any(isinstance(f, LogLoopPreventionFilter) for f in handler.filters)
    assert handler.formatter == mock_formatter
    assert handler.level == logging.DEBUG


def test_setup_logging_cloudwatch_disabled() -> None:
    # If cloudwatch is disabled, no cloudwatch handler is created.
    mock_cloudwatch_callable = MagicMock()
    mock_stream_handler = MagicMock()

    setup = partial(
        setup_logging,
        level=LogLevel.info,
        verbose_level=LogLevel.warning,
        stream=mock_stream_handler,
        cloudwatch_callable=mock_cloudwatch_callable,
    )

    # We patch logging so that we don't actually modify the global logging
    # configuration.
    with patch("core.service.logging.log.logging"):
        setup(cloudwatch_enabled=False)
        assert mock_cloudwatch_callable.call_count == 0

        setup(cloudwatch_enabled=True)
        assert mock_cloudwatch_callable.call_count == 1
