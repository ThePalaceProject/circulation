from __future__ import annotations

import json
import logging
import sys
from functools import partial
from unittest.mock import MagicMock, patch

import pytest
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


class TestJSONFormatter:
    @freeze_time("1990-05-05")
    def test_format(self) -> None:
        formatter = JSONFormatter()

        exc_info = None
        # Cause an exception so we can capture its exc_info()
        try:
            raise ValueError("fake exception")
        except ValueError as e:
            exc_info = sys.exc_info()

        record = logging.LogRecord(
            "some logger",
            logging.DEBUG,
            "pathname",
            104,
            "A message",
            {},
            exc_info,
            None,
        )
        data = json.loads(formatter.format(record))
        assert "some logger" == data["name"]
        assert "1990-05-05T00:00:00+00:00" == data["timestamp"]
        assert "DEBUG" == data["level"]
        assert "A message" == data["message"]
        assert "pathname" == data["filename"]
        assert "ValueError: fake exception" in data["traceback"]

    @pytest.mark.parametrize(
        "msg, args",
        [
            ("An important snowman: %s", "☃"),
            ("An important snowman: %s", "☃".encode()),
            (b"An important snowman: %s", "☃"),
            (b"An important snowman: %s", "☃".encode()),
        ],
    )
    def test_format_with_different_types_of_strings(
        self, msg: str | bytes, args: str | bytes
    ) -> None:
        # As long as all data is either Unicode or UTF-8, any combination
        # of Unicode and bytestrings can be combined in log messages.
        formatter = JSONFormatter()
        record = logging.LogRecord(
            "some logger", logging.DEBUG, "pathname", 104, msg, (args,), None, None
        )
        data = json.loads(formatter.format(record))
        # The resulting data is always a Unicode string.
        assert "An important snowman: ☃" == data["message"]


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
