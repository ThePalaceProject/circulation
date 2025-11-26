from __future__ import annotations

import functools
import json
import logging
import os
import sys
from collections.abc import Callable, Mapping
from functools import partial
from unittest.mock import MagicMock, PropertyMock, create_autospec, patch

import pytest
from celery import shared_task
from freezegun import freeze_time
from sqlalchemy.exc import SQLAlchemyError
from watchtower import CloudWatchLogHandler

from palace.manager.celery.task import Task
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.service.logging.log import (
    JSONFormatter,
    LogLoopPreventionFilter,
    create_cloudwatch_handler,
    create_stream_handler,
    setup_logging,
)
from palace.manager.sqlalchemy.model.admin import Admin
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.util import get_one_or_create
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
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
    def test_format(self, log_record: LogRecordCallable):
        formatter = JSONFormatter()
        record = log_record()
        data = json.loads(formatter.format(record))
        assert "host" in data
        assert data["name"] == "some logger"
        assert data["timestamp"] == "1990-05-05T00:00:00+00:00"
        assert data["level"] == "DEBUG"
        assert data["message"] == "A message"
        assert data["filename"] == "pathname"
        assert "traceback" not in data
        assert data["process"] == os.getpid()

        # If the record has no process, the process field is not included in the log.
        record = log_record()
        record.process = None
        data = json.loads(formatter.format(record))
        assert "process" not in data

    def test_format_thread(self, log_record: LogRecordCallable) -> None:
        formatter = JSONFormatter()
        record = log_record()

        # Since we are in the main thread, the thread field is not included in the log.
        data = json.loads(formatter.format(record))
        assert "thread" not in data

        # If the thread is None we also don't include it in the log.
        record.thread = None
        data = json.loads(formatter.format(record))
        assert "thread" not in data

        # But if we are not in the main thread, the thread field is included in the log.
        record.thread = 12
        data = json.loads(formatter.format(record))
        assert data["thread"] == 12

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
        assert "traceback" in data
        assert "ValueError: fake exception" in data["traceback"]

    def test_format_stack_info(self, log_record: LogRecordCallable) -> None:
        formatter = JSONFormatter()
        record = log_record(sinfo="some info")
        data = json.loads(formatter.format(record))
        assert data["stack"] == "some info"

        # if no stack info is provided, the stack field is not included in the log.
        record = log_record()
        data = json.loads(formatter.format(record))
        assert "stack" not in data

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
            assert "user_agent" not in request
            assert "forwarded_for" not in request

        with flask_app_fixture.test_request_context(
            "/test?query=string&foo=bar",
            method="POST",
            headers=[
                ("User-Agent", "UA"),
                ("X-Forwarded-For", "xyz, abc"),
                ("X-Forwarded-For", "123"),
            ],
            environ_base={"REMOTE_ADDR": "456"},
        ):
            data = json.loads(formatter.format(record))
            assert "request" in data
            request = data["request"]
            assert request["path"] == "/test"
            assert request["method"] == "POST"
            assert request["query"] == "query=string&foo=bar"
            assert request["user_agent"] == "UA"
            assert request["forwarded_for"] == ["xyz", "abc", "123", "456"]

        # If flask is not installed, the request data is not included in the log.
        with patch("palace.manager.service.logging.log.flask_request", None):
            data = json.loads(formatter.format(record))
            assert "request" not in data

    def test_flask_request_palace_data(
        self,
        log_record: LogRecordCallable,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ) -> None:
        # Outside a Flask request context, the request data is not included in the log.
        formatter = JSONFormatter()
        record = log_record()

        # No palace data included in the log
        with flask_app_fixture.test_request_context():
            data = json.loads(formatter.format(record))
        assert "request" in data
        request = data["request"]
        assert "library" not in request
        assert "patron" not in request
        assert "admin" not in request

        # Library request data
        library = db.library(short_name="Test", name="Official Library Of Test")
        with flask_app_fixture.test_request_context(library=library):
            data = json.loads(formatter.format(record))
        assert "request" in data
        request = data["request"]
        assert "library" in request
        assert request["library"]["uuid"] == str(library.uuid)
        assert request["library"]["name"] == "Official Library Of Test"
        assert request["library"]["short_name"] == "Test"

        # Patron data - all information included
        patron = db.patron()
        patron.external_identifier = "external_identifier"
        patron.authorization_identifier = "authorization_identifier"
        patron.username = "username"
        with flask_app_fixture.test_request_context(patron=patron):
            data = json.loads(formatter.format(record))
        assert "request" in data
        request = data["request"]
        assert "patron" in request
        assert (
            request["patron"]["authorization_identifier"] == "authorization_identifier"
        )
        assert request["patron"]["external_identifier"] == "external_identifier"
        assert request["patron"]["username"] == "username"

        # Patron data - missing username and external_identifier
        patron.external_identifier = None
        patron.username = None
        with flask_app_fixture.test_request_context(patron=patron):
            data = json.loads(formatter.format(record))
        assert "request" in data
        request = data["request"]
        assert "patron" in request
        assert (
            request["patron"]["authorization_identifier"] == "authorization_identifier"
        )
        assert "external_identifier" not in request["patron"]
        assert "username" not in request["patron"]

        # Patron data - No information to include
        patron.authorization_identifier = None
        with flask_app_fixture.test_request_context(patron=patron):
            data = json.loads(formatter.format(record))
        assert "request" in data
        request = data["request"]
        assert "patron" not in request

        # Admin data
        admin, _ = get_one_or_create(db.session, Admin, email="test@email.com")
        with flask_app_fixture.test_request_context(admin=admin):
            data = json.loads(formatter.format(record))
        assert "request" in data
        request = data["request"]
        assert "admin" in request
        assert request["admin"] == "test@email.com"

        # Database session in a bad state, no data included in log
        library = create_autospec(Library)
        type(library).uuid = PropertyMock(side_effect=SQLAlchemyError())
        with flask_app_fixture.test_request_context(library=library):
            data = json.loads(formatter.format(record))
        assert "request" in data
        request = data["request"]
        assert "library" not in request

    def test_uwsgi_worker(self, log_record: LogRecordCallable) -> None:
        # Outside a uwsgi context, the worker id is not included in the log.
        formatter = JSONFormatter()
        record = log_record()
        data = json.loads(formatter.format(record))
        assert "uwsgi" not in data

        # Inside a uwsgi context, the worker id is included in the log.
        with patch("palace.manager.service.logging.log.uwsgi") as mock_uwsgi:
            mock_uwsgi.worker_id.return_value = 42
            data = json.loads(formatter.format(record))
            assert "uwsgi" in data
            assert data["uwsgi"]["worker"] == 42

    def test_celery_task(self, log_record: LogRecordCallable) -> None:
        # If we are not in a celery task, the worker data is not included
        formatter = JSONFormatter()
        record = log_record()

        data = json.loads(formatter.format(record))
        assert "celery" not in data

        # If we are in a celery task, the worker data is included
        with patch("palace.manager.service.logging.log.celery_task") as mock_celery:
            mock_celery.configure_mock(
                **{
                    "name": "task_name",
                    "request.id": "request_id",
                    "request.root_id": "root_id",
                    "request.parent_id": "parent_id",
                    "request.correlation_id": "correlation_id",
                    "request.retries": 3,
                    "request.group": "group_id",
                    "request.replaced_task_nesting": 1,
                }
            )
            data = json.loads(formatter.format(record))
            assert "celery" in data
            assert data["celery"] == {
                "request_id": "request_id",
                "root_id": "root_id",
                "parent_id": "parent_id",
                "correlation_id": "correlation_id",
                "task_name": "task_name",
                "retries": 3,
                "group": "group_id",
                "replaced_task_nesting": 1,
            }

        # None values are filtered out
        with patch("palace.manager.service.logging.log.celery_task") as mock_celery:
            mock_celery.configure_mock(
                **{
                    "name": "task_name",
                    "request.id": "request_id",
                    "request.retries": 0,
                    "request.replaced_task_nesting": 0,
                    "request.root_id": None,
                    "request.parent_id": None,
                    "request.correlation_id": None,
                    "request.group": None,
                }
            )
            data = json.loads(formatter.format(record))
            assert "celery" in data
            assert data["celery"] == {
                "request_id": "request_id",
                "task_name": "task_name",
                "retries": 0,
                "replaced_task_nesting": 0,
            }

    def test_extra_palace_context(self, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(LogLevel.info)

        formatter = JSONFormatter()

        log = logging.getLogger("some logger")
        log.info(
            "Test log message",
            extra={
                "palace_custom": "custom_value",
                "palace_another": {1, 2, 3},
                "not_palace": "not included",
                "palace_not_json_serializable": object(),
                "palace_name": "Not Overwritten",
                "palace_none_value": None,
            },
        )

        [record] = caplog.records

        data = json.loads(formatter.format(record))

        # The custom Palace attributes are included in the log
        assert "custom" in data
        assert data["custom"] == "custom_value"
        assert "another" in data
        # Set is converted to list in the json conversion
        assert data["another"] == [1, 2, 3]

        # The non-Palace attributes are not included in the log
        assert "not_palace" not in data

        # If a Palace attribute is not JSON serializable, it is not included in the log instead of
        # raising an error.
        assert "not_json_serializable" not in data

        # If a Palace attribute is None, it is not included in the log.
        assert "none_value" not in data

        # Because "name" was already set by the formatter, it is not overwritten
        assert "name" in data
        assert data["name"] == "some logger"


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
    assert handler.level == logging.NOTSET


def test_create_stream_handler() -> None:
    mock_formatter = MagicMock()

    handler = create_stream_handler(formatter=mock_formatter)

    assert isinstance(handler, logging.StreamHandler)
    assert not any(isinstance(f, LogLoopPreventionFilter) for f in handler.filters)
    assert handler.formatter == mock_formatter
    assert handler.level == logging.NOTSET


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
    with patch("palace.manager.service.logging.log.logging"):
        setup(cloudwatch_enabled=False)
        assert mock_cloudwatch_callable.call_count == 0

        setup(cloudwatch_enabled=True)
        assert mock_cloudwatch_callable.call_count == 1


@shared_task(bind=True)
def celery_logging_test_task(task: Task) -> str:
    """A simple test task that logs a message."""
    task.log.info("Test log message from celery task")
    return task.request.id


def test_celery_task_logging_integration(
    celery_fixture: CeleryFixture,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Test that logging from within a real Celery task includes the expected
    Celery context data when formatted with JSONFormatter.

    We set the JSONFormatter on caplog's handler so records are formatted
    immediately when emitted, while the Celery task context is still active.
    """
    caplog.set_level(LogLevel.info)
    caplog.handler.setFormatter(JSONFormatter())

    task = celery_logging_test_task.delay()
    task.wait()

    assert len(caplog.records) == 1
    data = json.loads(caplog.text)

    assert "celery" in data
    celery_data = data["celery"]

    # These should always be present
    assert celery_data["request_id"] == task.id
    assert "celery_logging_test_task" in celery_data["task_name"]
    # retries should be 0 for a task that hasn't been retried
    assert celery_data["retries"] == 0
    # replaced_task_nesting should be 0 for a task that hasn't been replaced
    assert celery_data["replaced_task_nesting"] == 0

    # These are optional, and will not be set for a standalone task
    for key in ["root_id", "parent_id", "correlation_id", "group"]:
        assert key not in celery_data
