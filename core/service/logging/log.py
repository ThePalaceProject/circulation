from __future__ import annotations

import json
import logging
import socket
from collections.abc import Callable, Mapping, Sequence
from logging import Handler
from typing import TYPE_CHECKING, Any

from watchtower import CloudWatchLogHandler

from core.service.logging.configuration import LogLevel
from core.util.datetime_helpers import from_timestamp

if TYPE_CHECKING:
    from mypy_boto3_logs import CloudWatchLogsClient

try:
    from flask import request as flask_request
except ImportError:
    flask_request = None  # type: ignore[assignment]

try:
    import uwsgi
except ImportError:
    uwsgi = None


class JSONFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__()
        hostname = socket.gethostname()
        fqdn = socket.getfqdn()
        if len(fqdn) > len(hostname):
            hostname = fqdn
        self.hostname = hostname

    def format(self, record: logging.LogRecord) -> str:
        def ensure_str(s: Any) -> Any:
            """Ensure that unicode strings are used for a record's message.
            We don't want to try to interpolate an incompatible byte type; it
            could lead to a UnicodeDecodeError.
            """
            if isinstance(s, bytes):
                s = s.decode("utf-8")
            return s

        message = ensure_str(record.msg)
        if record.args:
            record_args: tuple[Any, ...] | dict[str, Any] | None = None
            if isinstance(record.args, Mapping):
                record_args = {
                    ensure_str(k): ensure_str(v) for k, v in record.args.items()
                }
            elif isinstance(record.args, Sequence):
                record_args = tuple(ensure_str(arg) for arg in record.args)

            if record_args is not None:
                try:
                    message = message % record_args
                except Exception as e:
                    # There was a problem formatting the log message,
                    # which points to a bug. A problem with the logging
                    # code shouldn't break the code that actually does the
                    # work, but we can't just let this slide -- we need to
                    # report the problem so it can be fixed.
                    message = (
                        "Log message could not be formatted. Exception: %r. Original message: message=%r args=%r"
                        % (e, message, record_args)
                    )
        data = dict(
            host=self.hostname,
            name=record.name,
            level=record.levelname,
            filename=record.filename,
            message=message,
            timestamp=from_timestamp(record.created).isoformat(),
        )
        if record.exc_info:
            data["traceback"] = self.formatException(record.exc_info)
        if record.process:
            data["process"] = record.process

        # If we are running in a Flask context, we include the request data in the log
        if flask_request:
            data["request"] = {
                "path": flask_request.path,
                "method": flask_request.method,
                "host": flask_request.host_url,
            }
            if flask_request.query_string:
                data["request"]["query"] = flask_request.query_string.decode()

        # If we are running in uwsgi context, we include the worker id in the log
        if uwsgi:
            data["uwsgi"] = {"worker": uwsgi.worker_id()}

        # Handle the case where we're running in a Celery task, this information is usually captured by
        # the Celery log formatter, but we are not using that formatter for our code.
        # See https://docs.celeryq.dev/en/stable/reference/celery.app.log.html#celery.app.log.TaskFormatter
        try:
            from celery import current_task

            if current_task:
                data["celery"] = {
                    "request_id": current_task.request.id,
                    "task_name": current_task.name,
                }
        except ImportError:
            pass

        return json.dumps(data)


class LogLoopPreventionFilter(logging.Filter):
    """
    A filter that makes sure no messages from botocore or the urllib3 connection pool
    are processed by the cloudwatch logs integration, as these messages can lead to an
    infinite loop.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name.startswith("botocore"):
            return False
        elif record.name.startswith("urllib3.connectionpool"):
            return False

        return True


def create_cloudwatch_handler(
    formatter: logging.Formatter,
    client: CloudWatchLogsClient,
    group: str,
    stream: str,
    interval: int,
    create_group: bool,
) -> logging.Handler:
    handler = CloudWatchLogHandler(
        log_group_name=group,
        log_stream_name=stream,
        send_interval=interval,
        boto3_client=client,
        create_log_group=create_group,
    )

    handler.addFilter(LogLoopPreventionFilter())
    handler.setFormatter(formatter)
    return handler


def create_stream_handler(formatter: logging.Formatter) -> logging.Handler:
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    return stream_handler


def setup_logging(
    level: LogLevel,
    verbose_level: LogLevel,
    stream: Handler,
    cloudwatch_enabled: bool,
    cloudwatch_callable: Callable[[], Handler],
) -> None:
    # Set up the root logger
    log_handlers = [stream]
    if cloudwatch_enabled:
        log_handlers.append(cloudwatch_callable())
    logging.basicConfig(force=True, level=level.value, handlers=log_handlers)

    # Set the loggers for various verbose libraries to the database
    # log level, which is probably higher than the normal log level.
    for logger in (
        "sqlalchemy.engine",
        "opensearch",
        "requests.packages.urllib3.connectionpool",
        "botocore",
        "urllib3.connectionpool",
    ):
        logging.getLogger(logger).setLevel(verbose_level.value)
