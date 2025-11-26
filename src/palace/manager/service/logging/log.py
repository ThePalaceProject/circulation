from __future__ import annotations

import logging
import socket
import threading
from collections.abc import Callable, Mapping, Sequence
from logging import Handler
from typing import TYPE_CHECKING, Any

from sqlalchemy.exc import SQLAlchemyError
from watchtower import CloudWatchLogHandler

from palace.manager.api.admin.util.flask import get_request_admin
from palace.manager.api.util.flask import get_request_library, get_request_patron
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.util.datetime_helpers import from_timestamp
from palace.manager.util.json import json_serializer

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

try:
    from celery import current_task as celery_task
except ImportError:
    celery_task = None


class JSONFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__()
        self.hostname = socket.getfqdn()
        self.main_thread_id = threading.main_thread().ident

    @staticmethod
    def _is_json_serializable(v: Any) -> bool:
        try:
            json_serializer(v)
            return True
        except (TypeError, ValueError):
            return False

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
        if record.thread and record.thread != self.main_thread_id:
            data["thread"] = record.thread
        if record.stack_info:
            data["stack"] = self.formatStack(record.stack_info)

        # If we are running in a Flask context, we include the request data in the log
        if flask_request:
            data["request"] = {
                "path": flask_request.path,
                "method": flask_request.method,
                "host": flask_request.host_url,
            }
            if flask_request.query_string:
                data["request"]["query"] = flask_request.query_string.decode()
            if user_agent := flask_request.headers.get("User-Agent"):
                data["request"]["user_agent"] = user_agent

            forwarded_for_list = []
            if forwarded_for := flask_request.headers.get("X-Forwarded-For"):
                forwarded_for_list.extend(
                    [ip.strip() for ip in forwarded_for.split(",")]
                )
            if remote_addr := flask_request.remote_addr:
                forwarded_for_list.append(remote_addr)
            if forwarded_for_list:
                data["request"]["forwarded_for"] = forwarded_for_list

            # If we have Palace specific request data, we also want to include that in the log
            try:
                if library := get_request_library(default=None):
                    data["request"]["library"] = {
                        "uuid": library.uuid,
                        "name": library.name,
                        "short_name": library.short_name,
                    }

                if patron := get_request_patron(default=None):
                    patron_information = {}
                    for key in (
                        "authorization_identifier",
                        "username",
                        "external_identifier",
                    ):
                        if value := getattr(patron, key, None):
                            patron_information[key] = value
                    if patron_information:
                        data["request"]["patron"] = patron_information

                if admin := get_request_admin(default=None):
                    data["request"]["admin"] = admin.email
            except SQLAlchemyError:
                # All the Palace specific data are SQLAlchemy objects, so if we are logging errors
                # when the database is in a bad state, we may not be able to access these objects.
                pass

        # If we are running in uwsgi context, we include the worker id in the log
        if uwsgi:
            data["uwsgi"] = {"worker": uwsgi.worker_id()}

        # Handle the case where we're running in a Celery task, this information is usually captured by
        # the Celery log formatter, but we are not using that formatter in our code.
        # See https://docs.celeryq.dev/en/stable/reference/celery.app.log.html#celery.app.log.TaskFormatter
        if celery_task:
            celery_request = celery_task.request
            celery_data = {
                "request_id": celery_request.id,
                "task_name": celery_task.name,
                "retries": celery_request.retries,
                "replaced_task_nesting": celery_request.replaced_task_nesting,
            }
            # Add helpful data for correlating tasks in a workflow, if the data is present.
            if celery_request.root_id and celery_request.root_id != celery_request.id:
                celery_data["root_id"] = celery_request.root_id
            if (
                celery_request.correlation_id
                and celery_request.correlation_id != celery_request.id
            ):
                celery_data["correlation_id"] = celery_request.correlation_id
            if celery_request.parent_id:
                celery_data["parent_id"] = celery_request.parent_id
            if celery_request.group:
                celery_data["group"] = celery_request.group
            data["celery"] = celery_data

        # Include any custom Palace-specific ('palace_' prefixed) attributes that have been added to
        # the LogRecord in our json output with the 'palace_' prefix removed.
        for key, value in record.__dict__.items():
            if (
                key != (log_data_key := key.removeprefix("palace_"))
                and value is not None
                and self._is_json_serializable(value)
                and log_data_key not in data
            ):
                data[log_data_key] = value

        return json_serializer(data)


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
        "httpx",
    ):
        logging.getLogger(logger).setLevel(verbose_level.value)
