import argparse
import logging
import traceback
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from palace.manager.core.monitor import TimestampData
from palace.manager.service.container import Services, container_instance
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.session import production_session
from palace.manager.util.datetime_helpers import strptime_utc, utc_now


def _normalize_cmd_args(
    cmd_args: Sequence[str | None] | None,
) -> list[str] | None:
    """Normalize command-line args, dropping any None values.

    This helper is used to handle test scenarios where cmd_args may
    contain None values that need to be filtered out before parsing.

    :param cmd_args: The raw command-line arguments, potentially containing None values.
    :return: A list of non-None string arguments, or None if cmd_args is None.
    """
    if cmd_args is None:
        return None
    return [arg for arg in cmd_args if arg is not None]


class Script:
    @property
    def _db(self) -> Session:
        if not hasattr(self, "_session"):
            self._session = production_session(self.__class__)
        return self._session

    @property
    def services(self) -> Services:
        return self._services

    @property
    def script_name(self) -> str:
        """Find or guess the name of the script.

        This is either the .name of the Script object or the name of
        the class.
        """
        return getattr(self, "name", self.__class__.__name__)

    @property
    def log(self) -> logging.Logger:
        if not hasattr(self, "_log"):
            self._log = logging.getLogger(self.script_name)
        return self._log

    @classmethod
    def parse_command_line(
        cls,
        _db: Session,
        cmd_args: Sequence[str | None] | None = None,
    ) -> argparse.Namespace:
        parser = cls.arg_parser(_db)
        return parser.parse_known_args(_normalize_cmd_args(cmd_args))[0]

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        raise NotImplementedError()

    @classmethod
    def parse_time(cls, time_string: str | None) -> datetime | None:
        """Try to pass the given string as a time."""
        if not time_string:
            return None
        for format in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
            for hours in ("", " %H:%M:%S"):
                full_format = format + hours
                try:
                    parsed = strptime_utc(time_string, full_format)
                    return parsed
                except ValueError:
                    continue
        raise ValueError("Could not parse time: %s" % time_string)

    def __init__(
        self,
        _db: Session | None = None,
        services: Services | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Basic constructor.

        :_db: A database session to be used instead of
        creating a new one. Useful in tests.
        """
        if _db:
            self._session = _db

        self._services = container_instance() if services is None else services

        # Call init_resources() to initialize the logging configuration.
        self._services.init_resources()

    def do_run(self, *args: Any, **kwargs: Any) -> Any:
        """Run the script. Subclasses must implement this method.

        If subclasses return a TimestampData object, it will be used to
        update the script's timestamp. Otherwise, the return value is ignored.
        """
        raise NotImplementedError()

    def run(self) -> None:
        DataSource.well_known_sources(self._db)
        start_time = utc_now()
        try:
            timestamp_data = self.do_run()
            if not isinstance(timestamp_data, TimestampData):
                # Ignore any nonstandard return value from do_run().
                timestamp_data = None
            self.update_timestamp(timestamp_data, start_time, None)
        except Exception as e:
            logging.error("Fatal exception while running script: %s", e, exc_info=e)
            stack_trace = traceback.format_exc()
            self.update_timestamp(None, start_time, stack_trace)
            raise

    def update_timestamp(
        self,
        timestamp_data: TimestampData | None,
        start_time: datetime,
        exception: str | None,
    ) -> None:
        """By default scripts have no timestamp of their own.

        Most scripts either work through Monitors or CoverageProviders,
        which have their own logic for creating timestamps, or they
        are designed to be run interactively from the command-line, so
        facts about when they last ran are not relevant.

        :param timestamp_data: The timestamp data returned by do_run().
        :param start_time: The time the script started running.
        :param exception: A stack trace for the exception, if any,
           that stopped the script from running.
        """
