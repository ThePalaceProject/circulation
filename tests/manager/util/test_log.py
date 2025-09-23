import logging
from collections.abc import MutableMapping
from typing import Any
from unittest.mock import Mock

import pytest
from pytest import LogCaptureFixture

from palace.manager.service.logging.configuration import LogLevel
from palace.manager.util.log import (
    ExtraDataLoggerAdapter,
    LoggerAdapterType,
    LoggerMixin,
    elapsed_time_logging,
    log_elapsed_time,
    pluralize,
)


class MockClass(LoggerMixin):
    @classmethod
    @log_elapsed_time(log_level=LogLevel.info, message_prefix="Test")
    def test_method(cls):
        pass

    @log_elapsed_time(
        log_level=LogLevel.debug, message_prefix="Test 12345", skip_start=True
    )
    def test_method_2(self):
        pass


def test_log_elapsed_time_cls(caplog: LogCaptureFixture):
    caplog.set_level(LogLevel.info)

    MockClass.test_method()
    assert len(caplog.records) == 2

    [first, second] = caplog.records
    assert first.name == "tests.manager.util.test_log.MockClass"
    assert first.message == "Test: Starting..."
    assert first.levelname == LogLevel.info

    assert second.name == "tests.manager.util.test_log.MockClass"
    assert "Test: Completed. (elapsed time:" in second.message
    assert second.levelname == LogLevel.info


def test_log_elapsed_time_instance(caplog: LogCaptureFixture):
    caplog.set_level(LogLevel.debug)

    MockClass().test_method_2()
    assert len(caplog.records) == 1
    [record] = caplog.records
    assert record.name == "tests.manager.util.test_log.MockClass"
    assert "Test 12345: Completed. (elapsed time:" in record.message
    assert record.levelname == LogLevel.debug


def test_log_elapsed_time_invalid(caplog: LogCaptureFixture):
    caplog.set_level(LogLevel.info)

    with pytest.raises(RuntimeError):
        log_elapsed_time(log_level=LogLevel.info, message_prefix="Test")(lambda: None)()
    assert len(caplog.records) == 0


def test_pluralize():
    assert pluralize(1, "dingo") == "1 dingo"
    assert pluralize(2, "dingo") == "2 dingos"
    assert pluralize(0, "dingo") == "0 dingos"

    assert pluralize(1, "foo", "bar") == "1 foo"
    assert pluralize(2, "foo", "bar") == "2 bar"


class MockExtraDataLoggerAdapter(ExtraDataLoggerAdapter):
    def process(
        self, msg: str, kwargs: MutableMapping[str, Any]
    ) -> tuple[str, MutableMapping[str, Any]]:
        value = str(self.extra.get("key", "key_missing"))
        new_msg = f"{msg} [{value}]"
        return new_msg, kwargs


class ClassThatUsesExtraDataAdapter:
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(self.name)

    @property
    def log(self) -> LoggerAdapterType:
        extra = {"key": "test_value"}
        return MockExtraDataLoggerAdapter(self.logger, extra)

    def do_something(self, message: str) -> None:
        self.log.info(message)


class TestExtraDataLoggerAdapter:
    @pytest.fixture
    def log_capture(self, caplog):
        caplog.set_level(logging.INFO)
        return caplog

    @pytest.mark.parametrize(
        "extra_data, expected_value",
        (
            pytest.param(
                {"key": "I'm special"}, "I'm special", id="extra_data_custom_value"
            ),
            pytest.param({}, "key_missing", id="no_extra_data"),
            pytest.param({"key": None}, "None", id="extra_data_none_value"),
            pytest.param({"key": True}, "True", id="extra_data_true"),
            pytest.param({"key": False}, "False", id="extra_data_faux"),
            pytest.param({"key": 0}, "0", id="extra_data_nada"),
            pytest.param({"key": 42}, "42", id="extra_data_ltuae"),
            pytest.param({"key": -1}, "-1", id="extra_data_less_than_zero"),
        ),
    )
    def test_logger_adapter_extra_data(
        self,
        extra_data: dict[str, Any] | None,
        expected_value: str,
        log_capture: LogCaptureFixture,
    ):
        logger = logging.getLogger("test_logger")
        adapter = MockExtraDataLoggerAdapter(logger, extra_data)
        adapter.info("Original message")
        assert f"Original message [{expected_value}]" in log_capture.text

    def test_using_the_adapter(self, log_capture):
        test_instance = ClassThatUsesExtraDataAdapter("test_instance")
        test_instance.do_something("Another test message")
        assert "Another test message [test_value]" in log_capture.text


class TestElapsedTimeLogging:
    class MockLogger:
        def __init__(self):
            self.messages: list[str] = []
            self.log_method = Mock(side_effect=lambda msg: self.messages.append(msg))

    @pytest.fixture
    def mock_logger(self) -> MockLogger:
        """Fixture that provides a mock log method with message collection."""
        return TestElapsedTimeLogging.MockLogger()

    def test_basic_usage(self, mock_logger: MockLogger):
        with elapsed_time_logging(log_method=mock_logger.log_method):
            pass

        assert len(mock_logger.messages) == 2
        assert mock_logger.messages[0] == "Starting..."
        assert "Completed. (elapsed time:" in mock_logger.messages[1]
        assert "seconds)" in mock_logger.messages[1]
        mock_logger.log_method.assert_called()

    def test_with_message_prefix(self, mock_logger: MockLogger):
        with elapsed_time_logging(
            log_method=mock_logger.log_method, message_prefix="Test Operation"
        ):
            pass

        assert len(mock_logger.messages) == 2
        assert mock_logger.messages[0] == "Test Operation: Starting..."
        assert "Test Operation: Completed. (elapsed time:" in mock_logger.messages[1]

    def test_skip_start_message(self, mock_logger: MockLogger):
        with elapsed_time_logging(log_method=mock_logger.log_method, skip_start=True):
            pass

        assert len(mock_logger.messages) == 1
        assert "Completed. (elapsed time:" in mock_logger.messages[0]
        assert "Starting..." not in mock_logger.messages[0]

    def test_with_prefix_and_skip_start(self, mock_logger: MockLogger):
        with elapsed_time_logging(
            log_method=mock_logger.log_method,
            message_prefix="Custom Task",
            skip_start=True,
        ):
            pass

        assert len(mock_logger.messages) == 1
        assert "Custom Task: Completed. (elapsed time:" in mock_logger.messages[0]

    def test_exception_raised(self, mock_logger: MockLogger):
        with pytest.raises(ValueError):
            with elapsed_time_logging(log_method=mock_logger.log_method):
                raise ValueError("Test exception")

        assert len(mock_logger.messages) == 2
        assert mock_logger.messages[0] == "Starting..."
        assert "Failed (raised ValueError). (elapsed time:" in mock_logger.messages[1]

    def test_exception_with_prefix(self, mock_logger: MockLogger):
        with pytest.raises(RuntimeError):
            with elapsed_time_logging(
                log_method=mock_logger.log_method, message_prefix="Error Task"
            ):
                raise RuntimeError("Something went wrong")

        assert len(mock_logger.messages) == 2
        assert mock_logger.messages[0] == "Error Task: Starting..."
        assert (
            "Error Task: Failed (raised RuntimeError). (elapsed time:"
            in mock_logger.messages[1]
        )

    def test_exception_with_skip_start(self, mock_logger: MockLogger):
        with pytest.raises(Exception):
            with elapsed_time_logging(
                log_method=mock_logger.log_method, skip_start=True
            ):
                raise Exception("Test")

        assert len(mock_logger.messages) == 1
        assert "Failed (raised Exception). (elapsed time:" in mock_logger.messages[0]

    def test_elapsed_time_format(self, mock_logger: MockLogger):
        import time

        with elapsed_time_logging(log_method=mock_logger.log_method):
            time.sleep(0.01)

        assert len(mock_logger.messages) == 2
        elapsed_msg = mock_logger.messages[1]

        import re

        match = re.search(r"elapsed time: (\d+\.\d{4}) seconds", elapsed_msg)
        assert match is not None
        elapsed_time = float(match.group(1))
        assert elapsed_time >= 0.01
        assert elapsed_time < 0.1

    def test_with_actual_logger(self, caplog: LogCaptureFixture):
        caplog.set_level(logging.INFO)
        logger = logging.getLogger("test_elapsed_time")

        with elapsed_time_logging(log_method=logger.info, message_prefix="Logger Test"):
            pass

        assert len(caplog.records) == 2
        assert caplog.records[0].message == "Logger Test: Starting..."
        assert "Logger Test: Completed. (elapsed time:" in caplog.records[1].message

    def test_nested_context_managers(self, mock_logger: MockLogger):
        with elapsed_time_logging(
            log_method=mock_logger.log_method, message_prefix="Outer"
        ):
            with elapsed_time_logging(
                log_method=mock_logger.log_method, message_prefix="Inner"
            ):
                pass

        assert len(mock_logger.messages) == 4
        assert mock_logger.messages[0] == "Outer: Starting..."
        assert mock_logger.messages[1] == "Inner: Starting..."
        assert "Inner: Completed." in mock_logger.messages[2]
        assert "Outer: Completed." in mock_logger.messages[3]
