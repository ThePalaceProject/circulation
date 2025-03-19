import logging
from collections.abc import MutableMapping
from typing import Any

import pytest
from pytest import LogCaptureFixture

from palace.manager.service.logging.configuration import LogLevel
from palace.manager.util.log import (
    ExtraDataLoggerAdapter,
    LoggerAdapterType,
    LoggerMixin,
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
