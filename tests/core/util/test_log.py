import pytest
from _pytest.logging import LogCaptureFixture

from core.service.logging.configuration import LogLevel
from core.util.log import LoggerMixin, log_elapsed_time


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
    caplog.set_level(LogLevel.info.value)

    MockClass.test_method()
    assert len(caplog.records) == 2

    [first, second] = caplog.records
    assert first.name == "tests.core.util.test_log.MockClass"
    assert first.message == "Test: Starting..."
    assert first.levelname == LogLevel.info.value

    assert second.name == "tests.core.util.test_log.MockClass"
    assert "Test: Completed. (elapsed time:" in second.message
    assert second.levelname == LogLevel.info.value


def test_log_elapsed_time_instance(caplog: LogCaptureFixture):
    caplog.set_level(LogLevel.debug.value)

    MockClass().test_method_2()
    assert len(caplog.records) == 1
    [record] = caplog.records
    assert record.name == "tests.core.util.test_log.MockClass"
    assert "Test 12345: Completed. (elapsed time:" in record.message
    assert record.levelname == LogLevel.debug.value


def test_log_elapsed_time_invalid(caplog: LogCaptureFixture):
    caplog.set_level(LogLevel.info.value)

    with pytest.raises(RuntimeError):
        log_elapsed_time(log_level=LogLevel.info, message_prefix="Test")(lambda: None)()
    assert len(caplog.records) == 0
