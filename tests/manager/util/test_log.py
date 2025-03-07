import pytest
from pytest import LogCaptureFixture

from palace.manager.service.logging.configuration import LogLevel
from palace.manager.util.log import LoggerMixin, log_elapsed_time, pluralize


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
