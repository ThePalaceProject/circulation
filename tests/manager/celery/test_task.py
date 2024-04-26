from unittest.mock import MagicMock, patch

from pytest import LogCaptureFixture
from sqlalchemy.pool import NullPool

from palace.manager.celery.task import Task
from palace.manager.service.logging.configuration import LogLevel


def test_task_session_maker() -> None:
    task = Task()

    # If session maker is not initialized, it should be None
    assert task._session_maker is None

    # If _session_maker is initialized, that value should be returned
    mock = MagicMock()
    task._session_maker = mock
    assert task.session_maker == mock

    # If _session_maker is None, it should be initialized
    task._session_maker = None
    with (
        patch("palace.manager.celery.task.SessionManager") as mock_session_manager,
        patch("palace.manager.celery.task.sessionmaker") as mock_sessionmaker,
    ):
        assert task.session_maker == mock_sessionmaker.return_value
        mock_session_manager.engine.assert_called_once_with(poolclass=NullPool)
        mock_sessionmaker.assert_called_once_with(
            bind=mock_session_manager.engine.return_value
        )
        mock_session_manager.setup_event_listener.assert_called_once_with(
            mock_sessionmaker.return_value
        )


def test_task_services() -> None:
    task = Task()

    # Task.services will return the result of container_instance()
    with patch(
        "palace.manager.celery.task.container_instance"
    ) as mock_container_instance:
        assert task.services == mock_container_instance.return_value
        mock_container_instance.assert_called_once_with()


def test_task_logger(caplog: LogCaptureFixture) -> None:
    # Task has a .log property that provides an appropriate logger.
    caplog.set_level(LogLevel.info)
    task = Task()
    task.log.info("test")
    assert "test" in caplog.text

    # There is also a class-level logger that can be used.
    caplog.clear()
    Task.logger().info("test")
    assert "test" in caplog.text
