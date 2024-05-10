from __future__ import annotations

import celery
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from palace.manager.celery.session import SessionMixin
from palace.manager.service.container import Services, container_instance
from palace.manager.sqlalchemy.session import SessionManager
from palace.manager.util.log import LoggerMixin


class Task(celery.Task, LoggerMixin, SessionMixin):
    """
    Celery task implementation for Palace.

    Our Celery app is configured to use this as the Task class implementation. This class
    provides some glue to allow tasks to access the database and services from the dependency
    injection container.

    In order to access this class within a Celery task, you must use the `bind=True` parameter
    when defining your task.
    See: https://docs.celeryq.dev/en/stable/userguide/tasks.html#bound-tasks

    For example:
    ```
    @shared_task(bind=True)
    def my_task(task: Task) -> None:
      ...
    ```

    This class follows the pattern suggested in the Celery documentation:
    https://docs.celeryq.dev/en/stable/userguide/tasks.html#custom-task-classes

    The `__init__` method is only called once per worker process, so we can safely create a session
    maker and services container here and reuse them for the life of the worker process.
    See: https://docs.celeryq.dev/en/stable/userguide/tasks.html#instantiation
    """

    _session_maker = None

    @property
    def session_maker(self) -> sessionmaker[Session]:
        """
        Get a new session for this worker process.

        This should generally be accessed via the `session` or `transaction` context managers
        defined in `SessionMixin`.

        This is using a `NullPool` connection pool for workers DB connections. This means that DB
        connections are opened on demand, so we won't have long-lived connections sitting idle,
        which should reduce the load on our PG instances.

        A null pool isn't exactly the trade-off I wanted to make here. What I would have liked is a
        connection pool that disconnects idle connections after some defined timeout, so we can have
        a connection pool, but when the worker is sitting idle the connections will eventually drop.

        The `QueuePool` pool class that is the default SQLAlchemy connection pool unfortunately does
        not offer this functionality. Instead, if we used the `QueuePool` pool class, we would have
        it would keep a connection open for each worker process, even if the process was idle for some
        time.

        This isn't ideal in the beginning when we have low worker utilization, but might be okay once
        we get all our tasks moved over. So we will need to evaluate what we want to do for connection
        pooling as this rolls out.

        TODO: Evaluate connection pooling strategy for Celery workers, once we have a better idea of
          worker utilization in production.
        """
        if self._session_maker is None:
            engine = SessionManager.engine(
                poolclass=NullPool, application_name=self.name
            )
            maker = sessionmaker(bind=engine)
            SessionManager.setup_event_listener(maker)
            self._session_maker = maker
        return self._session_maker

    @property
    def services(self) -> Services:
        return container_instance()
