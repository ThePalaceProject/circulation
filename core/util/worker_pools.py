from __future__ import annotations

import sys
from collections.abc import Callable
from queue import Queue
from threading import Thread
from types import TracebackType
from typing import Any, Literal

from sqlalchemy.orm import Session

from core.util.log import LoggerMixin

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

# Much of the work in this file is based on
# https://github.com/shazow/workerpool, with
# great appreciation.

# TODO: Consider supporting multiprocessing as well as
# (or instead of) multithreading.


class Worker(Thread, LoggerMixin):
    """A Thread that performs jobs"""

    @classmethod
    def factory(cls, worker_pool: Pool) -> Self:
        return cls(worker_pool)

    def __init__(self, jobs: Pool):
        super().__init__()
        self.daemon = True
        self.jobs = jobs

    def run(self) -> None:
        while True:
            try:
                self.do_job()
            except Exception as e:
                self.jobs.inc_error()
                self.log.error("Job raised error: %r", e, exc_info=e)
            finally:
                self.jobs.task_done()

    def do_job(self, *args: Any, **kwargs: Any) -> None:
        job = self.jobs.get()
        if callable(job):
            job(*args, **kwargs)
            return

        # This is a Job object. Do any setup and finalization, as well as
        # running the task.
        job.run(*args, **kwargs)


class DatabaseWorker(Worker):
    """A worker Thread that performs jobs with a database session"""

    @classmethod
    def factory(cls, worker_pool: Pool, _db: Session) -> Self:  # type: ignore[override]
        return cls(worker_pool, _db)

    def __init__(self, jobs: Pool, _db: Session):
        super().__init__(jobs)
        self._db = _db

    def do_job(self) -> None:
        super().do_job(self._db)


class Pool(LoggerMixin):
    """A pool of Worker threads and a job queue to keep them busy."""

    def __init__(
        self, size: int, worker_factory: Callable[..., Worker] | None = None
    ) -> None:
        self.jobs: Queue[Job] = Queue()

        self.size = size
        self.workers = list()

        self.job_total = 0
        self.error_count = 0

        # Use Worker for pool by default.
        self.worker_factory = worker_factory or Worker.factory
        for i in range(self.size):
            w = self.create_worker()
            self.workers.append(w)
            w.start()

    @property
    def success_rate(self) -> float:
        if self.job_total <= 0 or self.error_count <= 0:
            return float(1)
        return self.error_count / float(self.job_total)

    def create_worker(self) -> Worker:
        return self.worker_factory(self)

    def inc_error(self) -> None:
        self.error_count += 1

    def restart(self) -> Self:
        for w in self.workers:
            if not w.is_alive():
                w.start()
        return self

    __enter__ = restart

    def __exit__(
        self,
        type: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        self.join()
        if value is not None:
            self.log.error("Error with %r: %r", self, value, exc_info=value)
            raise value
        return False

    def get(self) -> Job:
        return self.jobs.get()

    def put(self, job: Job) -> None:
        self.job_total += 1
        return self.jobs.put(job)

    def task_done(self) -> None:
        return self.jobs.task_done()

    def join(self) -> None:
        self.jobs.join()
        self.log.info(
            "%d/%d job errors occurred. %.2f%% success rate.",
            self.error_count,
            self.job_total,
            self.success_rate * 100,
        )


class DatabasePool(Pool):
    """A pool of DatabaseWorker threads and a job queue to keep them busy."""

    def __init__(
        self,
        size: int,
        session_factory: Callable[[], Session],
        worker_factory: Callable[..., DatabaseWorker] | None = None,
    ):
        self.session_factory = session_factory

        self.worker_factory: Callable[..., DatabaseWorker] = (
            worker_factory or DatabaseWorker.factory
        )
        super().__init__(size, worker_factory=self.worker_factory)

    def create_worker(self) -> DatabaseWorker:
        worker_session = self.session_factory()
        return self.worker_factory(self, worker_session)


class Job:
    """Abstract parent class for a bit o' work that can be run in a Thread.
    For use with Worker.
    """

    def rollback(self, *args: Any, **kwargs: Any) -> None:
        """Cleans up the task if it errors"""

    def finalize(self, *args: Any, **kwargs: Any) -> None:
        """Finalizes the task if it is successful"""

    def do_run(self, *args: Any, **kwargs: Any) -> None:
        """Does the work"""
        raise NotImplementedError()

    def run(self, *args: Any, **kwargs: Any) -> None:
        try:
            self.do_run(*args, **kwargs)
        except Exception:
            self.rollback(*args, **kwargs)
            raise
        else:
            self.finalize(*args, **kwargs)


class DatabaseJob(Job):
    def rollback(self, _db: Session) -> None:
        _db.rollback()

    def finalize(self, _db: Session) -> None:
        _db.commit()
