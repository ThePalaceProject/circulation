import threading
from queue import Queue

from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.session import SessionManager
from palace.manager.util.worker_pools import (
    DatabaseJob,
    DatabasePool,
    DatabaseWorker,
    Pool,
    Worker,
)
from tests.fixtures.database import DatabaseTransactionFixture


class TestPool:
    def test_initializes_with_active_workers(self):
        original_thread_count = threading.active_count()
        with Pool(3) as pool:
            pool_thread_count = threading.active_count() - original_thread_count
            assert 3 == pool_thread_count
            assert 3 == pool.size
            assert 3 == len(pool.workers)

    def test_put_tracks_total_job_count(self):
        def task():
            return "T'Challa"

        with Pool(2) as pool:
            assert 0 == pool.job_total
            for i in range(4):
                pool.put(task)
            assert 4 == pool.job_total

    def test_pool_tracks_error_count(self):
        def broken_task():
            raise RuntimeError

        pool = Pool(2)
        try:
            # The pool instantiates with 0 errors.
            assert 0 == pool.error_count

            for i in range(3):
                pool.put(broken_task)
        finally:
            pool.join()

        # The pool maintains a count of its errors.
        assert 3 == pool.error_count

    def test_success_rate(self):
        def task():
            return "Shuri"

        def broken_task():
            raise RuntimeError

        pool = Pool(2)
        try:
            # When there are no tasks, the success rate is 1.0.
            assert 1.0 == pool.success_rate

            pool.put(task)
            pool.put(task)
            # When there are no errors, the success rate is 1.0.
            pool.join()
            assert 1.0 == pool.success_rate

            # When a job fails, it impacts the success rate.
            pool.put(broken_task)
        finally:
            pool.join()
        assert 1 / 3.0 == pool.success_rate


class TestDatabasePool:
    def test_workers_are_created_with_sessions(self, db: DatabaseTransactionFixture):
        session_factory = SessionManager.sessionmaker(session=db.session)
        bind = session_factory.kw["bind"]
        pool = DatabasePool(2, session_factory)
        try:
            for worker in pool.workers:
                assert isinstance(worker, DatabaseWorker)
                assert worker._db
                assert bind == worker._db.connection()
        finally:
            pool.join()


class MockQueue(Queue):
    error_count = 0

    def inc_error(self):
        self.error_count += 1


class TestWorker:
    def test_factory(self):
        mock_queue = object()
        result = Worker.factory(mock_queue)
        assert isinstance(result, Worker)
        assert mock_queue == result.jobs
        assert True == result.daemon

    def test_works_on_callable_job(self):
        results = list()

        def task():
            results.append("werk")

        try:
            q = MockQueue()
            for i in range(6):
                q.put(task)
            rihanna = Worker(q)
            rihanna.start()
        finally:
            q.join()

        assert ["werk", "werk", "werk", "werk", "werk", "werk"] == results

    def test_works_on_job_object(self):
        results = list()

        original = ["Who Can I * To", "* To You", "Water *s Dry", "* The World"]

        class MockJob:
            def __init__(self, idx):
                self.idx = idx

            def run(self):
                results.append(original[self.idx])

        try:
            q = MockQueue()
            for i in range(len(original)):
                q.put(MockJob(i))
            rb = Worker(q)
            rb.start()
        finally:
            q.join()

        assert sorted(original) == sorted(results)


class TestDatabaseJob:
    class WorkingJob(DatabaseJob):
        def do_run(self, _db):
            identifier = Identifier(type="Keep It", identifier="100")
            _db.add(identifier)

    class BrokenJob(DatabaseJob):
        def do_run(self, _db):
            identifier = Identifier(type="You Can", identifier="Keep It")
            _db.add(identifier)
            raise RuntimeError

    def test_manages_database_for_job_success_and_failure(
        self, db: DatabaseTransactionFixture
    ):
        session = db.session
        self.WorkingJob().run(session)
        try:
            self.BrokenJob().run(session)
        except RuntimeError:
            pass

        [identifier] = session.query(Identifier).all()
        assert "Keep It" == identifier.type
        assert "100" == identifier.identifier
