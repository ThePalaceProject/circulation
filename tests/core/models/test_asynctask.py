from _datetime import datetime, timedelta

from core.model import get_one_or_create
from core.model.asynctask import (
    AsyncTask,
    AsyncTaskStatus,
    AsyncTaskType,
    queue_task,
    start_next_task,
)
from tests.fixtures.database import DatabaseTransactionFixture


class TestAsyncTAsk:
    def test_start_next_task(self, db: DatabaseTransactionFixture):
        session = db.session

        # create a new task
        task1, _ = get_one_or_create(
            session,
            AsyncTask,
            task_type=AsyncTaskType.INVENTORY_REPORT,
            status=AsyncTaskStatus.READY,
        )

        # create a second task, but set the creation date back a minute
        oldest_task_date = datetime.now() - timedelta(seconds=60)
        task2, _ = get_one_or_create(
            session,
            AsyncTask,
            task_type=AsyncTaskType.INVENTORY_REPORT,
            status=AsyncTaskStatus.READY,
            created=oldest_task_date,
        )
        session.commit()

        # retrieve the first task
        first_retrieved: AsyncTask = start_next_task(
            session, task_type=AsyncTaskType.INVENTORY_REPORT
        )
        print(f"first_retrieved={first_retrieved}")
        assert first_retrieved.id == task2.id
        assert first_retrieved.created == oldest_task_date
        assert first_retrieved.status == AsyncTaskStatus.PROCESSING
        assert first_retrieved.processing_start_time

        # verify that it is no longer returned by the next_ready_task
        second_retrieved: AsyncTask = start_next_task(
            session, task_type=AsyncTaskType.INVENTORY_REPORT
        )
        assert second_retrieved.id == task1.id

    def test_queue_task(self, db: DatabaseTransactionFixture):
        session = db.session

        data = dict(key="value")
        task, is_new = queue_task(
            session, task_type=AsyncTaskType.INVENTORY_REPORT, data=data
        )

        assert is_new
        assert task.created
        assert task.status == AsyncTaskStatus.READY

        # try to insert an identical record
        task2, is_new = queue_task(
            session, task_type=AsyncTaskType.INVENTORY_REPORT, data=data
        )
        # we expect that a new one will not be created
        assert not is_new
        assert task2.id == task.id

        # change the data and retry: we expect a new task will be created
        new_data = dict(key="new value")
        task3, is_new = queue_task(
            session, task_type=AsyncTaskType.INVENTORY_REPORT, data=new_data
        )
        assert is_new
        assert task3.id != task.id
