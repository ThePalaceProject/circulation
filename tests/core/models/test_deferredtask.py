from datetime import datetime, timedelta, timezone

import pytest

from core.model import get_one_or_create
from core.model.deferredtask import (
    DeferredTask,
    DeferredTaskStatus,
    DeferredTaskType,
    queue_task,
    start_next_task,
)
from tests.fixtures.database import DatabaseTransactionFixture


class TestDeferredTask:
    def test_start_next_task(self, db: DatabaseTransactionFixture):
        session = db.session

        # create a new task
        task1, _ = get_one_or_create(
            session,
            DeferredTask,
            task_type=DeferredTaskType.INVENTORY_REPORT,
            status=DeferredTaskStatus.READY,
        )

        # create a second task, but set the creation date back a minute
        oldest_task_date = datetime.now(timezone.utc) - timedelta(seconds=60)
        task2, _ = get_one_or_create(
            session,
            DeferredTask,
            task_type=DeferredTaskType.INVENTORY_REPORT,
            status=DeferredTaskStatus.READY,
            created=oldest_task_date,
        )
        session.commit()

        # retrieve the first task
        first_retrieved = start_next_task(
            session, task_type=DeferredTaskType.INVENTORY_REPORT
        )

        assert first_retrieved
        assert first_retrieved.id == task2.id
        assert first_retrieved.created == oldest_task_date
        assert first_retrieved.status == DeferredTaskStatus.PROCESSING
        assert first_retrieved.processing_start_time

        # verify that it is no longer returned by the next_ready_task
        second_retrieved = start_next_task(
            session, task_type=DeferredTaskType.INVENTORY_REPORT
        )

        assert second_retrieved and second_retrieved.id == task1.id

    def test_queue_task(self, db: DatabaseTransactionFixture):
        session = db.session

        data = dict(key="value")
        task, is_new = queue_task(
            session, task_type=DeferredTaskType.INVENTORY_REPORT, data=data
        )

        assert is_new
        assert task.created
        assert task.status == DeferredTaskStatus.READY

        # try to insert an identical record
        task2, is_new = queue_task(
            session, task_type=DeferredTaskType.INVENTORY_REPORT, data=data
        )
        # we expect that a new one will not be created
        assert not is_new
        assert task2.id == task.id

        # change the data and retry: we expect a new task will be created
        new_data = dict(key="new value")
        task3, is_new = queue_task(
            session, task_type=DeferredTaskType.INVENTORY_REPORT, data=new_data
        )
        assert is_new
        assert task3.id != task.id

    def test_complete(self, db: DatabaseTransactionFixture):
        session = db.session
        task, is_new = queue_task(
            session, task_type=DeferredTaskType.INVENTORY_REPORT, data={}
        )

        assert task
        assert task.status == DeferredTaskStatus.READY
        assert not task.processing_start_time
        assert not task.processing_end_time
        with pytest.raises(Exception):
            task.complete()

        task2 = start_next_task(session, DeferredTaskType.INVENTORY_REPORT)
        assert task2
        assert task2.status == DeferredTaskStatus.PROCESSING
        assert task2.processing_start_time
        task2.complete()
        assert task2.processing_end_time
        with pytest.raises(Exception):
            task2.complete()

    def test_fail(self, db: DatabaseTransactionFixture):
        session = db.session
        task, is_new = queue_task(
            session, task_type=DeferredTaskType.INVENTORY_REPORT, data={}
        )

        assert task.status == DeferredTaskStatus.READY
        assert not task.processing_start_time
        assert not task.processing_end_time
        with pytest.raises(Exception):
            task.fail("details")

        task2 = start_next_task(session, DeferredTaskType.INVENTORY_REPORT)
        assert task2
        assert task2.status == DeferredTaskStatus.PROCESSING
        assert task2.processing_start_time
        task2.fail("details")
        with pytest.raises(Exception):
            task.fail("details")

        assert task.processing_end_time
        assert task2.status_details == "details"
        assert task2.status == DeferredTaskStatus.FAILURE
