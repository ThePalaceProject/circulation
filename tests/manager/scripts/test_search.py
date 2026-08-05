from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from palace.manager.scripts.search import RebuildSearchIndexScript
from palace.manager.service.redis.models.lock import (
    LockError,
    LockNotAcquired,
    TaskLock,
)
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture
from tests.fixtures.search import EndToEndSearchFixture
from tests.fixtures.services import ServicesFixture


class TestRebuildSearchIndexScript:
    @patch("palace.manager.scripts.search.search_reindex")
    def test_do_run_no_args(
        self,
        mock_search_reindex: MagicMock,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
    ):
        # If we are called with no arguments, we default to asynchronously rebuilding the search index.
        RebuildSearchIndexScript(db.session, cmd_args=[]).do_run()
        mock_search_reindex.s.return_value.delay.assert_called_once_with()
        mock_search_reindex.s.assert_called_once_with(batch_size=500)
        # But we don't delete the index before rebuilding.
        services_fixture.search_index.clear_search_documents.assert_not_called()

    @patch("palace.manager.scripts.search.search_reindex")
    def test_do_run_delete(
        self,
        mock_search_reindex: MagicMock,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
    ):
        # If we are called with the --delete argument, we clear the index before rebuilding.
        RebuildSearchIndexScript(db.session, cmd_args=["--delete"]).do_run()
        services_fixture.search_index.clear_search_documents.assert_called_once_with()
        mock_search_reindex.s.return_value.delay.assert_called_once_with()

    @pytest.mark.parametrize("batch_size", ["0", "-1"])
    def test_batch_size_must_be_at_least_one(
        self,
        batch_size: str,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
    ):
        # An empty batch never reaches the short batch that stops a rebuild, so both the
        # blocking loop and the requeued task would page forever holding the lock.
        with pytest.raises(SystemExit):
            RebuildSearchIndexScript(
                db.session, cmd_args=[f"--batch-size={batch_size}"]
            )

    @pytest.mark.parametrize("batch_size", ["2", "3", "500"])
    def test_do_run_blocking(
        self,
        batch_size: str,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # --blocking indexes everything in this process, rather than handing any of it
        # to Celery. The batch size is varied to exercise pagination, including the case
        # where a single batch covers every work.
        work1 = db.work(with_open_access_download=True)
        work2 = db.work(with_open_access_download=True)
        db.work(with_open_access_download=False)
        work4 = db.work(with_open_access_download=True)

        client = end_to_end_search_fixture.external_search.write_client
        client.indices.refresh()
        end_to_end_search_fixture.expect_results([], "")

        with patch("palace.manager.scripts.search.search_reindex") as mock_reindex:
            RebuildSearchIndexScript(
                db.session, cmd_args=["--blocking", "--batch-size", batch_size]
            ).do_run()

        # Nothing was queued.
        mock_reindex.s.assert_not_called()

        client.indices.refresh()
        end_to_end_search_fixture.expect_results(
            [work1, work2, work4], "", ordered=False
        )

    def test_do_run_blocking_advances_the_read_pointer(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # Having filled the index end to end, a blocking rebuild publishes it for reads,
        # which is what makes it a repair for a manager whose reads are stuck on an
        # older index.
        service = end_to_end_search_fixture.external_search.service
        client = end_to_end_search_fixture.external_search.write_client
        db.work(with_open_access_download=True)

        # Take the read pointer away, so reads are being served from nothing at all.
        client.indices.update_aliases(
            body={
                "actions": [
                    {"remove": {"index": "*", "alias": service.read_pointer_name()}}
                ]
            }
        )
        assert service.read_pointer() is None

        RebuildSearchIndexScript(db.session, cmd_args=["--blocking"]).do_run()

        read_pointer = service.read_pointer()
        assert read_pointer is not None
        assert (
            read_pointer.index
            == end_to_end_search_fixture.external_search.revision.name_for_index(
                service.base_revision_name
            )
        )

    def test_do_run_blocking_takes_the_reindex_lock(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # A blocking rebuild contends for the same lock as search_reindex, so it cannot
        # run alongside the scheduled reindex.
        lock = TaskLock(redis_client=redis_fixture.client, lock_name="search_reindex")
        lock.acquire()

        script = RebuildSearchIndexScript(db.session, cmd_args=["--blocking"])
        with pytest.raises(LockNotAcquired):
            script.do_run()

        lock.release()
        script.do_run()

    def test_do_run_blocking_stops_if_it_loses_the_lock(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # Once the lock is gone the scheduled reindex is free to start against the same
        # index, so a rebuild that can no longer extend it stops rather than carry on
        # indexing and publishing beside it.
        db.work(with_open_access_download=True)
        script = RebuildSearchIndexScript(db.session, cmd_args=["--blocking"])

        with patch.object(TaskLock, "extend_timeout", return_value=False):
            with pytest.raises(LockError, match="Lost the"):
                script.do_run()
