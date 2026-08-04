from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from palace.manager.scripts.search import RebuildSearchIndexScript
from palace.manager.service.redis.models.lock import LockNotAcquired, TaskLock
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
