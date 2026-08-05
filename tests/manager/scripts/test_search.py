from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from opensearchpy import OpenSearchException
from sqlalchemy.orm import Session

from palace.manager.scripts.search import RebuildSearchIndexScript
from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.service.redis.models.lock import (
    LockError,
    LockNotAcquired,
    TaskLock,
)
from palace.manager.sqlalchemy.model.work import Work
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
        redis_fixture: RedisFixture,
        services_fixture: ServicesFixture,
    ):
        # If we are called with the --delete argument, we clear the index before rebuilding.
        RebuildSearchIndexScript(db.session, cmd_args=["--delete"]).do_run()
        services_fixture.search_index.clear_search_documents.assert_called_once_with()
        mock_search_reindex.s.return_value.delay.assert_called_once_with()

        # The lock is given back before the task is queued, since the task has to take it.
        lock = TaskLock(redis_client=redis_fixture.client, lock_name="search_reindex")
        assert lock.locked() is False

    @pytest.mark.parametrize("blocking", [[], ["--blocking"]])
    @patch("palace.manager.scripts.search.search_reindex")
    def test_do_run_delete_leaves_the_index_alone_when_the_lock_is_held(
        self,
        mock_search_reindex: MagicMock,
        blocking: list[str],
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        services_fixture: ServicesFixture,
    ):
        # Emptying the index under a running reindex would leave reads served from an
        # empty index until that reindex finished refilling it, which is days away on the
        # managers this script is for. Both ways of running it wait for the lock instead.
        lock = TaskLock(redis_client=redis_fixture.client, lock_name="search_reindex")
        assert lock.acquire() is True

        with pytest.raises(LockNotAcquired):
            RebuildSearchIndexScript(
                db.session, cmd_args=["--delete", *blocking]
            ).do_run()

        services_fixture.search_index.clear_search_documents.assert_not_called()
        mock_search_reindex.s.return_value.delay.assert_not_called()

    def test_force_requires_blocking(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
    ):
        # A queued rebuild has no lock of its own to take, so --force without --blocking
        # would do nothing at all.
        with pytest.raises(SystemExit):
            RebuildSearchIndexScript(db.session, cmd_args=["--force"])

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

    def test_do_run_blocking_indexes_the_works_after_one_that_cannot_be_indexed(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # Work.to_search_documents drops a work whose document it cannot build, so a full
        # batch of works arrives as a short batch of documents. The rebuild pages by the
        # works it asked for, so it carries on to the works after the dropped one.
        works = [db.work(with_open_access_download=True) for _ in range(4)]

        to_search_documents = Work.to_search_documents
        dropped: list[int] = []

        def drop_one_document(
            session: Session, work_ids: Sequence[int]
        ) -> Sequence[dict[str, Any]]:
            documents = to_search_documents(session, work_ids)
            if not dropped and documents:
                dropped.append(documents[0]["_id"])
                return documents[1:]
            return documents

        # The document is lost from the first batch, so it cannot be the last one.
        with patch.object(Work, "to_search_documents", drop_one_document):
            RebuildSearchIndexScript(
                db.session, cmd_args=["--blocking", "--batch-size", "2"]
            ).do_run()

        assert len(dropped) == 1
        end_to_end_search_fixture.external_search.write_client.indices.refresh()
        end_to_end_search_fixture.expect_results(
            [work for work in works if work.id != dropped[0]], "", ordered=False
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

    def test_do_run_blocking_force_takes_the_lock_from_a_running_reindex(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # When the running reindex is the days-long one we are trying to get ahead of,
        # waiting for it is the wrong answer: --force takes the lock and rebuilds now.
        work = db.work(with_open_access_download=True)
        lock = TaskLock(redis_client=redis_fixture.client, lock_name="search_reindex")
        assert lock.acquire() is True

        RebuildSearchIndexScript(
            db.session, cmd_args=["--blocking", "--force"]
        ).do_run()

        # The rebuild ran, and the reindex it took the lock from is not holding it any
        # more, so it stops when it comes back for its next batch.
        assert lock.extend_timeout() is False
        end_to_end_search_fixture.external_search.write_client.indices.refresh()
        end_to_end_search_fixture.expect_results([work], "", ordered=False)

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

    @patch("palace.manager.scripts.search.exponential_backoff")
    def test_do_run_blocking_retries_a_failed_batch(
        self,
        mock_backoff: MagicMock,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # A rebuild has no offset to resume from, so a transient search failure would
        # cost the whole pass rather than the one batch it landed in.
        mock_backoff.return_value = 0
        work = db.work(with_open_access_download=True)

        with patch.object(
            ExternalSearchIndex, "add_documents", autospec=True
        ) as add_documents:
            # Rejected documents and a failed request are both worth another go.
            add_documents.side_effect = [[work.id], OpenSearchException(), None]
            RebuildSearchIndexScript(db.session, cmd_args=["--blocking"]).do_run()

        assert add_documents.call_count == 3

    @patch("palace.manager.scripts.search.exponential_backoff")
    def test_do_run_blocking_gives_up_on_a_batch_that_keeps_failing(
        self,
        mock_backoff: MagicMock,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # A search that is down rather than blipping ends the rebuild, with the failure
        # it ended on rather than one of our own.
        mock_backoff.return_value = 0
        db.work(with_open_access_download=True)

        with patch.object(
            ExternalSearchIndex, "add_documents", autospec=True
        ) as add_documents:
            add_documents.side_effect = OpenSearchException()
            with pytest.raises(OpenSearchException):
                RebuildSearchIndexScript(db.session, cmd_args=["--blocking"]).do_run()

        assert add_documents.call_count == 5

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
