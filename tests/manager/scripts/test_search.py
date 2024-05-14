from __future__ import annotations

import random

from palace.manager.core.metadata_layer import TimestampData
from palace.manager.scripts.search import (
    RebuildSearchIndexScript,
    SearchIndexCoverageRemover,
)
from palace.manager.sqlalchemy.model.coverage import WorkCoverageRecord
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.search import ExternalSearchFixtureFake


class TestRebuildSearchIndexScript:
    def test_do_run(
        self,
        db: DatabaseTransactionFixture,
        external_search_fake_fixture: ExternalSearchFixtureFake,
    ):
        index = external_search_fake_fixture.external_search
        work = db.work(with_license_pool=True)
        work2 = db.work(with_license_pool=True)
        wcr = WorkCoverageRecord
        decoys = [wcr.QUALITY_OPERATION, wcr.SUMMARY_OPERATION]

        # Set up some coverage records.
        for operation in decoys + [wcr.UPDATE_SEARCH_INDEX_OPERATION]:
            for w in (work, work2):
                wcr.add_for(w, operation, status=random.choice(wcr.ALL_STATUSES))

        coverage_qu = db.session.query(wcr).filter(
            wcr.operation == wcr.UPDATE_SEARCH_INDEX_OPERATION
        )
        original_coverage = [x.id for x in coverage_qu]

        # Run the script.
        script = RebuildSearchIndexScript(db.session, search_index_client=index)
        [progress] = script.do_run()

        # The mock methods were called with the values we expect.
        assert {work.id, work2.id} == set(
            map(
                lambda d: d["_id"], external_search_fake_fixture.service.documents_all()
            )
        )

        # The script returned a list containing a single
        # CoverageProviderProgress object containing accurate
        # information about what happened (from the CoverageProvider's
        # point of view).
        assert (
            "Items processed: 2. Successes: 2, transient failures: 0, persistent failures: 0"
            == progress.achievements
        )

        # The old WorkCoverageRecords for the works were deleted. Then
        # the CoverageProvider did its job and new ones were added.
        new_coverage = [x.id for x in coverage_qu]
        assert 2 == len(new_coverage)
        assert set(new_coverage) != set(original_coverage)


class TestSearchIndexCoverageRemover:
    SERVICE_NAME = "Search Index Coverage Remover"

    def test_do_run(self, db: DatabaseTransactionFixture):
        work = db.work()
        work2 = db.work()
        wcr = WorkCoverageRecord
        decoys = [wcr.QUALITY_OPERATION, wcr.SUMMARY_OPERATION]

        # Set up some coverage records.
        for operation in decoys + [wcr.UPDATE_SEARCH_INDEX_OPERATION]:
            for w in (work, work2):
                wcr.add_for(w, operation, status=random.choice(wcr.ALL_STATUSES))

        # Run the script.
        script = SearchIndexCoverageRemover(db.session)
        result = script.do_run()
        assert isinstance(result, TimestampData)
        assert "Coverage records deleted: 2" == result.achievements

        # UPDATE_SEARCH_INDEX_OPERATION records have been removed.
        # No other records are affected.
        for w in (work, work2):
            remaining = [x.operation for x in w.coverage_records]
            assert sorted(remaining) == sorted(decoys)
