from __future__ import annotations

from core.coverage import CoverageFailure, WorkPresentationProvider
from core.model import Work, WorkCoverageRecord
from core.search.coverage_remover import RemovesSearchCoverage
from core.search.migrator import (
    SearchDocumentReceiver,
    SearchDocumentReceiverType,
    SearchMigrationInProgress,
)


class SearchIndexCoverageProvider(RemovesSearchCoverage, WorkPresentationProvider):
    """Make sure all Works have up-to-date representation in the
    search index.
    """

    SERVICE_NAME = "Search index coverage provider"

    DEFAULT_BATCH_SIZE = 500

    OPERATION = WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION

    def __init__(self, *args, **kwargs):
        search_index_client = kwargs.pop("search_index_client", None)
        super().__init__(*args, **kwargs)
        self.search_index_client = search_index_client or self.services.search.index()

        #
        # Try to migrate to the latest schema. If the function returns None, it means
        # that no migration is necessary, and we're already at the latest version. If
        # we're already at the latest version, then simply upload search documents instead.
        #
        self.receiver = None
        self.migration: None | (
            SearchMigrationInProgress
        ) = self.search_index_client.start_migration()
        if self.migration is None:
            self.receiver: SearchDocumentReceiver = (
                self.search_index_client.start_updating_search_documents()
            )
        else:
            # We do have a migration, we must clear out the index and repopulate the index
            self.remove_search_coverage_records()

    def on_completely_finished(self):
        # Tell the search migrator that no more documents are going to show up.
        target: SearchDocumentReceiverType = self.migration or self.receiver
        target.finish()

    def run_once_and_update_timestamp(self):
        # We do not catch exceptions here, so that the on_completely finished should not run
        # if there was a runtime error
        result = super().run_once_and_update_timestamp()
        self.on_completely_finished()
        return result

    def process_batch(self, works) -> list[Work | CoverageFailure]:
        target: SearchDocumentReceiverType = self.migration or self.receiver
        failures = target.add_documents(
            documents=self.search_index_client.create_search_documents_from_works(works)
        )

        # Maintain a dictionary of works so that we can efficiently remove failed works later.
        work_map: dict[int, Work] = {}
        for work in works:
            work_map[work.id] = work

        # Remove all the works that failed and create failure records for them.
        results: list[Work | CoverageFailure] = []
        for failure in failures:
            work = work_map[failure.id]
            del work_map[failure.id]
            results.append(CoverageFailure(work, repr(failure)))

        # Append all the remaining works that didn't fail.
        for work in work_map.values():
            results.append(work)

        return results
