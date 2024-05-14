from palace.manager.core.metadata_layer import TimestampData
from palace.manager.scripts.coverage_provider import RunWorkCoverageProviderScript
from palace.manager.scripts.timestamp import TimestampScript
from palace.manager.search.coverage_provider import SearchIndexCoverageProvider
from palace.manager.search.coverage_remover import RemovesSearchCoverage
from palace.manager.search.external_search import ExternalSearchIndex


class RebuildSearchIndexScript(RunWorkCoverageProviderScript, RemovesSearchCoverage):
    """Completely delete the search index and recreate it."""

    def __init__(self, *args, **kwargs):
        search = kwargs.pop("search_index_client", None)
        super().__init__(SearchIndexCoverageProvider, *args, **kwargs)
        self.search: ExternalSearchIndex = search or self.services.search.index()

    def do_run(self):
        self.search.clear_search_documents()

        # Remove all search coverage records so the
        # SearchIndexCoverageProvider will start from scratch.
        count = self.remove_search_coverage_records()
        self.log.info("Deleted %d search coverage records.", count)

        # Now let the SearchIndexCoverageProvider do its thing.
        return super().do_run()


class SearchIndexCoverageRemover(TimestampScript, RemovesSearchCoverage):
    """Script that removes search index coverage for all works.

    This guarantees the SearchIndexCoverageProvider will add
    fresh coverage for every Work the next time it runs.
    """

    def do_run(self):
        count = self.remove_search_coverage_records()
        return TimestampData(
            achievements="Coverage records deleted: %(deleted)d" % dict(deleted=count)
        )
