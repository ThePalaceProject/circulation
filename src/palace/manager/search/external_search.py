from __future__ import annotations

import time
from collections.abc import Sequence

from opensearchpy.helpers.query import (
    FunctionScore,
)

from palace.manager.search.filter import Filter
from palace.manager.search.query import JSONQuery, Query
from palace.manager.search.service import (
    SearchDocument,
    SearchService,
    SearchServiceFailedDocument,
)
from palace.manager.sqlalchemy.model.lane import Pagination
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.log import LoggerMixin


class ExternalSearchIndex(LoggerMixin):
    def __init__(
        self,
        service: SearchService,
    ) -> None:
        """Constructor"""
        self._search_service = service

    def search_service(self) -> SearchService:
        """Get the underlying search service."""
        return self._search_service

    def clear_search_documents(self) -> None:
        self._search_service.index_clear_documents()

    def create_search_doc(self, query_string, filter, pagination, debug):
        if filter and filter.search_type == "json":
            query = JSONQuery(query_string, filter)
        else:
            query = Query(query_string, filter)

        search = query.build(self._search_service.read_search_client(), pagination)
        if debug:
            search = search.extra(explain=True)

        if filter is not None and filter.min_score is not None:
            search = search.extra(min_score=filter.min_score)

        if debug:
            # Don't restrict the fields at all -- get everything.
            # This makes it easy to investigate everything about the
            # results we do get.
            fields = ["*"]
        else:
            # All we absolutely need is the work ID, which is a
            # key into the database, plus the values of any script fields,
            # which represent data not available through the database.
            fields = ["work_id"]
            if filter:
                fields += list(filter.script_fields.keys())

        # Change the Search object so it only retrieves the fields
        # we're asking for.
        if fields:
            search = search.source(fields)

        return search

    def query_works(self, query_string, filter=None, pagination=None, debug=False):
        """Run a search query.

        This works by calling query_works_multi().

        :param query_string: The string to search for.
        :param filter: A Filter object, used to filter out works that
            would otherwise match the query string.
        :param pagination: A Pagination object, used to get a subset
            of the search results.
        :param debug: If this is True, debugging information will
            be gathered and logged. The search query will ask
            Opensearch for all available fields, not just the
            fields known to be used by the feed generation code.  This
            all comes at a slight performance cost.
        :return: A list of Hit objects containing information about
            the search results. This will include the values of any
            script fields calculated by Opensearch during the
            search process.
        """
        if isinstance(filter, Filter) and filter.match_nothing is True:
            # We already know this search should match nothing.  We
            # don't even need to perform the search.
            return []

        pagination = pagination or Pagination.default()
        query_data = (query_string, filter, pagination)
        query_hits = self.query_works_multi([query_data], debug)
        if not query_hits:
            return []

        result_list = list(query_hits)
        if not result_list:
            return []

        return result_list[0]

    def query_works_multi(self, queries, debug=False):
        """Run several queries simultaneously and return the results
        as a big list.

        :param queries: A list of (query string, Filter, Pagination) 3-tuples,
            each representing an Opensearch query to be run.

        :yield: A sequence of lists, one per item in `queries`,
            each containing the search results from that
            (query string, Filter, Pagination) 3-tuple.
        """
        # Create a MultiSearch.
        multi = self._search_service.read_search_multi_client()

        # Give it a Search object for every query definition passed in
        # as part of `queries`.
        for query_string, filter, pagination in queries:
            search = self.create_search_doc(
                query_string, filter=filter, pagination=pagination, debug=debug
            )
            function_scores = filter.scoring_functions if filter else None
            if function_scores:
                function_score = FunctionScore(
                    query=dict(match_all=dict()),
                    functions=function_scores,
                    score_mode="sum",
                )
                search = search.query(function_score)
            multi = multi.add(search)

        a = time.time()
        # NOTE: This is the code that actually executes the OpenSearch
        # request.
        resultset = [x for x in multi.execute()]

        if debug:
            b = time.time()
            self.log.debug("Search query %r completed in %.3fsec", query_string, b - a)
            for results in resultset:
                for i, result in enumerate(results):
                    self.log.debug(
                        f'{i:2d} "{result.sort_title}" ({result.sort_author}) work={result.meta["id"]} '
                        f'score={(0 if not result.meta["score"] else result.meta["score"]):0.3f}',
                    )

        for i, results in enumerate(resultset):
            # Tell the Pagination object about the page that was just
            # 'loaded' so that Pagination.next_page will work.
            #
            # The pagination itself happened inside the Opensearch
            # server when the query ran.
            pagination.page_loaded(results)
            yield results

    def count_works(self, filter):
        """Instead of retrieving works that match `filter`, count the total."""
        if filter is not None and filter.match_nothing is True:
            # We already know that the filter should match nothing.
            # We don't even need to perform the count.
            return 0
        qu = self.create_search_doc(
            query_string=None, filter=filter, pagination=None, debug=False
        )
        return qu.count()

    def remove_work(self, work: Work | int) -> None:
        """Remove the search document for `work` from the search index."""
        if isinstance(work, Work):
            work = work.id

        self._search_service.index_remove_document(doc_id=work)

    def add_document(self, document: SearchDocument) -> None:
        """Add a document to the search index."""
        self._search_service.index_submit_document(document=document)

    def add_documents(
        self, documents: Sequence[SearchDocument]
    ) -> list[SearchServiceFailedDocument]:
        """Add multiple documents to the search index."""
        return self._search_service.index_submit_documents(documents=documents)
