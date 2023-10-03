from __future__ import annotations

import contextlib
import datetime
import json
import logging
import re
import time
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

from attr import define
from flask_babel import lazy_gettext as _
from opensearch_dsl import SF, Search
from opensearch_dsl.query import (
    Bool,
    DisMax,
    Exists,
    FunctionScore,
    Match,
    MatchAll,
    MatchNone,
    MatchPhrase,
    MultiMatch,
    Nested,
)
from opensearch_dsl.query import Query as BaseQuery
from opensearch_dsl.query import Range, Regexp, Term, Terms
from opensearchpy import OpenSearch
from spellchecker import SpellChecker

from core.classifier import (
    AgeClassifier,
    Classifier,
    GradeLevelClassifier,
    KeywordBasedClassifier,
)
from core.config import CannotLoadConfiguration
from core.coverage import CoverageFailure, WorkPresentationProvider
from core.facets import FacetConstants
from core.lane import Pagination
from core.metadata_layer import IdentifierData
from core.model import (
    Collection,
    ConfigurationSetting,
    Contributor,
    DataSource,
    Edition,
    ExternalIntegration,
    Identifier,
    Library,
    Work,
    WorkCoverageRecord,
    numericrange_to_tuple,
)
from core.problem_details import INVALID_INPUT
from core.search.coverage_remover import RemovesSearchCoverage
from core.search.migrator import (
    SearchDocumentReceiver,
    SearchDocumentReceiverType,
    SearchMigrationInProgress,
    SearchMigrator,
)
from core.search.revision import SearchSchemaRevision
from core.search.revision_directory import SearchRevisionDirectory
from core.search.service import SearchService, SearchServiceOpensearch1
from core.selftest import HasSelfTests
from core.util import Values
from core.util.cache import CachedData
from core.util.datetime_helpers import from_timestamp
from core.util.languages import LanguageNames
from core.util.personal_names import display_name_to_sort_name
from core.util.problem_detail import ProblemDetail
from core.util.stopwords import ENGLISH_STOPWORDS


@contextlib.contextmanager
def mock_search_index(mock=None):
    """Temporarily mock the ExternalSearchIndex implementation
    returned by the load() class method.
    """
    try:
        ExternalSearchIndex.MOCK_IMPLEMENTATION = mock
        yield mock
    finally:
        ExternalSearchIndex.MOCK_IMPLEMENTATION = None


class ExternalSearchIndex(HasSelfTests):
    NAME = ExternalIntegration.OPENSEARCH

    # A test may temporarily set this to a mock of this class.
    # While that's true, load() will return the mock instead of
    # instantiating new ExternalSearchIndex objects.
    MOCK_IMPLEMENTATION = None

    WORKS_INDEX_PREFIX_KEY = "works_index_prefix"
    DEFAULT_WORKS_INDEX_PREFIX = "circulation-works"

    TEST_SEARCH_TERM_KEY = "a search term"
    DEFAULT_TEST_SEARCH_TERM = "test"
    CURRENT_ALIAS_SUFFIX = "current"

    SETTINGS = [
        {
            "key": ExternalIntegration.URL,
            "label": _("URL"),
            "required": True,
            "format": "url",
        },
        {
            "key": WORKS_INDEX_PREFIX_KEY,
            "label": _("Index prefix"),
            "default": DEFAULT_WORKS_INDEX_PREFIX,
            "required": True,
            "description": _(
                "Any Search indexes needed for this application will be created with this unique prefix. In most cases, the default will work fine. You may need to change this if you have multiple application servers using a single Search server."
            ),
        },
        {
            "key": TEST_SEARCH_TERM_KEY,
            "label": _("Test search term"),
            "default": DEFAULT_TEST_SEARCH_TERM,
            "description": _("Self tests will use this value as the search term."),
        },
    ]

    SITEWIDE = True

    @classmethod
    def search_integration(cls, _db) -> Optional[ExternalIntegration]:
        """Look up the ExternalIntegration for Opensearch."""
        return ExternalIntegration.lookup(
            _db, ExternalIntegration.OPENSEARCH, goal=ExternalIntegration.SEARCH_GOAL
        )

    @classmethod
    def load(cls, _db, *args, **kwargs):
        """Load a generic implementation."""
        if cls.MOCK_IMPLEMENTATION:
            return cls.MOCK_IMPLEMENTATION
        return cls(_db, *args, **kwargs)

    _bulk: Callable[..., Any]
    _revision: SearchSchemaRevision
    _revision_base_name: str
    _revision_directory: SearchRevisionDirectory
    _search: Search
    _search_migrator: SearchMigrator
    _search_service: SearchService
    _search_read_pointer: str
    _test_search_term: str

    def __init__(
        self,
        _db,
        url: Optional[str] = None,
        test_search_term: Optional[str] = None,
        revision_directory: Optional[SearchRevisionDirectory] = None,
        version: Optional[int] = None,
        custom_client_service: Optional[SearchService] = None,
    ):
        """Constructor

        :param revision_directory Override the directory of revisions that will be used. If this isn't provided,
               the default directory will be used.
        :param version The specific revision that will be used. If not specified, the highest version in the
               revision directory will be used.
        """
        self.log = logging.getLogger("External search index")

        # We can't proceed without a database.
        if not _db:
            raise CannotLoadConfiguration(
                "Cannot load Search configuration without a database.",
            )

        # Load the search integration.
        integration = self.search_integration(_db)
        if not integration:
            raise CannotLoadConfiguration("No search integration configured.")

        if not url:
            url = url or integration.url
            test_search_term = integration.setting(self.TEST_SEARCH_TERM_KEY).value

        self._test_search_term = test_search_term or self.DEFAULT_TEST_SEARCH_TERM

        if not url:
            raise CannotLoadConfiguration("No URL configured to the search server.")

        # Determine the base name we're going to use for storing revisions.
        self._revision_base_name = integration.setting(
            ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY
        ).value

        # Create the necessary search client, and the service used by the schema migrator.
        if custom_client_service:
            self._search_service = custom_client_service
        else:
            use_ssl = url.startswith("https://")
            self.log.info("Connecting to the search cluster at %s", url)
            new_client = OpenSearch(url, use_ssl=use_ssl, timeout=20, maxsize=25)
            self._search_service = SearchServiceOpensearch1(
                new_client, self._revision_base_name
            )

        # Locate the revision of the search index that we're going to use.
        # This will fail fast if the requested version isn't available.
        self._revision_directory = (
            revision_directory or SearchRevisionDirectory.create()
        )
        if version:
            self._revision = self._revision_directory.find(version)
        else:
            self._revision = self._revision_directory.highest()

        # initialize the cached data if not already done so
        CachedData.initialize(_db)

        # Get references to the read and write pointers.
        self._search_read_pointer = self._search_service.read_pointer_name()
        self._search_write_pointer = self._search_service.write_pointer_name()

    def search_service(self) -> SearchService:
        """Get the underlying search service."""
        return self._search_service

    def start_migration(self) -> Optional[SearchMigrationInProgress]:
        """Update to the latest schema, indexing the given works."""
        migrator = SearchMigrator(
            revisions=self._revision_directory,
            service=self._search_service,
        )
        return migrator.migrate(
            base_name=self._revision_base_name, version=self._revision.version
        )

    def start_updating_search_documents(self) -> SearchDocumentReceiver:
        """Start submitting search documents for whatever is the current write pointer."""
        return SearchDocumentReceiver(
            pointer=self._search_write_pointer, service=self._search_service
        )

    def clear_search_documents(self) -> None:
        self._search_service.index_clear_documents(pointer=self._search_write_pointer)

    def prime_query_values(self, _db):
        JSONQuery.data_sources = _db.query(DataSource).all()

    def create_search_doc(self, query_string, filter, pagination, debug):
        if filter and filter.search_type == "json":
            query = JSONQuery(query_string, filter)
        else:
            query = Query(query_string, filter)

        search = query.build(self._search_service.search_client(), pagination)
        if debug:
            search = search.extra(explain=True)

        if filter is not None and filter.min_score is not None:
            search = search.extra(min_score=filter.min_score)

        fields = None
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
        multi = self._search_service.search_multi_client()

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
                        '%02d "%s" (%s) work=%s score=%.3f shard=%s',
                        i,
                        result.sort_title,
                        result.sort_author,
                        result.meta["id"],
                        result.meta.explanation["value"] or 0,
                        result.meta["shard"],
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

    def create_search_documents_from_works(
        self, works: Iterable[Work]
    ) -> Iterable[dict]:
        """Create search documents for all the given works."""
        if not works:
            # There's nothing to do. Don't bother making any requests
            # to the search index.
            return []

        time1 = time.time()
        needs_add = []
        for work in works:
            needs_add.append(work)

        # Add/update any works that need adding/updating.
        docs = Work.to_search_documents(needs_add)
        time2 = time.time()

        self.log.info(
            "Created %i search documents in %.2f seconds" % (len(docs), time2 - time1)
        )
        return docs

    def remove_work(self, work):
        """Remove the search document for `work` from the search index."""
        self._search_service.index_remove_document(
            pointer=self._search_read_pointer, id=work.id
        )

    def _run_self_tests(self, _db):
        # Helper methods for setting up the self-tests:

        def _search():
            return self.create_search_doc(
                self._test_search_term, filter=None, pagination=None, debug=True
            )

        def _works():
            return self.query_works(
                self._test_search_term, filter=None, pagination=None, debug=True
            )

        # The self-tests:

        def _search_for_term():
            titles = [(f"{x.sort_title} ({x.sort_author})") for x in _works()]
            return titles

        yield self.run_test(
            ("Search results for '%s':" % self._test_search_term), _search_for_term
        )

        def _get_raw_doc():
            search = _search()
            return json.dumps(search.to_dict(), indent=1)

        yield self.run_test(
            ("Search document for '%s':" % (self._test_search_term)), _get_raw_doc
        )

        def _get_raw_results():
            return [json.dumps(x.to_dict(), indent=1) for x in _works()]

        yield self.run_test(
            ("Raw search results for '%s':" % (self._test_search_term)),
            _get_raw_results,
        )

        def _count_docs():
            service = self.search_service()
            client = service.search_client()
            return str(client.count())

        yield self.run_test(
            ("Total number of search results for '%s':" % (self._test_search_term)),
            _count_docs,
        )

        def _total_count():
            return str(self.count_works(None))

        yield self.run_test(
            "Total number of documents in this search index:", _total_count
        )

        def _collections():
            result = {}

            collections = _db.query(Collection)
            for collection in collections:
                filter = Filter(collections=[collection])
                result[collection.name] = self.count_works(filter)

            return json.dumps(result, indent=1)

        yield self.run_test("Total number of documents per collection:", _collections)

    def initialize_indices(self) -> bool:
        """Attempt to initialize the indices and pointers for a first time run"""
        service = self.search_service()
        read_pointer = service.read_pointer()
        if not read_pointer or service.is_pointer_empty(read_pointer):
            # A read pointer does not exist, or points to the empty index
            # This means either this is a new deployment or the first time
            # the new opensearch code was deployed.
            # In both cases doing a migration to the latest version is safe.
            migration = self.start_migration()
            if migration is not None:
                migration.finish()
            else:
                self.log.warning(
                    "Read pointer was set to empty, but no migration was available."
                )
                return False

        return True


class SearchBase:
    """A superclass containing helper methods for creating and modifying
    opensearch-dsl Query-type objects.
    """

    @classmethod
    def _boost(cls, boost, queries, filters=None, all_must_match=False):
        """Boost a query by a certain amount relative to its neighbors in a
        dis_max query.

        :param boost: Numeric value to boost search results that
           match `queries`.
        :param queries: One or more Query objects to use in a query context.
        :param filter: A Query object to use in a filter context.
        :param all_must_match: If this is False (the default), then only
           one of the `queries` must match for a search result to get
           the boost. If this is True, then all `queries` must match,
           or the boost will not apply.
        """
        filters = filters or []
        if isinstance(queries, BaseQuery):
            queries = [queries]

        if all_must_match or len(queries) == 1:
            # Every one of the subqueries in `queries` must match.
            # (If there's only one subquery, this simplifies the
            # final query slightly.)
            kwargs = dict(must=queries)
        else:
            # At least one of the queries in `queries` must match.
            kwargs = dict(should=queries, minimum_should_match=1)
        query = Bool(boost=float(boost), filter=filters, **kwargs)
        return query

    @classmethod
    def _nest(cls, subdocument, query):
        """Turn a normal query into a nested query.

        This is a helper method for a helper method; you should
        probably use _nestable() instead.

        :param subdocument: The name of the subdocument to query
        against, e.g. "contributors".

        :param query: An opensearch-dsl Query object (not the Query
        objects defined by this class).
        """
        return Nested(path=subdocument, query=query)

    @classmethod
    def _nestable(cls, field, query):
        """Make a query against a field nestable, if necessary."""
        if "s." in field:
            # This is a query against a field from a subdocument. We
            # can't run it against the top-level document; it has to
            # be run in the context of its subdocument.
            subdocument = field.split(".", 1)[0]
            query = cls._nest(subdocument, query)
        return query

    @classmethod
    def _match_term(cls, field, query_string):
        """A clause that matches the query string against a specific field in
        the search document.
        """
        match_query = Term(**{field: query_string})
        return cls._nestable(field, match_query)

    @classmethod
    def _match_range(cls, field, operation, value):
        """Match a ranged value for a field, using an operation other than
        equality.

        e.g. _match_range("field.name", "gte", 5) will match
        any value for field.name greater than 5.
        """
        match = {field: {operation: value}}
        return dict(range=match)

    @classmethod
    def make_target_age_query(cls, target_age, boost=1.1):
        """Create an Opensearch query object for a boolean query that
        matches works whose target ages overlap (partially or
        entirely) the given age range.

        :param target_age: A 2-tuple (lower limit, upper limit)
        :param boost: Boost works that fit precisely into the target
           age range by this amount, vis-a-vis works that don't.
        """
        (lower, upper) = target_age[0], target_age[1]
        # There must be _some_ overlap with the provided range.
        must = [
            cls._match_range("target_age.upper", "gte", lower),
            cls._match_range("target_age.lower", "lte", upper),
        ]

        # Results with ranges contained within the query range are
        # better.
        # e.g. for query 4-6, a result with 5-6 beats 6-7
        should = [
            cls._match_range("target_age.upper", "lte", upper),
            cls._match_range("target_age.lower", "gte", lower),
        ]
        filter_version = Bool(must=must)
        query_version = Bool(must=must, should=should, boost=float(boost))
        return filter_version, query_version

    @classmethod
    def _combine_hypotheses(self, hypotheses):
        """Build an Opensearch Query object that tests a number
        of hypotheses at once.

        :return: A DisMax query if there are hypotheses to be tested;
        otherwise a MatchAll query.
        """
        if hypotheses:
            qu = DisMax(queries=hypotheses)
        else:
            # We ended up with no hypotheses. Match everything.
            qu = MatchAll()
        return qu


class Query(SearchBase):
    """An attempt to find something in the search index."""

    # This dictionary establishes the relative importance of the
    # fields that someone might search for. These weights are used
    # directly -- an exact title match has a higher weight than an
    # exact author match. They are also used as the basis for other
    # weights: the weight of a fuzzy match for a given field is in
    # proportion to the weight of a non-fuzzy match for that field.
    WEIGHT_FOR_FIELD = dict(
        title=140.0,
        subtitle=130.0,
        series=120.0,
        author=120.0,
        summary=80.0,
        publisher=40.0,
        imprint=40.0,
    )
    # The contributor names in the contributors sub-document have the
    # same weight as the 'author' field in the main document.
    for field in ["contributors.sort_name", "contributors.display_name"]:
        WEIGHT_FOR_FIELD[field] = WEIGHT_FOR_FIELD["author"]

    # When someone searches for a person's name, they're most likely
    # searching for that person's contributions in one of these roles.
    SEARCH_RELEVANT_ROLES = [
        Contributor.PRIMARY_AUTHOR_ROLE,
        Contributor.AUTHOR_ROLE,
        Contributor.NARRATOR_ROLE,
    ]

    # If the entire search query is turned into a filter, all works
    # that match the filter will be given this weight.
    #
    # This is very high, but not high enough to outweigh e.g. an exact
    # title match.
    QUERY_WAS_A_FILTER_WEIGHT = 600

    # A keyword match is the best type of match we can get -- the
    # patron typed in a near-exact match for one of the fields.
    #
    # That said, this is a coefficient, not a weight -- a keyword
    # title match is better than a keyword subtitle match, etc.
    DEFAULT_KEYWORD_MATCH_COEFFICIENT = 1000

    # Normally we weight keyword matches very highly, but for
    # publishers and imprints, where a keyword match may also be a
    # partial author match ("Plympton") or topic match ("Penguin"), we
    # weight them much lower -- the author or topic is probably more
    # important.
    #
    # Again, these are coefficients, not weights. A keyword publisher
    # match is better than a keyword imprint match, even though they have
    # the same keyword match coefficient.
    KEYWORD_MATCH_COEFFICIENT_FOR_FIELD = dict(
        publisher=2,
        imprint=2,
    )

    # A normal coefficient for a normal sort of match.
    BASELINE_COEFFICIENT = 1

    # There are a couple places where we want to boost a query just
    # slightly above baseline.
    SLIGHTLY_ABOVE_BASELINE = 1.1

    # For each of these fields, we're going to test the hypothesis
    # that the query string is nothing but an attempt to match this
    # field.
    SIMPLE_MATCH_FIELDS = ["title", "subtitle", "series", "publisher", "imprint"]

    # For each of these fields, we're going to test the hypothesis
    # that the query string contains words from the book's title
    # _plus_ words from this field.
    #
    # Note that here we're doing an author query the cheap way, by
    # looking at the .author field -- the display name of the primary
    # author associated with the Work's presentation Editon -- not
    # the .display_names in the 'contributors' subdocument.
    MULTI_MATCH_FIELDS = ["subtitle", "series", "author"]

    # For each of these fields, we're going to test the hypothesis
    # that the query string is a good match for an aggressively
    # stemmed version of this field.
    STEMMABLE_FIELDS = ["title", "subtitle", "series"]

    # Although we index all text fields using an analyzer that
    # preserves stopwords, these are the only fields where we
    # currently think it's worth testing a hypothesis that stopwords
    # in a query string are _important_.
    STOPWORD_FIELDS = ["title", "subtitle", "series"]

    # SpellChecker is expensive to initialize, so keep around
    # a class-level instance.
    SPELLCHECKER = SpellChecker()

    def __init__(self, query_string, filter=None, use_query_parser=True):
        """Store a query string and filter.

        :param query_string: A user typed this string into a search box.
        :param filter: A Filter object representing the circumstances
            of the search -- for example, maybe we are searching within
            a specific lane.

        :param use_query_parser: Should we try to parse filter
            information out of the query string? Or did we already try
            that, and this constructor is being called recursively, to
            build a subquery from the _remaining_ portion of a larger
            query string?
        """
        self.query_string = query_string or ""
        self.filter = filter
        self.use_query_parser = use_query_parser

        # Pre-calculate some values that will be checked frequently
        # when generating the opensearch-dsl query.

        # Check if the string contains English stopwords.
        if query_string:
            self.words = query_string.split()
        else:
            self.words = []
        self.contains_stopwords = query_string and any(
            word in ENGLISH_STOPWORDS for word in self.words
        )

        # Determine how heavily to weight fuzzy hypotheses.
        #
        # The "fuzzy" version of a hypothesis tests the idea that
        # someone meant to trigger the original hypothesis, but they
        # made a typo.
        #
        # The strength of a fuzzy hypothesis is always lower than the
        # non-fuzzy version of the same hypothesis.
        #
        # Depending on the query, the stregnth of a fuzzy hypothesis
        # may be reduced even further -- that's determined here.
        if self.words:
            if self.SPELLCHECKER.unknown(self.words):
                # Spell check failed. This is the default behavior, if
                # only because peoples' names will generally fail spell
                # check. Fuzzy queries will be given their full weight.
                self.fuzzy_coefficient = 1.0
            else:
                # Everything seems to be spelled correctly. But sometimes
                # a word can be misspelled as another word, e.g. "came" ->
                # "cane", or a name may be misspelled as a word. We'll
                # still check the fuzzy hypotheses, but we can improve
                # results overall by giving them only half their normal
                # strength.
                self.fuzzy_coefficient = 0.5
        else:
            # Since this query does not contain any words, there is no
            # risk that a word might be misspelled. Do not create or
            # run the 'fuzzy' hypotheses at all.
            self.fuzzy_coefficient = 0

    def build(self, opensearch, pagination=None):
        """Make an opensearch-dsl Search object out of this query.

        :param opensearch: An opensearch-dsl Search object. This
            object is ready to run a search against an Opensearch server,
            but it doesn't represent any particular Opensearch query.

        :param pagination: A Pagination object indicating a slice of
            results to pull from the search index.

        :return: An opensearch-dsl Search object that's prepared
            to run this specific query.
        """
        query = self.search_query
        nested_filters = defaultdict(list)

        # Convert the resulting Filter into two objects -- one
        # describing the base filter and one describing the nested
        # filters.
        if self.filter:
            base_filter, nested_filters = self.filter.build()
        else:
            base_filter = None
            nested_filters = defaultdict(list)

        # Combine the query's base Filter with the universal base
        # filter -- works must be presentation-ready, etc.
        universal_base_filter = Filter.universal_base_filter()
        if universal_base_filter:
            query_filter = Filter._chain_filters(base_filter, universal_base_filter)
        else:
            query_filter = base_filter
        if query_filter:
            query = Bool(must=query, filter=query_filter)

        # We now have an opensearch-dsl Query object (which isn't
        # tied to a specific server). Turn it into a Search object
        # (which is).
        search = opensearch.query(query)

        # Now update the 'nested filters' dictionary with the
        # universal nested filters -- no suppressed license pools,
        # etc.
        universal_nested_filters = Filter.universal_nested_filters() or {}
        for key, values in list(universal_nested_filters.items()):
            nested_filters[key].extend(values)

        # Now we can convert any nested filters (universal or
        # otherwise) into nested queries.
        for path, subfilters in list(nested_filters.items()):
            for subfilter in subfilters:
                # This ensures that the filter logic is executed in
                # filter context rather than query context.
                subquery = Bool(filter=subfilter)
                search = search.filter(
                    name_or_query="nested", path=path, query=subquery
                )

        if self.filter:
            # Apply any necessary sort order.
            order_fields = self.filter.sort_order
            if order_fields:
                search = search.sort(*order_fields)

            # Add any necessary script fields.
            script_fields = self.filter.script_fields
            if script_fields:
                search = search.script_fields(**script_fields)
        # Apply any necessary query restrictions imposed by the
        # Pagination object. This may happen through modification or
        # by returning an entirely new Search object.
        if pagination:
            result = pagination.modify_search_query(search)
            if result is not None:
                search = result

        # All done!
        return search

    @property
    def search_query(self):
        """Build an opensearch-dsl Query object for this query string."""

        # The query will most likely be a dis_max query, which tests a
        # number of hypotheses about what the query string might
        # 'really' mean. For each book, the highest-rated hypothesis
        # will be assumed to be true, and the highest-rated titles
        # overall will become the search results.
        hypotheses = []

        if not self.query_string:
            # There is no query string. Match everything.
            return MatchAll()

        # Here are the hypotheses:

        # The query string might be a match against a single field:
        # probably title or series. These are the most common
        # searches.
        for field in self.SIMPLE_MATCH_FIELDS:
            for qu, weight in self.match_one_field_hypotheses(field):
                self._hypothesize(hypotheses, qu, weight)

        # As a coda to the above, the query string might be a match
        # against author. This is the same idea, but it's a little
        # more complicated because a book can have multiple
        # contributors and we're only interested in certain roles
        # (such as 'narrator').
        for qu, weight in self.match_author_hypotheses:
            self._hypothesize(hypotheses, qu, weight)

        # The query string may be looking for a certain topic or
        # subject matter.
        for qu, weight in self.match_topic_hypotheses:
            self._hypothesize(hypotheses, qu, weight)

        # The query string might *combine* terms from the title with
        # terms from some other major field -- probably author name.
        for other_field in self.MULTI_MATCH_FIELDS:
            # The weight of this hypothesis should be proportionate to
            # the difference between a pure match against title, and a
            # pure match against the field we're checking.
            for multi_match, weight in self.title_multi_match_for(other_field):
                self._hypothesize(hypotheses, multi_match, weight)

        # Finally, the query string might contain a filter portion
        # (e.g. a genre name or target age), with the remainder being
        # the "real" query string.
        #
        # In a query like "nonfiction asteroids", "nonfiction" would
        # be the filter portion and "asteroids" would be the query
        # portion.
        #
        # The query portion, if any, is turned into a set of
        # sub-hypotheses. We then hypothesize that we might filter out
        # a lot of junk by applying the filter and running the
        # sub-hypotheses against the filtered set of books.
        #
        # In other words, we should try searching across nonfiction
        # for "asteroids", and see if it gets better results than
        # searching for "nonfiction asteroids" in the text fields
        # (which it will).
        if self.use_query_parser:
            sub_hypotheses, filters = self.parsed_query_matches
            if sub_hypotheses or filters:
                if not sub_hypotheses:
                    # The entire search string was converted into a
                    # filter (e.g. "young adult romance"). Everything
                    # that matches this filter should be matched, and
                    # it should be given a relatively high boost.
                    sub_hypotheses = MatchAll()
                    boost = self.QUERY_WAS_A_FILTER_WEIGHT
                else:
                    # Part of the search string is a filter, and part
                    # of it is a bunch of hypotheses that combine with
                    # the filter to match the entire query
                    # string. We'll boost works that match the filter
                    # slightly, but overall the goal here is to get
                    # better results by filtering out junk.
                    boost = self.SLIGHTLY_ABOVE_BASELINE
                self._hypothesize(
                    hypotheses,
                    sub_hypotheses,
                    boost,
                    all_must_match=True,
                    filters=filters,
                )

        # That's it!

        # The score of any given book is the maximum score it gets from
        # any of these hypotheses.
        return self._combine_hypotheses(hypotheses)

    def match_one_field_hypotheses(self, base_field, query_string=None):
        """Yield a number of hypotheses representing different ways in
        which the query string might be an attempt to match
        a given field.

        :param base_field: The name of the field to search,
            e.g. "title" or "contributors.sort_name".

        :param query_string: The query string to use, if different from
            self.query_string.

        :yield: A sequence of (hypothesis, weight) 2-tuples.
        """
        # All hypotheses generated by this method will be weighted
        # relative to the standard weight for the field being checked.
        #
        # The final weight will be this field weight * a coefficient
        # determined by the type of match * a (potential) coefficient
        # associated with a fuzzy match.
        base_weight = self.WEIGHT_FOR_FIELD[base_field]

        query_string = query_string or self.query_string

        keyword_match_coefficient = self.KEYWORD_MATCH_COEFFICIENT_FOR_FIELD.get(
            base_field, self.DEFAULT_KEYWORD_MATCH_COEFFICIENT
        )

        fields = [
            # A keyword match means the field value is a near-exact
            # match for the query string. This is one of the best
            # search results we can possibly return.
            ("keyword", keyword_match_coefficient, Term),
            # This is the baseline query -- a phrase match against a
            # single field. Most queries turn out to represent
            # consecutive words from a single field.
            ("minimal", self.BASELINE_COEFFICIENT, MatchPhrase),
        ]

        if self.contains_stopwords and base_field in self.STOPWORD_FIELDS:
            # The query might benefit from a phrase match against an
            # index of this field that includes the stopwords.
            #
            # Boost this slightly above the baseline so that if
            # it matches, it'll beat out baseline queries.
            fields.append(("with_stopwords", self.SLIGHTLY_ABOVE_BASELINE, MatchPhrase))

        if base_field in self.STEMMABLE_FIELDS:
            # This query might benefit from a non-phrase Match against
            # a stemmed version of this field. This handles less
            # common cases where search terms are in the wrong order,
            # or where only the stemmed version of a word is a match.
            #
            # This hypothesis is run at a disadvantage relative to
            # baseline.
            fields.append((None, self.BASELINE_COEFFICIENT * 0.75, Match))

        for subfield, match_type_coefficient, query_class in fields:
            if subfield:
                field_name = base_field + "." + subfield
            else:
                field_name = base_field

            field_weight = base_weight * match_type_coefficient

            # Here's what minimum_should_match=2 does:
            #
            # If a query string has two or more words, at least two of
            # those words must match to trigger a Match
            # hypothesis. This prevents "Foo" from showing up as a top
            # result for "foo bar": you have to explain why they typed
            # "bar"!
            #
            # But if there are three words in the search query and
            # only two of them match, it may be the best we can
            # do. That's why we don't set minimum_should_match any
            # higher.
            standard_match_kwargs = dict(
                query=self.query_string,
                minimum_should_match=2,
            )
            if query_class == Match:
                kwargs = {field_name: standard_match_kwargs}
            else:
                # If we're doing a Term or MatchPhrase query,
                # minimum_should_match is not relevant -- we just need
                # to provide the query string.
                kwargs = {field_name: self.query_string}
            qu = query_class(**kwargs)
            yield qu, field_weight

            if self.fuzzy_coefficient and subfield == "minimal":
                # Trying one or more fuzzy versions of this hypothesis
                # would also be appropriate. We only do fuzzy searches
                # on the subfield with minimal stemming, because we
                # want to check against something close to what the
                # patron actually typed.
                for fuzzy_match, fuzzy_query_coefficient in self._fuzzy_matches(
                    field_name, **standard_match_kwargs
                ):
                    yield fuzzy_match, (field_weight * fuzzy_query_coefficient)

    @property
    def match_author_hypotheses(self):
        """Yield a sequence of query objects representing possible ways in
        which a query string might represent a book's author.

        :param query_string: The query string that might be the name
            of an author.

        :yield: A sequence of opensearch-dsl query objects to be
            considered as hypotheses.
        """

        # Ask Opensearch to match what was typed against
        # contributors.display_name.
        yield from self._author_field_must_match("display_name", self.query_string)

        # Although almost nobody types a sort name into a search box,
        # they may copy-and-paste one. Furthermore, we may only know
        # some contributors by their sort name.  Try to convert what
        # was typed into a sort name, and ask Opensearch to match
        # that against contributors.sort_name.
        sort_name = display_name_to_sort_name(self.query_string)
        if sort_name:
            yield from self._author_field_must_match("sort_name", sort_name)

    def _author_field_must_match(self, base_field, query_string=None):
        """Yield queries that match either the keyword or minimally stemmed
        version of one of the fields in the contributors sub-document.

        The contributor must also have an appropriate authorship role.

        :param base_field: The base name of the contributors field to
        match -- probably either 'display_name' or 'sort_name'.

        :param must_match: The query string to match against.
        """
        query_string = query_string or self.query_string
        field_name = "contributors.%s" % base_field
        for author_matches, weight in self.match_one_field_hypotheses(
            field_name, query_string
        ):
            yield self._role_must_also_match(author_matches), weight

    @classmethod
    def _role_must_also_match(cls, base_query):
        """Modify a query to add a restriction against the contributors
        sub-document, so that it also matches an appropriate role.

        NOTE: We can get fancier here by yielding several
        differently-weighted hypotheses that weight Primary Author
        higher than Author, and Author higher than Narrator. However,
        in practice this dramatically slows down searches without
        greatly improving results.

        :param base_query: An opensearch-dsl query object to use
           when adding restrictions.
        :param base_score: The relative score of the base query. The resulting
           hypotheses will be weighted based on this score.
        :return: A modified hypothesis.

        """
        match_role = Terms(**{"contributors.role": cls.SEARCH_RELEVANT_ROLES})
        match_both = Bool(must=[base_query, match_role])
        return cls._nest("contributors", match_both)

    @property
    def match_topic_hypotheses(self):
        """Yield a number of hypotheses representing different
        ways in which the query string might be a topic match.

        Currently there is only one such hypothesis.

        TODO: We probably want to introduce a fuzzy version of this
        hypothesis.
        """
        # Note that we are using the default analyzer, which gives us
        # the stemmed versions of these fields.
        qu = MultiMatch(
            query=self.query_string,
            fields=["summary", "classifications.term"],
            type="best_fields",
        )
        yield qu, self.WEIGHT_FOR_FIELD["summary"]

    def title_multi_match_for(self, other_field):
        """Helper method to create a MultiMatch hypothesis that crosses
        multiple fields.

        This strategy only works if everything is spelled correctly,
        since we can't combine a "cross_fields" Multimatch query
        with a fuzzy search.

        :yield: At most one (hypothesis, weight) 2-tuple.
        """
        if len(self.words) < 2:
            # To match two different fields we need at least two
            # words. We don't have that, so there's no point in even
            # making this hypothesis.
            return

        # We only search the '.minimal' variants of these fields.
        field_names = ["title.minimal", other_field + ".minimal"]

        # The weight of this hypothesis should be somewhere between
        # the weight of a pure title match, and the weight of a pure
        # match against the field we're checking.
        title_weight = self.WEIGHT_FOR_FIELD["title"]
        other_weight = self.WEIGHT_FOR_FIELD[other_field]
        combined_weight = other_weight * (other_weight / title_weight)

        hypothesis = MultiMatch(
            query=self.query_string,
            fields=field_names,
            type="cross_fields",
            # This hypothesis must be able to explain the entire query
            # string. Otherwise the weight contributed by the title
            # will boost _partial_ title matches over better matches
            # obtained some other way.
            operator="and",
            minimum_should_match="100%",
        )
        yield hypothesis, combined_weight

    @property
    def parsed_query_matches(self):
        """Deal with a query string that contains information that should be
        exactly matched against a controlled vocabulary
        (e.g. "nonfiction" or "grade 5") along with information that
        is more search-like (such as a title or author).

        The match information is pulled out of the query string and
        used to make a series of match_phrase queries. The rest of the
        information is used in a simple query that matches basic
        fields.
        """
        parser = QueryParser(self.query_string)
        return parser.match_queries, parser.filters

    def _fuzzy_matches(self, field_name, **kwargs):
        """Make one or more fuzzy Match versions of any MatchPhrase
        hypotheses, scoring them at a fraction of the original
        version.
        """
        # fuzziness="AUTO" means the number of typoes allowed is
        # proportional to the length of the query.
        #
        # max_expansions limits the number of possible alternates
        # Opensearch will consider for any given word.
        kwargs.update(fuzziness="AUTO", max_expansions=2)
        yield Match(**{field_name: kwargs}), self.fuzzy_coefficient * 0.50

        # Assuming that no typoes were made in the first
        # character of a word (usually a safe assumption) we
        # can bump the score up to 75% of the non-fuzzy
        # hypothesis.
        kwargs = dict(kwargs)
        kwargs["prefix_length"] = 1
        yield Match(**{field_name: kwargs}), self.fuzzy_coefficient * 0.75

    @classmethod
    def _hypothesize(cls, hypotheses, query, boost, filters=None, **kwargs):
        """Add a hypothesis to the ones to be tested for each book.

        :param hypotheses: A list of active hypotheses, to be
        appended to if necessary.

        :param query: An opensearch-dsl Query object (or list of
        Query objects) to be used as the basis for this hypothesis. If
        there's nothing here, no new hypothesis will be generated.

        :param boost: Boost the overall weight of this hypothesis
        relative to other hypotheses being tested.

        :param kwargs: Keyword arguments for the _boost method.
        """
        if query or filters:
            query = cls._boost(boost=boost, queries=query, filters=filters, **kwargs)
        if query:
            hypotheses.append(query)
        return hypotheses


class JSONQuery(Query):
    """An ES query created out of a JSON based query language
    Eg. { "query": { "and": [{"key": "title", "value": "book" }, {"key": "author", "value": "robert" }] } }
    Simply means "title=book and author=robert". The language is extensible, and easy to understand for clients to implement
    """

    class Conjunctives(Values):
        AND = "and"
        OR = "or"
        NOT = "not"

    class QueryLeaf(Values):
        KEY = "key"
        VALUE = "value"
        OP = "op"

    class Operators(Values):
        EQ = "eq"
        NEQ = "neq"
        GTE = "gte"
        LTE = "lte"
        LT = "lt"
        GT = "gt"
        REGEX = "regex"
        CONTAINS = "contains"

    # Reserved characters and their mapping to escaped characters
    RESERVED_CHARS = '.?+*|{}[]()"\\#@&<>~'
    RESERVED_CHARS_MAP = dict(map(lambda ch: (ord(ch), f"\\{ch}"), RESERVED_CHARS))

    _KEYWORD_ONLY = {"keyword": True}
    _LONG_TYPE = {"type": "long"}
    _BOOL_TYPE = {"type": "bool"}

    # The fields mappings in the search DB
    FIELD_MAPPING: Dict[str, Dict] = {
        "audience": dict(),
        "author": _KEYWORD_ONLY,
        "classifications.scheme": _KEYWORD_ONLY,
        "classifications.term": _KEYWORD_ONLY,
        "contributors.display_name": {**_KEYWORD_ONLY, **dict(path="contributors")},
        "contributors.family_name": {**_KEYWORD_ONLY, **dict(path="contributors")},
        "contributors.lc": dict(path="contributors"),
        "contributors.role": dict(path="contributors"),
        "contributors.sort_name": {**_KEYWORD_ONLY, **dict(path="contributors")},
        "contributors.viaf": dict(path="contributors"),
        "fiction": _KEYWORD_ONLY,
        "genres.name": dict(path="genres"),
        "genres.scheme": dict(path="genres"),
        "genres.term": dict(path="genres", **_LONG_TYPE),
        "genres.weight": dict(path="genres", **_LONG_TYPE),
        "identifiers.identifier": dict(path="identifiers"),
        "identifiers.type": dict(path="identifiers"),
        "imprint": _KEYWORD_ONLY,
        "language": dict(
            type="_text"
        ),  # Made up keyword type, because we don't want text fuzzyness on this
        "licensepools.available": dict(path="licensepools", **_BOOL_TYPE),
        "licensepools.availability_time": dict(path="licensepools", **_LONG_TYPE),
        "licensepools.collection_id": dict(path="licensepools", **_LONG_TYPE),
        "licensepools.data_source_id": dict(
            path="licensepools", ops=[Operators.EQ, Operators.NEQ], **_LONG_TYPE
        ),
        "licensepools.licensed": dict(path="licensepools", **_BOOL_TYPE),
        "licensepools.medium": dict(path="licensepools"),
        "licensepools.open_access": dict(path="licensepools", **_BOOL_TYPE),
        "licensepools.quality": dict(path="licensepools", **_LONG_TYPE),
        "licensepools.suppressed": dict(path="licensepools", **_BOOL_TYPE),
        "medium": _KEYWORD_ONLY,
        "presentation_ready": _BOOL_TYPE,
        "publisher": _KEYWORD_ONLY,
        "quality": _LONG_TYPE,
        "series": _KEYWORD_ONLY,
        "sort_author": dict(),
        "sort_title": dict(),
        "subtitle": _KEYWORD_ONLY,
        "target_age": dict(),
        "title": _KEYWORD_ONLY,
        "published": _LONG_TYPE,
    }

    # From the client, some field names may be abstracted
    FIELD_TRANSFORMS = {
        "genre": "genres.name",
        "open_access": "licensepools.open_access",
        "available": "licensepools.available",
        "classification": "classifications.term",
        "data_source": "licensepools.data_source_id",
    }

    # We are using "match" queries for the "equals" operator
    # so we must keep a tight leash on the how much of a spread
    # in the matches we want to keep
    # The "match" is used instead of "term" in order to have some
    # tolerance for spelling mistakes while making a query
    MATCH_ARGS = dict(
        auto_generate_synonyms_phrase_query=False,
        max_expansions=10,
        fuzziness="AUTO",
    )

    class ValueTransforms:
        @staticmethod
        def data_source(value: str) -> int:
            """Transform a datasource name into a datasource id"""
            if CachedData.cache is not None:
                sources = CachedData.cache.data_sources()
                for source in sources:
                    if (
                        source.name is not None
                        and source.id is not None
                        and source.name.lower() == value.lower()
                    ):
                        return source.id

            # No such value was found, so return a non-id
            return 0

        @staticmethod
        def published(value: str) -> float:
            """Expects a YYYY-MM-DD format string and returns a timestamp from epoch"""
            try:
                values = value.split("-")
                return datetime.datetime(
                    int(values[0]), int(values[1]), int(values[2])
                ).timestamp()
            except Exception as e:
                raise QueryParseException(
                    detail=f"Could not parse 'published' value '{value}'. Only use 'YYYY-MM-DD'"
                )

        @staticmethod
        def language(value: str) -> str:
            """Transform a possibly english language name to an alpha3 code"""
            transformed = LanguageNames.name_to_codes.get(value.lower(), {value})
            value = list(transformed)[0] if len(transformed) > 0 else value
            return value

    VALUE_TRANSORMS = {
        "data_source": ValueTransforms.data_source,
        "published": ValueTransforms.published,
        "language": ValueTransforms.language,
    }

    def __init__(self, query: Union[str, Dict], filter=None):
        if type(query) is str:
            try:
                query = json.loads(query)
            except Exception as e:
                raise QueryParseException(
                    detail=f"'{query}' is not a valid json"
                ) from None

        self.query = query
        self.filter = filter

    @property
    def search_query(self):
        query = None
        if "query" not in self.query:
            raise QueryParseException("'query' key must be present as the root")
        query = self._parse_json_query(self.query["query"])
        return query

    def _is_keyword(self, name: str) -> bool:
        return self.FIELD_MAPPING[name].get("keyword") == True

    def _nested_path(self, name: str) -> Union[str, None]:
        return self.FIELD_MAPPING[name].get("path")

    def _parse_json_query(self, query: Dict):
        """Eventually recursive json query parser"""
        es_query = None

        # Empty query remains empty
        if not query:
            return {}

        # This is minimal set of leaf keys, op is optional
        leaves = {self.QueryLeaf.KEY, self.QueryLeaf.VALUE}

        # Are we a {key, value, [op]} query
        if set(query.keys()).intersection(leaves) == leaves:
            es_query = self._parse_json_leaf(query)
        # Are we an {and, or} query
        elif set(self.Conjunctives.values()).issuperset(query.keys()):
            es_query = self._parse_json_join(query)
        else:
            raise QueryParseException(
                detail=f"Could not make sense of the query: {query}"
            )

        return es_query

    def _parse_json_leaf(self, query: Dict) -> Dict:
        """We have a leaf query, which means this becomes a keyword.term query"""
        op = query.get(self.QueryLeaf.OP, self.Operators.EQ)

        if op not in self.Operators:
            raise QueryParseException(detail=f"Unrecognized operator: {op}")

        old_key = query[self.QueryLeaf.KEY]
        value = query[self.QueryLeaf.VALUE]

        # In case values need to be transformed
        if old_key in self.VALUE_TRANSORMS:
            value = self.VALUE_TRANSORMS[old_key](value)

        # The contains/regex operators are a regex match
        # So we must replace special operators where encountered
        if op in {self.Operators.CONTAINS, self.Operators.REGEX}:
            value = value.translate(self.RESERVED_CHARS_MAP)

        key = self.FIELD_TRANSFORMS.get(
            old_key, old_key
        )  # Transform field name, if applicable

        if key not in self.FIELD_MAPPING.keys():
            raise QueryParseException(f"Unrecognized key: {old_key}")
        mapping = self.FIELD_MAPPING[key]

        nested_path = self._nested_path(key)
        if self._is_keyword(key):
            key = key + ".keyword"

        # Validate operator restrictions
        allowed_ops = mapping.get("ops")
        if allowed_ops is not None and op not in allowed_ops:
            raise QueryParseException(
                detail=f"Operator '{op}' is not allowed for '{old_key}'. Only use {allowed_ops}"
            )

        es_query = None

        def _match_or_term_query():
            """Only text type mappings get a 'match' search, others use a term search
            All variables are used from the function closure
            """
            if mapping.get("type", "text") != "text":
                return Term(**{key: value})
            else:
                return Match(**{key: {"query": value, **self.MATCH_ARGS}})

        if op == self.Operators.EQ:
            es_query = _match_or_term_query()
        elif op == self.Operators.NEQ:
            es_query = Bool(must_not=[_match_or_term_query()])
        elif op in {
            self.Operators.GT,
            self.Operators.GTE,
            self.Operators.LT,
            self.Operators.LTE,
        }:
            es_query = Range(**{key: {op: value}})
        elif op == self.Operators.REGEX:
            regex_query = dict(value=value, flags="ALL")
            es_query = Regexp(**{key: regex_query})
        elif op == self.Operators.CONTAINS:
            regex_query = dict(value=f".*{value}.*", flags="ALL")
            es_query = Regexp(**{key: regex_query})

        # For nested paths
        if nested_path:
            es_query = Nested(path=nested_path, query=es_query)

        if es_query is None:
            raise QueryParseException(detail=f"Could not parse query: {query}")

        return es_query

    def _parse_json_join(self, query: Dict) -> Dict:
        if len(query.keys()) != 1:
            raise QueryParseException(
                detail="A conjuction cannot have multiple parts in the same sub-query"
            )

        join = list(query.keys())[0]
        to_join = []
        for query_part in query[join]:
            q = self._parse_json_query(query_part)
            to_join.append(q)

        if join == self.Conjunctives.AND:
            joined_query = Bool(must=to_join)
        elif join == self.Conjunctives.OR:
            joined_query = Bool(should=to_join)
        elif join == self.Conjunctives.NOT:
            joined_query = Bool(must_not=to_join)

        return joined_query


@define
class QueryParseException(Exception):
    detail: str = ""


class QueryParser:
    """Attempt to parse filter information out of a query string.

    This class is where we make sense of queries like the following:

      asteroids nonfiction
      grade 5 dogs
      young adult romance
      divorce age 10 and up

    These queries contain information that can best be thought of in
    terms of a filter against specific fields ("nonfiction", "grade
    5", "romance"). Books either match these criteria or they don't.

    These queries may also contain information that can be thought of
    in terms of a search ("asteroids", "dogs") -- books may match
    these criteria to a greater or lesser extent.
    """

    def __init__(self, query_string, query_class=Query):
        """Parse the query string and create a list of clauses
        that will boost certain types of books.

        Use .query to get an opensearch-dsl Query object.

        :param query_class: Pass in a mock of Query here during testing
        to generate 'query' objects that are easier for you to test.
        """
        self.original_query_string = query_string.strip()
        self.query_class = query_class

        # We start with no match queries and no filter.
        self.match_queries = []
        self.filters = []

        # We handle genre first so that, e.g. 'Science Fiction' doesn't
        # get chomped up by the search for 'fiction'.

        # Handle the 'romance' part of 'young adult romance'
        genre, genre_match = KeywordBasedClassifier.genre_match(query_string)
        if genre:
            query_string = self.add_match_term_filter(
                genre.name, "genres.name", query_string, genre_match
            )

        # Handle the 'young adult' part of 'young adult romance'
        audience, audience_match = KeywordBasedClassifier.audience_match(query_string)
        if audience:
            query_string = self.add_match_term_filter(
                audience.replace(" ", "").lower(),
                "audience",
                query_string,
                audience_match,
            )

        # Handle the 'nonfiction' part of 'asteroids nonfiction'
        fiction = None
        if re.compile(r"\bnonfiction\b", re.IGNORECASE).search(query_string):
            fiction = "nonfiction"
        elif re.compile(r"\bfiction\b", re.IGNORECASE).search(query_string):
            fiction = "fiction"
        query_string = self.add_match_term_filter(
            fiction, "fiction", query_string, fiction
        )
        # Handle the 'grade 5' part of 'grade 5 dogs'
        age_from_grade, grade_match = GradeLevelClassifier.target_age_match(
            query_string
        )
        if age_from_grade and age_from_grade[0] == None:
            age_from_grade = None
        query_string = self.add_target_age_filter(
            age_from_grade, query_string, grade_match
        )

        # Handle the 'age 10 and up' part of 'divorce age 10 and up'
        age, age_match = AgeClassifier.target_age_match(query_string)
        if age and age[0] == None:
            age = None
        query_string = self.add_target_age_filter(age, query_string, age_match)

        self.final_query_string = query_string.strip()

        if len(self.final_query_string) == 0:
            # Someone who searched for 'young adult romance' ended up
            # with an empty query string -- they matched an audience
            # and a genre, and now there's nothing else to match.
            return

        # Someone who searched for 'asteroids nonfiction' ended up
        # with a query string of 'asteroids'. Their query string
        # has a filter-type component and a query-type component.
        #
        # What is likely to be in this query-type component?
        #
        # It could be anything that would go into a regular query. And
        # we have lots of different ways of checking a regular query --
        # different hypotheses, fuzzy matches, etc. So the simplest thing
        # to do is to create a Query object for the smaller search query
        # and see what its .search_query is.
        if (
            self.final_query_string
            and self.final_query_string != self.original_query_string
        ):
            recursive = self.query_class(
                self.final_query_string, use_query_parser=False
            ).search_query
            self.match_queries.append(recursive)

    def add_match_term_filter(self, query, field, query_string, matched_portion):
        """Create a match query that finds documents whose value for `field`
        matches `query`.

        Add it to `self.filters`, and remove the relevant portion
        of `query_string` so it doesn't get reused.
        """
        if not query:
            # This is not a relevant part of the query string.
            return query_string
        match_query = self.query_class._match_term(field, query)
        self.filters.append(match_query)
        return self._without_match(query_string, matched_portion)

    def add_target_age_filter(self, query, query_string, matched_portion):
        """Create a query that finds documents whose value for `target_age`
        matches `query`.

        Add a filter version of this query to `.match_queries` (so that
        all documents outside the target age are filtered out).

        Add a boosted version of this query to `.match_queries` (so
        that documents that cluster tightly around the target age are
        boosted over documents that span a huge age range).

        Remove the relevant portion of `query_string` so it doesn't get
        reused.
        """
        if not query:
            # This is not a relevant part of the query string.
            return query_string

        filter, query = self.query_class.make_target_age_query(query)
        self.filters.append(filter)
        self.match_queries.append(query)
        return self._without_match(query_string, matched_portion)

    @classmethod
    def _without_match(cls, query_string, match):
        """Take the portion of a query string that matched a controlled
        vocabulary, and remove it from the query string, so it
        doesn't get reused later.
        """
        # If the match was "children" and the query string was
        # "children's", we want to remove the "'s" as well as
        # the match. We want to remove everything up to the
        # next word boundary that's not an apostrophe or a
        # dash.
        word_boundary_pattern = r"\b%s[\w'\-]*\b"

        return re.compile(word_boundary_pattern % match.strip(), re.IGNORECASE).sub(
            "", query_string
        )


class Filter(SearchBase):
    """A filter for search results.

    This covers every reason you might want to not exclude a search
    result that would otherise match the query string -- wrong media,
    wrong language, not available in the patron's library, etc.

    This also covers every way you might want to order the search
    results: either by relevance to the search query (the default), or
    by a specific field (e.g. author) as described by a Facets object.

    It also covers additional calculated values you might need when
    presenting the search results.
    """

    # When search results include known script fields, we need to
    # wrap the works we would be returning in WorkSearchResults so
    # the useful information from the search engine isn't lost.
    KNOWN_SCRIPT_FIELDS = ["last_update"]

    # In general, someone looking for things "by this person" is
    # probably looking for one of these roles.
    AUTHOR_MATCH_ROLES = list(Contributor.AUTHOR_ROLES) + [
        Contributor.NARRATOR_ROLE,
        Contributor.EDITOR_ROLE,
        Contributor.DIRECTOR_ROLE,
        Contributor.ACTOR_ROLE,
    ]

    @classmethod
    def from_worklist(cls, _db, worklist, facets):
        """Create a Filter that finds only works that belong in the given
        WorkList and EntryPoint.

        :param worklist: A WorkList
        :param facets: A SearchFacets object.
        """
        library = worklist.get_library(_db)
        # For most configuration settings there is a single value --
        # either defined on the WorkList or defined by its parent.
        inherit_one = worklist.inherited_value
        media = inherit_one("media")
        languages = inherit_one("languages")
        fiction = inherit_one("fiction")
        audiences = inherit_one("audiences")
        target_age = inherit_one("target_age")
        collections = inherit_one("collection_ids") or library

        license_datasource_id = inherit_one("license_datasource_id")

        # For genre IDs and CustomList IDs, we might get a separate
        # set of restrictions from every item in the WorkList hierarchy.
        # _All_ restrictions must be met for a work to match the filter.
        inherit_some = worklist.inherited_values
        genre_id_restrictions = inherit_some("genre_ids")
        customlist_id_restrictions = inherit_some("customlist_ids")

        # See if there are any excluded audiobook sources on this
        # site.
        excluded = ConfigurationSetting.excluded_audio_data_sources(_db)
        excluded_audiobook_data_sources = [DataSource.lookup(_db, x) for x in excluded]
        if library is None:
            allow_holds = True
        else:
            allow_holds = library.settings.allow_holds
        return cls(
            collections,
            media,
            languages,
            fiction,
            audiences,
            target_age,
            genre_id_restrictions,
            customlist_id_restrictions,
            facets,
            excluded_audiobook_data_sources=excluded_audiobook_data_sources,
            allow_holds=allow_holds,
            license_datasource=license_datasource_id,
            lane_building=True,
        )

    def __init__(
        self,
        collections=None,
        media=None,
        languages=None,
        fiction=None,
        audiences=None,
        target_age=None,
        genre_restriction_sets=None,
        customlist_restriction_sets=None,
        facets=None,
        script_fields=None,
        **kwargs,
    ):
        """Constructor.

        All arguments are optional. Passing in an empty set of
        arguments will match everything in the search index that
        matches the universal filters (e.g. works must be
        presentation-ready).

        :param collections: Find only works that are licensed to one of
        these Collections.

        :param media: Find only works in this list of media (use the
        constants from Edition such as Edition.BOOK_MEDIUM).

        :param languages: Find only works in these languages (use
        ISO-639-2 alpha-3 codes).

        :param fiction: Find only works with this fiction status.

        :param audiences: Find only works with a target audience in this list.

        :param target_age: Find only works with a target age in this
        range. (Use a 2-tuple, or a number to represent a specific
        age.)

        :param genre_restriction_sets: A sequence of lists of Genre
        objects or IDs. Each list represents an independent
        restriction. For each restriction, a work only matches if it's
        in one of the appropriate Genres.

        :param customlist_restriction_sets: A sequence of lists of
        CustomList objects or IDs. Each list represents an independent
        restriction. For each restriction, a work only matches if it's
        in one of the appropriate CustomLists.

        :param facets: A faceting object that can put further restrictions
        on the match.

        :param script_fields: A list of registered script fields to
        run on the search results.

        (These minor arguments were made into unnamed keyword arguments
        to avoid cluttering the method signature:)

        :param excluded_audiobook_data_sources: A list of DataSources that
        provide audiobooks known to be unsupported on this system.
        Such audiobooks will always be excluded from results.

        :param identifiers: A list of Identifier or IdentifierData
        objects. Only books associated with one of these identifiers
        will be matched.

        :param allow_holds: If this is False, books with no available
        copies will be excluded from results.

        :param series: If this is set to a string, only books in a matching
        series will be included. If set to True, books that belong to _any_
        series will be included.

        :param author: If this is set to a Contributor or
        ContributorData, then only books where this person had an
        authorship role will be included.

        :param license_datasource: If this is set to a DataSource,
        only books with LicensePools from that DataSource will be
        included.

        :param updated_after: If this is set to a datetime, only books
        whose Work records (~bibliographic metadata) have been updated since
        that time will be included in results.

        :param match_nothing: If this is set to True, the search will
        not even be performed -- we know for some other reason that an
        empty set of search results should be returned.
        """

        if isinstance(collections, Library):
            # Find all works in this Library's collections.
            collections = collections.collections
        self.collection_ids = self._filter_ids(collections)

        self.media = media
        self.languages = languages
        self.fiction = fiction
        self._audiences = audiences

        if target_age:
            if isinstance(target_age, int):
                self.target_age = (target_age, target_age)
            elif isinstance(target_age, tuple) and len(target_age) == 2:
                self.target_age = target_age
            else:
                # It's a SQLAlchemy range object. Convert it to a tuple.
                self.target_age = numericrange_to_tuple(target_age)
        else:
            self.target_age = None

        # Filter the lists of database IDs to make sure we aren't
        # storing any database objects.
        if genre_restriction_sets:
            self.genre_restriction_sets = [
                self._filter_ids(x) for x in genre_restriction_sets
            ]
        else:
            self.genre_restriction_sets = []
        if customlist_restriction_sets:
            self.customlist_restriction_sets = [
                self._filter_ids(x) for x in customlist_restriction_sets
            ]
        else:
            self.customlist_restriction_sets = []

        # Pull less-important values out of the keyword arguments.
        excluded_audiobook_data_sources = kwargs.pop(
            "excluded_audiobook_data_sources", []
        )
        self.excluded_audiobook_data_sources = self._filter_ids(
            excluded_audiobook_data_sources
        )
        self.allow_holds = kwargs.pop("allow_holds", True)

        self.updated_after = kwargs.pop("updated_after", None)

        self.series = kwargs.pop("series", None)

        self.author = kwargs.pop("author", None)

        self.min_score = kwargs.pop("min_score", None)

        self.match_nothing = kwargs.pop("match_nothing", False)

        license_datasources = kwargs.pop("license_datasource", None)
        self.license_datasources = self._filter_ids(license_datasources)

        identifiers = kwargs.pop("identifiers", [])
        self.identifiers = list(self._scrub_identifiers(identifiers))

        self.lane_building = kwargs.pop("lane_building", False)

        # At this point there should be no keyword arguments -- you can't pass
        # whatever you want into this method.
        if kwargs:
            raise ValueError("Unknown keyword arguments: %r" % kwargs)

        # Establish default values for additional restrictions that may be
        # imposed by the Facets object.
        self.minimum_featured_quality = 0
        self.availability = None
        self.subcollection = None
        self.order = None
        self.order_ascending = False

        self.script_fields = script_fields or dict()

        # Give the Facets object a chance to modify any or all of this
        # information.
        if facets:
            facets.modify_search_filter(self)
            self.scoring_functions = facets.scoring_functions(self)
            self.search_type = getattr(facets, "search_type", "default")
        else:
            self.scoring_functions = []
            self.search_type = "default"

        # JSON type searches are exact matches and do not have scoring
        if self.search_type == "json":
            self.min_score = None

    @property
    def audiences(self):
        """Return the appropriate audiences for this query.

        This will be whatever audiences were provided, but it will
        probably also include the 'All Ages' audience.
        """

        if not self._audiences:
            return self._audiences

        as_is = self._audiences
        if isinstance(as_is, (bytes, str)):
            as_is = [as_is]

        # At this point we know we have a specific list of audiences.
        # We're either going to return that list as-is, or we'll
        # return that list plus ALL_AGES.
        with_all_ages = list(as_is) + [Classifier.AUDIENCE_ALL_AGES]

        if Classifier.AUDIENCE_ALL_AGES in as_is:
            # ALL_AGES is explicitly included.
            return as_is

        # If YOUNG_ADULT or ADULT is an audience, then ALL_AGES is
        # always going to be an additional audience.
        if any(
            x in as_is
            for x in [Classifier.AUDIENCE_YOUNG_ADULT, Classifier.AUDIENCE_ADULT]
        ):
            return with_all_ages

        # At this point, if CHILDREN is _not_ included, we know that
        # ALL_AGES is not included. Specifically, ALL_AGES content
        # does _not_ belong in ADULTS_ONLY or RESEARCH.
        if Classifier.AUDIENCE_CHILDREN not in as_is:
            return as_is

        # Now we know that CHILDREN is an audience. It's going to come
        # down to the upper bound on the target age.
        if (
            self.target_age
            and self.target_age[1] is not None
            and self.target_age[1] < Classifier.ALL_AGES_AGE_CUTOFF
        ):
            # The audience for this query does not include any kids
            # who are expected to have the reading fluency necessary
            # for ALL_AGES books.
            return as_is
        return with_all_ages

    def build(self, _chain_filters=None):
        """Convert this object to an Opensearch Filter object.

        :return: A 2-tuple (filter, nested_filters). Filters on fields
           within nested documents (such as
           'licensepools.collection_id') must be applied as subqueries
           to the query that will eventually be created from this
           filter. `nested_filters` is a dictionary that maps a path
           to a list of filters to apply to that path.

        :param _chain_filters: Mock function to use instead of
            Filter._chain_filters
        """

        # Since a Filter object can be modified after it's created, we
        # need to scrub all the inputs, whether or not they were
        # scrubbed in the constructor.
        scrub_list = self._scrub_list
        filter_ids = self._filter_ids

        chain = _chain_filters or self._chain_filters

        f = None
        nested_filters = defaultdict(list)
        if self.match_nothing:
            # This Filter should match nothing. There's no need to
            # get fancy.
            return MatchNone(), nested_filters

        collection_ids = filter_ids(self.collection_ids)
        if collection_ids:
            collection_match = Terms(**{"licensepools.collection_id": collection_ids})
            nested_filters["licensepools"].append(collection_match)

        license_datasources = filter_ids(self.license_datasources)
        if license_datasources:
            datasource_match = Terms(
                **{"licensepools.data_source_id": license_datasources}
            )
            nested_filters["licensepools"].append(datasource_match)

        if self.author is not None:
            nested_filters["contributors"].append(self.author_filter)

        if self.media:
            f = chain(f, Terms(medium=scrub_list(self.media)))

        if self.languages:
            f = chain(f, Terms(language=scrub_list(self.languages)))

        if self.fiction is not None:
            if self.fiction:
                value = "fiction"
            else:
                value = "nonfiction"
            f = chain(f, Term(fiction=value))

        if self.series:
            if self.series is True:
                # The book must belong to _some_ series.
                #
                # That is, series must exist (have a non-null value) and
                # have a value other than the empty string.
                f = chain(f, Exists(field="series"))
                f = chain(f, Bool(must_not=[Term(**{"series.keyword": ""})]))
            else:
                f = chain(f, Term(**{"series.keyword": self.series}))

        if self.audiences:
            f = chain(f, Terms(audience=scrub_list(self.audiences)))
        else:
            research = self._scrub(Classifier.AUDIENCE_RESEARCH)
            f = chain(f, Bool(must_not=[Term(audience=research)]))

        target_age_filter = self.target_age_filter
        if target_age_filter:
            f = chain(f, self.target_age_filter)

        for genre_ids in self.genre_restriction_sets:
            ids = filter_ids(genre_ids)
            nested_filters["genres"].append(
                Terms(**{"genres.term": filter_ids(genre_ids)})
            )

        for customlist_ids in self.customlist_restriction_sets:
            ids = filter_ids(customlist_ids)
            nested_filters["customlists"].append(Terms(**{"customlists.list_id": ids}))

        open_access = Term(**{"licensepools.open_access": True})
        if self.availability == FacetConstants.AVAILABLE_NOW:
            # Only open-access books and books with currently available
            # copies should be displayed.
            available = Term(**{"licensepools.available": True})
            nested_filters["licensepools"].append(
                Bool(should=[open_access, available], minimum_should_match=1)
            )
        elif self.availability == FacetConstants.AVAILABLE_OPEN_ACCESS:
            # Only open-access books should be displayed.
            nested_filters["licensepools"].append(open_access)
        elif self.availability == FacetConstants.AVAILABLE_NOT_NOW:
            # Only books that are _not_ currently available should be displayed.
            not_open_access = Term(**{"licensepools.open_access": False})
            licensed = Term(**{"licensepools.licensed": True})
            not_available = Term(**{"licensepools.available": False})
            nested_filters["licensepools"].append(
                Bool(must=[not_open_access, licensed, not_available])
            )

        if self.subcollection == FacetConstants.COLLECTION_FEATURED:
            # Exclude books with a quality of less than the library's
            # minimum featured quality.
            range_query = self._match_range(
                "quality", "gte", self.minimum_featured_quality
            )
            f = chain(f, Bool(must=range_query))

        if self.identifiers:
            # Check every identifier for a match.
            clauses = []
            for identifier in self._scrub_identifiers(self.identifiers):
                subclauses = []
                # Both identifier and type must match for the match
                # to count.
                for name, value in (
                    ("identifier", identifier.identifier),
                    ("type", identifier.type),
                ):
                    subclauses.append(Term(**{"identifiers.%s" % name: value}))
                clauses.append(Bool(must=subclauses))

            # At least one the identifiers must match for the work to
            # match.
            identifier_f = Bool(should=clauses, minimum_should_match=1)
            nested_filters["identifiers"].append(identifier_f)

        # Some sources of audiobooks may be excluded because the
        # server can't fulfill them or the anticipated client can't
        # play them.
        excluded = self.excluded_audiobook_data_sources
        if excluded:
            audio = Term(**{"licensepools.medium": Edition.AUDIO_MEDIUM})
            excluded_audio_source = Terms(**{"licensepools.data_source_id": excluded})
            excluded_audio = Bool(must=[audio, excluded_audio_source])
            not_excluded_audio = Bool(must_not=excluded_audio)
            nested_filters["licensepools"].append(not_excluded_audio)

        # If holds are not allowed, only license pools that are
        # currently available should be considered.
        if not self.allow_holds:
            licenses_available = Term(**{"licensepools.available": True})
            currently_available = Bool(should=[licenses_available, open_access])
            nested_filters["licensepools"].append(currently_available)

        # Perhaps only books whose bibliographic metadata was updated
        # recently should be included.
        if self.updated_after:
            # 'last update_time' is indexed as a number of seconds, but
            # .last_update is probably a datetime. Convert it here.
            updated_after = self.updated_after
            if isinstance(updated_after, datetime.datetime):
                updated_after = (updated_after - from_timestamp(0)).total_seconds()
            last_update_time_query = self._match_range(
                "last_update_time", "gte", updated_after
            )
            f = chain(f, Bool(must=last_update_time_query))

        return f, nested_filters

    @classmethod
    def universal_base_filter(cls, _chain_filters=None):
        """Build a set of restrictions on the main search document that are
        always applied, even in the absence of other filters.

        :param _chain_filters: Mock function to use instead of
            Filter._chain_filters

        :return: A Filter object.

        """

        _chain_filters = _chain_filters or cls._chain_filters

        base_filter = None

        # We only want to show works that are presentation-ready.
        base_filter = _chain_filters(base_filter, Term(**{"presentation_ready": True}))

        return base_filter

    @classmethod
    def universal_nested_filters(cls):
        """Build a set of restrictions on subdocuments that are
        always applied, even in the absence of other filters.
        """
        nested_filters = defaultdict(list)

        # TODO: It would be great to be able to filter out
        # LicensePools that have no delivery mechanisms. That's the
        # only part of Collection.restrict_to_ready_deliverable_works
        # not already implemented in this class.

        # We don't want to consider license pools that have been
        # suppressed, or of which there are currently no licensed
        # copies. This might lead to a Work being filtered out
        # entirely.
        #
        # It's easier to stay consistent by indexing all Works and
        # filtering them out later, than to do it by adding and
        # removing works from the index.
        not_suppressed = Term(**{"licensepools.suppressed": False})
        nested_filters["licensepools"].append(not_suppressed)

        owns_licenses = Term(**{"licensepools.licensed": True})
        open_access = Term(**{"licensepools.open_access": True})
        currently_owned = Bool(should=[owns_licenses, open_access])
        nested_filters["licensepools"].append(currently_owned)

        return nested_filters

    @property
    def sort_order(self):
        """Create a description, for use in an Opensearch document,
        explaining how search results should be ordered.

        :return: A list of dictionaries, each dictionary mapping a
            field name to an explanation of how to sort that
            field. Usually the explanation is a simple string, either
            'asc' or 'desc'.
        """
        if not self.order:
            return []

        # These sort order fields are inserted as necessary between
        # the primary sort order field and the tiebreaker field (work
        # ID). This makes it more likely that the sort order makes
        # sense to a human, by putting off the opaque tiebreaker for
        # as long as possible. For example, a feed sorted by author
        # will be secondarily sorted by title and work ID, not just by
        # work ID.
        default_sort_order = ["sort_author", "sort_title", "work_id"]

        order_field_keys = self.order
        if not isinstance(order_field_keys, list):
            order_field_keys = [order_field_keys]
        order_fields = [self._make_order_field(key) for key in order_field_keys]

        # Apply any parts of the default sort order not yet covered,
        # concluding (in most cases) with work_id, the tiebreaker field.
        for x in default_sort_order:
            if x not in order_field_keys:
                order_fields.append({x: "asc"})
        return order_fields

    @property
    def asc(self):
        "Convert order_ascending to Opensearch-speak."
        if self.order_ascending is False:
            return "desc"
        else:
            return "asc"

    def _make_order_field(self, key):
        if key == "last_update_time":
            # Sorting by last_update_time may be very simple or very
            # complex, depending on whether or not the filter
            # involves collection or list membership.
            if self.collection_ids or self.customlist_restriction_sets:
                # The complex case -- use a helper method.
                return self._last_update_time_order_by
            else:
                # The simple case, handled below.
                pass

        if "." not in key:
            # A simple case.
            return {key: self.asc}

        # At this point we're sorting by a nested field.
        nested = None
        if key == "licensepools.availability_time":
            nested, mode = self._availability_time_sort_order
        else:
            raise ValueError("I don't know how to sort by %s." % key)
        sort_description = dict(order=self.asc, mode=mode)
        if nested:
            sort_description["nested"] = nested
        return {key: sort_description}

    @property
    def _availability_time_sort_order(self):
        # We're sorting works by the time they became
        # available to a library. This means we only want to
        # consider the availability times of license pools
        # found in one of the library's collections.
        nested = None
        collection_ids = self._filter_ids(self.collection_ids)
        if collection_ids:
            nested = dict(
                path="licensepools",
                filter=dict(terms={"licensepools.collection_id": collection_ids}),
            )
        # If a book shows up in multiple collections, we're only
        # interested in the collection that had it the earliest.
        mode = "min"
        return nested, mode

    @property
    def last_update_time_script_field(self):
        """Return the configuration for a script field that calculates the
        'last update' time of a work. An 'update' happens when the
        work's metadata is changed, when it's added to a collection
        used by this Filter, or when it's added to one of the lists
        used by this Filter.
        """
        # First, set up the parameters we're going to pass into the
        # script -- a list of custom list IDs relevant to this filter,
        # and a list of collection IDs relevant to this filter.
        collection_ids = self._filter_ids(self.collection_ids)

        # The different restriction sets don't matter here. The filter
        # part of the query ensures that we only match works present
        # on one list in every restriction set. Here, we need to find
        # the latest time a work was added to _any_ relevant list.
        all_list_ids = set()
        for restriction in self.customlist_restriction_sets:
            all_list_ids.update(self._filter_ids(restriction))
        nested = dict(
            path="customlists",
            filter=dict(terms={"customlists.list_id": list(all_list_ids)}),
        )
        params = dict(collection_ids=collection_ids, list_ids=list(all_list_ids))
        # Messy, but this is the only way to get the "current mapping" for the index
        script_name = (
            SearchRevisionDirectory.create().highest().script_name("work_last_update")
        )
        return dict(script=dict(stored=script_name, params=params))

    @property
    def _last_update_time_order_by(self):
        """We're sorting works by the time of their 'last update'.

        Add the 'last update' field to the dictionary of script fields
        (so we can use the result afterwards), and define it a second
        time as the script to use for a sort value.
        """
        field = self.last_update_time_script_field
        if not "last_update" in self.script_fields:
            self.script_fields["last_update"] = field
        return dict(
            _script=dict(
                type="number",
                script=field["script"],
                order=self.asc,
            ),
        )

    # The Painless script to generate a 'featurability' score for
    # a work.
    #
    # A higher-quality work is more featurable. But we don't want
    # to constantly feature the very highest-quality works, and if
    # there are no high-quality works, we want medium-quality to
    # outrank low-quality.
    #
    # So we establish a cutoff -- the minimum featured quality --
    # beyond which a work is considered 'featurable'. All featurable
    # works get the same (high) score.
    #
    # Below that point, we prefer higher-quality works to
    # lower-quality works, such that a work's score is proportional to
    # the square of its quality.
    FEATURABLE_SCRIPT = (
        "Math.pow(Math.min(%(cutoff).5f, doc['quality'].value), %(exponent).5f) * 5"
    )

    # Used in tests to deactivate the random component of
    # featurability_scoring_functions.
    DETERMINISTIC = object()

    def featurability_scoring_functions(self, random_seed):
        """Generate scoring functions that weight works randomly, but
        with 'more featurable' works tending to be at the top.
        """

        exponent = 2
        cutoff = self.minimum_featured_quality**exponent
        script = self.FEATURABLE_SCRIPT % dict(cutoff=cutoff, exponent=exponent)
        quality_field = SF("script_score", script=dict(source=script))

        # Currently available works are more featurable.
        available = Term(**{"licensepools.available": True})
        nested = Nested(path="licensepools", query=available)
        available_now = dict(filter=nested, weight=5)

        function_scores = [quality_field, available_now]

        # Random chance can boost a lower-quality work, but not by
        # much -- this mainly ensures we don't get the exact same
        # books every time.
        if random_seed != self.DETERMINISTIC:
            random = SF(
                "random_score",
                seed=random_seed or int(time.time()),
                field="work_id",
                weight=1.1,
            )
            function_scores.append(random)

        if self.customlist_restriction_sets:
            list_ids = set()
            for restriction in self.customlist_restriction_sets:
                list_ids.update(restriction)
            # We're looking for works on certain custom lists. A work
            # that's _featured_ on one of these lists will be boosted
            # quite a lot versus one that's not.
            featured = Term(**{"customlists.featured": True})
            on_list = Terms(**{"customlists.list_id": list(list_ids)})
            featured_on_list = Bool(must=[featured, on_list])
            nested = Nested(path="customlists", query=featured_on_list)
            featured_on_relevant_list = dict(filter=nested, weight=11)
            function_scores.append(featured_on_relevant_list)
        return function_scores

    @property
    def target_age_filter(self):
        """Helper method to generate the target age subfilter.

        It's complicated because it has to handle cases where the upper
        or lower bound on target age is missing (indicating there is no
        upper or lower bound).
        """
        if not self.target_age:
            return None
        lower, upper = self.target_age
        if lower is None and upper is None:
            return None

        def does_not_exist(field):
            """A filter that matches if there is no value for `field`."""
            return Bool(must_not=[Exists(field=field)])

        def or_does_not_exist(clause, field):
            """Either the given `clause` matches or the given field
            does not exist.
            """
            return Bool(should=[clause, does_not_exist(field)], minimum_should_match=1)

        clauses = []

        both_limits = lower is not None and upper is not None

        if (
            self.lane_building
            and self.audiences
            and Classifier.AUDIENCE_CHILDREN in self.audiences
            and both_limits
        ):
            # If children is audience we want only works with defined age range that matches lane's range
            clauses.append(self._match_range("target_age.lower", "gte", lower))
            clauses.append(self._match_range("target_age.upper", "lte", upper))

            return Bool(must=clauses)

        if upper is not None:
            lower_does_not_exist = does_not_exist("target_age.lower")
            lower_in_range = self._match_range("target_age.lower", "lte", upper)
            lower_match = or_does_not_exist(lower_in_range, "target_age.lower")
            clauses.append(lower_match)

        if lower is not None:
            upper_does_not_exist = does_not_exist("target_age.upper")
            upper_in_range = self._match_range("target_age.upper", "gte", lower)
            upper_match = or_does_not_exist(upper_in_range, "target_age.upper")
            clauses.append(upper_match)

        if not clauses:
            # Neither upper nor lower age must match.
            return None

        if len(clauses) == 1:
            # Upper or lower age must match, but not both.
            return clauses[0]

        # Both upper and lower age must match.
        return Bool(must=clauses)

    @property
    def author_filter(self):
        """Build a filter that matches a 'contributors' subdocument only
        if it represents an author-level contribution by self.author.
        """
        if not self.author:
            return None
        authorship_role = Terms(**{"contributors.role": self.AUTHOR_MATCH_ROLES})
        clauses = []
        for field, value in [
            ("sort_name.keyword", self.author.sort_name),
            ("display_name.keyword", self.author.display_name),
            ("viaf", self.author.viaf),
            ("lc", self.author.lc),
        ]:
            if not value or value == Edition.UNKNOWN_AUTHOR:
                continue
            clauses.append(Term(**{"contributors.%s" % field: value}))

        same_person = Bool(should=clauses, minimum_should_match=1)
        return Bool(must=[authorship_role, same_person])

    @classmethod
    def _scrub(cls, s):
        """Modify a string for use in a filter match.

        e.g. "Young Adult" becomes "youngadult"

        :param s: The string to modify.
        """
        if not s:
            return s
        return s.lower().replace(" ", "")

    @classmethod
    def _scrub_list(cls, s):
        """The same as _scrub, except it always outputs
        a list of items.
        """
        if s is None:
            return []
        if isinstance(s, (bytes, str)):
            s = [s]
        return [cls._scrub(x) for x in s]

    @classmethod
    def _filter_ids(cls, ids):
        """Process a list of database objects, provided either as their
        IDs or as the objects themselves.

        :return: A list of IDs, or None if nothing was provided.
        """
        # Generally None means 'no restriction', while an empty list
        # means 'one of the values in this empty list' -- in other
        # words, they are opposites.
        if ids is None:
            return None

        processed = []

        if not isinstance(ids, list) and not isinstance(ids, set):
            ids = [ids]

        for id in ids:
            if not isinstance(id, int):
                # Turn a database object into an ID.
                id = id.id
            processed.append(id)
        return processed

    @classmethod
    def _scrub_identifiers(cls, identifiers):
        """Convert a mixed list of Identifier and IdentifierData objects
        into IdentifierData.
        """
        for i in identifiers:
            if isinstance(i, Identifier):
                i = IdentifierData(i.type, i.identifier)
            yield i

    @classmethod
    def _chain_filters(cls, existing, new):
        """Either chain two filters together or start a new chain."""
        if existing:
            # We're combining two filters.
            new = existing & new
        else:
            # There was no previous filter -- the 'new' one is it.
            pass
        return new


class SortKeyPagination(Pagination):
    """An Opensearch-specific implementation of Pagination that
    paginates search results by tracking where in a sorted list the
    previous page left off, rather than using a numeric index into the
    list.
    """

    def __init__(self, last_item_on_previous_page=None, size=Pagination.DEFAULT_SIZE):
        self.size = size
        self.last_item_on_previous_page = last_item_on_previous_page

        # These variables are set by page_loaded(), after the query
        # is run.
        self.page_has_loaded = False
        self.last_item_on_this_page = None
        self.this_page_size = None

    @classmethod
    def from_request(cls, get_arg, default_size=None):
        """Instantiate a SortKeyPagination object from a Flask request."""
        size = cls.size_from_request(get_arg, default_size)
        if isinstance(size, ProblemDetail):
            return size
        pagination_key = get_arg("key", None)
        if pagination_key:
            try:
                pagination_key = json.loads(pagination_key)
            except ValueError as e:
                return INVALID_INPUT.detailed(
                    _("Invalid page key: %(key)s", key=pagination_key)
                )
        return cls(pagination_key, size)

    def items(self):
        """Yield the URL arguments necessary to convey the current page
        state.
        """
        pagination_key = self.pagination_key
        if pagination_key:
            yield ("key", self.pagination_key)
        yield ("size", self.size)

    @property
    def pagination_key(self):
        """Create the pagination key for this page."""
        if not self.last_item_on_previous_page:
            return None
        return json.dumps(self.last_item_on_previous_page)

    @property
    def offset(self):
        # This object never uses the traditional offset system; offset
        # is determined relative to the last item on the previous
        # page.
        return 0

    @property
    def total_size(self):
        # Although we technically know the total size after the first
        # page of results has been obtained, we don't use this feature
        # in pagination, so act like we don't.
        return None

    def modify_database_query(self, qu):
        raise NotImplementedError(
            "SortKeyPagination does not work with database queries."
        )

    def modify_search_query(self, search):
        """Modify the given Search object so that it starts
        picking up items immediately after the previous page.

        :param search: An opensearch-dsl Search object.
        """
        if self.last_item_on_previous_page:
            search = search.update_from_dict(
                dict(search_after=self.last_item_on_previous_page)
            )
        return super().modify_search_query(search)

    @property
    def previous_page(self):
        # TODO: We can get the previous page by flipping the sort
        # order and asking for the _next_ page of the reversed list,
        # using the sort keys of the _first_ item as the search_after.
        # But this is really confusing, it requires more context than
        # SortKeyPagination currently has, and this feature isn't
        # necessary for our current implementation.
        return None

    @property
    def next_page(self):
        """If possible, create a new SortKeyPagination representing the
        next page of results.
        """
        if self.this_page_size == 0:
            # This page is empty; there is no next page.
            return None
        if not self.last_item_on_this_page:
            # This probably means load_page wasn't called. At any
            # rate, we can't say anything about the next page.
            return None
        return SortKeyPagination(self.last_item_on_this_page, self.size)

    def page_loaded(self, page):
        """An actual page of results has been fetched. Keep any internal state
        that would be useful to know when reasoning about earlier or
        later pages.

        Specifically, keep track of the sort value of the last item on
        this page, so that self.next_page will create a
        SortKeyPagination object capable of generating the subsequent
        page.

        :param page: A list of opensearch-dsl Hit objects.
        """
        super().page_loaded(page)
        if page:
            last_item = page[-1]
            values = list(last_item.meta.sort)
        else:
            # There's nothing on this page, so there's no next page
            # either.
            values = None
        self.last_item_on_this_page = values


class WorkSearchResult:
    """Wraps a Work object to give extra information obtained from
    Opensearch.

    This object acts just like a Work (though isinstance(x, Work) will
    fail), with one exception: you can access the raw Opensearch Hit
    result as ._hit.

    This is useful when a Work needs to be 'tagged' with information
    obtained through Opensearch, such as its 'last modified' date
    the context of a specific lane.
    """

    def __init__(self, work, hit):
        self._work = work
        self._hit = hit

    def __getattr__(self, k):
        return getattr(self._work, k)


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
        self.search_index_client = search_index_client or ExternalSearchIndex(self._db)

        #
        # Try to migrate to the latest schema. If the function returns None, it means
        # that no migration is necessary, and we're already at the latest version. If
        # we're already at the latest version, then simply upload search documents instead.
        #
        self.receiver = None
        self.migration: Optional[
            SearchMigrationInProgress
        ] = self.search_index_client.start_migration()
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

    def process_batch(self, works) -> List[Work | CoverageFailure]:
        target: SearchDocumentReceiverType = self.migration or self.receiver
        failures = target.add_documents(
            documents=self.search_index_client.create_search_documents_from_works(works)
        )

        # Maintain a dictionary of works so that we can efficiently remove failed works later.
        work_map: Dict[int, Work] = {}
        for work in works:
            work_map[work.id] = work

        # Remove all the works that failed and create failure records for them.
        results: List[Work | CoverageFailure] = []
        for failure in failures:
            work = work_map[failure.id]
            del work_map[failure.id]
            results.append(CoverageFailure(work, repr(failure)))

        # Append all the remaining works that didn't fail.
        for work in work_map.values():
            results.append(work)

        return results
