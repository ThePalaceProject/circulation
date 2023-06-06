import json
import re
import time
import uuid
from datetime import datetime
from typing import Callable, Collection, List

import pytest
from opensearch_dsl import Q
from opensearch_dsl.function import RandomScore, ScriptScore
from opensearch_dsl.query import (
    Bool,
    DisMax,
    Match,
    MatchAll,
    MatchNone,
    MatchPhrase,
    MultiMatch,
    Nested,
)
from opensearch_dsl.query import Query as opensearch_dsl_query
from opensearch_dsl.query import Range, Term, Terms
from opensearchpy.exceptions import OpenSearchException
from psycopg2.extras import NumericRange

from core.classifier import Classifier
from core.config import CannotLoadConfiguration, Configuration
from core.external_search import (
    CurrentMapping,
    ExternalSearchIndex,
    Filter,
    JSONQuery,
    MockExternalSearchIndex,
    MockSearchResult,
    Query,
    QueryParseException,
    QueryParser,
    SearchBase,
    SearchIndexCoverageProvider,
    SortKeyPagination,
    WorkSearchResult,
    mock_search_index,
)
from core.lane import Facets, FeaturedFacets, Pagination, SearchFacets, WorkList
from core.metadata_layer import ContributorData, IdentifierData
from core.model import (
    ConfigurationSetting,
    Contribution,
    Contributor,
    CustomList,
    DataSource,
    Edition,
    Genre,
    LicensePool,
    WorkCoverageRecord,
    get_one_or_create,
)
from core.model.classification import Subject
from core.model.work import Work
from core.problem_details import INVALID_INPUT
from core.util.cache import CachedData
from core.util.datetime_helpers import datetime_utc, from_timestamp
from tests.fixtures.database import (
    DatabaseTransactionFixture,
    DBStatementCounter,
    PerfTimer,
)
from tests.fixtures.search import EndToEndSearchFixture, ExternalSearchFixture

RESEARCH = Term(audience=Classifier.AUDIENCE_RESEARCH.lower())


class TestExternalSearch:
    def test_load(self, external_search_fixture: ExternalSearchFixture):
        session = external_search_fixture.db.session

        # Normally, load() returns a brand new ExternalSearchIndex
        # object.
        loaded = ExternalSearchIndex.load(session, in_testing=True)
        assert isinstance(loaded, ExternalSearchIndex)

        # However, inside the mock_search_index context manager,
        # load() returns whatever object was mocked.
        mock = object()
        with mock_search_index(mock):
            assert mock == ExternalSearchIndex.load(session, in_testing=True)

    def test_constructor(self, external_search_fixture: ExternalSearchFixture):
        session = external_search_fixture.db.session

        # The configuration of the search ExternalIntegration becomes the
        # configuration of the ExternalSearchIndex.
        #
        # This basically just verifies that the test search term is taken
        # from the ExternalIntegration.
        class MockIndex(ExternalSearchIndex):
            def set_works_index_and_alias(self, _db):
                self.set_works_index_and_alias_called_with = _db

        index = MockIndex(session)
        assert session == index.set_works_index_and_alias_called_with
        assert "test_search_term" == index.test_search_term

    # TODO: would be good to check the put_script calls, but the
    # current constructor makes put_script difficult to mock.

    def test_opensearch_error_in_constructor_becomes_cannotloadconfiguration(
        self, external_search_fixture: ExternalSearchFixture
    ):
        session = external_search_fixture.db.session

        """If we're unable to establish a connection to the Opensearch
        server, CannotLoadConfiguration (which the circulation manager can
        understand) is raised instead of an Opensearch-specific exception.
        """

        # Unlike other tests in this module, this one runs even if no
        # OpenSearch server is running, since it's testing what
        # happens if there's a problem communicating with that server.
        class Mock(ExternalSearchIndex):
            def set_works_index_and_alias(self, _db):
                raise OpenSearchException("very bad")

        with pytest.raises(CannotLoadConfiguration) as excinfo:
            Mock(session)
        assert "Exception communicating with Search server: " in str(excinfo.value)
        assert "very bad" in str(excinfo.value)

    def test_works_index_name(self, external_search_fixture: ExternalSearchFixture):
        session = external_search_fixture.db.session

        """The name of the search index is the prefix (defined in
        ExternalSearchTest.setup) plus a version number associated
        with this version of the core code.
        """
        version = external_search_fixture.search.mapping.VERSION_NAME
        assert (
            f"test_index-{version}"
            == external_search_fixture.search.works_index_name(session)
        )

    def test_setup_index_creates_new_index(
        self, external_search_fixture: ExternalSearchFixture
    ):
        current_index = external_search_fixture.search.works_index
        # This calls self.search.setup_index (which is what we're testing)
        # and also registers the index to be torn down at the end of the test.
        external_search_fixture.setup_index("the_other_index")

        # Both indices exist.
        assert True == external_search_fixture.search.indices.exists(current_index)
        assert True == external_search_fixture.search.indices.exists("the_other_index")

        # The index for the app's search is still the original index.
        assert current_index == external_search_fixture.search.works_index

        # The alias hasn't been passed over to the new index.
        alias = "test_index-" + external_search_fixture.search.CURRENT_ALIAS_SUFFIX
        assert alias == external_search_fixture.search.works_alias
        assert True == external_search_fixture.search.indices.exists_alias(
            alias, index=current_index
        )
        assert False == external_search_fixture.search.indices.exists_alias(
            alias, index="the_other_index"
        )

    def test_set_works_index_and_alias(
        self, external_search_fixture: ExternalSearchFixture
    ):
        session = external_search_fixture.db.session
        search = external_search_fixture.search

        # If the index or alias don't exist, set_works_index_and_alias
        # will create them.
        external_search_fixture.integration.set_setting(
            ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY, "banana"
        )
        search.set_works_index_and_alias(session)

        expected_index = "banana-" + CurrentMapping.version_name()
        expected_alias = "banana-" + search.CURRENT_ALIAS_SUFFIX
        assert expected_index == search.works_index
        assert expected_alias == search.works_alias

        # If the index and alias already exist, set_works_index_and_alias
        # does nothing.
        search.set_works_index_and_alias(session)
        assert expected_index == search.works_index
        assert expected_alias == search.works_alias

    def test_setup_current_alias(self, external_search_fixture: ExternalSearchFixture):
        session = external_search_fixture.db.session
        search = external_search_fixture.search

        # The index was generated from the string in configuration.
        version = CurrentMapping.version_name()
        index_name = "test_index-" + version
        assert index_name == search.works_index
        assert True == search.indices.exists(index_name)

        # The alias is also created from the configuration.
        alias = "test_index-" + search.CURRENT_ALIAS_SUFFIX
        assert alias == search.works_alias
        assert True == search.indices.exists_alias(alias, index_name)

        # If the -current alias is already set on a different index, it
        # won't be reassigned. Instead, search will occur against the
        # index itself.
        ExternalSearchIndex.reset()
        external_search_fixture.integration.set_setting(
            ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY, "my-app"
        )
        self.search = ExternalSearchIndex(session)

        assert "my-app-%s" % version == self.search.works_index
        assert "my-app-" + self.search.CURRENT_ALIAS_SUFFIX == self.search.works_alias

    def test_transfer_current_alias(
        self, external_search_fixture: ExternalSearchFixture
    ):
        session = external_search_fixture.db.session
        search = external_search_fixture.search

        # An error is raised if you try to set the alias to point to
        # an index that doesn't already exist.
        pytest.raises(
            ValueError, search.transfer_current_alias, session, "no-such-index"
        )

        original_index = search.works_index

        # If the -current alias doesn't exist, it's created
        # and everything is updated accordingly.
        search.indices.delete_alias(
            index=original_index, name="test_index-current", ignore=[404]
        )
        search.setup_index(new_index="test_index-v9999")
        search.transfer_current_alias(session, "test_index-v9999")
        assert "test_index-v9999" == search.works_index
        assert "test_index-current" == search.works_alias

        # If the -current alias already exists on the index,
        # it's used without a problem.
        search.transfer_current_alias(session, "test_index-v9999")
        assert "test_index-v9999" == search.works_index
        assert "test_index-current" == search.works_alias

        # If the -current alias is being used on a different version of the
        # index, it's deleted from that index and placed on the new one.
        search.setup_index(original_index)
        search.transfer_current_alias(session, original_index)
        assert original_index == search.works_index
        assert "test_index-current" == search.works_alias

        # It has been removed from other index.
        assert False == search.indices.exists_alias(
            index="test_index-v9999", name="test_index-current"
        )

        # And only exists on the new index.
        alias_indices = list(search.indices.get_alias(name="test_index-current").keys())
        assert [original_index] == alias_indices

        # If the index doesn't have the same base name, an error is raised.
        pytest.raises(ValueError, search.transfer_current_alias, session, "banana-v10")

    def test_query_works(self):
        # Verify that query_works operates by calling query_works_multi.
        # The actual functionality of query_works and query_works_multi
        # have many end-to-end tests in TestExternalSearchWithWorks.
        class Mock(ExternalSearchIndex):
            def __init__(self):
                self.query_works_multi_calls = []
                self.queued_results = []

            def query_works_multi(self, queries, debug=False):
                self.query_works_multi_calls.append((queries, debug))
                return self.queued_results.pop()

        search = Mock()

        # If the filter is designed to match nothing,
        # query_works_multi isn't even called -- we just return an
        # empty list.
        query = object()
        pagination = object()
        filter = Filter(match_nothing=True)
        assert [] == search.query_works(query, filter, pagination)
        assert [] == search.query_works_multi_calls

        # Otherwise, query_works_multi is called with a list
        # containing a single query, and the list of resultsets is
        # turned into a single list of results.
        search.queued_results.append([["r1", "r2"]])
        filter = object()
        results = search.query_works(query, filter, pagination)
        assert ["r1", "r2"] == results
        call = search.query_works_multi_calls.pop()
        assert ([(query, filter, pagination)], False) == call
        assert [] == search.query_works_multi_calls

        # If no Pagination object is provided, a default is used.
        search.queued_results.append([["r3", "r4"]])
        results = search.query_works(query, filter, None, True)
        assert ["r3", "r4"] == results
        ([query_tuple], debug) = search.query_works_multi_calls.pop()
        assert True == debug
        assert query == query_tuple[0]
        assert filter == query_tuple[1]

        pagination = query_tuple[2]
        default = Pagination.default()
        assert isinstance(pagination, Pagination)
        assert pagination.offset == default.offset
        assert pagination.size == default.size

    def test__run_self_tests(self, external_search_fixture: ExternalSearchFixture):
        transaction = external_search_fixture.db
        session = transaction.session
        index = MockExternalSearchIndex()

        # First, see what happens when the search returns no results.
        test_results = [x for x in index._run_self_tests(session, in_testing=True)]

        assert "Search results for 'a search term':" == test_results[0].name
        assert True == test_results[0].success
        assert [] == test_results[0].result

        assert "Search document for 'a search term':" == test_results[1].name
        assert True == test_results[1].success
        assert "[]" == test_results[1].result

        assert "Raw search results for 'a search term':" == test_results[2].name
        assert True == test_results[2].success
        assert [] == test_results[2].result

        assert (
            "Total number of search results for 'a search term':"
            == test_results[3].name
        )
        assert True == test_results[3].success
        assert "0" == test_results[3].result

        assert "Total number of documents in this search index:" == test_results[4].name
        assert True == test_results[4].success
        assert "0" == test_results[4].result

        assert "Total number of documents per collection:" == test_results[5].name
        assert True == test_results[5].success
        assert "{}" == test_results[5].result

        # Set up the search index so it will return a result.
        collection = transaction.collection()

        search_result = MockSearchResult("Sample Book Title", "author", {}, "id")
        index.index("index", "id", search_result)
        test_results = [x for x in index._run_self_tests(session, in_testing=True)]

        assert "Search results for 'a search term':" == test_results[0].name
        assert True == test_results[0].success
        assert ["Sample Book Title (author)"] == test_results[0].result

        assert "Search document for 'a search term':" == test_results[1].name
        assert True == test_results[1].success
        result = json.loads(test_results[1].result)
        sample_book = {
            "author": "author",
            "meta": {"id": "id", "_sort": ["Sample Book Title", "author", "id"]},
            "id": "id",
            "title": "Sample Book Title",
        }
        assert sample_book == result

        assert "Raw search results for 'a search term':" == test_results[2].name
        assert True == test_results[2].success
        result = json.loads(test_results[2].result[0])
        assert sample_book == result

        assert (
            "Total number of search results for 'a search term':"
            == test_results[3].name
        )
        assert True == test_results[3].success
        assert "1" == test_results[3].result

        assert "Total number of documents in this search index:" == test_results[4].name
        assert True == test_results[4].success
        assert "1" == test_results[4].result

        assert "Total number of documents per collection:" == test_results[5].name
        assert True == test_results[5].success
        result = json.loads(test_results[5].result)
        assert {collection.name: 1} == result

    def test_update_mapping(self, external_search_fixture: ExternalSearchFixture):
        search = external_search_fixture.search

        search.mapping.add_properties({"long": ["new_long_property"]})
        put_mapping = search._update_index_mapping(dry_run=True)
        assert "new_long_property" in put_mapping
        put_mapping = search._update_index_mapping(dry_run=False)
        assert "new_long_property" in put_mapping
        put_mapping = search._update_index_mapping(dry_run=True)
        assert "new_long_property" not in put_mapping

        new_mapping = search.indices.get_mapping(search.works_index)
        new_mapping = new_mapping[search.works_index]["mappings"]
        assert "new_long_property" in new_mapping["properties"]


class TestCurrentMapping:
    def test_character_filters(self):
        # Verify the functionality of the regular expressions we tell
        # Opensearch to use when normalizing fields that will be used
        # for searching.
        filters = []
        for filter_name in CurrentMapping.AUTHOR_CHAR_FILTER_NAMES:
            configuration = CurrentMapping.CHAR_FILTERS[filter_name]
            find = re.compile(configuration["pattern"])
            replace = configuration["replacement"]
            # Hack to (imperfectly) convert Java regex format to Python format.
            # $1 -> \1
            replace = replace.replace("$", "\\")
            filters.append((find, replace))

        def filters_to(start, finish):
            """When all the filters are applied to `start`,
            the result is `finish`.
            """
            for find, replace in filters:
                start = find.sub(replace, start)
            assert start == finish

        # Only the primary author is considered for sorting purposes.
        filters_to("Adams, John Joseph ; Yu, Charles", "Adams, John Joseph")

        # The special system author '[Unknown]' is replaced with
        # REPLACEMENT CHARACTER so it will be last in sorted lists.
        filters_to("[Unknown]", "\N{REPLACEMENT CHARACTER}")

        # Periods are removed.
        filters_to("Tepper, Sheri S.", "Tepper, Sheri S")
        filters_to("Tepper, Sheri S", "Tepper, Sheri S")

        # The initials of authors who go by initials are normalized
        # so that their books all sort together.
        filters_to("Wells, HG", "Wells, HG")
        filters_to("Wells, H G", "Wells, HG")
        filters_to("Wells, H.G.", "Wells, HG")
        filters_to("Wells, H. G.", "Wells, HG")

        # It works with up to three initials.
        filters_to("Tolkien, J. R. R.", "Tolkien, JRR")

        # Parentheticals are removed.
        filters_to("Wells, H. G. (Herbert George)", "Wells, HG")


class TestExternalSearchWithWorksData:
    adult_work: Work
    age_2_10: Work
    age_4_5: Work
    age_5_6: Work
    age_9_10: Work
    all_ages_work: Work
    children_work: Work
    dodger: Work
    les_mis: Work
    lincoln: Work
    lincoln_vampire: Work
    moby_dick: Work
    moby_duck: Work
    modern_romance: Work
    no_age: Work
    no_copies: Work
    not_presentation_ready: Work
    obama: Work
    presidential: CustomList
    pride: Work
    pride_audio: Work
    publisher_match: Work
    research_work: Work
    sherlock: Work
    sherlock_pool_2: LicensePool
    sherlock_spanish: Work
    subtitle_match: Work
    summary_match: Work
    suppressed: Work
    tess: Work
    tiffany: Work
    tiny_book: Work
    tiny_collection: Collection
    title_match: Work
    washington: Work
    ya_romance: Work
    ya_work: Work


class TestExternalSearchWithWorks:
    """These tests run against a real search index with works in it.
    The setup is very slow, so all the tests are in the same method.
    Don't add new methods to this class - add more tests into test_query_works,
    or add a new test class.
    """

    @staticmethod
    def _populate_works(
        fixture: EndToEndSearchFixture,
    ) -> TestExternalSearchWithWorksData:
        transaction = fixture.external_search.db
        _work: Callable = fixture.external_search.default_work

        result = TestExternalSearchWithWorksData()
        result.moby_dick = _work(
            title="Moby Dick",
            authors="Herman Melville",
            fiction=True,
        )
        result.moby_dick.presentation_edition.subtitle = "Or, the Whale"
        result.moby_dick.presentation_edition.series = "Classics"
        result.moby_dick.summary_text = "Ishmael"
        result.moby_dick.presentation_edition.publisher = "Project Gutenberg"
        result.moby_dick.last_update_time = datetime_utc(2019, 1, 1)

        result.moby_duck = _work(
            title="Moby Duck", authors="Donovan Hohn", fiction=False
        )
        result.moby_duck.presentation_edition.subtitle = (
            "The True Story of 28,800 Bath Toys Lost at Sea"
        )
        result.moby_duck.summary_text = "A compulsively readable narrative"
        result.moby_duck.presentation_edition.publisher = "Penguin"
        result.moby_duck.last_update_time = datetime_utc(2019, 1, 2)
        # This book is not currently loanable. It will still show up
        # in search results unless the library's settings disable it.
        result.moby_duck.license_pools[0].licenses_available = 0

        result.title_match = _work(title="Match")

        result.subtitle_match = _work(title="SubtitleM")
        result.subtitle_match.presentation_edition.subtitle = "Match"

        result.summary_match = _work(title="SummaryM")
        result.summary_match.summary_text = "It's a Match! The story of a work whose summary contained an important keyword."

        result.publisher_match = _work(title="PublisherM")
        result.publisher_match.presentation_edition.publisher = "Match"

        result.tess = _work(title="Tess of the d'Urbervilles")

        result.tiffany = _work(title="Breakfast at Tiffany's")

        result.les_mis = _work()
        result.les_mis.presentation_edition.title = "Les Mis\u00E9rables"

        result.modern_romance = _work(title="Modern Romance")

        result.lincoln = _work(genre="Biography & Memoir", title="Abraham Lincoln")

        result.washington = _work(genre="Biography", title="George Washington")

        result.lincoln_vampire = _work(
            title="Abraham Lincoln: Vampire Hunter", genre="Fantasy"
        )

        result.children_work = _work(
            title="Alice in Wonderland", audience=Classifier.AUDIENCE_CHILDREN
        )

        result.all_ages_work = _work(
            title="The Annotated Alice", audience=Classifier.AUDIENCE_ALL_AGES
        )

        result.ya_work = _work(
            title="Go Ask Alice", audience=Classifier.AUDIENCE_YOUNG_ADULT
        )

        result.adult_work = _work(
            title="Still Alice", audience=Classifier.AUDIENCE_ADULT
        )

        result.research_work = _work(
            title="Curiouser and Curiouser: Surrealism and Repression in 'Alice in Wonderland'",
            audience=Classifier.AUDIENCE_RESEARCH,
        )

        result.ya_romance = _work(
            title="Gumby In Love",
            audience=Classifier.AUDIENCE_YOUNG_ADULT,
            genre="Romance",
        )
        result.ya_romance.presentation_edition.subtitle = (
            "Modern Fairytale Series, Volume 7"
        )
        result.ya_romance.presentation_edition.series = "Modern Fairytales"

        result.no_age = _work()
        result.no_age.summary_text = (
            "President Barack Obama's election in 2008 energized the United States"
        )

        # Set the series to the empty string rather than None -- this isn't counted
        # as the book belonging to a series.
        result.no_age.presentation_edition.series = ""

        result.age_4_5 = _work()
        result.age_4_5.target_age = NumericRange(4, 5, "[]")
        result.age_4_5.summary_text = (
            "President Barack Obama's election in 2008 energized the United States"
        )

        result.age_5_6 = _work(fiction=False)
        result.age_5_6.target_age = NumericRange(5, 6, "[]")

        result.obama = _work(title="Barack Obama", genre="Biography & Memoir")
        result.obama.target_age = NumericRange(8, 8, "[]")
        result.obama.summary_text = (
            "President Barack Obama's election in 2008 energized the United States"
        )

        result.dodger = _work()
        result.dodger.target_age = NumericRange(8, 8, "[]")
        result.dodger.summary_text = (
            "Willie finds himself running for student council president"
        )

        result.age_9_10 = _work()
        result.age_9_10.target_age = NumericRange(9, 10, "[]")
        result.age_9_10.summary_text = (
            "President Barack Obama's election in 2008 energized the United States"
        )

        result.age_2_10 = _work()
        result.age_2_10.target_age = NumericRange(2, 10, "[]")

        result.pride = _work(title="Pride and Prejudice (E)")
        result.pride.presentation_edition.medium = Edition.BOOK_MEDIUM

        result.pride_audio = _work(title="Pride and Prejudice (A)")
        result.pride_audio.presentation_edition.medium = Edition.AUDIO_MEDIUM

        result.sherlock = _work(
            title="The Adventures of Sherlock Holmes", with_open_access_download=True
        )
        result.sherlock.presentation_edition.language = "eng"

        result.sherlock_spanish = _work(title="Las Aventuras de Sherlock Holmes")
        result.sherlock_spanish.presentation_edition.language = "spa"

        # Create a custom list that contains a few books.
        result.presidential, ignore = transaction.customlist(
            name="Nonfiction about US Presidents", num_entries=0
        )
        for work in [result.washington, result.lincoln, result.obama]:
            result.presidential.add_entry(work)

        # Create a second collection that only contains a few books.
        result.tiny_collection = transaction.collection("A Tiny Collection")
        result.tiny_book = transaction.work(
            title="A Tiny Book",
            with_license_pool=True,
            collection=result.tiny_collection,
        )
        result.tiny_book.license_pools[0].self_hosted = True

        # Both collections contain 'The Adventures of Sherlock
        # Holmes", but each collection licenses the book through a
        # different mechanism.
        result.sherlock_pool_2 = transaction.licensepool(
            edition=result.sherlock.presentation_edition,
            collection=result.tiny_collection,
        )

        sherlock_2, is_new = result.sherlock_pool_2.calculate_work()
        assert result.sherlock == sherlock_2
        assert 2 == len(result.sherlock.license_pools)

        # These books look good for some search results, but they
        # will be filtered out by the universal filters, and will
        # never show up in results.

        # We own no copies of this book.
        result.no_copies = _work(title="Moby Dick 2")
        result.no_copies.license_pools[0].licenses_owned = 0

        # This book's only license pool has been suppressed.
        result.suppressed = _work(title="Moby Dick 2")
        result.suppressed.license_pools[0].suppressed = True

        # This book is not presentation_ready.
        result.not_presentation_ready = _work(title="Moby Dick 2")
        result.not_presentation_ready.presentation_ready = False
        return result

    def test_query_works(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        fixture = end_to_end_search_fixture
        transaction = fixture.external_search.db
        session = transaction.session

        data = self._populate_works(fixture)
        fixture.populate_search_index()

        # An end-to-end test of the search functionality.
        #
        # Works created during setup are added to a real search index.
        # We then run actual Opensearch queries against the
        # search index and verify that the work IDs returned
        # are the ones we expect.

        # First, run some basic checks to make sure the search
        # document query doesn't contain over-zealous joins. This test
        # class is the main place where we make a large number of
        # works and generate search documents for them.
        assert 1 == len(data.moby_dick.to_search_document()["licensepools"])
        assert (
            "Audio"
            == data.pride_audio.to_search_document()["licensepools"][0]["medium"]
        )

        # Set up convenient aliases for methods we'll be calling a
        # lot.
        query = fixture.external_search.search.query_works
        expect = fixture.expect_results

        # First, test pagination.
        first_item = Pagination(size=1, offset=0)
        expect(data.moby_dick, "moby dick", None, first_item)

        second_item = first_item.next_page
        expect(data.moby_duck, "moby dick", None, second_item)

        two_per_page = Pagination(size=2, offset=0)
        expect(
            [data.moby_dick, data.moby_duck],
            "moby dick",
            None,
            two_per_page,
        )

        # Now try some different search queries.

        # Search in title.
        assert 2 == len(query("moby"))

        # Search in author name
        expect(data.moby_dick, "melville")

        # Search in subtitle
        expect(data.moby_dick, "whale")

        # Search in series.
        expect(data.moby_dick, "classics")

        # Search in summary.
        expect(data.moby_dick, "ishmael")

        # Search in publisher name.
        expect(data.moby_dick, "gutenberg")

        # Title > subtitle > word found in summary > publisher
        order = [
            data.title_match,
            data.subtitle_match,
            data.summary_match,
            data.publisher_match,
        ]
        expect(order, "match")

        # A search for a partial title match + a partial author match
        # considers only books that match both fields.
        expect([data.moby_dick], "moby melville")

        # Match a quoted phrase
        # 'Moby-Dick' is the first result because it's an exact title
        # match. 'Moby Duck' is the second result because it's a fuzzy
        # match,
        expect([data.moby_dick, data.moby_duck], '"moby dick"')

        # Match a stemmed word: 'running' is stemmed to 'run', and
        # so is 'runs'.
        expect(data.dodger, "runs")

        # Match a misspelled phrase: 'movy' -> 'moby'.
        expect([data.moby_dick, data.moby_duck], "movy", ordered=False)

        # Match a misspelled author: 'mleville' -> 'melville'
        expect(data.moby_dick, "mleville")

        # TODO: This is clearly trying to match "Moby Dick", but it
        # matches nothing. This is because at least two of the strings
        # in a query must match. Neither "di" nor "ck" matches a fuzzy
        # search on its own, which means "moby" is the only thing that
        # matches, and that's not enough.
        expect([], "moby di ck")

        # Here, "dic" is close enough to "dick" that the fuzzy match
        # kicks in. With both "moby" and "dic" matching, it's okay
        # that "k" was a dud.
        expect([data.moby_dick], "moby dic k")

        # A query without an apostrophe matches a word that contains
        # one.  (this is a feature of the stemmer.)
        expect(data.tess, "durbervilles")
        expect(data.tiffany, "tiffanys")

        # A query with an 'e' matches a word that contains an
        # e-with-acute. (this is managed by the 'asciifolding' filter in
        # the analyzers)
        expect(data.les_mis, "les miserables")

        # Find results based on fiction status.
        #
        # Here, Moby-Dick (fiction) is privileged over Moby Duck
        # (nonfiction)
        expect([data.moby_dick], "fiction moby")

        # Here, Moby Duck is privileged over Moby-Dick.
        expect([data.moby_duck], "nonfiction moby")

        # Find results based on series.
        classics = Filter(series="Classics")
        expect(data.moby_dick, "moby", classics)

        # This finds books that belong to _some_ series.
        some_series = Filter(series=True)
        expect(
            [data.moby_dick, data.ya_romance],
            "",
            some_series,
            ordered=False,
        )

        # Find results based on genre.

        # If the entire search query is converted into a filter, every
        # book matching that filter is boosted above books that match
        # the search string as a query.
        expect([data.ya_romance, data.modern_romance], "romance")

        # Find results based on audience.
        expect(data.children_work, "children's")

        expect(
            [data.ya_work, data.ya_romance],
            "young adult",
            ordered=False,
        )

        # Find results based on grade level or target age.
        for q in ("grade 4", "grade 4-6", "age 9"):
            # ages 9-10 is a better result because a book targeted
            # toward a narrow range is a better match than a book
            # targeted toward a wide range.
            expect([data.age_9_10, data.age_2_10], q)

        # TODO: The target age query only scores how big the overlap
        # is, it doesn't look at how large the non-overlapping part of
        # the range is. So the 2-10 book can show up before the 9-10
        # book. This could be improved.
        expect(
            [data.age_9_10, data.age_2_10],
            "age 10-12",
            ordered=False,
        )

        # Books whose target age are closer to the requested range
        # are ranked higher.
        expect(
            [data.age_4_5, data.age_5_6, data.age_2_10],
            "age 3-5",
        )

        # Search by a combination of genre and audience.

        # The book with 'Romance' in the title does not show up because
        # it's not a YA book.
        expect([data.ya_romance], "young adult romance")

        # Search by a combination of target age and fiction
        #
        # Two books match the age range, but the one with a
        # tighter age range comes first.
        expect([data.age_4_5, data.age_2_10], "age 5 fiction")

        # Search by a combination of genre and title

        # Two books match 'lincoln', but only the biography is returned
        expect([data.lincoln], "lincoln biography")

        # Search by age + genre + summary
        results = query("age 8 president biography")

        # There are a number of results, but the top one is a presidential
        # biography for 8-year-olds.
        assert 5 == len(results)
        assert data.obama.id == results[0].work_id

        # Now we'll test filters.

        # Both self.pride and self.pride_audio match the search query,
        # but the filters eliminate one or the other from
        # consideration.
        book_filter = Filter(media=Edition.BOOK_MEDIUM)
        audio_filter = Filter(media=Edition.AUDIO_MEDIUM)
        expect(data.pride, "pride and prejudice", book_filter)
        expect(data.pride_audio, "pride and prejudice", audio_filter)

        # Filters on languages
        english = Filter(languages="eng")
        spanish = Filter(languages="spa")
        both = Filter(languages=["eng", "spa"])

        expect(data.sherlock, "sherlock", english)
        expect(data.sherlock_spanish, "sherlock", spanish)
        expect(
            [data.sherlock, data.sherlock_spanish],
            "sherlock",
            both,
            ordered=False,
        )

        # Filters on fiction status
        fiction = Filter(fiction=True)
        nonfiction = Filter(fiction=False)
        both = Filter()

        expect(data.moby_dick, "moby dick", fiction)
        expect(data.moby_duck, "moby dick", nonfiction)
        expect([data.moby_dick, data.moby_duck], "moby dick", both)

        # Filters on series
        classics = Filter(series="classics")
        expect(data.moby_dick, "moby", classics)

        # Filters on audience
        adult = Filter(audiences=Classifier.AUDIENCE_ADULT)
        ya = Filter(audiences=Classifier.AUDIENCE_YOUNG_ADULT)
        children = Filter(audiences=Classifier.AUDIENCE_CHILDREN)
        ya_and_children = Filter(
            audiences=[Classifier.AUDIENCE_CHILDREN, Classifier.AUDIENCE_YOUNG_ADULT]
        )
        research = Filter(audiences=[Classifier.AUDIENCE_RESEARCH])

        def expect_alice(expect_works, filter):
            return expect(expect_works, "alice", filter, ordered=False)

        expect_alice([data.adult_work, data.all_ages_work], adult)
        expect_alice([data.ya_work, data.all_ages_work], ya)
        expect_alice([data.children_work, data.all_ages_work], children)
        expect_alice(
            [
                data.children_work,
                data.ya_work,
                data.all_ages_work,
            ],
            ya_and_children,
        )

        # The 'all ages' work appears except when the audience would make
        # that inappropriate...
        expect_alice([data.research_work], research)
        expect_alice([], Filter(audiences=Classifier.AUDIENCE_ADULTS_ONLY))

        # ...or when the target age does not include children expected
        # to have the necessary reading fluency.
        expect_alice(
            [data.children_work],
            Filter(audiences=Classifier.AUDIENCE_CHILDREN, target_age=(2, 3)),
        )

        # If there is no filter, the research work is excluded by
        # default, but everything else is included.
        default_filter = Filter()
        expect_alice(
            [
                data.children_work,
                data.ya_work,
                data.adult_work,
                data.all_ages_work,
            ],
            default_filter,
        )

        # Filters on age range
        age_8 = Filter(target_age=8)
        age_5_8 = Filter(target_age=(5, 8))
        age_5_10 = Filter(target_age=(5, 10))
        age_8_10 = Filter(target_age=(8, 10))

        # As the age filter changes, different books appear and
        # disappear. no_age is always present since it has no age
        # restrictions.
        expect(
            [data.no_age, data.obama, data.dodger],
            "president",
            age_8,
            ordered=False,
        )

        expect(
            [
                data.no_age,
                data.age_4_5,
                data.obama,
                data.dodger,
            ],
            "president",
            age_5_8,
            ordered=False,
        )

        expect(
            [
                data.no_age,
                data.age_4_5,
                data.obama,
                data.dodger,
                data.age_9_10,
            ],
            "president",
            age_5_10,
            ordered=False,
        )

        expect(
            [
                data.no_age,
                data.obama,
                data.dodger,
                data.age_9_10,
            ],
            "president",
            age_8_10,
            ordered=False,
        )

        # Filters on license source.
        gutenberg = DataSource.lookup(session, DataSource.GUTENBERG)
        gutenberg_only = Filter(license_datasource=gutenberg)
        expect(
            [data.moby_dick, data.moby_duck],
            "moby",
            gutenberg_only,
            ordered=False,
        )

        overdrive = DataSource.lookup(session, DataSource.OVERDRIVE)
        overdrive_only = Filter(license_datasource=overdrive)
        expect([], "moby", overdrive_only, ordered=False)

        # Filters on last modified time.

        # Obviously this query string matches "Moby-Dick", but it's
        # filtered out because its last update time is before the
        # `updated_after`. "Moby Duck" shows up because its last update
        # time is right on the edge.
        after_moby_duck = Filter(updated_after=data.moby_duck.last_update_time)
        expect([data.moby_duck], "moby dick", after_moby_duck)

        # Filters on genre

        biography, ignore = Genre.lookup(session, "Biography & Memoir")
        fantasy, ignore = Genre.lookup(session, "Fantasy")
        biography_filter = Filter(genre_restriction_sets=[[biography]])
        fantasy_filter = Filter(genre_restriction_sets=[[fantasy]])
        both = Filter(genre_restriction_sets=[[fantasy, biography]])

        expect(data.lincoln, "lincoln", biography_filter)
        expect(data.lincoln_vampire, "lincoln", fantasy_filter)
        expect(
            [data.lincoln, data.lincoln_vampire],
            "lincoln",
            both,
            ordered=False,
        )

        # Filters on list membership.

        # This ignores 'Abraham Lincoln, Vampire Hunter' because that
        # book isn't on the self.presidential list.
        on_presidential_list = Filter(customlist_restriction_sets=[[data.presidential]])
        expect(data.lincoln, "lincoln", on_presidential_list)

        # This filters everything, since the query is restricted to
        # an empty set of lists.
        expect([], "lincoln", Filter(customlist_restriction_sets=[[]]))

        # Filter based on collection ID.

        # "A Tiny Book" isn't in the default collection.
        default_collection_only = Filter(collections=transaction.default_collection())
        expect([], "a tiny book", default_collection_only)

        # It is in the tiny_collection.
        other_collection_only = Filter(collections=data.tiny_collection)
        expect(data.tiny_book, "a tiny book", other_collection_only)

        # If a book is present in two different collections which are
        # being searched, it only shows up in search results once.
        f = Filter(
            collections=[
                transaction.default_collection(),
                data.tiny_collection,
            ],
            languages="eng",
        )
        expect(data.sherlock, "sherlock holmes", f)

        # Filter on identifier -- one or many.
        for results in [
            [data.lincoln],
            [data.sherlock, data.pride_audio],
        ]:
            identifiers = [w.license_pools[0].identifier for w in results]
            f = Filter(identifiers=identifiers)
            expect(results, None, f, ordered=False)

        # Setting .match_nothing on a Filter makes it always return nothing,
        # even if it would otherwise return works.
        nothing = Filter(fiction=True, match_nothing=True)
        expect([], None, nothing)

        # Filters that come from site or library settings.

        # The source for the 'Pride and Prejudice' audiobook has been
        # excluded, so it won't show up in search results.
        f = Filter(
            excluded_audiobook_data_sources=[
                data.pride_audio.license_pools[0].data_source
            ]
        )
        expect([data.pride], "pride and prejudice", f)

        # Here, a different data source is excluded, and it shows up.
        f = Filter(
            excluded_audiobook_data_sources=[
                DataSource.lookup(session, DataSource.BIBLIOTHECA)
            ]
        )
        expect(
            [data.pride, data.pride_audio],
            "pride and prejudice",
            f,
            ordered=False,
        )

        # "Moby Duck" is not currently available, so it won't show up in
        # search results if allow_holds is False.
        f = Filter(allow_holds=False)
        expect([data.moby_dick], "moby duck", f)

        # Finally, let's do some end-to-end tests of
        # WorkList.works()
        #
        # That's a simple method that puts together a few pieces
        # which are tested separately, so we don't need to go all-out.
        def pages(worklist):
            """Iterate over a WorkList until it ends, and return all of the
            pages.
            """
            pagination = SortKeyPagination(size=2)
            facets = Facets(
                transaction.default_library(), None, None, order=Facets.ORDER_TITLE
            )
            pages = []
            while pagination:
                pages.append(
                    worklist.works(
                        session, facets, pagination, fixture.external_search.search
                    )
                )
                pagination = pagination.next_page

            # The last page should always be empty -- that's how we
            # knew we'd reached the end.
            assert [] == pages[-1]

            # Return all the other pages for verification.
            return pages[:-1]

        # Test a WorkList based on a custom list.
        presidential = WorkList()
        presidential.initialize(
            transaction.default_library(), customlists=[data.presidential]
        )
        p1, p2 = pages(presidential)
        assert [data.lincoln, data.obama] == p1
        assert [data.washington] == p2

        # Test a WorkList based on a language.
        spanish = WorkList()
        spanish.initialize(transaction.default_library(), languages=["spa"])
        assert [[data.sherlock_spanish]] == pages(spanish)

        # Test a WorkList based on a genre.
        biography_wl = WorkList()
        biography_wl.initialize(transaction.default_library(), genres=[biography])
        assert [[data.lincoln, data.obama]] == pages(biography_wl)

        # Search results may be sorted by some field other than search
        # quality.
        f = SearchFacets
        by_author = f(
            library=transaction.default_library(),
            collection=f.COLLECTION_FULL,
            availability=f.AVAILABLE_ALL,
            order=f.ORDER_AUTHOR,
        )
        by_author = Filter(facets=by_author)

        by_title = f(
            library=transaction.default_library(),
            collection=f.COLLECTION_FULL,
            availability=f.AVAILABLE_ALL,
            order=f.ORDER_TITLE,
        )
        by_title = Filter(facets=by_title)

        # By default, search results sorted by a bibliographic field
        # are also filtered to eliminate low-quality results.  In a
        # real collection the default filter level works well, but it
        # makes it difficult to test the feature in this limited test
        # collection.
        expect([data.moby_dick], "moby dick", by_author)
        expect([data.ya_romance], "romance", by_author)
        expect([], "moby", by_author)
        expect([], "president", by_author)

        # Let's lower the score so we can test the ordering properly.
        by_title.min_score = 50
        by_author.min_score = 50

        expect([data.moby_dick, data.moby_duck], "moby", by_title)
        expect([data.moby_duck, data.moby_dick], "moby", by_author)
        expect(
            [data.ya_romance, data.modern_romance],
            "romance",
            by_title,
        )
        expect(
            [data.modern_romance, data.ya_romance],
            "romance",
            by_author,
        )

        # Lower it even more and we can start picking up search results
        # that only match because of words in the description.
        by_title.min_score = 10
        by_author.min_score = 10
        results = [
            data.no_age,
            data.age_4_5,
            data.dodger,
            data.age_9_10,
            data.obama,
        ]
        expect(results, "president", by_title)

        # Reverse the sort order to demonstrate that these works are being
        # sorted by title rather than randomly.
        by_title.order_ascending = False
        expect(list(reversed(results)), "president", by_title)

        # Finally, verify that we can run multiple queries
        # simultaneously.

        # Different query strings.
        fixture.expect_results_multi(
            [[data.moby_dick], [data.moby_duck]],
            [("moby dick", None, first_item), ("moby duck", None, first_item)],
        )

        # Same query string, different pagination settings.
        fixture.expect_results_multi(
            [[data.moby_dick], [data.moby_duck]],
            [("moby dick", None, first_item), ("moby dick", None, second_item)],
        )

        # Same query string, same pagination settings, different
        # filters. This is different from calling _expect_results() on
        # a Filter with match_nothing=True. There, the query isn't
        # even run.  Here the query must be run, even though one
        # branch will return no results.
        match_nothing = Filter(match_nothing=True)
        fixture.expect_results_multi(
            [[data.moby_duck], []],
            [
                ("moby dick", Filter(fiction=False), first_item),
                (None, match_nothing, first_item),
            ],
        )

        # Case-insensitive genre search, genre is saved as 'Fantasy'
        expect([data.lincoln_vampire], "fantasy")

    def test_remove_work(self, end_to_end_search_fixture: EndToEndSearchFixture):
        search = end_to_end_search_fixture.external_search.search
        data = self._populate_works(end_to_end_search_fixture)
        end_to_end_search_fixture.populate_search_index()
        search.remove_work(data.moby_dick)
        search.remove_work(data.moby_duck)

        # Immediately querying never works, the search index needs to refresh its cache/index/data
        search.indices.refresh()

        end_to_end_search_fixture.expect_results([], "Moby")


class TestFacetFiltersData:
    becoming: Work
    duck: Work
    horse: Work
    moby: Work


class TestFacetFilters:
    @staticmethod
    def _populate_works(
        data: EndToEndSearchFixture,
    ) -> TestFacetFiltersData:
        _work: Callable = data.external_search.default_work

        result = TestFacetFiltersData()
        # A low-quality open-access work.
        result.horse = _work(
            title="Diseases of the Horse", with_open_access_download=True
        )
        result.horse.quality = 0.2

        # A high-quality open-access work.
        result.moby = _work(title="Moby Dick", with_open_access_download=True)
        result.moby.quality = 0.8

        # A currently available commercially-licensed work.
        result.duck = _work(title="Moby Duck")
        result.duck.license_pools[0].licenses_available = 1
        result.duck.quality = 0.5

        # A currently unavailable commercially-licensed work.
        result.becoming = _work(title="Becoming")
        result.becoming.license_pools[0].licenses_available = 0
        result.becoming.quality = 0.9
        return result

    def test_facet_filtering(self, end_to_end_search_fixture: EndToEndSearchFixture):
        fixture = end_to_end_search_fixture
        transaction = fixture.external_search.db
        session = transaction.session

        data = self._populate_works(fixture)
        fixture.populate_search_index()

        # Add all the works created in the setup to the search index.
        SearchIndexCoverageProvider(
            session, search_index_client=fixture.external_search.search
        ).run_once_and_update_timestamp()

        # Sleep to give the index time to catch up.
        time.sleep(1)

        def expect(availability, collection, works):
            facets = Facets(
                transaction.default_library(),
                availability,
                collection,
                order=Facets.ORDER_TITLE,
            )
            fixture.expect_results(works, None, Filter(facets=facets), ordered=False)

        # Get all the books in alphabetical order by title.
        expect(
            Facets.COLLECTION_FULL,
            Facets.AVAILABLE_ALL,
            [
                data.becoming,
                data.horse,
                data.moby,
                data.duck,
            ],
        )

        # Show only works that can be borrowed right now.
        expect(
            Facets.COLLECTION_FULL,
            Facets.AVAILABLE_NOW,
            [data.horse, data.moby, data.duck],
        )

        # Show only works that can *not* be borrowed right now.
        expect(Facets.COLLECTION_FULL, Facets.AVAILABLE_NOT_NOW, [data.becoming])

        # Show only open-access works.
        expect(
            Facets.COLLECTION_FULL,
            Facets.AVAILABLE_OPEN_ACCESS,
            [data.horse, data.moby],
        )

        # Show only featured-quality works.
        expect(
            Facets.COLLECTION_FEATURED,
            Facets.AVAILABLE_ALL,
            [data.becoming, data.moby],
        )


class TestSearchOrderData:
    a1: Work
    a2: LicensePool
    a: Work
    b1: Work
    b2: LicensePool
    b: Work
    by_publication_date: CustomList
    c1: Work
    c2: LicensePool
    c: Work
    collection1: Collection
    collection2: Collection
    collection3: Collection
    d: Work
    e: Work
    extra_list: CustomList
    list1: CustomList
    list2: CustomList
    list3: CustomList
    moby_dick: Work
    moby_duck: Work
    staff_picks: CustomList
    untitled: Work


class TestSearchOrder:
    @staticmethod
    def _populate_works(
        fixture: EndToEndSearchFixture,
    ) -> TestSearchOrderData:
        transaction = fixture.external_search.db
        _work: Callable = fixture.external_search.default_work

        result = TestSearchOrderData()
        # We're going to create three works:
        # a: "Moby Dick"
        # b: "Moby Duck"
        # c: "[untitled]"
        #
        # The metadata of these books will be set up to generate
        # intuitive orders under most of the ordering scenarios.
        #
        # The most complex ordering scenario is ORDER_LAST_UPDATE,
        # which orders books differently depending on the modification
        # date of the Work, the date a LicensePool for the work was
        # first seen in a collection associated with the filter, and
        # the date the work was first seen on a custom list associated
        # with the filter.
        #
        # The modification dates of the works will be set in the order
        # of their creation.
        #
        # We're going to put all three works in two different
        # collections with different dates. All three works will be
        # added to two different custom lists, and works a and c will
        # be added to a third custom list.
        #
        # The dates associated with the "collection add" and "list add"
        # events will be set up to create the following orderings:
        #
        # a, b, c - when no collections or custom lists are associated with
        #           the Filter.
        # a, c, b - when collection 1 is associated with the Filter.
        # b, a, c - when collections 1 and 2 are associated with the Filter.
        # b, c, a - when custom list 1 is associated with the Filter.
        # c, a, b - when collection 1 and custom list 2 are associated with
        #           the Filter.
        # c, a - when two sets of custom list restrictions [1], [3]
        #        are associated with the filter.
        result.moby_dick = _work(
            title="moby dick", authors="Herman Melville", fiction=True
        )
        result.moby_dick.presentation_edition.subtitle = "Or, the Whale"
        result.moby_dick.presentation_edition.series = "Classics"
        result.moby_dick.presentation_edition.series_position = 10
        result.moby_dick.summary_text = "Ishmael"
        result.moby_dick.presentation_edition.publisher = "Project Gutenberg"
        result.moby_dick.random = 0.1

        result.moby_duck = _work(
            title="Moby Duck", authors="donovan hohn", fiction=False
        )
        result.moby_duck.presentation_edition.subtitle = (
            "The True Story of 28,800 Bath Toys Lost at Sea"
        )
        result.moby_duck.summary_text = "A compulsively readable narrative"
        result.moby_duck.presentation_edition.series_position = 1
        result.moby_duck.presentation_edition.publisher = "Penguin"
        result.moby_duck.random = 0.9

        result.untitled = _work(title="[Untitled]", authors="[Unknown]")
        result.untitled.random = 0.99
        result.untitled.presentation_edition.series_position = 5

        # It's easier to refer to the books as a, b, and c when not
        # testing sorts that rely on the metaresult.
        result.a = result.moby_dick
        result.b = result.moby_duck
        result.c = result.untitled

        result.a.last_update_time = datetime_utc(2000, 1, 1)
        result.b.last_update_time = datetime_utc(2001, 1, 1)
        result.c.last_update_time = datetime_utc(2002, 1, 1)

        # Each work has one LicensePool associated with the default
        # collection.
        result.collection1 = transaction.default_collection()
        result.collection1.name = "Collection 1 - ACB"
        [result.a1] = result.a.license_pools
        [result.b1] = result.b.license_pools
        [result.c1] = result.c.license_pools
        result.a1.availability_time = datetime_utc(2010, 1, 1)
        result.c1.availability_time = datetime_utc(2011, 1, 1)
        result.b1.availability_time = datetime_utc(2012, 1, 1)

        # Here's a second collection with the same books in a different
        # order.
        result.collection2 = transaction.collection(name="Collection 2 - BAC")
        result.a2 = transaction.licensepool(
            edition=result.a.presentation_edition,
            collection=result.collection2,
            with_open_access_download=True,
        )
        result.a.license_pools.append(result.a2)
        result.b2 = transaction.licensepool(
            edition=result.b.presentation_edition,
            collection=result.collection2,
            with_open_access_download=True,
        )
        result.b.license_pools.append(result.b2)
        result.c2 = transaction.licensepool(
            edition=result.c.presentation_edition,
            collection=result.collection2,
            with_open_access_download=True,
        )
        result.c.license_pools.append(result.c2)
        result.b2.availability_time = datetime_utc(2020, 1, 1)
        result.a2.availability_time = datetime_utc(2021, 1, 1)
        result.c2.availability_time = datetime_utc(2022, 1, 1)

        # Here are three custom lists which contain the same books but
        # with different first appearances.
        result.list1, ignore = transaction.customlist(
            name="Custom list 1 - BCA", num_entries=0
        )
        result.list1.add_entry(result.b, first_appearance=datetime_utc(2030, 1, 1))
        result.list1.add_entry(result.c, first_appearance=datetime_utc(2031, 1, 1))
        result.list1.add_entry(result.a, first_appearance=datetime_utc(2032, 1, 1))

        result.list2, ignore = transaction.customlist(
            name="Custom list 2 - CAB", num_entries=0
        )
        result.list2.add_entry(result.c, first_appearance=datetime_utc(2001, 1, 1))
        result.list2.add_entry(result.a, first_appearance=datetime_utc(2014, 1, 1))
        result.list2.add_entry(result.b, first_appearance=datetime_utc(2015, 1, 1))

        result.list3, ignore = transaction.customlist(
            name="Custom list 3 -- CA", num_entries=0
        )
        result.list3.add_entry(result.a, first_appearance=datetime_utc(2032, 1, 1))
        result.list3.add_entry(result.c, first_appearance=datetime_utc(1999, 1, 1))

        # Create two custom lists which contain some of the same books,
        # but with different first appearances.

        result.by_publication_date, ignore = transaction.customlist(
            name="First appearance on list is publication date", num_entries=0
        )
        result.by_publication_date.add_entry(
            result.moby_duck, first_appearance=datetime_utc(2011, 3, 1)
        )
        result.by_publication_date.add_entry(
            result.untitled, first_appearance=datetime_utc(2018, 1, 1)
        )

        result.staff_picks, ignore = transaction.customlist(
            name="First appearance is date book was made a staff pick", num_entries=0
        )
        result.staff_picks.add_entry(
            result.moby_dick, first_appearance=datetime_utc(2015, 5, 2)
        )
        result.staff_picks.add_entry(
            result.moby_duck, first_appearance=datetime_utc(2012, 8, 30)
        )

        # Create two extra works, d and e, which are only used to
        # demonstrate one case.
        #
        # The custom list and the collection both put d earlier than e, but the
        # last_update_time wins out, and it puts e before d.
        result.collection3 = transaction.collection()
        result.d = transaction.work(
            collection=result.collection3, with_license_pool=True
        )
        result.e = transaction.work(
            collection=result.collection3, with_license_pool=True
        )
        result.d.license_pools[0].availability_time = datetime_utc(2010, 1, 1)
        result.e.license_pools[0].availability_time = datetime_utc(2011, 1, 1)

        result.extra_list, ignore = transaction.customlist(num_entries=0)
        result.extra_list.add_entry(result.d, first_appearance=datetime_utc(2020, 1, 1))
        result.extra_list.add_entry(result.e, first_appearance=datetime_utc(2021, 1, 1))

        result.e.last_update_time = datetime_utc(2090, 1, 1)
        result.d.last_update_time = datetime_utc(2091, 1, 1)
        return result

    def test_ordering(self, end_to_end_search_fixture: EndToEndSearchFixture):
        fixture = end_to_end_search_fixture
        transaction = fixture.external_search.db
        data = self._populate_works(fixture)
        fixture.populate_search_index()

        def assert_order(sort_field, order, **filter_kwargs):
            """Verify that when the books created during test setup are ordered by
            the given `sort_field`, they show up in the given `order`.

            Also verify that when the search is ordered descending,
            the same books show up in the opposite order. This proves
            that `sort_field` isn't being ignored creating a test that
            only succeeds by chance.

            :param sort_field: Sort by this field.
            :param order: A list of books in the expected order.
            :param filter_kwargs: Extra keyword arguments to be passed
               into the `Filter` constructor.
            """
            expect = fixture.expect_results
            facets = Facets(
                transaction.default_library(),
                Facets.COLLECTION_FULL,
                Facets.AVAILABLE_ALL,
                order=sort_field,
                order_ascending=True,
            )
            expect(order, None, Filter(facets=facets, **filter_kwargs))

            facets.order_ascending = False
            expect(list(reversed(order)), None, Filter(facets=facets, **filter_kwargs))

            # Get each item in the list as a separate page. This
            # proves that pagination works for this sort order for
            # both Pagination and SortKeyPagination.
            facets.order_ascending = True
            for pagination_class in (Pagination, SortKeyPagination):
                pagination = pagination_class(size=1)
                to_process = list(order) + [[]]
                while to_process:
                    filter = Filter(facets=facets, **filter_kwargs)
                    expect_result = to_process.pop(0)
                    expect(expect_result, None, filter, pagination=pagination)
                    pagination = pagination.next_page

                # We are now off the edge of the list -- we got an
                # empty page of results and there is no next page.
                assert None == pagination

            # Now try the same tests but in reverse order.
            facets.order_ascending = False
            for pagination_class in (Pagination, SortKeyPagination):
                pagination = pagination_class(size=1)
                to_process = list(reversed(order)) + [[]]
                results = []
                pagination = SortKeyPagination(size=1)
                while to_process:
                    filter = Filter(facets=facets, **filter_kwargs)
                    expect_result = to_process.pop(0)
                    expect(expect_result, None, filter, pagination=pagination)
                    pagination = pagination.next_page
                # We are now off the edge of the list -- we got an
                # empty page of results and there is no next page.
                assert None == pagination

        # We can sort by title.
        assert_order(
            Facets.ORDER_TITLE,
            [
                data.untitled,
                data.moby_dick,
                data.moby_duck,
            ],
            collections=[transaction.default_collection()],
        )

        # We can sort by author; 'Hohn' sorts before 'Melville' sorts
        # before "[Unknown]"
        assert_order(
            Facets.ORDER_AUTHOR,
            [
                data.moby_duck,
                data.moby_dick,
                data.untitled,
            ],
            collections=[transaction.default_collection()],
        )

        # We can sort by series position. Here, the books aren't in
        # the same series; in a real scenario we would also filter on
        # the value of 'series'.
        assert_order(
            Facets.ORDER_SERIES_POSITION,
            [
                data.moby_duck,
                data.untitled,
                data.moby_dick,
            ],
            collections=[transaction.default_collection()],
        )

        # We can sort by internal work ID, which isn't very useful.
        assert_order(
            Facets.ORDER_WORK_ID,
            [
                data.moby_dick,
                data.moby_duck,
                data.untitled,
            ],
            collections=[transaction.default_collection()],
        )

        # We can sort by the time the Work's LicensePools were first
        # seen -- this would be used when showing patrons 'new' stuff.
        #
        # The LicensePools showed up in different orders in different
        # collections, so filtering by collection will give different
        # results.
        assert_order(
            Facets.ORDER_ADDED_TO_COLLECTION,
            [data.a, data.c, data.b],
            collections=[data.collection1],
        )

        assert_order(
            Facets.ORDER_ADDED_TO_COLLECTION,
            [data.b, data.a, data.c],
            collections=[data.collection2],
        )

        # If a work shows up with multiple availability times through
        # multiple collections, the earliest availability time for
        # that work is used. All the dates in collection 1 predate the
        # dates in collection 2, so collection 1's ordering holds
        # here.
        assert_order(
            Facets.ORDER_ADDED_TO_COLLECTION,
            [data.a, data.c, data.b],
            collections=[data.collection1, data.collection2],
        )

        # Finally, here are the tests of ORDER_LAST_UPDATE, as described
        # above in setup().
        assert_order(
            Facets.ORDER_LAST_UPDATE,
            [
                data.a,
                data.b,
                data.c,
                data.e,
                data.d,
            ],
        )

        assert_order(
            Facets.ORDER_LAST_UPDATE,
            [data.a, data.c, data.b],
            collections=[data.collection1],
        )

        assert_order(
            Facets.ORDER_LAST_UPDATE,
            [data.b, data.a, data.c],
            collections=[data.collection1, data.collection2],
        )

        assert_order(
            Facets.ORDER_LAST_UPDATE,
            [data.b, data.c, data.a],
            customlist_restriction_sets=[[data.list1]],
        )

        assert_order(
            Facets.ORDER_LAST_UPDATE,
            [data.c, data.a, data.b],
            collections=[data.collection1],
            customlist_restriction_sets=[[data.list2]],
        )

        assert_order(
            Facets.ORDER_LAST_UPDATE,
            [data.c, data.a],
            customlist_restriction_sets=[
                [data.list1],
                [data.list3],
            ],
        )

        assert_order(
            Facets.ORDER_LAST_UPDATE,
            [data.e, data.d],
            collections=[data.collection3],
            customlist_restriction_sets=[[data.extra_list]],
        )


class TestAuthorFilterData:
    full: Contributor
    display_name: Contributor
    sort_name: Contributor
    viaf: Contributor
    lc: Contributor
    works: List[Work]
    literary_wonderlands: Work
    ubik: Work
    justice: Work
    sword: Work
    mercy: Work
    provenance: Work
    raven: Work


class TestAuthorFilter:
    # Test the various techniques used to find books where a certain
    # person had an authorship role.

    @staticmethod
    def _populate_works(
        data: EndToEndSearchFixture,
    ) -> TestAuthorFilterData:
        transaction, session = (
            data.external_search.db,
            data.external_search.db.session,
        )
        _work = data.external_search.default_work

        # Create a number of Contributor objects--some fragmentary--
        # representing the same person.
        result = TestAuthorFilterData()
        result.full = Contributor(
            display_name="Ann Leckie",
            sort_name="Leckie, Ann",
            viaf="73520345",
            lc="n2013008575",
        )
        result.display_name = Contributor(
            sort_name=Edition.UNKNOWN_AUTHOR, display_name="ann leckie"
        )
        result.sort_name = Contributor(sort_name="LECKIE, ANN")
        result.viaf = Contributor(sort_name=Edition.UNKNOWN_AUTHOR, viaf="73520345")
        result.lc = Contributor(sort_name=Edition.UNKNOWN_AUTHOR, lc="n2013008575")

        # Create a different Work for every Contributor object.
        # Alternate among the various 'author match' roles.
        result.works = []
        roles = list(Filter.AUTHOR_MATCH_ROLES)
        for i, (contributor, title, attribute) in enumerate(
            [
                (result.full, "Ancillary Justice", "justice"),
                (result.display_name, "Ancillary Sword", "sword"),
                (result.sort_name, "Ancillary Mercy", "mercy"),
                (result.viaf, "Provenance", "provenance"),
                (result.lc, "Raven Tower", "raven"),
            ]
        ):
            session.add(contributor)
            edition, ignore = transaction.edition(
                title=title, authors=[], with_license_pool=True
            )
            contribution, was_new = get_one_or_create(
                session,
                Contribution,
                edition=edition,
                contributor=contributor,
                role=roles[i % len(roles)],
            )
            work = data.external_search.default_work(
                presentation_edition=edition,
            )
            result.works.append(work)
            setattr(result, attribute, work)

        # This work is a decoy. The author we're looking for
        # contributed to the work in an ineligible role, so it will
        # always be filtered out.
        edition, ignore = transaction.edition(
            title="Science Fiction: The Best of the Year (2007 Edition)",
            authors=[],
            with_license_pool=True,
        )
        contribution, is_new = get_one_or_create(
            session,
            Contribution,
            edition=edition,
            contributor=result.full,
            role=Contributor.CONTRIBUTOR_ROLE,
        )
        result.literary_wonderlands = data.external_search.default_work(
            presentation_edition=edition
        )

        # Another decoy. This work is by a different person and will
        # always be filtered out.
        result.ubik = data.external_search.default_work(
            title="Ubik", authors=["Phillip K. Dick"]
        )
        return result

    def test_author_match(self, end_to_end_search_fixture: EndToEndSearchFixture):
        fixture = end_to_end_search_fixture
        data = self._populate_works(fixture)
        fixture.populate_search_index()

        # By providing a Contributor object with all the identifiers,
        # we get every work with an author-type contribution from
        # someone who can be identified with that Contributor.
        fixture.expect_results(
            data.works,
            None,
            Filter(author=data.full),
            ordered=False,
        )

        # If we provide a Contributor object with partial information,
        # we can only get works that are identifiable with that
        # Contributor through the information provided.
        #
        # In all cases below we will find 'Ancillary Justice', since
        # the Contributor associated with that work has all the
        # identifiers.  In each case we will also find one additional
        # work -- the one associated with the Contributor whose
        # data overlaps what we're passing in.
        for filter, extra in [
            (Filter(author=data.display_name), data.sword),
            (Filter(author=data.sort_name), data.mercy),
            (Filter(author=data.viaf), data.provenance),
            (Filter(author=data.lc), data.raven),
        ]:
            fixture.expect_results([data.justice, extra], None, filter, ordered=False)

        # ContributorData also works here.

        # By specifying two types of author identification we'll find
        # three books -- the one that knows its author's sort_name,
        # the one that knows its author's VIAF number, and the one
        # that knows both.
        author = ContributorData(sort_name="Leckie, Ann", viaf="73520345")
        fixture.expect_results(
            [data.justice, data.mercy, data.provenance],
            None,
            Filter(author=author),
            ordered=False,
        )

        # The filter can also accommodate very minor variants in names
        # such as those caused by capitalization differences and
        # accented characters.
        for variant in ("ann leckie", "Àñn Léckiê"):
            author = ContributorData(display_name=variant)
            fixture.expect_results(
                [data.justice, data.sword],
                None,
                Filter(author=author),
                ordered=False,
            )

        # It cannot accommodate misspellings, no matter how minor.
        author = ContributorData(display_name="Anne Leckie")
        fixture.expect_results([], None, Filter(author=author))

        # If the information in the ContributorData is inconsistent,
        # the results may also be inconsistent.
        author = ContributorData(sort_name="Dick, Phillip K.", lc="n2013008575")
        fixture.expect_results(
            [data.justice, data.raven, data.ubik],
            None,
            Filter(author=author),
            ordered=False,
        )


class TestExactMatchesData:
    modern_romance: Work
    ya_romance: Work
    parent_book: Work
    behind_the_scenes: Work
    biography_of_peter_graves: Work
    book_by_peter_graves: Work
    book_by_someone_else: Work


class TestExactMatches:
    """Verify that exact or near-exact title and author matches are
    privileged over matches that span fields.
    """

    @staticmethod
    def _populate_works(
        data: EndToEndSearchFixture,
    ) -> TestExactMatchesData:
        _work = data.external_search.default_work

        # Here the title is 'Modern Romance'
        result = TestExactMatchesData()
        result.modern_romance = _work(
            title="Modern Romance",
            authors=["Aziz Ansari", "Eric Klinenberg"],
        )
        # Here 'Modern' is in the subtitle and 'Romance' is the genre.
        result.ya_romance = _work(
            title="Gumby In Love",
            authors="Pokey",
            audience=Classifier.AUDIENCE_YOUNG_ADULT,
            genre="Romance",
        )
        result.ya_romance.presentation_edition.subtitle = (
            "Modern Fairytale Series, Book 3"
        )
        result.parent_book = _work(
            title="Our Son Aziz",
            authors=["Fatima Ansari", "Shoukath Ansari"],
            genre="Biography & Memoir",
        )
        result.behind_the_scenes = _work(
            title="The Making of Biography With Peter Graves",
            genre="Entertainment",
        )
        result.biography_of_peter_graves = _work(
            "He Is Peter Graves",
            authors="Kelly Ghostwriter",
            genre="Biography & Memoir",
        )
        result.book_by_peter_graves = _work(
            title="My Experience At The University of Minnesota",
            authors="Peter Graves",
            genre="Entertainment",
        )
        result.book_by_someone_else = _work(
            title="The Deadly Graves", authors="Peter Ansari", genre="Mystery"
        )
        return result

    def test_exact_matches(self, end_to_end_search_fixture: EndToEndSearchFixture):
        fixture = end_to_end_search_fixture
        data = self._populate_works(fixture)
        fixture.populate_search_index()
        expect = fixture.expect_results

        # A full title match takes precedence over a match that's
        # split across genre and subtitle.
        expect(
            [
                data.modern_romance,  # "modern romance" in title
                data.ya_romance,  # "modern" in subtitle, genre "romance"
            ],
            "modern romance",
        )

        # A full author match takes precedence over a partial author
        # match. A partial author match ("peter ansari") doesn't show up
        # all all because it can't match two words.
        expect(
            [
                data.modern_romance,  # "Aziz Ansari" in author
                data.parent_book,  # "Aziz" in title, "Ansari" in author
            ],
            "aziz ansari",
        )

        # 'peter graves' is a string that has exact matches in both
        # title and author.

        # Books with author 'Peter Graves' are the top match, since
        # "peter graves" matches the entire string. Books with "Peter
        # Graves" in the title are the next results, ordered by how
        # much other stuff is in the title. A partial match split
        # across fields ("peter" in author, "graves" in title) is the
        # last result.
        order = [
            data.book_by_peter_graves,
            data.biography_of_peter_graves,
            data.behind_the_scenes,
            data.book_by_someone_else,
        ]
        expect(order, "peter graves")

        # Now we throw in "biography", a term that is both a genre and
        # a search term in its own right.
        #
        # 1. A book whose title mentions all three terms
        # 2. A book in genre "biography" whose title
        #    matches the other two terms
        # 3. A book with an author match containing two of the terms.
        #    'biography' just doesn't match. That's okay --
        #    if there are more than two search terms, only two must match.

        order = [
            data.behind_the_scenes,  # all words match in title
            data.biography_of_peter_graves,  # title + genre 'biography'
            data.book_by_peter_graves,  # author (no 'biography')
        ]

        expect(order, "peter graves biography")


class TestFeaturedFacetsData:
    hq_not_available: Work
    hq_available: Work
    hq_available_2: Work
    not_featured_on_list: Work
    featured_on_list: Work
    best_seller_list: Work


class TestFeaturedFacets:
    """Test how a FeaturedFacets object affects search ordering."""

    @staticmethod
    def _populate_works(
        data: EndToEndSearchFixture,
    ) -> TestFeaturedFacetsData:
        transaction, session = (
            data.external_search.db,
            data.external_search.db.session,
        )
        _work = data.external_search.default_work

        result = TestFeaturedFacetsData()
        result.hq_not_available = _work(title="HQ but not available")
        result.hq_not_available.quality = 1
        result.hq_not_available.license_pools[0].licenses_available = 0

        result.hq_available = _work(title="HQ and available")
        result.hq_available.quality = 1

        result.hq_available_2 = _work(title="Also HQ and available")
        result.hq_available_2.quality = 1

        result.not_featured_on_list = _work(title="On a list but not featured")
        result.not_featured_on_list.quality = 0.19

        # This work has nothing going for it other than the fact
        # that it's been featured on a custom list.
        result.featured_on_list = _work(title="Featured on a list")
        result.featured_on_list.quality = 0.18
        result.featured_on_list.license_pools[0].licenses_available = 0

        result.best_seller_list, ignore = transaction.customlist(num_entries=0)
        result.best_seller_list.add_entry(result.featured_on_list, featured=True)
        result.best_seller_list.add_entry(result.not_featured_on_list)
        return result

    def test_scoring_functions(self, end_to_end_search_fixture: EndToEndSearchFixture):
        fixture = end_to_end_search_fixture
        data = self._populate_works(fixture)
        fixture.populate_search_index()

        # Verify that FeaturedFacets sets appropriate scoring functions
        # for OpenSearch queries.
        f = FeaturedFacets(minimum_featured_quality=0.55, random_seed=42)
        filter = Filter()
        f.modify_search_filter(filter)

        # In most cases, there are three things that can boost a work's score.
        [featurable, available_now, random] = f.scoring_functions(filter)

        # It can be high-quality enough to be featured.
        assert isinstance(featurable, ScriptScore)
        source = filter.FEATURABLE_SCRIPT % dict(
            cutoff=f.minimum_featured_quality**2, exponent=2
        )
        assert source == featurable.script["source"]

        # It can be currently available.
        availability_filter = available_now["filter"]
        assert (
            dict(
                nested=dict(
                    path="licensepools",
                    query=dict(term={"licensepools.available": True}),
                )
            )
            == availability_filter.to_dict()
        )
        assert 5 == available_now["weight"]

        # It can get lucky.
        assert isinstance(random, RandomScore)
        assert 42 == random.seed
        assert 1.1 == random.weight

        # If the FeaturedFacets is set to be deterministic (which only happens
        # in tests), the RandomScore is removed.
        f.random_seed = filter.DETERMINISTIC
        [featurable_2, available_now_2] = f.scoring_functions(filter)
        assert featurable_2 == featurable
        assert available_now_2 == available_now

        # If custom lists are in play, it can also be featured on one
        # of its custom lists.
        filter.customlist_restriction_sets = [[1, 2], [3]]
        [featurable_2, available_now_2, featured_on_list] = f.scoring_functions(filter)
        assert featurable_2 == featurable
        assert available_now_2 == available_now

        # Any list will do -- the customlist restriction sets aren't
        # relevant here.
        featured_filter = featured_on_list["filter"]
        assert (
            dict(
                nested=dict(
                    path="customlists",
                    query=dict(
                        bool=dict(
                            must=[
                                {"term": {"customlists.featured": True}},
                                {"terms": {"customlists.list_id": [1, 2, 3]}},
                            ]
                        )
                    ),
                )
            )
            == featured_filter.to_dict()
        )
        assert 11 == featured_on_list["weight"]

    def test_run(self, end_to_end_search_fixture: EndToEndSearchFixture):
        fixture = end_to_end_search_fixture
        transaction, session = (
            fixture.external_search.db,
            fixture.external_search.db.session,
        )
        data = self._populate_works(fixture)
        fixture.populate_search_index()

        def works(worklist, facets):
            return worklist.works(
                session, facets, None, fixture.external_search.search, debug=True
            )

        def assert_featured(description, worklist, facets, expect):
            # Generate a list of featured works for the given `worklist`
            # and compare that list against `expect`.
            actual = works(worklist, facets)
            fixture.assert_works(description, expect, actual)

        worklist = WorkList()
        worklist.initialize(transaction.default_library())
        facets = FeaturedFacets(1, random_seed=Filter.DETERMINISTIC)

        # Even though hq_not_available is higher-quality than
        # not_featured_on_list, not_featured_on_list shows up first because
        # it's available right now.
        w = works(worklist, facets)
        assert w.index(data.not_featured_on_list) < w.index(data.hq_not_available)

        # not_featured_on_list shows up before featured_on_list because
        # it's higher-quality and list membership isn't relevant.
        assert w.index(data.not_featured_on_list) < w.index(data.featured_on_list)

        # Create a WorkList that's restricted to best-sellers.
        best_sellers = WorkList()
        best_sellers.initialize(
            transaction.default_library(), customlists=[data.best_seller_list]
        )
        # The featured work appears above the non-featured work,
        # even though it's lower quality and is not available.
        assert_featured(
            "Works from WorkList based on CustomList",
            best_sellers,
            facets,
            [data.featured_on_list, data.not_featured_on_list],
        )

        # By changing the minimum_featured_quality you can control
        # at what point a work is considered 'featured' -- at which
        # point its quality stops being taken into account.
        #
        # An extreme case of this is to set the minimum_featured_quality
        # to 0, which makes all works 'featured' and stops quality
        # from being considered altogether. Basically all that matters
        # is availability.
        all_featured_facets = FeaturedFacets(0, random_seed=Filter.DETERMINISTIC)
        # We don't know exactly what order the books will be in,
        # because even without the random element Opensearch is
        # slightly nondeterministic, but we do expect that all of the
        # available books will show up before all of the unavailable
        # books.
        only_availability_matters = worklist.works(
            session, facets, None, fixture.external_search.search, debug=True
        )
        assert 5 == len(only_availability_matters)
        last_two = only_availability_matters[-2:]
        assert data.hq_not_available in last_two
        assert data.featured_on_list in last_two

        # Up to this point we've been avoiding the random element,
        # but we can introduce that now by passing in a numeric seed.
        # In normal usage, the current time is used as the seed.
        #
        # The random element is relatively small, so it mainly acts
        # to rearrange works whose scores were similar before.
        #
        # The order of the works when using random depends on 4 things:
        # - The seed
        # - The id (work_id)
        # - The index name
        # - The shard id
        # If any of those change the order of works in this result may change,
        # and hence the order of works in this assert must also change
        # E.g. If the index version changes from v5 to v6, this may affect the order of works queried
        # Keeping everything else the same, the order of works will remain reproducible across test runs
        random_facets = FeaturedFacets(1, random_seed=43)
        assert_featured(
            "Works permuted by a random seed",
            worklist,
            random_facets,
            [
                data.hq_available_2,
                data.hq_available,
                data.hq_not_available,
                data.not_featured_on_list,
                data.featured_on_list,
            ],
        )


class TestSearchBase:
    def test__boost(self):
        # Verify that _boost() converts a regular query (or list of queries)
        # into a boosted query.
        m = SearchBase._boost
        q1 = Q("simple_query_string", query="query 1")
        q2 = Q("simple_query_string", query="query 2")

        boosted_one = m(10, q1)
        assert "bool" == boosted_one.name
        assert 10.0 == boosted_one.boost
        assert [q1] == boosted_one.must

        # By default, if you pass in multiple queries, only one of them
        # must match for the boost to apply.
        boosted_multiple = m(4.5, [q1, q2])
        assert "bool" == boosted_multiple.name
        assert 4.5 == boosted_multiple.boost
        assert 1 == boosted_multiple.minimum_should_match
        assert [q1, q2] == boosted_multiple.should

        # Here, every query must match for the boost to apply.
        boosted_multiple = m(4.5, [q1, q2], all_must_match=True)
        assert "bool" == boosted_multiple.name
        assert 4.5 == boosted_multiple.boost
        assert [q1, q2] == boosted_multiple.must

    def test__nest(self):
        # Test the _nest method, which turns a normal query into a
        # nested query.
        query = Term(**{"nested_field": "value"})
        nested = SearchBase._nest("subdocument", query)
        assert Nested(path="subdocument", query=query) == nested

    def test_nestable(self):
        # Test the _nestable helper method, which turns a normal
        # query into an appropriate nested query, if necessary.
        m = SearchBase._nestable

        # A query on a field that's not in a subdocument is
        # unaffected.
        field = "name.minimal"
        normal_query = Term(**{field: "name"})
        assert normal_query == m(field, normal_query)

        # A query on a subdocument field becomes a nested query on
        # that subdocument.
        field = "contributors.sort_name.minimal"
        subdocument_query = Term(**{field: "name"})
        nested = m(field, subdocument_query)
        assert Nested(path="contributors", query=subdocument_query) == nested

    def test__match_term(self):
        # _match_term creates a Match Opensearch object which does a
        # match against a specific field.
        m = SearchBase._match_term
        qu = m("author", "flannery o'connor")
        assert Term(author="flannery o'connor") == qu

        # If the field name references a subdocument, the query is
        # embedded in a Nested object that describes how to match it
        # against that subdocument.
        field = "genres.name"
        qu = m(field, "Biography")
        assert Nested(path="genres", query=Term(**{field: "Biography"})) == qu

    def test__match_range(self):
        # Test the _match_range helper method.
        # This is used to create an Opensearch query term
        # that only matches if a value is in a given range.

        # This only matches if field.name has a value >= 5.
        r = SearchBase._match_range("field.name", "gte", 5)
        assert r == {"range": {"field.name": {"gte": 5}}}

    def test__combine_hypotheses(self):
        # Verify that _combine_hypotheses creates a DisMax query object
        # that chooses the best one out of whichever queries it was passed.
        m = SearchBase._combine_hypotheses

        h1 = Term(field="value 1")
        h2 = Term(field="value 2")
        hypotheses = [h1, h2]
        combined = m(hypotheses)
        assert DisMax(queries=hypotheses) == combined

        # If there are no hypotheses to test, _combine_hypotheses creates
        # a MatchAll instead.
        assert MatchAll() == m([])

    def test_make_target_age_query(self):
        # Search for material suitable for children between the
        # ages of 5 and 10.
        #
        # This gives us two similar queries: one to use as a filter
        # and one to use as a boost query.
        as_filter, as_query = Query.make_target_age_query((5, 10))

        # Here's the filter part: a book's age range must be include the
        # 5-10 range, or it gets filtered out.
        filter_clauses = [
            Range(**{"target_age.upper": dict(gte=5)}),
            Range(**{"target_age.lower": dict(lte=10)}),
        ]
        assert Bool(must=filter_clauses) == as_filter

        # Here's the query part: a book gets boosted if its
        # age range fits _entirely_ within the target age range.
        query_clauses = [
            Range(**{"target_age.upper": dict(lte=10)}),
            Range(**{"target_age.lower": dict(gte=5)}),
        ]
        assert Bool(boost=1.1, must=filter_clauses, should=query_clauses) == as_query


class TestQuery:
    def test_constructor(self):
        # Verify that the Query constructor sets members with
        # no processing.
        filter = Filter()
        query = Query("query string", filter)
        assert "query string" == query.query_string
        assert filter == query.filter

        # The query string does not contain English stopwords.
        assert False == query.contains_stopwords

        # Every word in the query string passes spellcheck,
        # so a fuzzy query will be given less weight.
        assert 0.5 == query.fuzzy_coefficient

        # Try again with a query containing a stopword and
        # a word that fails spellcheck.
        query = Query("just a xlomph")
        assert True == query.contains_stopwords
        assert 1 == query.fuzzy_coefficient

        # Try again with a query that contains no query string.
        # The fuzzy hypotheses will not be run at all.
        query = Query(None)
        assert None == query.contains_stopwords
        assert 0 == query.fuzzy_coefficient

    def test_build(self, db: DatabaseTransactionFixture):
        # Verify that the build() method combines the 'query' part of
        # a Query and the 'filter' part to create a single
        # Opensearch Search object, complete with (if necessary)
        # subqueries, sort ordering, and script fields.

        class MockSearch:
            """A mock of the Opensearch-DSL `Search` object.

            Calls to Search methods tend to create a new Search object
            based on the old one. This mock simulates that behavior.
            If necessary, you can look at all MockSearch objects
            created by to get to a certain point by following the
            .parent relation.
            """

            def __init__(
                self,
                parent=None,
                query=None,
                nested_filter_calls=None,
                order=None,
                script_fields=None,
            ):
                self.parent = parent
                self._query = query
                self.nested_filter_calls = nested_filter_calls or []
                self.order = order
                self._script_fields = script_fields

            def filter(self, **kwargs):
                """Simulate the application of a nested filter.

                :return: A new MockSearch object.
                """
                new_filters = self.nested_filter_calls + [kwargs]
                return MockSearch(
                    self, self._query, new_filters, self.order, self._script_fields
                )

            def query(self, query):
                """Simulate the creation of an Opensearch-DSL `Search`
                object from an Opensearch-DSL `Query` object.

                :return: A New MockSearch object.
                """
                return MockSearch(
                    self,
                    query,
                    self.nested_filter_calls,
                    self.order,
                    self._script_fields,
                )

            def sort(self, *order_fields):
                """Simulate the application of a sort order."""
                return MockSearch(
                    self,
                    self._query,
                    self.nested_filter_calls,
                    order_fields,
                    self._script_fields,
                )

            def script_fields(self, **kwargs):
                """Simulate the addition of script fields."""
                return MockSearch(
                    self, self._query, self.nested_filter_calls, self.order, kwargs
                )

        class MockQuery(Query):
            # A Mock of the Query object from external_search
            # (not the one from Opensearch-DSL).
            @property
            def search_query(self):
                return Q("simple_query_string", query=self.query_string)

        class MockPagination:
            def modify_search_query(self, search):
                return search.filter(name_or_query="pagination modified")

        # That's a lot of mocks, but here's one more. Mock the Filter
        # class's universal_base_filter() and
        # universal_nested_filters() methods. These methods queue up
        # all kinds of modifications to queries, so it's better to
        # replace them with simpler versions.
        class MockFilter:
            universal_base_term = Q("term", universal_base_called=True)
            universal_nested_term = Q("term", universal_nested_called=True)
            universal_nested_filter = dict(nested_called=[universal_nested_term])

            @classmethod
            def universal_base_filter(cls):
                cls.universal_called = True
                return cls.universal_base_term

            @classmethod
            def universal_nested_filters(cls):
                cls.nested_called = True
                return cls.universal_nested_filter

            @classmethod
            def validate_universal_calls(cls):
                """Verify that both universal methods were called
                and that the return values were incorporated into
                the query being built by `search`.

                This method modifies the `search` object in place so
                that the rest of a test can ignore all the universal
                stuff.
                """
                assert True == cls.universal_called
                assert True == cls.nested_called

                # Reset for next time.
                cls.base_called = None
                cls.nested_called = None

        original_base = Filter.universal_base_filter
        original_nested = Filter.universal_nested_filters
        Filter.universal_base_filter = MockFilter.universal_base_filter
        Filter.universal_nested_filters = MockFilter.universal_nested_filters

        # Test the simple case where the Query has no filter.
        qu = MockQuery("query string", filter=None)
        search = MockSearch()
        pagination = MockPagination()
        built = qu.build(search, pagination)

        # The return value is a new MockSearch object based on the one
        # that was passed in.
        assert isinstance(built, MockSearch)
        assert search == built.parent.parent.parent

        # The (mocked) universal base query and universal nested
        # queries were called.
        MockFilter.validate_universal_calls()

        # The mocked universal base filter was the first
        # base filter to be applied.
        universal_base_term = built._query.filter.pop(0)
        assert MockFilter.universal_base_term == universal_base_term

        # The pagination filter was the last one to be applied.
        pagination = built.nested_filter_calls.pop()
        assert dict(name_or_query="pagination modified") == pagination

        # The mocked universal nested filter was applied
        # just before that.
        universal_nested = built.nested_filter_calls.pop()
        assert (
            dict(
                name_or_query="nested",
                path="nested_called",
                query=Bool(filter=[MockFilter.universal_nested_term]),
            )
            == universal_nested
        )

        # The result of Query.search_query is used as the basis
        # for the Search object.
        assert Bool(must=qu.search_query) == built._query

        # Now test some cases where the query has a filter.

        # If there's a filter, a boolean Query object is created to
        # combine the original Query with the filter.
        filter = Filter(fiction=True)
        qu = MockQuery("query string", filter=filter)
        built = qu.build(search)
        MockFilter.validate_universal_calls()

        # The 'must' part of this new Query came from calling
        # Query.query() on the original Query object.
        #
        # The 'filter' part came from calling Filter.build() on the
        # main filter.
        underlying_query = built._query

        # The query we passed in is used as the 'must' part of the
        assert underlying_query.must == [qu.search_query]
        main_filter, nested_filters = filter.build()

        # The filter we passed in was combined with the universal
        # base filter into a boolean query, with its own 'must'.
        main_filter.must = main_filter.must + [MockFilter.universal_base_term]
        assert underlying_query.filter == [main_filter]

        # There are no nested filters, apart from the universal one.
        assert {} == nested_filters
        universal_nested = built.nested_filter_calls.pop()
        assert (
            dict(
                name_or_query="nested",
                path="nested_called",
                query=Bool(filter=[MockFilter.universal_nested_term]),
            )
            == universal_nested
        )
        assert [] == built.nested_filter_calls

        # At this point the universal filters are more trouble than they're
        # worth. Disable them for the rest of the test.
        MockFilter.universal_base_term = None
        MockFilter.universal_nested_filter = None

        # Now let's try a combination of regular filters and nested filters.
        filter = Filter(fiction=True, collections=[db.default_collection()])
        qu = MockQuery("query string", filter=filter)
        built = qu.build(search)
        underlying_query = built._query

        # We get a main filter (for the fiction restriction) and one
        # nested filter.
        main_filter, nested_filters = filter.build()
        [nested_licensepool_filter] = nested_filters.pop("licensepools")
        assert {} == nested_filters

        # As before, the main filter has been applied to the underlying
        # query.
        assert underlying_query.filter == [main_filter]

        # The nested filter was converted into a Bool query and passed
        # into Search.filter(). This applied an additional filter on the
        # 'licensepools' subdocument.
        [filter_call] = built.nested_filter_calls
        assert "nested" == filter_call["name_or_query"]
        assert "licensepools" == filter_call["path"]
        filter_as_query = filter_call["query"]
        assert Bool(filter=nested_licensepool_filter) == filter_as_query

        # Now we're going to test how queries are built to accommodate
        # various restrictions imposed by a Facets object.
        def from_facets(*args, **kwargs):
            """Build a Query object from a set of facets, then call
            build() on it.
            """
            facets = Facets(db.default_library(), *args, **kwargs)
            filter = Filter(facets=facets)
            qu = MockQuery("query string", filter=filter)
            built = qu.build(search)

            # Return the rest to be verified in a test-specific way.
            return built

        # When using the 'featured' collection...
        built = from_facets(Facets.COLLECTION_FEATURED, None, None)

        # There is no nested filter.
        assert [] == built.nested_filter_calls

        # A non-nested filter is applied on the 'quality' field.
        [quality_filter] = built._query.filter
        quality_range = Filter._match_range(
            "quality",
            "gte",
            db.default_library().minimum_featured_quality,
        )
        assert Q("bool", must=[quality_range], must_not=[RESEARCH]) == quality_filter

        # When using the AVAILABLE_OPEN_ACCESS availability restriction...
        built = from_facets(Facets.COLLECTION_FULL, Facets.AVAILABLE_OPEN_ACCESS, None)

        # An additional nested filter is applied.
        [available_now] = built.nested_filter_calls
        assert "nested" == available_now["name_or_query"]
        assert "licensepools" == available_now["path"]

        # It finds only license pools that are open access.
        nested_filter = available_now["query"]
        open_access = dict(term={"licensepools.open_access": True})
        assert nested_filter.to_dict() == {"bool": {"filter": [open_access]}}

        # When using the AVAILABLE_NOW restriction...
        built = from_facets(Facets.COLLECTION_FULL, Facets.AVAILABLE_NOW, None)

        # An additional nested filter is applied.
        [available_now] = built.nested_filter_calls
        assert "nested" == available_now["name_or_query"]
        assert "licensepools" == available_now["path"]

        # It finds only license pools that are open access *or* that have
        # active licenses.
        nested_filter = available_now["query"]
        available = {"term": {"licensepools.available": True}}
        assert nested_filter.to_dict() == {
            "bool": {
                "filter": [
                    {
                        "bool": {
                            "should": [open_access, available],
                            "minimum_should_match": 1,
                        }
                    }
                ]
            }
        }

        # When using the AVAILABLE_NOT_NOW restriction...
        built = from_facets(Facets.COLLECTION_FULL, Facets.AVAILABLE_NOT_NOW, None)

        # An additional nested filter is applied.
        [not_available_now] = built.nested_filter_calls
        assert "nested" == available_now["name_or_query"]
        assert "licensepools" == available_now["path"]

        # It finds only license pools that are licensed, but not
        # currently available or open access.
        nested_filter = not_available_now["query"]
        not_available = {"term": {"licensepools.available": False}}
        licensed = {"term": {"licensepools.licensed": True}}
        not_open_access = {"term": {"licensepools.open_access": False}}
        assert nested_filter.to_dict() == {
            "bool": {
                "filter": [
                    {"bool": {"must": [not_open_access, licensed, not_available]}}
                ]
            }
        }

        # If the Filter specifies script fields, those fields are
        # added to the Query through a call to script_fields()
        script_fields = dict(field1="Definition1", field2="Definition2")
        filter = Filter(script_fields=script_fields)
        qu = MockQuery("query string", filter=filter)
        built = qu.build(search)
        assert script_fields == built._script_fields

        # If the Filter specifies a sort order, Filter.sort_order is
        # used to convert it to appropriate Opensearch syntax, and
        # the MockSearch object is modified appropriately.
        built = from_facets(
            None, None, order=Facets.ORDER_AUTHOR, order_ascending=False
        )

        # We asked for sorting by author, and that's the primary
        # sort field.
        order = list(built.order)
        assert dict(sort_author="desc") == order.pop(0)

        # But a number of other sort fields are also employed to act
        # as tiebreakers.
        for tiebreaker_field in ("sort_title", "work_id"):
            assert {tiebreaker_field: "asc"} == order.pop(0)
        assert [] == order

        # Finally, undo the mock of the Filter class methods
        Filter.universal_base_filter = original_base
        Filter.universal_nested_filters = original_nested

    def test_build_match_nothing(self, db: DatabaseTransactionFixture):
        # No matter what the Filter looks like, if its .match_nothing
        # is set, it gets built into a simple filter that matches
        # nothing, with no nested subfilters.
        filter = Filter(
            fiction=True,
            collections=[db.default_collection()],
            match_nothing=True,
        )
        main, nested = filter.build()
        assert MatchNone() == main
        assert {} == nested

    def test_search_query(self):
        # The search_query property calls a number of other methods
        # to generate hypotheses, then creates a dis_max query
        # to find the most likely hypothesis for any given book.

        class Mock(Query):

            _match_phrase_called_with = []
            _boosts = {}
            _filters = {}
            _kwargs = {}

            def match_one_field_hypotheses(self, field):
                yield "match %s" % field, 1

            @property
            def match_author_hypotheses(self):
                yield "author query 1", 2
                yield "author query 2", 3

            @property
            def match_topic_hypotheses(self):
                yield "topic query", 4

            def title_multi_match_for(self, other_field):
                yield "multi match title+%s" % other_field, 5

            # Define this as a constant so it's easy to check later
            # in the test.
            SUBSTRING_HYPOTHESES = (
                "hypothesis based on substring",
                "another such hypothesis",
            )

            @property
            def parsed_query_matches(self):
                return self.SUBSTRING_HYPOTHESES, "only valid with this filter"

            def _hypothesize(
                self,
                hypotheses,
                new_hypothesis,
                boost="default",
                filters=None,
                **kwargs,
            ):
                self._boosts[new_hypothesis] = boost
                if kwargs:
                    self._kwargs[new_hypothesis] = kwargs
                if filters:
                    self._filters[new_hypothesis] = filters
                hypotheses.append(new_hypothesis)
                return hypotheses

            def _combine_hypotheses(self, hypotheses):
                self._combine_hypotheses_called_with = hypotheses
                return hypotheses

        # Before we get started, try an easy case. If there is no query
        # string we get a match_all query that returns everything.
        query = Mock(None)
        result = query.search_query
        assert dict(match_all=dict()) == result.to_dict()

        # Now try a real query string.
        q = "query string"
        query = Mock(q)
        result = query.search_query

        # The final result is the result of calling _combine_hypotheses
        # on a number of hypotheses. Our mock class just returns
        # the hypotheses as-is, for easier testing.
        assert result == query._combine_hypotheses_called_with

        # We ended up with a number of hypothesis:
        assert result == [
            # Several hypotheses checking whether the search query is an attempt to
            # match a single field -- the results of calling match_one_field()
            # many times.
            "match title",
            "match subtitle",
            "match series",
            "match publisher",
            "match imprint",
            # The results of calling match_author_queries() once.
            "author query 1",
            "author query 2",
            # The results of calling match_topic_queries() once.
            "topic query",
            # The results of calling multi_match() for three fields.
            "multi match title+subtitle",
            "multi match title+series",
            "multi match title+author",
            # The 'query' part of the return value of
            # parsed_query_matches()
            Mock.SUBSTRING_HYPOTHESES,
        ]

        # That's not the whole story, though. parsed_query_matches()
        # said it was okay to test certain hypotheses, but only
        # in the context of a filter.
        #
        # That filter was passed in to _hypothesize. Our mock version
        # of _hypothesize added it to the 'filters' dict to indicate
        # we know that those filters go with the substring
        # hypotheses. That's the only time 'filters' was touched.
        assert {
            Mock.SUBSTRING_HYPOTHESES: "only valid with this filter"
        } == query._filters

        # Each call to _hypothesize included a boost factor indicating
        # how heavily to weight that hypothesis. Rather than do
        # anything with this information -- which is mostly mocked
        # anyway -- we just stored it in _boosts.
        boosts = sorted(list(query._boosts.items()), key=lambda x: str(x[0]))
        boosts = sorted(boosts, key=lambda x: x[1])
        assert boosts == [
            ("match imprint", 1),
            ("match publisher", 1),
            ("match series", 1),
            ("match subtitle", 1),
            ("match title", 1),
            # The only non-mocked value here is this one. The
            # substring hypotheses have their own weights, which
            # we don't see in this test. This is saying that if a
            # book matches those sub-hypotheses and _also_ matches
            # the filter, then whatever weight it got from the
            # sub-hypotheses should be boosted slighty. This gives
            # works that match the filter an edge over works that
            # don't.
            (Mock.SUBSTRING_HYPOTHESES, 1.1),
            ("author query 1", 2),
            ("author query 2", 3),
            ("topic query", 4),
            ("multi match title+author", 5),
            ("multi match title+series", 5),
            ("multi match title+subtitle", 5),
        ]

    def test_match_one_field_hypotheses(self):
        # Test our ability to generate hypotheses that a search string
        # is trying to match a single field of data.
        class Mock(Query):
            WEIGHT_FOR_FIELD = dict(
                regular_field=2,
                stopword_field=3,
                stemmable_field=4,
            )
            STOPWORD_FIELDS = ["stopword_field"]
            STEMMABLE_FIELDS = ["stemmable_field"]

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.fuzzy_calls = {}

            def _fuzzy_matches(self, field_name, **kwargs):
                self.fuzzy_calls[field_name] = kwargs
                # 0.66 is an arbitrarily chosen value -- look
                # for it in the validate_fuzzy() helper method.
                yield "fuzzy match for %s" % field_name, 0.66

        # Let's start with the simplest case: no stopword variant, no
        # stemmed variant, no fuzzy variants.
        query = Mock("book")
        query.fuzzy_coefficient = 0
        m = query.match_one_field_hypotheses

        # We'll get a Term query and a MatchPhrase query.
        term, phrase = list(m("regular_field"))

        # The Term hypothesis tries to find an exact match for 'book'
        # in this field. It is boosted 1000x relative to the baseline
        # weight for this field.
        def validate_keyword(field, hypothesis, expect_weight):
            hypothesis, weight = hypothesis
            assert Term(**{"%s.keyword" % field: "book"}) == hypothesis
            assert expect_weight == weight

        validate_keyword("regular_field", term, 2000)

        # The MatchPhrase hypothesis tries to find a partial phrase
        # match for 'book' in this field. It is boosted 1x relative to
        # the baseline weight for this field.
        def validate_minimal(field, hypothesis, expect_weight):
            hypothesis, weight = hypothesis
            assert MatchPhrase(**{"%s.minimal" % field: "book"}) == hypothesis
            assert expect_weight == weight

        validate_minimal("regular_field", phrase, 2)

        # Now let's try the same query, but with fuzzy searching
        # turned on.
        query.fuzzy_coefficient = 0.5
        term, phrase, fuzzy = list(m("regular_field"))
        # The first two hypotheses are the same.
        validate_keyword("regular_field", term, 2000)
        validate_minimal("regular_field", phrase, 2)

        # But we've got another hypothesis yielded by a call to
        # _fuzzy_matches. It goes against the 'minimal' field and its
        # weight is the weight of that field's non-fuzzy hypothesis,
        # multiplied by a value determined by _fuzzy_matches()
        def validate_fuzzy(field, hypothesis, phrase_weight):
            minimal_field = field + ".minimal"
            hypothesis, weight = fuzzy
            assert "fuzzy match for %s" % minimal_field == hypothesis
            assert phrase_weight * 0.66 == weight

            # Validate standard arguments passed into _fuzzy_matches.
            # Since a fuzzy match is kind of loose, we don't allow a
            # match on a single word of a multi-word query. At least
            # two of the words have to be involved.
            assert (
                dict(minimum_should_match=2, query="book")
                == query.fuzzy_calls[minimal_field]
            )

        validate_fuzzy("regular_field", fuzzy, 2)

        # Now try a field where stopwords might be relevant.
        term, phrase, fuzzy = list(m("stopword_field"))

        # There was no new hypothesis, because our query doesn't
        # contain any stopwords.  Let's make it look like it does.
        query.contains_stopwords = True
        term, phrase, fuzzy, stopword = list(m("stopword_field"))

        # We have the term query, the phrase match query, and the
        # fuzzy query. Note that they're boosted relative to the base
        # weight for the stopword_field query, which is 3.
        validate_keyword("stopword_field", term, 3000)
        validate_minimal("stopword_field", phrase, 3)
        validate_fuzzy("stopword_field", fuzzy, 3)

        # We also have a new hypothesis which matches the version of
        # stopword_field that leaves the stopwords in place.  This
        # hypothesis is boosted just above the baseline hypothesis.
        hypothesis, weight = stopword
        assert hypothesis == MatchPhrase(**{"stopword_field.with_stopwords": "book"})
        assert weight == 3 * Mock.SLIGHTLY_ABOVE_BASELINE

        # Finally, let's try a stemmable field.
        term, phrase, fuzzy, stemmable = list(m("stemmable_field"))
        validate_keyword("stemmable_field", term, 4000)
        validate_minimal("stemmable_field", phrase, 4)
        validate_fuzzy("stemmable_field", fuzzy, 4)

        # The stemmable field becomes a Match hypothesis at 75% of the
        # baseline weight for this field. We set
        # minimum_should_match=2 here for the same reason we do it for
        # the fuzzy search -- a normal Match query is kind of loose.
        hypothesis, weight = stemmable
        assert hypothesis == Match(
            stemmable_field=dict(minimum_should_match=2, query="book")
        )
        assert weight == 4 * 0.75

    def test_match_author_hypotheses(self):
        # Test our ability to generate hypotheses that a query string
        # is an attempt to identify the author of a book. We do this
        # by calling _author_field_must_match several times -- that's
        # where most of the work happens.
        class Mock(Query):
            def _author_field_must_match(self, base_field, query_string=None):
                yield f"{base_field} must match {query_string}"

        query = Mock("ursula le guin")
        hypotheses = list(query.match_author_hypotheses)

        # We test three hypotheses: the query string is the author's
        # display name, it's the author's sort name, or it matches the
        # author's sort name when automatically converted to a sort
        # name.
        assert [
            "display_name must match ursula le guin",
            "sort_name must match le guin, ursula",
        ] == hypotheses

        # If the string passed in already looks like a sort name, we
        # don't try to convert it -- but someone's name may contain a
        # comma, so we do check both fields.
        query = Mock("le guin, ursula")
        hypotheses = list(query.match_author_hypotheses)
        assert [
            "display_name must match le guin, ursula",
            "sort_name must match le guin, ursula",
        ] == hypotheses

    def test__author_field_must_match(self):
        class Mock(Query):
            def match_one_field_hypotheses(self, field_name, query_string):
                hypothesis = f"maybe {field_name} matches {query_string}"
                yield hypothesis, 6

            def _role_must_also_match(self, hypothesis):
                return [hypothesis, "(but the role must be appropriate)"]

        query = Mock("ursula le guin")
        m = query._author_field_must_match

        # We call match_one_field_hypothesis with the field name, and
        # run the result through _role_must_also_match() to ensure we
        # only get works where this author made a major contribution.
        [(hypothesis, weight)] = list(m("display_name"))
        assert [
            "maybe contributors.display_name matches ursula le guin",
            "(but the role must be appropriate)",
        ] == hypothesis
        assert 6 == weight

        # We can pass in a different query string to override
        # .query_string. This is how we test a match against our guess
        # at an author's sort name.
        [(hypothesis, weight)] = list(m("sort_name", "le guin, ursula"))
        assert [
            "maybe contributors.sort_name matches le guin, ursula",
            "(but the role must be appropriate)",
        ] == hypothesis
        assert 6 == weight

    def test__role_must_also_match(self):
        class Mock(Query):
            @classmethod
            def _nest(cls, subdocument, base):
                return ("nested", subdocument, base)

        # Verify that _role_must_also_match() puts an appropriate
        # restriction on a match against a field in the 'contributors'
        # sub-document.
        original_query = Term(**{"contributors.sort_name": "ursula le guin"})
        modified = Mock._role_must_also_match(original_query)

        # The resulting query was run through Mock._nest. In a real
        # scenario this would turn it into a nested query against the
        # 'contributors' subdocument.
        nested, subdocument, modified_base = modified
        assert "nested" == nested
        assert "contributors" == subdocument

        # The original query was combined with an extra clause, which
        # only matches people if their contribution to a book was of
        # the type that library patrons are likely to search for.
        extra = Terms(**{"contributors.role": ["Primary Author", "Author", "Narrator"]})
        assert Bool(must=[original_query, extra]) == modified_base

    def test_match_topic_hypotheses(self):
        query = Query("whales")
        [(hypothesis, weight)] = list(query.match_topic_hypotheses)

        # There's a single hypothesis -- a MultiMatch covering both
        # summary text and classifications. The score for a book is
        # whichever of the two types of fields is a better match for
        # 'whales'.
        assert (
            MultiMatch(
                query="whales",
                fields=["summary", "classifications.term"],
                type="best_fields",
            )
            == hypothesis
        )
        # The weight of the hypothesis is the base weight associated
        # with the 'summary' field.
        assert Query.WEIGHT_FOR_FIELD["summary"] == weight

    def test_title_multi_match_for(self):
        # Test our ability to hypothesize that a query string might
        # contain some text from the title plus some text from
        # some other field.

        # If there's only one word in the query, then we don't bother
        # making this hypothesis at all.
        assert [] == list(Query("grasslands").title_multi_match_for("other field"))

        query = Query("grass lands")
        [(hypothesis, weight)] = list(query.title_multi_match_for("author"))

        expect = MultiMatch(
            query="grass lands",
            fields=["title.minimal", "author.minimal"],
            type="cross_fields",
            operator="and",
            minimum_should_match="100%",
        )
        assert expect == hypothesis

        # The weight of this hypothesis is between the weight of a
        # pure title match and the weight of a pure author match.
        title_weight = Query.WEIGHT_FOR_FIELD["title"]
        author_weight = Query.WEIGHT_FOR_FIELD["author"]
        assert weight == author_weight * (author_weight / title_weight)

    def test_parsed_query_matches(self):
        # Test our ability to take a query like "asteroids
        # nonfiction", and turn it into a single hypothesis
        # encapsulating the idea: "what if they meant to do a search
        # on 'asteroids' but with a nonfiction filter?

        query = Query("nonfiction asteroids")

        # The work of this method is simply delegated to QueryParser.
        parser = QueryParser(query.query_string)
        expect = (parser.match_queries, parser.filters)

        assert expect == query.parsed_query_matches

    def test_hypothesize(self):
        # Verify that _hypothesize() adds a query to a list,
        # boosting it if necessary.
        class Mock(Query):
            boost_extras = []

            @classmethod
            def _boost(cls, boost, queries, filters=None, **kwargs):
                if filters or kwargs:
                    cls.boost_extras.append((filters, kwargs))
                return "%s boosted by %d" % (queries, boost)

        hypotheses = []

        # _hypothesize() does nothing if it's not passed a real
        # query.
        Mock._hypothesize(hypotheses, None, 100)
        assert [] == hypotheses
        assert [] == Mock.boost_extras

        # If it is passed a real query, _boost() is called on the
        # query object.
        Mock._hypothesize(hypotheses, "query object", 10)
        assert ["query object boosted by 10"] == hypotheses
        assert [] == Mock.boost_extras

        Mock._hypothesize(hypotheses, "another query object", 1)
        assert [
            "query object boosted by 10",
            "another query object boosted by 1",
        ] == hypotheses
        assert [] == Mock.boost_extras

        # If a filter or any other arguments are passed in, those arguments
        # are propagated to _boost().
        hypotheses = []
        Mock._hypothesize(
            hypotheses,
            "query with filter",
            2,
            filters="some filters",
            extra="extra kwarg",
        )
        assert ["query with filter boosted by 2"] == hypotheses
        assert [("some filters", dict(extra="extra kwarg"))] == Mock.boost_extras


class TestQueryParser:
    """Test the class that tries to derive structure from freeform
    text search requests.
    """

    def test_constructor(self):
        # The constructor parses the query string, creates any
        # necessary query objects, and turns the remaining part of
        # the query into a 'simple query string'-type query.

        class MockQuery(Query):
            """Create 'query' objects that are easier to test than
            the ones the Query class makes.
            """

            @classmethod
            def _match_term(cls, field, query):
                return (field, query)

            @classmethod
            def make_target_age_query(cls, query, boost="default boost"):
                return ("target age (filter)", query), (
                    "target age (query)",
                    query,
                    boost,
                )

            @property
            def search_query(self):
                # Mock the creation of an extremely complicated DisMax
                # query -- we just want to verify that such a query
                # was created.
                return "A huge DisMax for %r" % self.query_string

        parser = QueryParser("science fiction about dogs", MockQuery)

        # The original query string is always stored as .original_query_string.
        assert "science fiction about dogs" == parser.original_query_string

        # The part of the query that couldn't be parsed is always stored
        # as final_query_string.
        assert "about dogs" == parser.final_query_string

        # Leading and trailing whitespace is never regarded as
        # significant and it is stripped from the query string
        # immediately.
        whitespace = QueryParser(" abc ", MockQuery)
        assert "abc" == whitespace.original_query_string

        # parser.filters contains the filters that we think we were
        # able to derive from the query string.
        assert [("genres.name", "Science Fiction")] == parser.filters

        # parser.match_queries contains the result of putting the rest
        # of the query string into a Query object (or, here, our
        # MockQuery) and looking at its .search_query. In a
        # real scenario, this will result in a huge DisMax query
        # that tries to consider all the things someone might be
        # searching for, _in addition to_ applying a filter.
        assert ["A huge DisMax for 'about dogs'"] == parser.match_queries

        # Now that you see how it works, let's define a helper
        # function which makes it easy to verify that a certain query
        # string becomes a certain set of filters, plus a certain set
        # of queries, plus a DisMax for some remainder string.
        def assert_parses_as(query_string, filters, remainder, extra_queries=None):
            if not isinstance(filters, list):
                filters = [filters]
            queries = extra_queries or []
            if not isinstance(queries, list):
                queries = [queries]
            parser = QueryParser(query_string, MockQuery)
            assert filters == parser.filters

            if remainder:
                queries.append(MockQuery(remainder).search_query)
            assert queries == parser.match_queries

        # Here's the same test from before, using the new
        # helper function.
        assert_parses_as(
            "science fiction about dogs",
            ("genres.name", "Science Fiction"),
            "about dogs",
        )

        # Test audiences.

        assert_parses_as(
            "children's picture books", ("audience", "children"), "picture books"
        )

        # (It's possible for the entire query string to be eaten up,
        # such that there is no remainder match at all.)
        assert_parses_as(
            "young adult romance",
            [("genres.name", "Romance"), ("audience", "youngadult")],
            "",
        )

        # Test fiction/nonfiction status.
        assert_parses_as("fiction dinosaurs", ("fiction", "fiction"), "dinosaurs")

        # (Genres are parsed before fiction/nonfiction; otherwise
        # "science fiction" would be chomped by a search for "fiction"
        # and "nonfiction" would not be picked up.)
        assert_parses_as(
            "science fiction or nonfiction dinosaurs",
            [("genres.name", "Science Fiction"), ("fiction", "nonfiction")],
            "or  dinosaurs",
        )

        # Test target age.
        #
        # These are a little different because the target age
        # restriction shows up twice: once as a filter (to eliminate
        # all books that don't fit the target age restriction) and
        # once as a query (to boost books that cluster tightly around
        # the target age, at the expense of books that span a wider
        # age range).
        assert_parses_as(
            "grade 5 science",
            [("genres.name", "Science"), ("target age (filter)", (10, 10))],
            "",
            ("target age (query)", (10, 10), "default boost"),
        )

        assert_parses_as(
            "divorce ages 10 and up",
            ("target age (filter)", (10, 14)),
            "divorce  and up",  # TODO: not ideal
            ("target age (query)", (10, 14), "default boost"),
        )

        # Nothing can be parsed out from this query--it's an author's name
        # and will be handled by another query.
        parser = QueryParser("octavia butler")
        assert [] == parser.match_queries
        assert "octavia butler" == parser.final_query_string

        # Finally, try parsing a query without using MockQuery.
        query = QueryParser("nonfiction asteroids")
        [nonfiction] = query.filters
        [asteroids] = query.match_queries

        # It creates real Opensearch-DSL query objects.

        # The filter is a very simple Term query.
        assert Term(fiction="nonfiction") == nonfiction

        # The query part is an extremely complicated DisMax query, so
        # I won't test the whole thing, but it's what you would get if
        # you just tried a search for "asteroids".
        assert isinstance(asteroids, DisMax)
        assert asteroids == Query("asteroids").search_query

    def test_add_match_term_filter(self):
        # TODO: this method could use a standalone test, but it's
        # already covered by the test_constructor.
        pass

    def test_add_target_age_filter(self):
        parser = QueryParser("")
        parser.filters = []
        parser.match_queries = []
        remainder = parser.add_target_age_filter(
            (10, 11), "penguins grade 5-6", "grade 5-6"
        )
        assert "penguins " == remainder

        # Here's the filter part: a book's age range must be include the
        # 10-11 range, or it gets filtered out.
        filter_clauses = [
            Range(**{"target_age.upper": dict(gte=10)}),
            Range(**{"target_age.lower": dict(lte=11)}),
        ]
        assert [Bool(must=filter_clauses)] == parser.filters

        # Here's the query part: a book gets boosted if its
        # age range fits _entirely_ within the target age range.
        query_clauses = [
            Range(**{"target_age.upper": dict(lte=11)}),
            Range(**{"target_age.lower": dict(gte=10)}),
        ]
        assert [
            Bool(boost=1.1, must=filter_clauses, should=query_clauses)
        ] == parser.match_queries

    def test__without_match(self):
        # Test our ability to remove matched text from a string.
        m = QueryParser._without_match
        assert " fiction" == m("young adult fiction", "young adult")
        assert " of dinosaurs" == m("science of dinosaurs", "science")

        # If the match cuts off in the middle of a word, we remove
        # everything up to the end of the word.
        assert " books" == m("children's books", "children")
        assert "" == m("adulting", "adult")


class FilterFixture:
    """A fixture preconfigured with data for testing filters."""

    transaction: DatabaseTransactionFixture
    literary_fiction: Genre
    fantasy: Genre
    horror: Genre
    best_sellers: CustomList
    staff_picks: CustomList

    @classmethod
    def create(cls, transaction: DatabaseTransactionFixture) -> "FilterFixture":
        data = FilterFixture()
        data.transaction = transaction
        session = transaction.session
        # Look up three Genre objects which can be used to make filters.
        data.literary_fiction, ignore = Genre.lookup(session, "Literary Fiction")
        data.fantasy, ignore = Genre.lookup(session, "Fantasy")
        data.horror, ignore = Genre.lookup(session, "Horror")

        # Create two empty CustomLists which can be used to make filters.
        data.best_sellers, ignore = transaction.customlist(num_entries=0)
        data.staff_picks, ignore = transaction.customlist(num_entries=0)
        return data


@pytest.fixture()
def filter_fixture(db) -> FilterFixture:
    """A fixture preconfigured with data for testing filters."""
    return FilterFixture.create(db)


class TestFilter:
    def test_constructor(self, filter_fixture: FilterFixture):
        data, transaction, session = (
            filter_fixture,
            filter_fixture.transaction,
            filter_fixture.transaction.session,
        )

        # Verify that the Filter constructor sets members with
        # minimal processing.
        collection = transaction.default_collection()

        media = object()
        languages = object()
        fiction = object()
        audiences = ["somestring"]
        author = object()
        match_nothing = object()
        min_score = object()

        # Test the easy stuff -- these arguments just get stored on
        # the Filter object. If necessary, they'll be cleaned up
        # later, during build().
        filter = Filter(
            media=media,
            languages=languages,
            fiction=fiction,
            audiences=audiences,
            author=author,
            match_nothing=match_nothing,
            min_score=min_score,
        )
        assert media == filter.media
        assert languages == filter.languages
        assert fiction == filter.fiction
        assert audiences == filter.audiences
        assert author == filter.author
        assert match_nothing == filter.match_nothing
        assert min_score == filter.min_score

        # Test the `collections` argument.

        # If you pass in a library, you get all of its collections.
        library_filter = Filter(collections=transaction.default_library())
        assert [transaction.default_collection().id] == library_filter.collection_ids

        # If the library has no collections, the collection filter
        # will filter everything out.
        transaction.default_library().collections = []
        library_filter = Filter(collections=transaction.default_library())
        assert [] == library_filter.collection_ids

        # If you pass in Collection objects, you get their IDs.
        collection_filter = Filter(collections=transaction.default_collection())
        assert [transaction.default_collection().id] == collection_filter.collection_ids
        collection_filter = Filter(collections=[transaction.default_collection()])
        assert [transaction.default_collection().id] == collection_filter.collection_ids

        # If you pass in IDs, they're left alone.
        ids = [10, 11, 22]
        collection_filter = Filter(collections=ids)
        assert ids == collection_filter.collection_ids

        # If you pass in nothing, there is no collection filter. This
        # is different from the case above, where the library had no
        # collections and everything was filtered out.
        empty_filter = Filter()
        assert None == empty_filter.collection_ids

        # Test the `target_age` argument.
        assert None == empty_filter.target_age

        one_year = Filter(target_age=8)
        assert (8, 8) == one_year.target_age

        year_range = Filter(target_age=(8, 10))
        assert (8, 10) == year_range.target_age

        year_range = Filter(target_age=NumericRange(3, 6, "()"))
        assert (4, 5) == year_range.target_age

        # Test genre_restriction_sets

        # In these three cases, there are no restrictions on genre.
        assert [] == empty_filter.genre_restriction_sets
        assert [] == Filter(genre_restriction_sets=[]).genre_restriction_sets
        assert [] == Filter(genre_restriction_sets=None).genre_restriction_sets

        # Restrict to books that are literary fiction AND (horror OR
        # fantasy).
        restricted = Filter(
            genre_restriction_sets=[
                [data.horror, data.fantasy],
                [data.literary_fiction],
            ]
        )
        assert [
            [data.horror.id, data.fantasy.id],
            [data.literary_fiction.id],
        ] == restricted.genre_restriction_sets

        # This is a restriction: 'only books that have no genre'
        assert [[]] == Filter(genre_restriction_sets=[[]]).genre_restriction_sets

        # Test customlist_restriction_sets

        # In these three cases, there are no restrictions.
        assert [] == empty_filter.customlist_restriction_sets
        assert (
            [] == Filter(customlist_restriction_sets=None).customlist_restriction_sets
        )
        assert [] == Filter(customlist_restriction_sets=[]).customlist_restriction_sets

        # Restrict to books that are on *both* the best sellers list and the
        # staff picks list.
        restricted = Filter(
            customlist_restriction_sets=[
                [data.best_sellers],
                [data.staff_picks],
            ]
        )
        assert [
            [data.best_sellers.id],
            [data.staff_picks.id],
        ] == restricted.customlist_restriction_sets

        # This is a restriction -- 'only books that are not on any lists'.
        assert [[]] == Filter(
            customlist_restriction_sets=[[]]
        ).customlist_restriction_sets

        # Test the license_datasource argument
        overdrive = DataSource.lookup(session, DataSource.OVERDRIVE)
        overdrive_only = Filter(license_datasource=overdrive)
        assert [overdrive.id] == overdrive_only.license_datasources

        overdrive_only = Filter(license_datasource=overdrive.id)
        assert [overdrive.id] == overdrive_only.license_datasources

        # If you pass in a Facets object, its modify_search_filter()
        # and scoring_functions() methods are called.
        class Mock:
            def modify_search_filter(self, filter):
                self.modify_search_filter_called_with = filter

            def scoring_functions(self, filter):
                self.scoring_functions_called_with = filter
                return ["some scoring functions"]

        facets = Mock()
        filter = Filter(facets=facets)
        assert filter == facets.modify_search_filter_called_with
        assert filter == facets.scoring_functions_called_with
        assert ["some scoring functions"] == filter.scoring_functions

        # Some arguments to the constructor only exist as keyword
        # arguments, but you can't pass in whatever keywords you want.
        with pytest.raises(ValueError) as excinfo:
            Filter(no_such_keyword="nope")
        assert "Unknown keyword arguments" in str(excinfo.value)

    def test_from_worklist(self, filter_fixture: FilterFixture):
        data, transaction, session = (
            filter_fixture,
            filter_fixture.transaction,
            filter_fixture.transaction.session,
        )

        # Any WorkList can be converted into a Filter.
        #
        # WorkList.inherited_value() and WorkList.inherited_values()
        # are used to determine what should go into the constructor.

        # Disable any excluded audiobook data sources -- they will
        # introduce unwanted extra clauses into our filters.
        excluded_audio_sources = ConfigurationSetting.sitewide(
            session, Configuration.EXCLUDED_AUDIO_DATA_SOURCES
        )
        excluded_audio_sources.value = json.dumps([])

        library = transaction.default_library()
        assert True == library.allow_holds

        parent = transaction.lane(display_name="Parent Lane", library=library)
        parent.media = Edition.AUDIO_MEDIUM
        parent.languages = ["eng", "fra"]
        parent.fiction = True
        parent.audiences = {Classifier.AUDIENCE_CHILDREN}
        parent.target_age = NumericRange(10, 11, "[]")
        parent.genres = [data.horror, data.fantasy]
        parent.customlists = [data.best_sellers]
        parent.license_datasource = DataSource.lookup(session, DataSource.GUTENBERG)

        # This lane inherits most of its configuration from its parent.
        inherits = transaction.lane(display_name="Child who inherits", parent=parent)
        inherits.genres = [data.literary_fiction]
        inherits.customlists = [data.staff_picks]

        class Mock:
            def modify_search_filter(self, filter):
                self.called_with = filter

            def scoring_functions(self, filter):
                return []

        facets = Mock()

        filter = Filter.from_worklist(session, inherits, facets)
        assert [transaction.default_collection().id] == filter.collection_ids
        assert parent.media == filter.media
        assert parent.languages == filter.languages
        assert parent.fiction == filter.fiction
        assert parent.audiences + [Classifier.AUDIENCE_ALL_AGES] == filter.audiences
        assert [parent.license_datasource_id] == filter.license_datasources
        assert (parent.target_age.lower, parent.target_age.upper) == filter.target_age
        assert True == filter.allow_holds

        # Filter.from_worklist passed the mock Facets object in to
        # the Filter constructor, which called its modify_search_filter()
        # method.
        assert facets.called_with is not None

        # For genre and custom list restrictions, the child values are
        # appended to the parent's rather than replacing it.
        assert [parent.genre_ids, inherits.genre_ids] == [
            set(x) for x in filter.genre_restriction_sets
        ]

        assert [
            parent.customlist_ids,
            inherits.customlist_ids,
        ] == filter.customlist_restriction_sets

        # If any other value is set on the child lane, the parent value
        # is overridden.
        inherits.media = Edition.BOOK_MEDIUM
        filter = Filter.from_worklist(session, inherits, facets)
        assert inherits.media == filter.media

        # This lane doesn't inherit anything from its parent.
        does_not_inherit = transaction.lane(
            display_name="Child who does not inherit", parent=parent
        )
        does_not_inherit.inherit_parent_restrictions = False

        # Because of that, the final filter we end up with is
        # nearly empty. The only restriction here is the collection
        # restriction imposed by the fact that `does_not_inherit`
        # is, itself, associated with a specific library.
        filter = Filter.from_worklist(session, does_not_inherit, facets)

        built_filters, subfilters = self.assert_filter_builds_to([], filter)

        # The collection restriction is not reflected in the main
        # filter; rather it's in a subfilter that will be applied to the
        # 'licensepools' subdocument, where the collection ID lives.

        [subfilter] = subfilters.pop("licensepools")
        assert {
            "terms": {
                "licensepools.collection_id": [transaction.default_collection().id]
            }
        } == subfilter.to_dict()

        # No other subfilters were specified.
        assert {} == subfilters

        # If the library does not allow holds, this information is
        # propagated to its Filter.
        library.setting(library.ALLOW_HOLDS).value = False
        filter = Filter.from_worklist(session, parent, facets)
        assert False == library.allow_holds

        # Any excluded audio sources in the sitewide settings
        # will be propagated to all Filters.
        overdrive = DataSource.lookup(session, DataSource.OVERDRIVE)
        excluded_audio_sources.value = json.dumps([overdrive.name])
        filter = Filter.from_worklist(session, parent, facets)
        assert [overdrive.id] == filter.excluded_audiobook_data_sources

        # A bit of setup to test how WorkList.collection_ids affects
        # the resulting Filter.

        # Here's a collection associated with the default library.
        for_default_library = WorkList()
        for_default_library.initialize(transaction.default_library())

        # Its filter uses all the collections associated with that library.
        filter = Filter.from_worklist(session, for_default_library, None)
        assert [transaction.default_collection().id] == filter.collection_ids

        # Here's a child of that WorkList associated with a different
        # library.
        library2 = transaction.library()
        collection2 = transaction.collection()
        library2.collections.append(collection2)
        for_other_library = WorkList()
        for_other_library.initialize(library2)
        for_default_library.append_child(for_other_library)

        # Its filter uses the collection from the second library.
        filter = Filter.from_worklist(session, for_other_library, None)
        assert [collection2.id] == filter.collection_ids

        # If for whatever reason, collection_ids on the child is not set,
        # all collections associated with the WorkList's library will be used.
        for_other_library.collection_ids = None
        filter = Filter.from_worklist(session, for_other_library, None)
        assert [collection2.id] == filter.collection_ids

        # If no library is associated with a WorkList, we assume that
        # holds are allowed. (Usually this is controleld by a library
        # setting.)
        for_other_library.library_id = None
        filter = Filter.from_worklist(session, for_other_library, None)
        assert True == filter.allow_holds

    def assert_filter_builds_to(self, expect, filter, _chain_filters=None):
        """Helper method for the most common case, where a
        Filter.build() returns a main filter and no nested filters.
        """
        final_query = {"bool": {"must_not": [RESEARCH.to_dict()]}}

        if expect:
            final_query["bool"]["must"] = expect
        main, nested = filter.build(_chain_filters)
        assert final_query == main.to_dict()

        return main, nested

    def test_audiences(self):
        # Verify that the .audiences property correctly represents the
        # combination of what's in the ._audiences list and application
        # policies.
        filter = Filter()
        assert filter.audiences == None

        # The output is a list whether audiences is a string...
        filter = Filter(audiences=Classifier.AUDIENCE_ALL_AGES)
        assert filter.audiences == [Classifier.AUDIENCE_ALL_AGES]
        # ...or a list.
        filter = Filter(audiences=[Classifier.AUDIENCE_ALL_AGES])
        assert filter.audiences == [Classifier.AUDIENCE_ALL_AGES]

        # "all ages" should always be an audience if the audience is
        # young adult or adult.
        filter = Filter(audiences=Classifier.AUDIENCE_YOUNG_ADULT)
        assert filter.audiences == [
            Classifier.AUDIENCE_YOUNG_ADULT,
            Classifier.AUDIENCE_ALL_AGES,
        ]
        filter = Filter(audiences=Classifier.AUDIENCE_ADULT)
        assert filter.audiences == [
            Classifier.AUDIENCE_ADULT,
            Classifier.AUDIENCE_ALL_AGES,
        ]
        filter = Filter(
            audiences=[Classifier.AUDIENCE_ADULT, Classifier.AUDIENCE_YOUNG_ADULT]
        )
        assert filter.audiences == [
            Classifier.AUDIENCE_ADULT,
            Classifier.AUDIENCE_YOUNG_ADULT,
            Classifier.AUDIENCE_ALL_AGES,
        ]

        # If the audience is meant for adults, then "all ages" should not
        # be included
        for audience in (Classifier.AUDIENCE_ADULTS_ONLY, Classifier.AUDIENCE_RESEARCH):
            filter = Filter(audiences=audience)
            assert Classifier.AUDIENCE_ALL_AGES not in filter.audiences

        # If the audience and target age is meant for children, then the
        # audience should only be for children
        filter = Filter(audiences=Classifier.AUDIENCE_CHILDREN, target_age=5)
        assert filter.audiences == [Classifier.AUDIENCE_CHILDREN]

        # If the children's target age includes children older than
        # ALL_AGES_AGE_CUTOFF, or there is no target age, the
        # audiences includes "all ages".
        all_children = Filter(audiences=Classifier.AUDIENCE_CHILDREN)
        nine_years = Filter(audiences=Classifier.AUDIENCE_CHILDREN, target_age=9)
        for filter in (all_children, nine_years):
            assert filter.audiences == [
                Classifier.AUDIENCE_CHILDREN,
                Classifier.AUDIENCE_ALL_AGES,
            ]

    def test_build(self, filter_fixture: FilterFixture):
        data, transaction, session = (
            filter_fixture,
            filter_fixture.transaction,
            filter_fixture.transaction.session,
        )

        # Test the ability to turn a Filter into an OpenSearch
        # filter object.

        # build() takes the information in the Filter object, scrubs
        # it, and uses _chain_filters to chain together a number of
        # alternate hypotheses. It returns a 2-tuple with a main Filter
        # and a dictionary describing additional filters to be applied
        # to subdocuments.
        #
        # Let's try it with some simple cases before mocking
        # _chain_filters for a more detailed test.

        # Start with an empty filter. No filter is built and there are no
        # nested filters.
        filter = Filter()
        built_filters, subfilters = self.assert_filter_builds_to([], filter)
        assert {} == subfilters

        # Add a medium clause to the filter.
        filter.media = "a medium"
        medium_built = {"terms": {"medium": ["amedium"]}}
        built_filters, subfilters = self.assert_filter_builds_to([medium_built], filter)
        assert {} == subfilters

        # Add a language clause to the filter.
        filter.languages = ["lang1", "LANG2"]
        language_built = {"terms": {"language": ["lang1", "lang2"]}}

        # Now both the medium clause and the language clause must match.
        built_filters, subfilters = self.assert_filter_builds_to(
            [medium_built, language_built], filter
        )
        assert {} == subfilters

        chain = self._mock_chain

        filter.collection_ids = [transaction.default_collection()]
        filter.fiction = True
        filter._audiences = "CHILDREN"
        filter.target_age = (2, 3)
        overdrive = DataSource.lookup(session, DataSource.OVERDRIVE)
        filter.excluded_audiobook_data_sources = [overdrive.id]
        filter.allow_holds = False
        last_update_time = datetime_utc(2019, 1, 1)
        i1 = transaction.identifier()
        i2 = transaction.identifier()
        filter.identifiers = [i1, i2]
        filter.updated_after = last_update_time

        # We want books from a specific license source.
        filter.license_datasources = overdrive

        # We want books by a specific author.
        filter.author = ContributorData(sort_name="Ebrity, Sel")

        # We want books that are literary fiction, *and* either
        # fantasy or horror.
        filter.genre_restriction_sets = [
            [data.literary_fiction],
            [data.fantasy, data.horror],
        ]

        # We want books that are on _both_ of the custom lists.
        filter.customlist_restriction_sets = [[data.best_sellers], [data.staff_picks]]

        # At this point every item on this Filter that can be set, has been
        # set. When we run build, we'll end up with the output of our mocked
        # chain() method -- a list of small filters.
        built, nested = filter.build(_chain_filters=chain)

        # This time we do see a nested filter. The information
        # necessary to enforce the 'current collection', 'excluded
        # audiobook sources', 'no holds', and 'license source'
        # restrictions is kept in the nested 'licensepools' document,
        # so those restrictions must be described in terms of nested
        # filters on that document.
        [
            licensepool_filter,
            datasource_filter,
            excluded_audiobooks_filter,
            no_holds_filter,
        ] = nested.pop("licensepools")

        # The 'current collection' filter.
        assert {
            "terms": {
                "licensepools.collection_id": [transaction.default_collection().id]
            }
        } == licensepool_filter.to_dict()

        # The 'only certain data sources' filter.
        assert {
            "terms": {"licensepools.data_source_id": [overdrive.id]}
        } == datasource_filter.to_dict()

        # The 'excluded audiobooks' filter.
        audio = Q("term", **{"licensepools.medium": Edition.AUDIO_MEDIUM})
        excluded_audio_source = Q(
            "terms", **{"licensepools.data_source_id": [overdrive.id]}
        )
        excluded_audio = Bool(must=[audio, excluded_audio_source])
        not_excluded_audio = Bool(must_not=excluded_audio)
        assert not_excluded_audio == excluded_audiobooks_filter

        # The 'no holds' filter.
        open_access = Q("term", **{"licensepools.open_access": True})
        licenses_available = Q("term", **{"licensepools.available": True})
        currently_available = Bool(should=[licenses_available, open_access])
        assert currently_available == no_holds_filter

        # The best-seller list and staff picks restrictions are also
        # expressed as nested filters.
        [best_sellers_filter, staff_picks_filter] = nested.pop("customlists")
        assert {
            "terms": {"customlists.list_id": [data.best_sellers.id]}
        } == best_sellers_filter.to_dict()
        assert {
            "terms": {"customlists.list_id": [data.staff_picks.id]}
        } == staff_picks_filter.to_dict()

        # The author restriction is also expressed as a nested filter.
        [contributor_filter] = nested.pop("contributors")

        # It's value is the value of .author_filter, which is tested
        # separately in test_author_filter.
        assert isinstance(filter.author_filter, Bool)
        assert filter.author_filter == contributor_filter

        # The genre restrictions are also expressed as nested filters.
        literary_fiction_filter, fantasy_or_horror_filter = nested.pop("genres")

        # There are two different restrictions on genre, because
        # genre_restriction_sets was set to two lists of genres.
        assert {
            "terms": {"genres.term": [data.literary_fiction.id]}
        } == literary_fiction_filter.to_dict()
        assert {
            "terms": {"genres.term": [data.fantasy.id, data.horror.id]}
        } == fantasy_or_horror_filter.to_dict()

        # There's a restriction on the identifier.
        [identifier_restriction] = nested.pop("identifiers")

        # The restriction includes subclases, each of which matches
        # the identifier and type of one of the Identifier objects.
        subclauses = [
            Bool(
                must=[
                    Term(identifiers__identifier=x.identifier),
                    Term(identifiers__type=x.type),
                ]
            )
            for x in [i1, i2]
        ]

        # Any identifier will work, but at least one must match.
        assert Bool(minimum_should_match=1, should=subclauses) == identifier_restriction

        # There are no other nested filters.
        assert {} == nested

        # Every other restriction imposed on the Filter object becomes an
        # Opensearch filter object in this list.
        (medium, language, fiction, audience, target_age, updated_after) = built

        # Test them one at a time.
        #
        # Throughout this test, notice that the data model objects --
        # Collections (above), Genres, and CustomLists -- have been
        # replaced with their database IDs. This is done by
        # filter_ids.
        #
        # Also, audience, medium, and language have been run through
        # scrub_list, which turns scalar values into lists, removes
        # spaces, and converts to lowercase.

        # These we tested earlier -- we're just making sure the same
        # documents are put into the full filter.
        assert medium_built == medium.to_dict()
        assert language_built == language.to_dict()

        assert {"term": {"fiction": "fiction"}} == fiction.to_dict()
        assert {"terms": {"audience": ["children"]}} == audience.to_dict()

        # The contents of target_age_filter are tested below -- this
        # just tests that the target_age_filter is included.
        assert filter.target_age_filter == target_age

        # There's a restriction on the last updated time for bibliographic
        # metadata. The datetime is converted to a number of seconds since
        # the epoch, since that's how we index times.
        expect = (last_update_time - from_timestamp(0)).total_seconds()
        assert {
            "bool": {"must": [{"range": {"last_update_time": {"gte": expect}}}]}
        } == updated_after.to_dict()

        # We tried fiction; now try nonfiction.
        filter = Filter()
        filter.fiction = False
        built_filters, subfilters = self.assert_filter_builds_to(
            [{"term": {"fiction": "nonfiction"}}], filter
        )
        assert {} == subfilters

    def test_build_series(self):
        # Test what happens when a series restriction is placed on a Filter.
        f = Filter(series="Talking Hedgehog Mysteries")
        built, nested = f.build()
        assert {} == nested

        # A match against a keyword field only matches on an exact
        # string match.
        assert built.to_dict()["bool"]["must"] == [
            {"term": {"series.keyword": "Talking Hedgehog Mysteries"}}
        ]

        # Find books that are in _some_ series--which one doesn't
        # matter.
        f = Filter(series=True)
        built, nested = f.build()

        assert {} == nested
        # The book must have an indexed series.
        assert built.to_dict()["bool"]["must"] == [{"exists": {"field": "series"}}]

        # But the 'series' that got indexed must not be the empty string.
        assert {"term": {"series.keyword": ""}} in built.to_dict()["bool"]["must_not"]

    def test_sort_order(self, filter_fixture: FilterFixture):
        data, transaction, session = (
            filter_fixture,
            filter_fixture.transaction,
            filter_fixture.transaction.session,
        )

        # Test the Filter.sort_order property.

        # No sort order.
        f = Filter()
        assert [] == f.sort_order
        assert False == f.order_ascending

        def validate_sort_order(filter, main_field):
            """Validate the 'easy' part of the sort order -- the tiebreaker
            fields. Return the 'difficult' part.

            :return: The first part of the sort order -- the field that
            is potentially difficult.
            """

            # The tiebreaker fields are always in the same order, but
            # if the main sort field is one of the tiebreaker fields,
            # it's removed from the list -- there's no need to sort on
            # that field a second time.
            default_sort_fields = [
                {x: "asc"}
                for x in ["sort_author", "sort_title", "work_id"]
                if x != main_field
            ]
            assert default_sort_fields == filter.sort_order[1:]
            return filter.sort_order[0]

        # A simple field, either ascending or descending.
        f.order = "field"
        assert False == f.order_ascending
        first_field = validate_sort_order(f, "field")
        assert dict(field="desc") == first_field

        f.order_ascending = True
        first_field = validate_sort_order(f, "field")
        assert dict(field="asc") == first_field

        # When multiple fields are given, they are put at the
        # beginning and any remaining tiebreaker fields are added.
        f.order = ["series_position", "work_id", "some_other_field"]
        assert [
            dict(series_position="asc"),
            dict(work_id="asc"),
            dict(some_other_field="asc"),
            dict(sort_author="asc"),
            dict(sort_title="asc"),
        ] == f.sort_order

        # You can't sort by some random subdocument field, because there's
        # not enough information to know how to aggregate multiple values.
        #
        # You _can_ sort by license pool availability time and first
        # appearance on custom list -- those are tested below -- but it's
        # complicated.
        f.order = "subdocument.field"
        with pytest.raises(ValueError) as excinfo:
            f.sort_order()
        assert "I don't know how to sort by subdocument.field" in str(excinfo.value)

        # It's possible to sort by every field in
        # Facets.SORT_ORDER_TO_OPENSEARCH_FIELD_NAME.
        used_orders = Facets.SORT_ORDER_TO_OPENSEARCH_FIELD_NAME
        added_to_collection = used_orders[Facets.ORDER_ADDED_TO_COLLECTION]
        series_position = used_orders[Facets.ORDER_SERIES_POSITION]
        last_update = used_orders[Facets.ORDER_LAST_UPDATE]
        for sort_field in list(used_orders.values()):
            if sort_field in (added_to_collection, series_position, last_update):
                # These are complicated cases, tested below.
                continue
            f.order = sort_field
            first_field = validate_sort_order(f, sort_field)
            assert {sort_field: "asc"} == first_field

        # A slightly more complicated case is when a feed is ordered by
        # series position -- there the second field is title rather than
        # author.
        f.order = series_position
        assert [
            {x: "asc"}
            for x in ["series_position", "sort_title", "sort_author", "work_id"]
        ] == f.sort_order

        # A more complicated case is when a feed is ordered by date
        # added to the collection. This requires an aggregate function
        # and potentially a nested filter.
        f.order = added_to_collection
        first_field = validate_sort_order(f, added_to_collection)

        # Here there's no nested filter but there is an aggregate
        # function. If a book is available through multiple
        # collections, we sort by the _earliest_ availability time.
        simple_nested_configuration = {
            "licensepools.availability_time": {"mode": "min", "order": "asc"}
        }
        assert simple_nested_configuration == first_field

        # Setting a collection ID restriction will add a nested filter.
        f.collection_ids = [transaction.default_collection()]
        first_field = validate_sort_order(f, "licensepools.availability_time")

        # The nested filter ensures that when sorting the results, we
        # only consider availability times from license pools that
        # match our collection filter.
        #
        # Filter.build() will apply the collection filter separately
        # to the 'filter' part of the query -- that's what actually
        # stops books from showing up if they're in the wrong collection.
        #
        # This just makes sure that the books show up in the right _order_
        # for any given set of collections.
        nested_filter = first_field["licensepools.availability_time"].pop("nested")
        assert {
            "path": "licensepools",
            "filter": {
                "terms": {
                    "licensepools.collection_id": [transaction.default_collection().id]
                }
            },
        } == nested_filter

        # Apart from the nested filter, this is the same ordering
        # configuration as before.
        assert simple_nested_configuration == first_field

        # An ordering by "last update" may be simple, if there are no
        # collections or lists associated with the filter.
        f.order = last_update
        f.collection_ids = []
        first_field = validate_sort_order(f, last_update)
        assert dict(last_update_time="asc") == first_field

        # Or it can be *incredibly complicated*, if there _are_
        # collections or lists associated with the filter. Which,
        # unfortunately, is almost all the time.
        f.collection_ids = [transaction.default_collection().id]
        f.customlist_restriction_sets = [[1], [1, 2]]
        first_field = validate_sort_order(f, last_update)

        # Here, the ordering is done by a script that runs on the
        # OpenSearch server.
        sort = first_field.pop("_script")
        assert {} == first_field

        # The script returns a numeric value and we want to sort those
        # values in ascending order.
        assert "asc" == sort.pop("order")
        assert "number" == sort.pop("type")

        script = sort.pop("script")
        assert {} == sort

        # The script is the 'simplified.work_last_update' stored script.
        assert CurrentMapping.script_name("work_last_update") == script.pop("stored")

        # Two parameters are passed into the script -- the IDs of the
        # collections and the lists relevant to the query. This is so
        # the query knows which updates should actually be considered
        # for purposes of this query.
        params = script.pop("params")
        assert {} == script

        assert [transaction.default_collection().id] == params.pop("collection_ids")
        assert [1, 2] == params.pop("list_ids")
        assert {} == params

    def test_author_filter(self):
        # Test an especially complex subfilter for authorship.

        # If no author filter is set up, there is no author filter.
        no_filter = Filter(author=None)
        assert None == no_filter.author_filter

        def check_filter(contributor, *shoulds):
            # Create a Filter with an author restriction and verify
            # that its .author_filter looks the way we expect.
            actual = Filter(author=contributor).author_filter

            # We only count contributions that were in one of the
            # matching roles.
            role_match = Terms(**{"contributors.role": Filter.AUTHOR_MATCH_ROLES})

            # Among the other restrictions on fields in the
            # 'contributors' subdocument (sort name, VIAF, etc.), at
            # least one must also be met.
            author_match = [Term(**should) for should in shoulds]
            expect = Bool(
                must=[role_match, Bool(minimum_should_match=1, should=author_match)]
            )
            assert expect == actual

        # You can apply the filter on any one of these four fields,
        # using a Contributor or a ContributorData
        for contributor_field in ("sort_name", "display_name", "viaf", "lc"):
            for cls in Contributor, ContributorData:
                contributor = cls(**{contributor_field: "value"})
                index_field = contributor_field
                if contributor_field in ("sort_name", "display_name"):
                    # Sort name and display name are indexed both as
                    # searchable text fields and filterable keywords.
                    # We're filtering, so we want to use the keyword
                    # version.
                    index_field += ".keyword"
                check_filter(contributor, {"contributors.%s" % index_field: "value"})

        # You can also apply the filter using a combination of these
        # fields.  At least one of the provided fields must match.
        for cls in Contributor, ContributorData:
            contributor = cls(
                display_name="Ann Leckie",
                sort_name="Leckie, Ann",
                viaf="73520345",
                lc="n2013008575",
            )
            check_filter(
                contributor,
                {"contributors.sort_name.keyword": contributor.sort_name},
                {"contributors.display_name.keyword": contributor.display_name},
                {"contributors.viaf": contributor.viaf},
                {"contributors.lc": contributor.lc},
            )

        # If an author's name is Edition.UNKNOWN_AUTHOR, matches
        # against that field are not counted; otherwise all works with
        # unknown authors would show up.
        unknown_viaf = ContributorData(
            sort_name=Edition.UNKNOWN_AUTHOR,
            display_name=Edition.UNKNOWN_AUTHOR,
            viaf="123",
        )
        check_filter(unknown_viaf, {"contributors.viaf": "123"})

        # This can result in a filter that will match nothing because
        # it has a Bool with a 'minimum_should_match' but no 'should'
        # clauses.
        totally_unknown = ContributorData(
            sort_name=Edition.UNKNOWN_AUTHOR,
            display_name=Edition.UNKNOWN_AUTHOR,
        )
        check_filter(totally_unknown)

        # This is fine -- if the search engine is asked for books by
        # an author about whom absolutely nothing is known, it's okay
        # to return no books.

    def test_target_age_filter(self):
        # Test an especially complex subfilter for target age.

        # We're going to test the construction of this subfilter using
        # a number of inputs.

        # First, let's create a filter that matches "ages 2 to 5".
        two_to_five = Filter(target_age=(2, 5))
        filter = two_to_five.target_age_filter

        # The result is the combination of two filters -- both must
        # match.
        #
        # One filter matches against the lower age range; the other
        # matches against the upper age range.
        assert "bool" == filter.name
        lower_match, upper_match = filter.must

        # We must establish that two-year-olds are not too old
        # for the book.
        def dichotomy(filter):
            """Verify that `filter` is a boolean filter that
            matches one of a number of possibilities. Return those
            possibilities.
            """
            assert "bool" == filter.name
            assert 1 == filter.minimum_should_match
            return filter.should

        more_than_two, no_upper_limit = dichotomy(upper_match)

        # Either the upper age limit must be greater than two...
        assert {"range": {"target_age.upper": {"gte": 2}}} == more_than_two.to_dict()

        # ...or the upper age limit must be missing entirely.
        def assert_matches_nonexistent_field(f, field):
            """Verify that a filter only matches when there is
            no value for the given field.
            """
            assert f.to_dict() == {"bool": {"must_not": [{"exists": {"field": field}}]}}

        assert_matches_nonexistent_field(no_upper_limit, "target_age.upper")

        # We must also establish that five-year-olds are not too young
        # for the book. Again, there are two ways of doing this.
        less_than_five, no_lower_limit = dichotomy(lower_match)

        # Either the lower age limit must be less than five...
        assert {"range": {"target_age.lower": {"lte": 5}}} == less_than_five.to_dict()

        # ...or the lower age limit must be missing entirely.
        assert_matches_nonexistent_field(no_lower_limit, "target_age.lower")

        # Now let's try a filter that matches "ten and under"
        ten_and_under = Filter(target_age=(None, 10))
        filter = ten_and_under.target_age_filter

        # There are two clauses, and one of the two must match.
        less_than_ten, no_lower_limit = dichotomy(filter)

        # Either the lower part of the age range must be <= ten, or
        # there must be no lower age limit. If neither of these are
        # true, then ten-year-olds are too young for the book.
        assert {"range": {"target_age.lower": {"lte": 10}}} == less_than_ten.to_dict()
        assert_matches_nonexistent_field(no_lower_limit, "target_age.lower")

        # Next, let's try a filter that matches "twelve and up".
        twelve_and_up = Filter(target_age=(12, None))
        filter = twelve_and_up.target_age_filter

        # There are two clauses, and one of the two must match.
        more_than_twelve, no_upper_limit = dichotomy(filter)

        # Either the upper part of the age range must be >= twelve, or
        # there must be no upper age limit. If neither of these are true,
        # then twelve-year-olds are too old for the book.
        assert {
            "range": {"target_age.upper": {"gte": 12}}
        } == more_than_twelve.to_dict()
        assert_matches_nonexistent_field(no_upper_limit, "target_age.upper")

        # Finally, test filters that put no restriction on target age.
        no_target_age = Filter()
        assert None == no_target_age.target_age_filter

        no_target_age = Filter(target_age=(None, None))
        assert None == no_target_age.target_age_filter

    def test__scrub(self):
        # Test the _scrub helper method, which transforms incoming strings
        # to the type of strings Opensearch uses.
        m = Filter._scrub
        assert None == m(None)
        assert "foo" == m("foo")
        assert "youngadult" == m("Young Adult")

    def test__scrub_list(self):
        # Test the _scrub_list helper method, which scrubs incoming
        # strings and makes sure they are in a list.
        m = Filter._scrub_list
        assert [] == m(None)
        assert [] == m([])
        assert ["foo"] == m("foo")
        assert ["youngadult", "adult"] == m(["Young Adult", "Adult"])

    def test__filter_ids(self, db: DatabaseTransactionFixture):
        # Test the _filter_ids helper method, which converts database
        # objects to their IDs.
        m = Filter._filter_ids
        assert None == m(None)
        assert [] == m([])
        assert [1, 2, 3] == m([1, 2, 3])

        library = db.default_library()
        assert [library.id] == m([library])

    def test__scrub_identifiers(self, db: DatabaseTransactionFixture):
        # Test the _scrub_identifiers helper method, which converts
        # Identifier objects to IdentifierData.
        i1 = db.identifier()
        i2 = db.identifier()
        si1, si2 = Filter._scrub_identifiers([i1, i2])
        for before, after in ((i1, si1), (i2, si2)):
            assert isinstance(si1, IdentifierData)
            assert before.identifier == after.identifier
            assert before.type == after.type

        # If you pass in an IdentifierData you get it back.
        assert [si1] == list(Filter._scrub_identifiers([si1]))

    def test__chain_filters(self):
        # Test the _chain_filters method, which combines
        # two Opensearch filter objects.
        f1 = Q("term", key="value")
        f2 = Q("term", key2="value2")

        m = Filter._chain_filters

        # If this filter is the start of the chain, it's returned unaltered.
        assert f1 == m(None, f1)

        # Otherwise, a new filter is created.
        chained = m(f1, f2)

        # The chained filter is the conjunction of the two input
        # filters.
        assert chained == f1 & f2

    def test_universal_base_filter(self):
        # Test the base filters that are always applied.

        # We only want to show works that are presentation ready.
        base = Filter.universal_base_filter(self._mock_chain)
        assert [Term(presentation_ready=True)] == base

    def test_universal_nested_filters(self):
        # Test the nested filters that are always applied.

        nested = Filter.universal_nested_filters()

        # Currently all nested filters operate on the 'licensepools'
        # subdocument.
        [not_suppressed, currently_owned] = nested.pop("licensepools")
        assert {} == nested

        # Let's look at those filters.

        # The first one is simple -- the license pool must not be
        # suppressed.
        assert Term(**{"licensepools.suppressed": False}) == not_suppressed

        # The second one is a little more complex
        owned = Term(**{"licensepools.licensed": True})
        open_access = Term(**{"licensepools.open_access": True})

        # We only count license pools that are open-access _or_ that have
        # currently owned licenses.
        assert Bool(should=[owned, open_access]) == currently_owned

    def _mock_chain(self, filters, new_filter):
        """A mock of _chain_filters so we don't have to check
        test results against super-complicated Opensearch
        filter objects.

        Instead, we'll get a list of smaller filter objects.
        """
        if filters is None:
            # There are no active filters.
            filters = []
        if isinstance(filters, opensearch_dsl_query):
            # An initial filter was passed in. Convert it to a list.
            filters = [filters]
        filters.append(new_filter)
        return filters


class TestSortKeyPagination:
    """Test the Opensearch-implementation of Pagination that does
    pagination by tracking the last item on the previous page,
    rather than by tracking the number of items seen so far.
    """

    def test_from_request(self):
        # No arguments -> Class defaults.
        pagination = SortKeyPagination.from_request({}.get, None)
        assert isinstance(pagination, SortKeyPagination)
        assert SortKeyPagination.DEFAULT_SIZE == pagination.size
        assert None == pagination.pagination_key

        # Override the default page size.
        pagination = SortKeyPagination.from_request({}.get, 100)
        assert isinstance(pagination, SortKeyPagination)
        assert 100 == pagination.size
        assert None == pagination.pagination_key

        # The most common usages.
        pagination = SortKeyPagination.from_request(dict(size="4").get)
        assert isinstance(pagination, SortKeyPagination)
        assert 4 == pagination.size
        assert None == pagination.pagination_key

        pagination_key = json.dumps(["field 1", 2])

        pagination = SortKeyPagination.from_request(dict(key=pagination_key).get)
        assert isinstance(pagination, SortKeyPagination)
        assert SortKeyPagination.DEFAULT_SIZE == pagination.size
        assert pagination_key == pagination.pagination_key

        # Invalid size -> problem detail
        error = SortKeyPagination.from_request(dict(size="string").get)
        assert INVALID_INPUT.uri == error.uri
        assert "Invalid page size: string" == str(error.detail)

        # Invalid pagination key -> problem detail
        error = SortKeyPagination.from_request(dict(key="not json").get)
        assert INVALID_INPUT.uri == error.uri
        assert "Invalid page key: not json" == str(error.detail)

        # Size too large -> cut down to MAX_SIZE
        pagination = SortKeyPagination.from_request(dict(size="10000").get)
        assert isinstance(pagination, SortKeyPagination)
        assert SortKeyPagination.MAX_SIZE == pagination.size
        assert None == pagination.pagination_key

    def test_items(self):
        # Test the values added to URLs to propagate pagination
        # settings across requests.
        pagination = SortKeyPagination(size=20)
        assert [("size", 20)] == list(pagination.items())
        key = ["the last", "item"]
        pagination.last_item_on_previous_page = key
        assert [("key", json.dumps(key)), ("size", 20)] == list(pagination.items())

    def test_pagination_key(self):
        # SortKeyPagination has no pagination key until it knows
        # about the last item on the previous page.
        pagination = SortKeyPagination()
        assert None == pagination.pagination_key

        key = ["the last", "item"]
        pagination.last_item_on_previous_page = key
        assert pagination.pagination_key == json.dumps(key)

    def test_unimplemented_features(self):
        # Check certain features of a normal Pagination object that
        # are not implemented in SortKeyPagination.

        # Set up a realistic SortKeyPagination -- certain things
        # will remain undefined.
        pagination = SortKeyPagination(last_item_on_previous_page=object())
        pagination.this_page_size = 100
        pagination.last_item_on_this_page = object()

        # The offset is always zero.
        assert 0 == pagination.offset

        # The total size is always undefined, even though we could
        # theoretically track it.
        assert None == pagination.total_size

        # The previous page is always undefined, through theoretically
        # we could navigate backwards.
        assert None == pagination.previous_page

        with pytest.raises(NotImplementedError) as excinfo:
            pagination.modify_database_query(object())
        assert "SortKeyPagination does not work with database queries." in str(
            excinfo.value
        )

    def test_modify_search_query(self):
        class MockSearch:
            update_from_dict_called_with = "not called"
            getitem_called_with = "not called"

            def update_from_dict(self, dict):
                self.update_from_dict_called_with = dict
                return self

            def __getitem__(self, slice):
                self.getitem_called_with = slice
                return "modified search object"

        search = MockSearch()

        # We start off in a state where we don't know the last item on the
        # previous page.
        pagination = SortKeyPagination()

        # In this case, modify_search_query slices out the first
        # 'page' of results and returns a modified search object.
        assert "modified search object" == pagination.modify_search_query(search)
        assert slice(0, 50) == search.getitem_called_with

        # update_from_dict was not called. We don't know where to
        # start our search, so we start at the beginning.
        assert "not called" == search.update_from_dict_called_with

        # Now let's say we find out the last item on the previous page
        # -- in real life, this would be because we call page_loaded()
        # and then next_page().
        last_item = object()
        pagination.last_item_on_previous_page = last_item

        # Reset the object so we can verify __getitem__ gets called
        # again.
        search.getitem_called_with = "not called"

        # With .last_item_on_previous_page set, modify_search_query()
        # calls update_from_dict() on our mock OpenSearch `Search`
        # object, passing in the last item on the previous page.

        # The return value of modify_search_query() becomes the active
        # Search object.
        assert "modified search object" == pagination.modify_search_query(search)

        # Now we can see that the Opensearch object was modified to
        # use the 'search_after' feature.
        assert dict(search_after=last_item) == search.update_from_dict_called_with

        # And the resulting object was modified _again_ to get the
        # first 'page' of results following last_item.
        assert slice(0, 50) == search.getitem_called_with

    def test_page_loaded(self):
        # Test what happens to a SortKeyPagination object when a page of
        # results is loaded.
        this_page = SortKeyPagination()

        # Mock an Opensearch 'hit' object -- we'll be accessing
        # hit.meta.sort.
        class MockMeta:
            def __init__(self, sort_key):
                self.sort = sort_key

        class MockItem:
            def __init__(self, sort_key):
                self.meta = MockMeta(sort_key)

        # Make a page of results, each with a unique sort key.
        hits = [MockItem(["sort", "key", num]) for num in range(5)]
        last_hit = hits[-1]

        # Tell the page about the results.
        assert False == this_page.page_has_loaded
        this_page.page_loaded(hits)
        assert True == this_page.page_has_loaded

        # We know the size.
        assert 5 == this_page.this_page_size

        # We know the sort key of the last item in the page.
        assert last_hit.meta.sort == this_page.last_item_on_this_page

        # This code has coverage elsewhere, but just so you see how it
        # works -- we can now get the next page...
        next_page = this_page.next_page

        # And it's defined in terms of the last item on its
        # predecessor. When we pass the new pagination object into
        # create_search_doc, it'll call this object's
        # modify_search_query method. The resulting search query will
        # pick up right where the previous page left off.
        assert last_hit.meta.sort == next_page.last_item_on_previous_page

    def test_next_page(self):
        # To start off, we can't say anything about the next page,
        # because we don't know anything about _this_ page.
        first_page = SortKeyPagination()
        assert None == first_page.next_page

        # Let's learn about this page.
        first_page.this_page_size = 10
        last_item = object()
        first_page.last_item_on_this_page = last_item

        # When we call next_page, the last item on this page becomes the
        # next page's "last item on previous_page"
        next_page = first_page.next_page
        assert last_item == next_page.last_item_on_previous_page

        # Again, we know nothing about this page, since we haven't
        # loaded it yet.
        assert None == next_page.this_page_size
        assert None == next_page.last_item_on_this_page

        # In the unlikely event that we know the last item on the
        # page, but the page size is zero, there is no next page.
        first_page.this_page_size = 0
        assert None == first_page.next_page


class TestBulkUpdate:
    def test_works_not_presentation_ready_kept_in_index(
        self, db: DatabaseTransactionFixture
    ):
        w1 = db.work()
        w1.set_presentation_ready()
        w2 = db.work()
        w2.set_presentation_ready()
        w3 = db.work()
        index = MockExternalSearchIndex()
        successes, failures = index.bulk_update([w1, w2, w3])

        # All three works are regarded as successes, because their
        # state was successfully mirrored to the index.
        assert {w1, w2, w3} == set(successes)
        assert [] == failures

        # All three works were inserted into the index, even the one
        # that's not presentation-ready.
        ids = {x[-1] for x in list(index.docs.keys())}
        assert {w1.id, w2.id, w3.id} == ids

        # If a work stops being presentation-ready, it is kept in the
        # index.
        w2.presentation_ready = False
        successes, failures = index.bulk_update([w1, w2, w3])
        assert {w1.id, w2.id, w3.id} == {x[-1] for x in list(index.docs.keys())}
        assert {w1, w2, w3} == set(successes)
        assert [] == failures


class TestSearchErrors:
    def test_search_connection_timeout(
        self, external_search_fixture: ExternalSearchFixture
    ):
        search, transaction = (
            external_search_fixture,
            external_search_fixture.db,
        )

        attempts = []

        def bulk_with_timeout(docs, raise_on_error=False, raise_on_exception=False):
            attempts.append(docs)

            def error(doc):
                return dict(
                    index=dict(
                        status="TIMEOUT",
                        exception="ConnectionTimeout",
                        error="Connection Timeout!",
                        _id=doc["_id"],
                        data=doc,
                    )
                )

            errors = list(map(error, docs))
            return 0, errors

        search.search.bulk = bulk_with_timeout

        work = transaction.work()
        work.set_presentation_ready()
        successes, failures = search.search.bulk_update([work])
        assert [] == successes
        assert 1 == len(failures)
        assert work == failures[0][0]
        assert "Connection Timeout!" == failures[0][1]

        # When all the documents fail, it tries again once with the same arguments.
        assert [work.id, work.id] == [docs[0]["_id"] for docs in attempts]

    def test_search_single_document_error(
        self, external_search_fixture: ExternalSearchFixture
    ):
        search, transaction = (
            external_search_fixture,
            external_search_fixture.db,
        )

        successful_work = transaction.work()
        successful_work.set_presentation_ready()
        failing_work = transaction.work()
        failing_work.set_presentation_ready()

        def bulk_with_error(docs, raise_on_error=False, raise_on_exception=False):
            failures = [
                dict(
                    data=dict(_id=failing_work.id),
                    error="There was an error!",
                    exception="Exception",
                )
            ]
            success_count = 1
            return success_count, failures

        search.search.bulk = bulk_with_error

        successes, failures = search.search.bulk_update([successful_work, failing_work])
        assert [successful_work] == successes
        assert 1 == len(failures)
        assert failing_work == failures[0][0]
        assert "There was an error!" == failures[0][1]


class TestWorkSearchResult:
    # Test the WorkSearchResult class, which wraps together a data
    # model Work and an OpenSearch Hit into something that looks
    # like a Work.

    def test_constructor(self, db: DatabaseTransactionFixture):
        work = db.work()
        hit = object()
        result = WorkSearchResult(work, hit)

        # The original Work object is available as ._work
        assert work == result._work

        # The Opensearch Hit object is available as ._hit
        assert hit == result._hit

        # Any other attributes are delegated to the Work.
        assert work.sort_title == result.sort_title


class TestSearchIndexCoverageProvider:
    def test_operation(self, db: DatabaseTransactionFixture):
        index = MockExternalSearchIndex()
        provider = SearchIndexCoverageProvider(db.session, search_index_client=index)
        assert WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION == provider.operation

    def test_to_search_document(self, db: DatabaseTransactionFixture):
        """Compare the new and old to_search_document functions
        TODO: classifications
        """
        customlist, editions = db.customlist()
        works = [
            db.work(
                authors=[db.contributor()],
                with_license_pool=True,
                genre="history",
            ),
            editions[0].work,
        ]

        work1: Work = works[0]
        work2: Work = works[1]

        work1.target_age = NumericRange(lower=18, upper=22, bounds="()")
        work2.target_age = NumericRange(lower=18, upper=99, bounds="[]")

        genre1, is_new = Genre.lookup(db.session, "Psychology")
        genre2, is_new = Genre.lookup(db.session, "Cooking")
        subject1 = db.subject(type=Subject.SIMPLIFIED_GENRE, identifier="subject1")
        subject1.genre = genre1
        subject2 = db.subject(type=Subject.SIMPLIFIED_GENRE, identifier="subject2")
        subject2.genre = genre2
        source = DataSource.lookup(db.session, DataSource.AXIS_360)

        # works.extend([transaction.work() for i in range(500)])

        result = Work.to_search_documents__DONOTUSE(works)
        inapp = Work.to_search_documents(works)

        # Top level keys should be the same
        assert len(result) == len(inapp)

        inapp_work1 = list(filter(lambda x: x["work_id"] == work1.id, inapp))[0]
        inapp_work2 = list(filter(lambda x: x["work_id"] == work2.id, inapp))[0]

        # target ages
        assert inapp_work1["target_age"]["lower"] == 19
        assert inapp_work1["target_age"]["upper"] == 21
        assert inapp_work2["target_age"]["lower"] == 18
        assert inapp_work2["target_age"]["upper"] == 99

        assert len(inapp_work1["genres"]) == 1
        assert inapp_work2["genres"] == None

        assert len(inapp_work1["licensepools"]) == 1
        assert len(inapp_work2["licensepools"]) == 1  # customlist adds a pool

        assert len(inapp_work2["customlists"]) == 1
        assert inapp_work1["customlists"] == None

        result_work1 = list(filter(lambda x: x["work_id"] == work1.id, result))[0]
        result_work2 = list(filter(lambda x: x["work_id"] == work2.id, result))[0]

        # Every item must be equivalent
        for result_item, inapp_item in [
            (result_work1, inapp_work1),
            (result_work2, inapp_work2),
        ]:
            for key in result_item.keys():
                assert result_item[key] == inapp_item[key]

    def test_to_search_documents_performance(self, db: DatabaseTransactionFixture):
        works = [db.work(with_license_pool=True, genre="history") for i in range(20)]

        connection = db.database.connection
        with DBStatementCounter(connection) as old_counter:
            with PerfTimer() as t1:
                result = Work.to_search_documents(works)

        with DBStatementCounter(connection) as new_counter:
            with PerfTimer() as t2:
                inapp = Work.to_search_documents(works)

        # Do not be 100x performance
        assert t2.execution_time < t1.execution_time * 5

        # 4 queries per batch only
        assert new_counter.get_count() <= 4

    def test_to_search_documents_with_missing_data(
        self, db: DatabaseTransactionFixture
    ):
        # Missing edition relationship
        work: Work = db.work(with_license_pool=True)
        work.presentation_edition_id = None
        [result] = Work.to_search_documents([work])
        assert result["identifiers"] == None

        # Missing just some attributes
        work: Work = db.work(with_license_pool=True)
        work.presentation_edition.title = None
        work.target_age = None
        [result] = Work.to_search_documents([work])
        assert result["title"] == None
        assert result["target_age"]["lower"] == None

    def test_success(self, db: DatabaseTransactionFixture):
        work = db.work()
        work.set_presentation_ready()
        index = MockExternalSearchIndex()
        provider = SearchIndexCoverageProvider(db.session, search_index_client=index)
        results = provider.process_batch([work])

        # We got one success and no failures.
        assert [work] == results

        # The work was added to the search index.
        assert 1 == len(index.docs)

    def test_failure(self, db: DatabaseTransactionFixture):
        class DoomedExternalSearchIndex(MockExternalSearchIndex):
            """All documents sent to this index will fail."""

            def bulk(self, docs, **kwargs):
                return 0, [
                    dict(
                        data=dict(_id=failing_work["_id"]),
                        error="There was an error!",
                        exception="Exception",
                    )
                    for failing_work in docs
                ]

        work = db.work()
        work.set_presentation_ready()
        index = DoomedExternalSearchIndex()
        provider = SearchIndexCoverageProvider(db.session, search_index_client=index)
        results = provider.process_batch([work])

        # We have one transient failure.
        [record] = results
        assert work == record.obj
        assert True == record.transient
        assert "There was an error!" == record.exception


class TestJSONQuery:
    @staticmethod
    def _leaf(key, value, op="eq"):
        return dict(key=key, value=value, op=op)

    @staticmethod
    def _jq(query):
        return JSONQuery(dict(query=query))

    match_args = JSONQuery.MATCH_ARGS

    def test_search_query(self, external_search_fixture: ExternalSearchFixture):
        q = {"key": "medium", "value": "Book"}
        q = self._jq(q)
        q.search_query.to_dict() == {
            "match": {"medium.keyword": {"query": "Book", **self.match_args}}
        }

        q = {"or": [self._leaf("medium", "Book"), self._leaf("medium", "Audio")]}
        q = self._jq(q)
        q.search_query.to_dict() == {
            "bool": {
                "should": [
                    {"match": {"medium.keyword": {"query": "Book", **self.match_args}}},
                    {
                        "match": {
                            "medium.keyword": {"query": "Audio", **self.match_args}
                        }
                    },
                ]
            }
        }

        q = {"and": [self._leaf("medium", "Book"), self._leaf("medium", "Audio")]}
        q = self._jq(q)
        q.search_query.to_dict() == {
            "bool": {
                "must": [
                    {"match": {"medium.keyword": {"query": "Book", **self.match_args}}},
                    {
                        "match": {
                            "medium.keyword": {"query": "Audio", **self.match_args}
                        }
                    },
                ]
            }
        }

        q = {
            "and": [
                self._leaf("title", "Title"),
                {"or": [self._leaf("medium", "Book"), self._leaf("medium", "Audio")]},
            ]
        }
        q = self._jq(q)
        q.search_query.to_dict() == {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                {
                                    "match": {
                                        "medium.keyword": {
                                            "query": "Book",
                                            **self.match_args,
                                        }
                                    }
                                },
                                {
                                    "match": {
                                        "medium.keyword": {
                                            "query": "Audio",
                                            **self.match_args,
                                        }
                                    }
                                },
                            ]
                        }
                    },
                    {"match": {"title.keyword": {"query": "Title", **self.match_args}}},
                ]
            }
        }

        q = {"or": [self._leaf("medium", "Book"), self._leaf("medium", "Audio", "neq")]}
        q = self._jq(q)
        assert q.search_query.to_dict() == {
            "bool": {
                "should": [
                    {"match": {"medium.keyword": {"query": "Book", **self.match_args}}},
                    {
                        "bool": {
                            "must_not": [
                                {
                                    "match": {
                                        "medium.keyword": {
                                            "query": "Audio",
                                            **self.match_args,
                                        }
                                    }
                                }
                            ]
                        }
                    },
                ]
            }
        }

        q = {
            "and": [
                self._leaf("title", "Title"),
                {"not": [self._leaf("author", "Geoffrey")]},
            ]
        }
        q = self._jq(q)
        assert q.search_query.to_dict() == {
            "bool": {
                "must": [
                    {"match": {"title.keyword": {"query": "Title", **self.match_args}}},
                    {
                        "bool": {
                            "must_not": [
                                {
                                    "match": {
                                        "author.keyword": {
                                            "query": "Geoffrey",
                                            **self.match_args,
                                        }
                                    }
                                }
                            ]
                        }
                    },
                ]
            }
        }

    @pytest.mark.parametrize(
        "key,value,op",
        [
            ("target_age", 18, "lte"),
            ("target_age", 18, "lt"),
            ("target_age", 18, "gt"),
            ("target_age", 18, "gte"),
        ],
    )
    def test_search_query_range(self, key, value, op):
        q = self._leaf(key, value, op)
        q = self._jq(q)
        assert q.search_query.to_dict() == {"range": {f"{key}": {op: value}}}

    @pytest.mark.parametrize(
        "key,value,is_keyword",
        [
            ("contributors.display_name", "name", True),
            ("contributors.lc", "name", False),
            ("genres.name", "name", False),
            ("licensepools.medium", "Book", False),
        ],
    )
    def test_search_query_nested(self, key, value, is_keyword):
        q = self._jq(self._leaf(key, value))
        term = key if not is_keyword else f"{key}.keyword"
        root = key.split(".")[0]
        assert q.search_query.to_dict() == {
            "nested": {
                "path": root,
                "query": {"match": {term: {"query": value, **self.match_args}}},
            }
        }

    @pytest.mark.parametrize(
        "query,error_match",
        [
            (dict(key="author", op="eg", value="name"), "Unrecognized operator: eg"),
            (dict(key="arthur", op="eq", value="name"), "Unrecognized key: arthur"),
            (
                dict(kew="author", op="eq", value="name"),
                "Could not make sense of the query",
            ),
            ({"and": [], "or": []}, "A conjuction cannot have multiple parts"),
        ],
    )
    def test_errors(self, query, error_match):
        q = self._jq(query)

        with pytest.raises(QueryParseException, match=error_match):
            q.search_query  # fetch the property

    def test_regex_query(self):
        q = self._jq(self._leaf("title", "book", op="regex"))
        assert q.search_query.to_dict() == {
            "regexp": {
                "title.keyword": {
                    "flags": "ALL",
                    "value": "book",
                }
            }
        }

    def test_field_transforms(self):
        q = self._jq(self._leaf("classification", "cls"))
        assert q.search_query.to_dict() == {
            "match": {
                "classifications.term.keyword": {"query": "cls", **self.match_args}
            }
        }
        q = self._jq(self._leaf("open_access", True))
        assert q.search_query.to_dict() == {
            "nested": {
                "path": "licensepools",
                "query": {"term": {"licensepools.open_access": True}},
            }
        }

    def test_value_transforms(self, db: DatabaseTransactionFixture):
        # If we're running this unit test alone, we must intialize the data first
        CachedData.initialize(db.session)

        gutenberg = (
            db.session.query(DataSource)
            .filter(DataSource.name == DataSource.GUTENBERG)
            .first()
        )
        q = self._jq(self._leaf("data_source", DataSource.GUTENBERG))
        assert q.search_query.to_dict() == {
            "nested": {
                "path": "licensepools",
                "query": {"term": {"licensepools.data_source_id": gutenberg.id}},
            }
        }

        # Test case-insensitivity for data sources
        q = self._jq(self._leaf("data_source", DataSource.GUTENBERG.upper()))
        assert q.search_query.to_dict() == {
            "nested": {
                "path": "licensepools",
                "query": {"term": {"licensepools.data_source_id": gutenberg.id}},
            }
        }

        dt = datetime(1990, 1, 1)
        q = self._jq(self._leaf("published", "1990-01-01"))
        assert q.search_query.to_dict() == {"term": {"published": dt.timestamp()}}

        with pytest.raises(QueryParseException) as exc:
            q = self._jq(self._leaf("published", "1990-01-x1"))
            q.search_query
            assert (
                "Could not parse 'published' value '1990-01-x1'. Only use 'YYYY-MM-DD'"
            )

        # Test language code transformations
        q = self._jq(self._leaf("language", "EngliSH"))
        assert q.search_query.to_dict() == {"term": {"language": "eng"}}

        # Nothing found, stay the same
        q = self._jq(self._leaf("language", "NoLanguage"))
        assert q.search_query.to_dict() == {"term": {"language": "NoLanguage"}}

        # Already a language code
        q = self._jq(self._leaf("language", "eng"))
        assert q.search_query.to_dict() == {"term": {"language": "eng"}}

    def test_operator_restrictions(self):
        q = self._jq(self._leaf("data_source", DataSource.GUTENBERG, "gt"))
        with pytest.raises(QueryParseException) as exc:
            q.search_query
        assert "Operator 'gt' is not allowed for 'data_source'. Only use ['eq']" == str(
            exc.value
        )

    @pytest.mark.parametrize(
        "key,value,is_text",
        [
            ("title", "value", True),
            ("licensepools.open_access", True, False),
            ("published", "1990-01-01", False),
        ],
    )
    def test_type_queries(self, key, value, is_text):
        """Bool and long types are term queries, whereas text is a match query"""
        q = self._jq(self._leaf(key, value))
        q.search_query.to_dict().keys() == ["match" if is_text else "term"]

    @pytest.mark.parametrize(
        "value,escaped,contains",
        [
            ("&search##", r"\&search\#\#", True),
            ("sea+@~r\\ch", "sea\\+\\@\\~r\\\\ch", True),
            ("sea+@~r\\ch", "sea+@~r\\ch", False),
        ],
    )
    def test_special_chars(self, value, escaped, contains):
        q = self._jq(self._leaf("title", value, "contains" if contains else "eq"))
        if contains:
            assert (
                q.search_query.to_dict()["regexp"]["title.keyword"]["value"]
                == f".*{escaped}.*"
            )
        else:
            assert (
                q.search_query.to_dict()["match"]["title.keyword"]["query"] == escaped
            )


class TestExternalSearchJSONQueryData:
    audio_work: Work
    book_work: Work
    facets: SearchFacets
    filter: Filter
    random_works: List[Work]


class TestExternalSearchJSONQuery:
    @staticmethod
    def _leaf(key, value, op="eq"):
        return dict(key=key, value=value, op=op)

    @staticmethod
    def _jq(query):
        return JSONQuery(dict(query=query))

    @staticmethod
    def _populate_works(
        data: EndToEndSearchFixture,
    ) -> TestExternalSearchJSONQueryData:
        transaction, session = (
            data.external_search.db,
            data.external_search.db.session,
        )
        _work: Callable = data.external_search.default_work

        result = TestExternalSearchJSONQueryData()
        result.book_work = transaction.work(with_open_access_download=True)
        result.book_work.presentation_edition.medium = "Book"

        result.audio_work = transaction.work(with_open_access_download=True)
        result.audio_work.presentation_edition.medium = "Audio"

        result.random_works = []
        specifics = [
            dict(language="spa", authors=["charlie"]),
            dict(language="spa", authors=["alpha"]),
            dict(language="ger", authors=["beta"]),
            dict(
                with_open_access_download=False,
                with_license_pool=True,
                authors=["delta"],
            ),
        ]
        for i in range(4):
            new_data = dict(
                title=uuid.uuid4(),
                with_open_access_download=True,
            )
            new_data.update(**specifics[i])
            w = transaction.work(**new_data)
            result.random_works.append(w)

        session.commit()

        result.facets = facets = SearchFacets(search_type="json")
        result.filter = Filter(facets=facets)
        return result

    @staticmethod
    def expect(
        fixture: EndToEndSearchFixture,
        data: TestExternalSearchJSONQueryData,
        partial_query,
        works,
    ):
        query = dict(query=partial_query)
        resp = fixture.external_search.search.query_works(query, data.filter)

        assert len(resp.hits) == len(works)

        respids = {h.work_id for h in resp.hits}
        expectedids = {w.id for w in works}
        assert respids == expectedids
        return resp

    def test_search_basic(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        fixture = end_to_end_search_fixture
        data = self._populate_works(fixture)
        fixture.populate_search_index()

        self.expect(fixture, data, self._leaf("medium", "Audio"), [data.audio_work])

        w1: Work = data.random_works[0]
        self.expect(fixture, data, self._leaf("title", w1.title), [w1])
        self.expect(
            fixture,
            data,
            self._leaf(
                "contributors.display_name",
                w1.presentation_edition.contributions[0].contributor.display_name,
            ),
            [w1],
        )

        w2: Work = data.random_works[1]
        self.expect(
            fixture,
            data,
            {"or": [self._leaf("title", w1.title), self._leaf("title", w2.title)]},
            [w1, w2],
        )
        self.expect(
            fixture,
            data,
            {"and": [self._leaf("title", w1.title), self._leaf("title", w2.title)]},
            [],
        )

        self.expect(
            fixture,
            data,
            {"and": [self._leaf("language", "German")]},
            [data.random_works[2]],
        )

    def test_field_transform(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        fixture = end_to_end_search_fixture
        data = self._populate_works(fixture)
        fixture.populate_search_index()

        """Fields transforms should apply and criterias should match"""
        self.expect(
            fixture, data, self._leaf("open_access", False), [data.random_works[3]]
        )

    def test_search_not(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        fixture = end_to_end_search_fixture
        data = self._populate_works(fixture)
        fixture.populate_search_index()

        self.expect(
            fixture,
            data,
            {
                "and": [
                    self._leaf("medium", "Book"),
                    {"not": [self._leaf("language", "Spanish")]},
                ]
            },
            [
                data.book_work,
                data.random_works[2],
                data.random_works[3],
            ],
        )

    def test_search_with_facets_ordering(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        fixture = end_to_end_search_fixture
        data = self._populate_works(fixture)
        fixture.populate_search_index()

        data.facets = SearchFacets(order="author", search_type="json")
        data.filter = Filter(facets=data.facets)
        assert data.filter.min_score == None

        w = data.random_works
        expected = [w[1], w[2], w[0]]
        response = self.expect(
            fixture,
            data,
            {
                "or": [
                    self._leaf("language", "Spanish"),
                    self._leaf("language", "German"),
                ]
            },
            expected,
        )
        # assert the ordering is as expected
        assert [h.work_id for h in response.hits] == [e.id for e in expected]
