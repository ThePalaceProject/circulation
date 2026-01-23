import re
from collections.abc import Callable

import pytest
from opensearchpy.helpers.query import (
    Term,
)
from psycopg2.extras import NumericRange

from palace.manager.core.classifier import Classifier
from palace.manager.feed.facets.feed import Facets
from palace.manager.feed.facets.search import SearchFacets
from palace.manager.feed.worklist.base import WorkList
from palace.manager.search.external_search import (
    ExternalSearchIndex,
)
from palace.manager.search.filter import Filter
from palace.manager.search.pagination import Pagination, SortKeyPagination
from palace.manager.search.v5 import SearchV5
from palace.manager.sqlalchemy.model.classification import Genre
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.customlist import CustomList
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import LicensePool, LicensePoolStatus
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.datetime_helpers import datetime_utc
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.search import (
    EndToEndSearchFixture,
    ExternalSearchFixtureFake,
)
from tests.mocks.search import SearchServiceFailureMode

RESEARCH = Term(audience=Classifier.AUDIENCE_RESEARCH.lower())


class TestExternalSearch:
    # TODO: would be good to check the put_script calls, but the
    # current constructor makes put_script difficult to mock.

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

    def test_remove_work(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        duck_life = db.work(
            title="Moby's life as a Duck", with_open_access_download=True
        )
        moby_dick = db.work(title="Moby Dick", with_open_access_download=True)
        client = end_to_end_search_fixture.external_search.client
        index = end_to_end_search_fixture.external_search_index
        end_to_end_search_fixture.populate_search_index()

        end_to_end_search_fixture.expect_results(
            [duck_life, moby_dick], "Moby", ordered=False
        )

        index.remove_work(moby_dick)
        index.remove_work(duck_life.id)

        # Refresh search index so we can query the changes
        client.indices.refresh()
        end_to_end_search_fixture.expect_results([], "Moby")

    def test_add_document(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
        db: DatabaseTransactionFixture,
    ):
        client = end_to_end_search_fixture.external_search.client
        index = end_to_end_search_fixture.external_search_index

        butterfly = db.work(
            title="Nietzsche's Butterfly", with_open_access_download=True
        )
        client.indices.refresh()
        end_to_end_search_fixture.expect_results([], "Butterfly")
        index.add_document(butterfly.to_search_document())
        client.indices.refresh()
        end_to_end_search_fixture.expect_results([butterfly], "Butterfly")

    def test_add_documents(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
        db: DatabaseTransactionFixture,
    ):
        client = end_to_end_search_fixture.external_search.client
        index = end_to_end_search_fixture.external_search_index

        butterfly = db.work(
            title="Nietzsche's Butterfly", with_open_access_download=True
        )
        chaos = db.work(title="Chaos", with_open_access_download=True)
        client.indices.refresh()
        end_to_end_search_fixture.expect_results([], "")
        index.add_documents([w.to_search_document() for w in [butterfly, chaos]])
        client.indices.refresh()
        end_to_end_search_fixture.expect_results([butterfly, chaos], "")

    def test_clear_search_documents(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
        db: DatabaseTransactionFixture,
    ):
        client = end_to_end_search_fixture.external_search.client
        index = end_to_end_search_fixture.external_search_index

        work = db.work(with_open_access_download=True)
        end_to_end_search_fixture.populate_search_index()
        client.indices.refresh()

        end_to_end_search_fixture.expect_results([work], "")

        index.clear_search_documents()
        client.indices.refresh()
        end_to_end_search_fixture.expect_results([], "")


class TestSearchV5:
    def test_character_filters(self):
        # Verify the functionality of the regular expressions we tell
        # Opensearch to use when normalizing fields that will be used
        # for searching.
        filters = []
        for filter_name in SearchV5.AUTHOR_CHAR_FILTER_NAMES:
            configuration = SearchV5.CHAR_FILTERS[filter_name]
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
        result.les_mis.presentation_edition.title = "Les Mis\u00e9rables"

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
        result.no_copies = _work(title="Moby Dick ii")
        result.no_copies.license_pools[0].licenses_owned = 0
        result.no_copies.license_pools[0].status = LicensePoolStatus.EXHAUSTED

        # This book's only license pool has been suppressed.
        result.suppressed = _work(title="Moby Dick iii")
        result.suppressed.license_pools[0].suppressed = True

        # This book is not presentation_ready.
        result.not_presentation_ready = _work(title="Moby Dick iv")
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
        query = fixture.external_search_index.query_works
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
                transaction.default_library(),
                None,
                order=Facets.ORDER_TITLE,
                distributor=None,
                collection_name=None,
            )
            pages = []
            while pagination:
                pages.append(
                    worklist.works(
                        session,
                        facets,
                        pagination,
                        search_engine=fixture.external_search_index,
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
        spanish_wl = WorkList()
        spanish_wl.initialize(transaction.default_library(), languages=["spa"])
        assert [[data.sherlock_spanish]] == pages(spanish_wl)

        # Test a WorkList based on a genre.
        biography_wl = WorkList()
        biography_wl.initialize(transaction.default_library(), genres=[biography])
        assert [[data.lincoln, data.obama]] == pages(biography_wl)

        # Search results may be sorted by some field other than search
        # quality.
        facets = SearchFacets
        by_author_facet = facets(
            library=transaction.default_library(),
            availability=facets.AVAILABLE_ALL,
            order=facets.ORDER_AUTHOR,
        )
        by_author = Filter(facets=by_author_facet)

        by_title_facet = facets(
            library=transaction.default_library(),
            availability=facets.AVAILABLE_ALL,
            order=facets.ORDER_TITLE,
        )
        by_title = Filter(facets=by_title_facet)

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


class TestBulkUpdate:
    def test_works_not_presentation_ready_kept_in_index(
        self,
        db: DatabaseTransactionFixture,
        external_search_fake_fixture: ExternalSearchFixtureFake,
    ):
        w1 = db.work()
        w1.set_presentation_ready()
        w2 = db.work()
        w2.set_presentation_ready()
        w3 = db.work()
        index = external_search_fake_fixture.external_search

        failures = index.add_documents(
            Work.to_search_documents(db.session, [w1.id, w2.id, w3.id])
        )

        # All three works are regarded as successes, because their
        # state was successfully mirrored to the index.
        assert [] == failures

        # All three works were inserted into the index, even the one
        # that's not presentation-ready.
        ids = set(
            map(
                lambda d: d["_id"], external_search_fake_fixture.service.documents_all()
            )
        )
        assert {w1.id, w2.id, w3.id} == ids

        # If a work stops being presentation-ready, it is kept in the
        # index.
        w2.presentation_ready = False
        failures = index.add_documents(
            Work.to_search_documents(db.session, [w1.id, w2.id, w3.id])
        )
        assert {w1.id, w2.id, w3.id} == set(
            map(
                lambda d: d["_id"], external_search_fake_fixture.service.documents_all()
            )
        )
        assert [] == failures


class TestSearchErrors:
    def test_search_connection_timeout(
        self, external_search_fake_fixture: ExternalSearchFixtureFake
    ):
        search, transaction = (
            external_search_fake_fixture,
            external_search_fake_fixture.db,
        )

        search.service.set_failing_mode(
            mode=SearchServiceFailureMode.FAIL_INDEXING_DOCUMENTS_TIMEOUT
        )
        work = transaction.work()
        work.set_presentation_ready()

        failures = search.external_search.add_documents(
            Work.to_search_documents(transaction.session, [work.id])
        )
        assert 1 == len(failures)
        assert work.id == failures[0].id
        assert "Connection Timeout!" == failures[0].error_message

        # Submissions are not retried by the base service
        assert [work.id] == [
            docs["_id"] for docs in search.service.document_submission_attempts
        ]

    def test_search_single_document_error(
        self, external_search_fake_fixture: ExternalSearchFixtureFake
    ):
        search, transaction = (
            external_search_fake_fixture,
            external_search_fake_fixture.db,
        )

        search.service.set_failing_mode(
            mode=SearchServiceFailureMode.FAIL_INDEXING_DOCUMENTS
        )
        work = transaction.work()
        work.set_presentation_ready()

        failures = search.external_search.add_documents(
            Work.to_search_documents(transaction.session, [work.id])
        )
        assert 1 == len(failures)
        assert work.id == failures[0].id
        assert "There was an error!" == failures[0].error_message

        # Submissions are not retried by the base service
        assert [work.id] == [
            docs["_id"] for docs in search.service.document_submission_attempts
        ]
