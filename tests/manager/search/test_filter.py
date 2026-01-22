from collections.abc import Callable
from dataclasses import dataclass

import pytest
from opensearchpy import Q
from opensearchpy.helpers.function import FieldValueFactor, RandomScore, ScriptScore
from opensearchpy.helpers.query import Bool, Query as opensearch_dsl_query, Term, Terms
from psycopg2._range import NumericRange

from palace.manager.core.classifier import Classifier
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.feed.facets.feed import Facets, FeaturedFacets
from palace.manager.feed.worklist.base import WorkList
from palace.manager.search.filter import Filter
from palace.manager.search.pagination import Pagination, SortKeyPagination
from palace.manager.search.revision_directory import SearchRevisionDirectory
from palace.manager.sqlalchemy.model.classification import Genre
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.contributor import Contribution, Contributor
from palace.manager.sqlalchemy.model.customlist import CustomList
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import LicensePool, LicensePoolStatus
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import get_one_or_create
from palace.manager.util.datetime_helpers import datetime_utc, from_timestamp
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture
from tests.fixtures.search import EndToEndSearchFixture
from tests.manager.search.test_external_search import RESEARCH


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
        literary_fiction, ignore = Genre.lookup(session, "Literary Fiction")
        assert literary_fiction is not None
        data.literary_fiction = literary_fiction
        fantasy, ignore = Genre.lookup(session, "Fantasy")
        assert fantasy is not None
        data.fantasy = fantasy
        horror, ignore = Genre.lookup(session, "Horror")
        assert horror is not None
        data.horror = horror
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
        default_library = transaction.default_library()
        active_collection = transaction.default_collection()
        inactive_collection = transaction.default_inactive_collection()

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

        # If you pass in a library, you get all of its active collections.
        library_filter = Filter(collections=default_library)
        assert [active_collection.id] == library_filter.collection_ids

        # If the library has no active collections, the collection filter
        # will filter everything out.
        active_collection.associated_libraries = []
        assert default_library.associated_collections == [inactive_collection]
        assert default_library.active_collections == []
        library_filter = Filter(collections=default_library)
        assert library_filter.collection_ids == []

        # If the library has no collections, the collection filter
        # will filter everything out.
        inactive_collection.associated_libraries = []
        assert default_library.associated_collections == []
        assert default_library.active_collections == []
        library_filter = Filter(collections=default_library)
        assert library_filter.collection_ids == []

        # If you pass in Collection objects, you get their IDs.
        collection_filter = Filter(collections=active_collection)
        assert [active_collection.id] == collection_filter.collection_ids
        collection_filter = Filter(collections=[active_collection, inactive_collection])
        assert collection_filter.collection_ids == [
            active_collection.id,
            inactive_collection.id,
        ]

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

    def test_from_worklist(
        self, filter_fixture: FilterFixture, library_fixture: LibraryFixture
    ):
        data, transaction, session = (
            filter_fixture,
            filter_fixture.transaction,
            filter_fixture.transaction.session,
        )

        # Any WorkList can be converted into a Filter.
        #
        # WorkList.inherited_value() and WorkList.inherited_values()
        # are used to determine what should go into the constructor.

        library = transaction.default_library()
        active_collection = transaction.default_collection()
        inactive_collection = transaction.default_inactive_collection()
        settings = library.settings
        assert settings.allow_holds is True

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

        # Only the active collections for a library will be included in
        # the search filter.
        assert set(library.associated_collections) == {
            active_collection,
            inactive_collection,
        }
        assert library.active_collections == [active_collection]

        filter = Filter.from_worklist(session, inherits, facets)
        assert filter.collection_ids == [active_collection.id]
        assert parent.media == filter.media
        assert parent.languages == filter.languages
        assert parent.fiction == filter.fiction
        assert parent.audiences + [Classifier.AUDIENCE_ALL_AGES] == filter.audiences
        assert [parent.license_datasource_id] == filter.license_datasources
        assert (parent.target_age.lower, parent.target_age.upper) == filter.target_age
        assert filter.allow_holds is True

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
            "terms": {"licensepools.collection_id": [active_collection.id]}
        } == subfilter.to_dict()

        # No other subfilters were specified.
        assert {} == subfilters

        # If the library does not allow holds, this information is
        # propagated to its Filter.
        settings = library_fixture.settings(library)
        settings.allow_holds = False
        filter = Filter.from_worklist(session, parent, facets)
        assert settings.allow_holds is False

        # A bit of setup to test how WorkList.collection_ids affects
        # the resulting Filter.

        # Here's a collection associated with the default library.
        for_default_library = WorkList()
        for_default_library.initialize(library)

        # Its filter uses all the collections associated with that library.
        filter = Filter.from_worklist(session, for_default_library, None)
        assert [active_collection.id] == filter.collection_ids

        # Here's a child of that WorkList associated with a different
        # library.
        library2 = transaction.library()
        collection2 = transaction.collection()
        collection2.associated_libraries.append(library2)
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

        if filter.library_id:
            suppressed_for = Terms(**{"suppressed_for": [filter.library_id]})
            final_query["bool"]["must_not"].insert(0, suppressed_for.to_dict())

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

        filter.library_id = transaction.default_library().id
        filter.collection_ids = [transaction.default_collection()]
        filter.fiction = True
        filter._audiences = "CHILDREN"
        filter.target_age = (2, 3)
        overdrive = DataSource.lookup(session, DataSource.OVERDRIVE)
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
        (
            library_suppression,
            medium,
            language,
            fiction,
            audience,
            target_age,
            updated_after,
        ) = built

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

        assert {
            "bool": {
                "must_not": [
                    {"terms": {"suppressed_for": [transaction.default_library().id]}}
                ]
            }
        } == library_suppression.to_dict()

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

    def test_build_library_content_filtering(
        self, filter_fixture: FilterFixture, library_fixture: LibraryFixture
    ) -> None:
        """Test that library-level content filtering excludes works
        matching filtered audiences or genres.
        """
        transaction = filter_fixture.transaction
        library = transaction.default_library()
        settings = library_fixture.settings(library)

        # Test 1: No filtering - empty settings don't affect results
        # Library with no filtered audiences or genres
        filter = Filter(library=library)
        built, nested = filter.build()
        # Only the suppressed_for and research audience filters should be present
        must_not = built.to_dict()["bool"]["must_not"]
        # suppressed_for filter
        assert {"terms": {"suppressed_for": [library.id]}} in must_not
        # default research audience exclusion
        assert {"term": {"audience": "research"}} in must_not
        # Should only have these two must_not clauses
        assert len(must_not) == 2

        # Test 2: Filter by audiences
        settings.filtered_audiences = ["Adult", "Adults Only"]
        filter = Filter(library=library)
        built, nested = filter.build()
        must_not = built.to_dict()["bool"]["must_not"]
        # Should include audience exclusion filter (scrubbed to lowercase/no spaces)
        assert {"terms": {"audience": ["adult", "adultsonly"]}} in must_not

        # Test 3: Filter by genres
        settings.filtered_audiences = []
        settings.filtered_genres = ["Romance", "Horror"]
        filter = Filter(library=library)
        built, nested = filter.build()
        must_not = built.to_dict()["bool"]["must_not"]
        # Should include nested genre exclusion filter
        genre_filter = {
            "nested": {
                "path": "genres",
                "query": {"terms": {"genres.name": ["Romance", "Horror"]}},
            }
        }
        assert genre_filter in must_not

        # Test 4: Both filters applied (AND logic)
        settings.filtered_audiences = ["Young Adult"]
        settings.filtered_genres = ["Horror"]
        filter = Filter(library=library)
        built, nested = filter.build()
        must_not = built.to_dict()["bool"]["must_not"]
        # Should include both audience and genre exclusion filters
        assert {"terms": {"audience": ["youngadult"]}} in must_not
        genre_filter = {
            "nested": {
                "path": "genres",
                "query": {"terms": {"genres.name": ["Horror"]}},
            }
        }
        assert genre_filter in must_not

        # Test 5: No library - no library content filtering
        filter = Filter(library=None)
        built, nested = filter.build()
        must_not = built.to_dict()["bool"]["must_not"]
        # Only the default research audience exclusion should be present
        assert must_not == [{"term": {"audience": "research"}}]

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
        script_name = (
            SearchRevisionDirectory.create().highest().script_name("work_last_update")
        )
        assert script_name == script.pop("stored")

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

    def test_target_age_filter(self, filter_fixture: FilterFixture):
        # Test an especially complex subfilter for target age.

        transaction, session = (
            filter_fixture.transaction,
            filter_fixture.transaction.session,
        )

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

        # Test filters that put no restriction on target age.
        no_target_age = Filter()
        assert None == no_target_age.target_age_filter

        no_target_age = Filter(target_age=(None, None))
        assert None == no_target_age.target_age_filter

        # Test that children lanes include only works with defined target age range
        children_lane = transaction.lane("Children lane")
        children_lane.target_age = (0, 3)
        assert Classifier.AUDIENCE_CHILDREN in children_lane.audiences

        children_filter = Filter.from_worklist(session, children_lane, None)

        target_age_filter_that_only_includes_from_0_to_3 = {
            "bool": {
                "must": [
                    {"range": {"target_age.lower": {"gte": 0}}},
                    {"range": {"target_age.upper": {"lte": 3}}},
                ]
            }
        }
        assert (
            target_age_filter_that_only_includes_from_0_to_3
            == children_filter.target_age_filter.to_dict()
        )

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
        [not_suppressed, active_status] = nested.pop("licensepools")
        assert {} == nested

        # Let's look at those filters.

        # The first one is simple -- the license pool must not be
        # suppressed.
        assert Term(**{"licensepools.suppressed": False}) == not_suppressed

        # For the second one, the licensepool must be active status
        assert (
            Term(**{"licensepools.status": LicensePoolStatus.ACTIVE}) == active_status
        )

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


@dataclass
class LibraryContentFilteringData:
    """Data for library content filtering tests."""

    adult_book: Work
    ya_book: Work
    children_book: Work
    romance_book: Work
    horror_book: Work
    fantasy_book: Work
    ya_romance_book: Work


@pytest.fixture
def library_content_filtering_data(
    end_to_end_search_fixture: EndToEndSearchFixture,
) -> LibraryContentFilteringData:
    """Create and index works for library content filtering tests."""
    create_work = end_to_end_search_fixture.external_search.default_work

    # Works with different audiences
    adult_book = create_work(title="Adult Fiction", audience=Classifier.AUDIENCE_ADULT)
    ya_book = create_work(
        title="YA Adventure", audience=Classifier.AUDIENCE_YOUNG_ADULT
    )
    children_book = create_work(
        title="Kids Story", audience=Classifier.AUDIENCE_CHILDREN
    )

    # Works with different genres
    romance_book = create_work(title="Love Story", genre="Romance")
    horror_book = create_work(title="Scary Tale", genre="Horror")
    fantasy_book = create_work(title="Magic Quest", genre="Fantasy")

    # Work with both specific audience and genre
    ya_romance_book = create_work(
        title="Teen Romance",
        audience=Classifier.AUDIENCE_YOUNG_ADULT,
        genre="Romance",
    )

    end_to_end_search_fixture.populate_search_index()

    return LibraryContentFilteringData(
        adult_book=adult_book,
        ya_book=ya_book,
        children_book=children_book,
        romance_book=romance_book,
        horror_book=horror_book,
        fantasy_book=fantasy_book,
        ya_romance_book=ya_romance_book,
    )


class TestLibraryContentFiltering:
    """Test that library-level content filtering correctly excludes works
    from search results based on filtered_audiences and filtered_genres settings.
    """

    def test_library_content_filtering_end_to_end(
        self,
        end_to_end_search_fixture: EndToEndSearchFixture,
        library_content_filtering_data: LibraryContentFilteringData,
        library_fixture: LibraryFixture,
    ):
        """End-to-end test verifying that library content filters are applied
        correctly when querying OpenSearch.
        """
        fixture = end_to_end_search_fixture
        data = library_content_filtering_data
        transaction = fixture.external_search.db
        library = transaction.default_library()
        settings = library_fixture.settings(library)

        # Helper to create a filter with library and expect results
        def expect_with_filter(expected_works: list[Work], query: str = ""):
            f = Filter(library=library)
            fixture.expect_results(expected_works, query, filter=f, ordered=False)

        # Test 1: No filtering - all works returned
        settings.filtered_audiences = []
        settings.filtered_genres = []
        expect_with_filter(
            [
                data.adult_book,
                data.ya_book,
                data.children_book,
                data.romance_book,
                data.horror_book,
                data.fantasy_book,
                data.ya_romance_book,
            ]
        )

        # Test 2: Filter out Adult audience
        settings.filtered_audiences = ["Adult"]
        settings.filtered_genres = []
        expect_with_filter(
            [
                data.ya_book,
                data.children_book,
                # Romance, horror, fantasy have default Adult audience, so filtered
                data.ya_romance_book,
            ]
        )

        # Test 3: Filter out Young Adult audience
        settings.filtered_audiences = ["Young Adult"]
        settings.filtered_genres = []
        expect_with_filter(
            [
                data.adult_book,
                data.children_book,
                data.romance_book,
                data.horror_book,
                data.fantasy_book,
                # ya_book and ya_romance_book are filtered (Young Adult)
            ]
        )

        # Test 4: Filter out Romance genre
        settings.filtered_audiences = []
        settings.filtered_genres = ["Romance"]
        expect_with_filter(
            [
                data.adult_book,
                data.ya_book,
                data.children_book,
                # romance_book filtered
                data.horror_book,
                data.fantasy_book,
                # ya_romance_book filtered (has Romance genre)
            ]
        )

        # Test 5: Filter out Horror genre
        settings.filtered_audiences = []
        settings.filtered_genres = ["Horror"]
        expect_with_filter(
            [
                data.adult_book,
                data.ya_book,
                data.children_book,
                data.romance_book,
                # horror_book filtered
                data.fantasy_book,
                data.ya_romance_book,
            ]
        )

        # Test 6: Filter both audience and genre (AND logic)
        # Filter Adult audience AND Romance genre
        settings.filtered_audiences = ["Adult"]
        settings.filtered_genres = ["Romance"]
        expect_with_filter(
            [
                # adult_book filtered (Adult)
                data.ya_book,
                data.children_book,
                # romance_book filtered (Adult + Romance)
                # horror_book filtered (Adult)
                # fantasy_book filtered (Adult)
                # ya_romance_book filtered (Romance)
            ]
        )

        # Test 7: Multiple audiences filtered
        settings.filtered_audiences = ["Adult", "Young Adult"]
        settings.filtered_genres = []
        expect_with_filter(
            [
                # adult_book filtered
                # ya_book filtered
                data.children_book,
                # romance_book filtered (Adult)
                # horror_book filtered (Adult)
                # fantasy_book filtered (Adult)
                # ya_romance_book filtered (Young Adult)
            ]
        )

        # Test 8: Multiple genres filtered
        settings.filtered_audiences = []
        settings.filtered_genres = ["Romance", "Horror"]
        expect_with_filter(
            [
                data.adult_book,
                data.ya_book,
                data.children_book,
                # romance_book filtered
                # horror_book filtered
                data.fantasy_book,
                # ya_romance_book filtered (Romance)
            ]
        )


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

        def expect(availability, works):
            facets = Facets(
                transaction.default_library(),
                availability,
                order=Facets.ORDER_TITLE,
                distributor=None,
                collection_name=None,
            )
            fixture.expect_results(works, None, Filter(facets=facets), ordered=False)

        # Get all the books in alphabetical order by title.
        expect(
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
            Facets.AVAILABLE_NOW,
            [data.horse, data.moby, data.duck],
        )

        # Show only works that can *not* be borrowed right now.
        expect(Facets.AVAILABLE_NOT_NOW, [data.becoming])

        # Show only open-access works.
        expect(
            Facets.AVAILABLE_OPEN_ACCESS,
            [data.horse, data.moby],
        )


class TestSearchOrderData:
    a1: LicensePool
    a2: LicensePool
    a: Work
    b1: LicensePool
    b2: LicensePool
    b: Work
    by_publication_date: CustomList
    c1: LicensePool
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

        result.moby_duck = _work(
            title="Moby Duck", authors="donovan hohn", fiction=False
        )
        result.moby_duck.presentation_edition.subtitle = (
            "The True Story of 28,800 Bath Toys Lost at Sea"
        )
        result.moby_duck.summary_text = "A compulsively readable narrative"
        result.moby_duck.presentation_edition.series_position = 1
        result.moby_duck.presentation_edition.publisher = "Penguin"

        result.untitled = _work(title="[Untitled]", authors="[Unknown]")
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
        result.collection1.integration_configuration.name = "Collection 1 - ACB"
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
                Facets.AVAILABLE_ALL,
                order=sort_field,
                distributor=None,
                collection_name=None,
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
                assert pagination is None

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

    def test_lane_priority_level_ordering(
        self, end_to_end_search_fixture: EndToEndSearchFixture
    ):
        fixture = end_to_end_search_fixture

        data = self._populate_works(fixture)

        def assert_book_is_in_collection(book, in_collection, not_in_collection):
            book_collections = [x.collection for x in book.license_pools]
            assert (
                in_collection in book_collections
                and not_in_collection not in book_collections
            )

        collection1_books = {
            data.b,
            data.c,
            data.a,
        }

        collection2_books = collection1_books

        collection3_books = {
            data.d,
            data.e,
        }

        # ensure that all collection 1 books are in collection1 and not in collection3
        for book in collection1_books:
            assert_book_is_in_collection(book, data.collection1, data.collection3)
        # ensure that all collection 2 books (which are the same as collection 1) are in collection2
        # and not in collection3

        for book in collection2_books:
            assert_book_is_in_collection(book, data.collection2, data.collection3)
        # ensure that all collection 3 books are in collection1 and not in collection1
        for book in collection3_books:
            assert_book_is_in_collection(book, data.collection3, data.collection1)

        assert data.e.license_pools[0].collection
        # collection 1 has the highest priority
        data.collection1._set_settings(lane_priority_level=10)
        # collection 2 has lowest priority, but since all books in collection 2 are also in collection 1
        # the highest priority of a collection associated with a work is used.
        data.collection2._set_settings(lane_priority_level=1)
        data.collection3._set_settings(lane_priority_level=1)

        fixture.populate_search_index()
        facets = FeaturedFacets(minimum_featured_quality=0, entrypoint_is_default=True)

        filter = Filter(facets=facets, collections=[data.collection1, data.collection3])

        def get_results():
            hits = fixture.external_search_index.query_works(
                None,
                filter,
                None,
                debug=True,
            )

            return [x.work_id for x in hits]

        def to_work_id_set(book_set):
            return {x.id for x in book_set}

        results = get_results()
        assert set(results[0:3]) == to_work_id_set(collection1_books)
        assert set(results[3:5]) == to_work_id_set(collection3_books)

        # now reverse the priority for 1 and 3 while keeping collection 2 the same
        data.collection1._set_settings(lane_priority_level=1)
        data.collection2._set_settings(lane_priority_level=1)
        data.collection3._set_settings(lane_priority_level=10)

        fixture.populate_search_index()
        # expect collection 3 books to come first.
        results = get_results()
        assert set(results[0:2]) == to_work_id_set(collection3_books)
        assert set(results[2:5]) == to_work_id_set(collection1_books)

        # now give 2 priority over 3 while keeping 1 the same.
        data.collection1._set_settings(lane_priority_level=1)
        data.collection2._set_settings(lane_priority_level=10)
        data.collection3._set_settings(lane_priority_level=5)

        fixture.populate_search_index()
        # expect collection to come after 1/2 books since the priority of 2 exceeds 3.
        results = get_results()
        assert set(results[0:3]) == to_work_id_set(collection2_books)
        assert set(results[3:5]) == to_work_id_set(collection3_books)


class TestAuthorFilterData:
    full: Contributor
    display_name: Contributor
    sort_name: Contributor
    viaf: Contributor
    lc: Contributor
    works: list[Work]
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
            sort_name="Leckie, Ann",  # type: ignore[call-arg]
            viaf="73520345",
            lc="n2013008575",
        )
        result.display_name = Contributor(
            sort_name=Edition.UNKNOWN_AUTHOR, display_name="ann leckie"  # type: ignore[call-arg]
        )
        result.sort_name = Contributor(sort_name="LECKIE, ANN")  # type: ignore[call-arg]
        result.viaf = Contributor(sort_name=Edition.UNKNOWN_AUTHOR, viaf="73520345")  # type: ignore[call-arg]
        result.lc = Contributor(sort_name=Edition.UNKNOWN_AUTHOR, lc="n2013008575")  # type: ignore[call-arg]

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
            role=Contributor.Role.CONTRIBUTOR,
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


class TestFeaturedFacetsData:
    hq_not_available: Work
    hq_available: Work
    hq_available_2: Work
    not_featured_on_list: Work
    featured_on_list: Work
    best_seller_list: CustomList
    default_quality: Work


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

        result.default_quality = _work(title="Of default featurability quality")
        result.default_quality.quality = Filter.FEATURABLE_SCRIPT_DEFAULT_WORK_QUALITY

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
        [
            featurable,
            available_now,
            lane_priority_level,
            random,
        ] = f.scoring_functions(filter)

        # It can be high-quality enough to be featured.
        assert isinstance(featurable, ScriptScore)
        source = filter.FEATURABLE_SCRIPT.format(
            cutoff=f.minimum_featured_quality**2,
            exponent=2,
            default_quality=Filter.FEATURABLE_SCRIPT_DEFAULT_WORK_QUALITY,
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

        assert isinstance(lane_priority_level, FieldValueFactor)
        assert {
            "field_value_factor": {
                "field": "lane_priority_level",
                "factor": 1,
                "missing": 5,
                "modifier": "none",
            }
        } == lane_priority_level.to_dict()

        # If the FeaturedFacets is set to be deterministic (which only happens
        # in tests), the RandomScore is removed.
        f.random_seed = filter.DETERMINISTIC
        [
            featurable_2,
            available_now_2,
            lane_priority_level,
        ] = f.scoring_functions(filter)
        assert featurable_2 == featurable
        assert available_now_2 == available_now

        assert isinstance(lane_priority_level, FieldValueFactor)
        assert {
            "field_value_factor": {
                "field": "lane_priority_level",
                "factor": 1,
                "missing": 5,
                "modifier": "none",
            }
        } == lane_priority_level.to_dict()

        # If custom lists are in play, it can also be featured on one
        # of its custom lists.
        filter.customlist_restriction_sets = [[1, 2], [3]]
        [featurable_2, available_now_2, lane_priority_level, featured_on_list] = (
            f.scoring_functions(filter)
        )
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

        assert isinstance(lane_priority_level, FieldValueFactor)
        assert {
            "field_value_factor": {
                "field": "lane_priority_level",
                "factor": 1,
                "missing": 5,
                "modifier": "none",
            }
        } == lane_priority_level.to_dict()

    @pytest.mark.parametrize(
        "default_or_no_quality", [Filter.FEATURABLE_SCRIPT_DEFAULT_WORK_QUALITY, None]
    )
    def test_run(
        self, end_to_end_search_fixture: EndToEndSearchFixture, default_or_no_quality
    ):
        fixture = end_to_end_search_fixture
        transaction, session = (
            fixture.external_search.db,
            fixture.external_search.db.session,
        )
        data = self._populate_works(fixture)
        # Search involving the `default_quality` work should behave identically,
        # whether it has the default quality or no (i.e., missing) quality.
        # The missing quality case should not cause an exception during search.
        data.default_quality.quality = default_or_no_quality
        # It is unclear why this is necessary, but without it, this test occasionally fails,
        # approximately 1 out of 8 times when running in CI. The failure typically occurs on
        # the first assertion.
        session.expire_all()
        fixture.populate_search_index()

        def works(worklist, facets):
            return worklist.works(
                session,
                facets,
                None,
                search_engine=fixture.external_search_index,
                debug=True,
            )

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
        expect = [data.featured_on_list, data.not_featured_on_list]
        # Generate a list of featured works for the given `worklist`
        # and compare that list against `expect`.
        actual = works(best_sellers, facets)
        fixture.assert_works("Works from WorkList based on CustomList", expect, actual)

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
            session,
            facets,
            None,
            search_engine=fixture.external_search_index,
            debug=True,
        )
        assert 6 == len(only_availability_matters)
        last_two = only_availability_matters[-2:]
        assert data.hq_not_available in last_two
        assert data.featured_on_list in last_two

        # Up to this point we've been avoiding the random element,
        # but we can introduce that now by letting the random_seed
        # parameter be None.
        #
        # The random element is relatively small, so it mainly acts
        # to rearrange works whose scores were similar before.
        #
        # In this test we have two groups of works -- one group is
        # high-quality and available, the other group is low-quality.
        # With the random element in play, the high-quality works will
        # be randomly permuted among themselves, and the low-quality
        # works will be randomly permuted among themselves.
        # However, the high-quality works will always show up before
        # the low-quality works.
        random_facets = FeaturedFacets(1)
        expect_high_quality = [
            data.hq_available_2,
            data.hq_available,
        ]
        expect_low_quality = [
            data.default_quality,
            data.hq_not_available,
            data.not_featured_on_list,
            data.featured_on_list,
        ]
        # Generate a list of featured works for the given `worklist`
        # and compare that list against `expect`.
        actual_random = works(worklist, random_facets)
        assert len(actual_random) == 6
        fixture.assert_works(
            "Works permuted by a random seed (high quality)",
            expect_high_quality,
            actual_random[:2],
            should_be_ordered=False,
        )
        fixture.assert_works(
            "Works permuted by a random seed (low quality)",
            expect_low_quality,
            actual_random[2:],
            should_be_ordered=False,
        )
