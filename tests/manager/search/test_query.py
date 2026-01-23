import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any

import pytest
from opensearchpy import Q
from opensearchpy.helpers.query import (
    Bool,
    DisMax,
    Match,
    MatchNone,
    MatchPhrase,
    MultiMatch,
    Range,
    Term,
    Terms,
)

from palace.manager.core.classifier import Classifier
from palace.manager.feed.facets.feed import Facets
from palace.manager.feed.facets.search import SearchFacets
from palace.manager.search.filter import Filter
from palace.manager.search.query import (
    JSONQuery,
    Query,
    QueryParseException,
    QueryParser,
)
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.cache import CachedData
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.search import EndToEndSearchFixture, ExternalSearchFixture


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
            universal_base_term: Query | None = Q("term", universal_base_called=True)
            universal_nested_term: Query | None = Q(
                "term", universal_nested_called=True
            )
            universal_nested_filter: dict[str, list[Query | None]] | None = dict(
                nested_called=[universal_nested_term]
            )
            universal_called = False
            nested_called = False

            @classmethod
            def universal_base_filter(cls) -> Query | None:
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
        Filter.universal_base_filter = MockFilter.universal_base_filter  # type: ignore[assignment]
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

        # When using the AVAILABLE_OPEN_ACCESS availability restriction...
        built = from_facets(Facets.AVAILABLE_OPEN_ACCESS, None, None, None)

        # An additional nested filter is applied.
        [available_now] = built.nested_filter_calls
        assert "nested" == available_now["name_or_query"]
        assert "licensepools" == available_now["path"]

        # It finds only license pools that are open access.
        nested_filter = available_now["query"]
        open_access = dict(term={"licensepools.open_access": True})
        assert nested_filter.to_dict() == {"bool": {"filter": [open_access]}}

        # When using the AVAILABLE_NOW restriction...
        built = from_facets(Facets.AVAILABLE_NOW, None, None, None)

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
        built = from_facets(Facets.AVAILABLE_NOT_NOW, None, None, None)

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

        # Distributor builds
        _id = DataSource.lookup(db.session, DataSource.OVERDRIVE).id
        built = from_facets(
            Facets.AVAILABLE_ALL,
            None,
            DataSource.OVERDRIVE,
            None,
        )
        [datasource_only] = built.nested_filter_calls
        nested_filter = datasource_only["query"]
        assert nested_filter.to_dict() == {
            "bool": {"filter": [{"terms": {"licensepools.data_source_id": [_id]}}]}
        }

        # Collection Name builds
        collection = db.default_collection()
        built = from_facets(Facets.AVAILABLE_ALL, None, None, collection.name)
        [collection_only] = built.nested_filter_calls
        nested_filter = collection_only["query"]
        assert nested_filter.to_dict() == {
            "bool": {
                "filter": [{"terms": {"licensepools.collection_id": [collection.id]}}]
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
            None,
            order=Facets.ORDER_AUTHOR,
            distributor=None,
            collection_name=None,
            order_ascending=False,
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


class TestJSONQuery:
    @staticmethod
    def _leaf(key, value, op="eq"):
        return dict(key=key, value=value, op=op)

    @staticmethod
    def _jq(query):
        return JSONQuery(dict(query=query))

    def test_search_query(self, external_search_fixture: ExternalSearchFixture):
        q_dict = {"key": "medium", "value": "Book"}
        q = self._jq(q_dict)
        assert q.search_query.to_dict() == {"term": {"medium.keyword": "Book"}}

        q = {"or": [self._leaf("medium", "Book"), self._leaf("medium", "Audio")]}
        q = self._jq(q)
        assert q.search_query.to_dict() == {
            "bool": {
                "should": [
                    {"term": {"medium.keyword": "Book"}},
                    {"term": {"medium.keyword": "Audio"}},
                ]
            }
        }

        q = {"and": [self._leaf("medium", "Book"), self._leaf("medium", "Audio")]}
        q = self._jq(q)
        assert q.search_query.to_dict() == {
            "bool": {
                "must": [
                    {"term": {"medium.keyword": "Book"}},
                    {"term": {"medium.keyword": "Audio"}},
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
        assert q.search_query.to_dict() == {
            "bool": {
                "must": [
                    {"term": {"title.keyword": "Title"}},
                    {
                        "bool": {
                            "should": [
                                {"term": {"medium.keyword": "Book"}},
                                {"term": {"medium.keyword": "Audio"}},
                            ]
                        }
                    },
                ]
            }
        }

        q = {"or": [self._leaf("medium", "Book"), self._leaf("medium", "Audio", "neq")]}
        q = self._jq(q)
        assert q.search_query.to_dict() == {
            "bool": {
                "should": [
                    {"term": {"medium.keyword": "Book"}},
                    {"bool": {"must_not": [{"term": {"medium.keyword": "Audio"}}]}},
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
                    {"term": {"title.keyword": "Title"}},
                    {"bool": {"must_not": [{"term": {"author.keyword": "Geoffrey"}}]}},
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
            ("licensepools.open_access", True, False),
        ],
    )
    def test_search_query_nested(self, key, value, is_keyword):
        q = self._jq(self._leaf(key, value))
        term = key if not is_keyword else f"{key}.keyword"
        root = key.split(".")[0]
        assert q.search_query.to_dict() == {
            "nested": {"path": root, "query": {"term": {term: value}}}
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
            ({"and": [], "or": []}, "A conjunction cannot have multiple parts"),
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
            "term": {"classifications.term.keyword": "cls"}
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
        assert gutenberg is not None
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

        # Test audience value transform
        q = self._jq(self._leaf("audience", "Young Adult"))
        assert q.search_query.to_dict() == {"term": {"audience": "YoungAdult"}}

    def test_operator_restrictions(self):
        q = self._jq(self._leaf("data_source", DataSource.GUTENBERG, "gt"))
        with pytest.raises(QueryParseException) as exc:
            q.search_query
        assert (
            "Operator 'gt' is not allowed for 'data_source'. Only use ['eq', 'neq']"
            == str(exc.value)
        )

    def test_allowed_operators_for_data_source(self, db: DatabaseTransactionFixture):
        # If we're running this unit test alone, we must intialize the data first
        CachedData.initialize(db.session)

        gutenberg = (
            db.session.query(DataSource)
            .filter(DataSource.name == DataSource.GUTENBERG)
            .first()
        )
        assert gutenberg is not None
        q = self._jq(self._leaf("data_source", DataSource.GUTENBERG, "neq"))
        assert q.search_query.to_dict() == {
            "nested": {
                "path": "licensepools",
                "query": {
                    "bool": {
                        "must_not": [
                            {"term": {"licensepools.data_source_id": gutenberg.id}}
                        ]
                    }
                },
            }
        }

        q = self._jq(self._leaf("data_source", DataSource.GUTENBERG, "eq"))
        assert q.search_query.to_dict() == {
            "nested": {
                "path": "licensepools",
                "query": {"term": {"licensepools.data_source_id": gutenberg.id}},
            }
        }

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
            assert q.search_query.to_dict()["term"]["title.keyword"] == escaped


class TestExternalSearchJSONQueryData:
    audio_work: Work
    book_work: Work
    facets: SearchFacets
    filter: Filter
    random_works: list[Work]


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
        specifics: list[dict[str, Any]] = [
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
        resp = fixture.external_search_index.query_works(query, data.filter)

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
