from opensearchpy import Q
from opensearchpy.helpers.query import Bool, DisMax, MatchAll, Nested, Range, Term

from palace.manager.search.query import Query
from palace.manager.search.search_base import SearchBase


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
