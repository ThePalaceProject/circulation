"""Helper functions for creating and modifying opensearch-dsl Query-type objects."""

from __future__ import annotations

from collections.abc import Sequence

from opensearchpy.helpers.query import (
    Bool,
    DisMax,
    MatchAll,
    Nested,
    Query as BaseQuery,
    Term,
)


def boost(
    boost: float,
    queries: BaseQuery | list[BaseQuery],
    filters: list[BaseQuery] | None = None,
    all_must_match: bool = False,
) -> Bool:
    """Boost a query by a certain amount relative to its neighbors in a dis_max query.

    :param boost: Numeric value to boost search results that match `queries`.
    :param queries: One or more Query objects to use in a query context.
    :param filters: Query objects to use in a filter context.
    :param all_must_match: If this is False (the default), then only one of the
        `queries` must match for a search result to get the boost. If this is True,
        then all `queries` must match, or the boost will not apply.
    """
    filters = filters or []
    if isinstance(queries, BaseQuery):
        queries = [queries]

    if all_must_match or len(queries) == 1:
        # Every one of the subqueries in `queries` must match.
        # (If there's only one subquery, this simplifies the final query slightly.)
        return Bool(boost=float(boost), filter=filters, must=queries)
    else:
        # At least one of the queries in `queries` must match.
        return Bool(
            boost=float(boost),
            filter=filters,
            should=queries,
            minimum_should_match=1,
        )


def nest(subdocument: str, query: BaseQuery) -> Nested:
    """Turn a normal query into a nested query.

    This is a helper function; you should probably use nestable() instead.

    :param subdocument: The name of the subdocument to query against, e.g. "contributors".
    :param query: An opensearch-dsl Query object.
    """
    return Nested(path=subdocument, query=query)


def nestable(field: str, query: BaseQuery) -> BaseQuery:
    """Make a query against a field nestable, if necessary."""
    if "s." in field:
        # This is a query against a field from a subdocument. We
        # can't run it against the top-level document; it has to
        # be run in the context of its subdocument.
        subdocument = field.split(".", 1)[0]
        query = nest(subdocument, query)
    return query


def match_term(field: str, query_string: str) -> BaseQuery:
    """A clause that matches the query string against a specific field in the search document."""
    match_query = Term(**{field: query_string})
    return nestable(field, match_query)


def match_range(
    field: str, operation: str, value: int | float
) -> dict[str, dict[str, dict[str, int | float]]]:
    """Match a ranged value for a field, using an operation other than equality.

    e.g. _match_range("field.name", "gte", 5) will match any value for field.name >= 5.
    """
    match = {field: {operation: value}}
    return dict(range=match)


def make_target_age_query(
    target_age: tuple[int, int], boost: float = 1.1
) -> tuple[Bool, Bool]:
    """Create an Opensearch query object for a boolean query that matches works
    whose target ages overlap (partially or entirely) the given age range.

    :param target_age: A 2-tuple (lower limit, upper limit)
    :param boost: Boost works that fit precisely into the target age range by this
        amount, vis-a-vis works that don't.
    :return: A tuple of (filter_version, query_version) Bool queries.
    """
    (lower, upper) = target_age[0], target_age[1]
    # There must be _some_ overlap with the provided range.
    must = [
        match_range("target_age.upper", "gte", lower),
        match_range("target_age.lower", "lte", upper),
    ]

    # Results with ranges contained within the query range are better.
    # e.g. for query 4-6, a result with 5-6 beats 6-7
    should = [
        match_range("target_age.upper", "lte", upper),
        match_range("target_age.lower", "gte", lower),
    ]
    filter_version = Bool(must=must)
    query_version = Bool(must=must, should=should, boost=float(boost))
    return filter_version, query_version


def combine_hypotheses(hypotheses: Sequence[BaseQuery]) -> BaseQuery:
    """Build an Opensearch Query object that tests a number of hypotheses at once.

    :return: A DisMax query if there are hypotheses to be tested; otherwise a MatchAll query.
    """
    if hypotheses:
        return DisMax(queries=hypotheses)
    # We ended up with no hypotheses. Match everything.
    return MatchAll()
