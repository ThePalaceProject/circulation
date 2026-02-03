from __future__ import annotations

import datetime
import json
import re
from collections import defaultdict

from attrs import define
from opensearchpy.helpers.query import (
    Bool,
    Match,
    MatchAll,
    MatchPhrase,
    MultiMatch,
    Nested,
    Query as BaseQuery,
    Range,
    Regexp,
    Term,
    Terms,
)
from spellchecker import SpellChecker

from palace.manager.core.classifier.age import AgeClassifier, GradeLevelClassifier
from palace.manager.core.classifier.keyword import KeywordBasedClassifier
from palace.manager.core.exceptions import BasePalaceException
from palace.manager.search.filter import Filter
from palace.manager.search.query_helpers import (
    boost as boost_query,
    combine_hypotheses,
    make_target_age_query,
    match_term,
    nest,
)
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.util import Values
from palace.manager.util.cache import CachedData
from palace.manager.util.languages import LanguageNames
from palace.manager.util.personal_names import display_name_to_sort_name
from palace.manager.util.stopwords import ENGLISH_STOPWORDS


class Query:
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
        Contributor.Role.PRIMARY_AUTHOR,
        Contributor.Role.AUTHOR,
        Contributor.Role.NARRATOR,
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
        return combine_hypotheses(hypotheses)

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
        return nest("contributors", match_both)

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

        :param kwargs: Keyword arguments for the boost_query function.
        """
        if query or filters:
            query = boost_query(boost=boost, queries=query, filters=filters, **kwargs)
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

    # The fields mappings in the search DB
    FIELD_MAPPING: dict[str, dict] = {
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
        "genres.term": dict(path="genres"),
        "genres.weight": dict(path="genres"),
        "identifiers.identifier": dict(path="identifiers"),
        "identifiers.type": dict(path="identifiers"),
        "imprint": _KEYWORD_ONLY,
        "language": dict(
            type="_text"
        ),  # Made up keyword type, because we don't want text fuzzyness on this
        "licensepools.available": dict(path="licensepools"),
        "licensepools.availability_time": dict(path="licensepools"),
        "licensepools.collection_id": dict(path="licensepools"),
        "licensepools.data_source_id": dict(
            path="licensepools", ops=[Operators.EQ, Operators.NEQ]
        ),
        "licensepools.licensed": dict(path="licensepools"),
        "licensepools.medium": dict(path="licensepools"),
        "licensepools.open_access": dict(path="licensepools"),
        "licensepools.quality": dict(path="licensepools"),
        "licensepools.suppressed": dict(path="licensepools"),
        "medium": _KEYWORD_ONLY,
        "presentation_ready": dict(),
        "publisher": _KEYWORD_ONLY,
        "quality": dict(),
        "series": _KEYWORD_ONLY,
        "sort_author": dict(),
        "sort_title": dict(),
        "subtitle": _KEYWORD_ONLY,
        "target_age": dict(),
        "title": _KEYWORD_ONLY,
        "published": dict(),
    }

    # From the client, some field names may be abstracted
    FIELD_TRANSFORMS = {
        "genre": "genres.name",
        "open_access": "licensepools.open_access",
        "available": "licensepools.available",
        "classification": "classifications.term",
        "data_source": "licensepools.data_source_id",
    }

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

        @staticmethod
        def audience(value: str) -> str:
            """Transform audience to format used by search indexer"""
            return value.replace(" ", "")

    VALUE_TRANSORMS = {
        "data_source": ValueTransforms.data_source,
        "published": ValueTransforms.published,
        "language": ValueTransforms.language,
        "audience": ValueTransforms.audience,
    }

    def __init__(self, query: str | dict, filter=None):
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

    def _nested_path(self, name: str) -> str | None:
        return self.FIELD_MAPPING[name].get("path")

    def _parse_json_query(self, query: dict):
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

    def _parse_json_leaf(self, query: dict) -> BaseQuery:
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

        es_query: BaseQuery | None = None

        if op == self.Operators.EQ:
            es_query = Term(**{key: value})
        elif op == self.Operators.NEQ:
            es_query = Bool(must_not=[Term(**{key: value})])
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

    def _parse_json_join(self, query: dict) -> Bool:
        if len(query.keys()) != 1:
            raise QueryParseException(
                detail="A conjunction cannot have multiple parts in the same sub-query"
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
class QueryParseException(BasePalaceException):
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
        match_query = match_term(field, query)
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

        filter, query = make_target_age_query(query)
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
