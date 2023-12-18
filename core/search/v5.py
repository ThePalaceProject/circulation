from core.search.document import (
    BASIC_TEXT,
    BOOLEAN,
    FILTERABLE_TEXT,
    FLOAT,
    INTEGER,
    LONG,
    SearchMappingDocument,
    SearchMappingFieldType,
    icu_collation_keyword,
    keyword,
    nested,
    sort_author_keyword,
)
from core.search.revision import SearchSchemaRevision


class SearchV5(SearchSchemaRevision):
    SEARCH_VERSION = 5
    """
    The body of this mapping looks for bibliographic information in
    the core document, primarily used for matching search
    requests. It also has nested documents, which are used for
    filtering and ranking Works when generating other types of
    feeds:

    * licensepools -- the Work has these LicensePools (includes current
      availability as a boolean, but not detailed availability information)
    * customlists -- the Work is on these CustomLists
    * contributors -- these Contributors worked on the Work
    """

    # Definition of the work_last_update_script.
    WORK_LAST_UPDATE_SCRIPT = """
double champion = -1;
// Start off by looking at the work's last update time.
for (candidate in doc['last_update_time']) {
    if (champion == -1 || candidate > champion) { champion = candidate; }
}
if (params.collection_ids != null && params.collection_ids.length > 0) {
    // Iterate over all licensepools looking for a pool in a collection
    // relevant to this filter. When one is found, check its
    // availability time to see if it's later than the last update time.
    for (licensepool in params._source.licensepools) {
        if (!params.collection_ids.contains(licensepool['collection_id'])) { continue; }
        double candidate = licensepool['availability_time'];
        if (champion == -1 || candidate > champion) { champion = candidate; }
    }
}
if (params.list_ids != null && params.list_ids.length > 0) {

    // Iterate over all customlists looking for a list relevant to
    // this filter. When one is found, check the previous work's first
    // appearance on that list to see if it's later than the last
    // update time.
    for (customlist in params._source.customlists) {
        if (!params.list_ids.contains(customlist['list_id'])) { continue; }
        double candidate = customlist['first_appearance'];
        if (champion == -1 || candidate > champion) { champion = candidate; }
    }
}

return champion;
"""

    # Use regular expressions to normalized values in sortable fields.
    # These regexes are applied in order; that way "H. G. Wells"
    # becomes "H G Wells" becomes "HG Wells".
    CHAR_FILTERS = {
        "remove_apostrophes": dict(
            type="pattern_replace",
            pattern="'",
            replacement="",
        )
    }

    AUTHOR_CHAR_FILTER_NAMES = []
    for name, pattern, replacement in [
        # The special author name "[Unknown]" should sort after everything
        # else. REPLACEMENT CHARACTER is the final valid Unicode character.
        ("unknown_author", r"\[Unknown\]", "\N{REPLACEMENT CHARACTER}"),
        # Works by a given primary author should be secondarily sorted
        # by title, not by the other contributors.
        ("primary_author_only", r"\s+;.*", ""),
        # Remove parentheticals (e.g. the full name of someone who
        # goes by initials).
        ("strip_parentheticals", r"\s+\([^)]+\)", ""),
        # Remove periods from consideration.
        ("strip_periods", r"\.", ""),
        # Collapse spaces for people whose sort names end with initials.
        ("collapse_three_initials", r" ([A-Z]) ([A-Z]) ([A-Z])$", " $1$2$3"),
        ("collapse_two_initials", r" ([A-Z]) ([A-Z])$", " $1$2"),
    ]:
        normalizer = dict(
            type="pattern_replace", pattern=pattern, replacement=replacement
        )
        CHAR_FILTERS[name] = normalizer
        AUTHOR_CHAR_FILTER_NAMES.append(name)

    def __init__(self):
        super().__init__()

        self._normalizers = {}
        self._char_filters = {}
        self._filters = {}
        self._analyzers = {}

        # Set up character filters.
        #
        self._char_filters = self.CHAR_FILTERS

        # This normalizer is used on freeform strings that
        # will be used as tokens in filters. This way we can,
        # e.g. ignore capitalization when considering whether
        # two books belong to the same series or whether two
        # author names are the same.
        self._normalizers["filterable_string"] = dict(
            type="custom", filter=["lowercase", "asciifolding"]
        )

        # Set up analyzers.
        #

        # We use three analyzers:
        #
        # 1. An analyzer based on Opensearch's default English
        #    analyzer, with a normal stemmer -- used as the default
        #    view of a text field such as 'description'.
        #
        # 2. An analyzer that's exactly the same as #1 but with a less
        #    aggressive stemmer -- used as the 'minimal' view of a
        #    text field such as 'description.minimal'.
        #
        # 3. An analyzer that's exactly the same as #2 but with
        #    English stopwords left in place instead of filtered out --
        #    used as the 'with_stopwords' view of a text field such as
        #    'title.with_stopwords'.
        #
        # The analyzers are identical except for the end of the filter
        # chain.
        #
        # All three analyzers are based on Opensearch's default English
        # analyzer, defined here:
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-lang-analyzer.html#english-analyzer

        # First, recreate the filters from the default English
        # analyzer. We'll be using these to build our own analyzers.

        # Filter out English stopwords.
        self._filters["english_stop"] = dict(type="stop", stopwords=["_english_"])
        # The default English stemmer, used in the en_default analyzer.
        self._filters["english_stemmer"] = dict(type="stemmer", language="english")
        # A less aggressive English stemmer, used in the en_minimal analyzer.
        self._filters["minimal_english_stemmer"] = dict(
            type="stemmer", language="minimal_english"
        )
        # A filter that removes English posessives such as "'s"
        self._filters["english_posessive_stemmer"] = dict(
            type="stemmer", language="possessive_english"
        )

        # Some potentially useful filters that are currently not used:
        #
        # * keyword_marker -- Exempt certain keywords from stemming
        # * synonym -- Introduce synonyms for words
        #   (but probably better to use synonym_graph during the search
        #    -- it's more flexible).

        # Here's the common analyzer configuration. The comment NEW
        # means this is something we added on top of Opensearch's
        # default configuration for the English analyzer.
        common_text_analyzer = dict(
            type="custom",
            char_filter=["html_strip", "remove_apostrophes"],  # NEW
            tokenizer="standard",
        )
        common_filter = [
            "lowercase",
            "asciifolding",  # NEW
        ]

        # The default_text_analyzer uses Opensearch's standard
        # English stemmer and removes stopwords.
        self._analyzers["en_default_text_analyzer"] = dict(common_text_analyzer)
        self._analyzers["en_default_text_analyzer"]["filter"] = common_filter + [
            "english_stop",
            "english_stemmer",
        ]

        # The minimal_text_analyzer uses a less aggressive English
        # stemmer, and removes stopwords.
        self._analyzers["en_minimal_text_analyzer"] = dict(common_text_analyzer)
        self._analyzers["en_minimal_text_analyzer"]["filter"] = common_filter + [
            "english_stop",
            "minimal_english_stemmer",
        ]

        # The en_with_stopwords_text_analyzer uses the less aggressive
        # stemmer and does not remove stopwords.
        self._analyzers["en_with_stopwords_text_analyzer"] = dict(common_text_analyzer)
        self._analyzers["en_with_stopwords_text_analyzer"]["filter"] = common_filter + [
            "minimal_english_stemmer"
        ]

        # Now we need to define a special analyzer used only by the
        # 'sort_author' property.

        # Here's a special filter used only by that analyzer. It
        # duplicates the filter used by the icu_collation_keyword data
        # type.
        self._filters["en_sortable_filter"] = dict(
            type="icu_collation", language="en", country="US"
        )

        # Here's the analyzer used by the 'sort_author' property.
        # It's the same as icu_collation_keyword, but it has some
        # extra character filters -- regexes that do things like
        # convert "Tolkien, J. R. R." to "Tolkien, JRR".
        #
        # This is necessary because normal icu_collation_keyword
        # fields can't specify char_filter.
        self._analyzers["en_sort_author_analyzer"] = dict(
            tokenizer="keyword",
            filter=["en_sortable_filter"],
            char_filter=self.AUTHOR_CHAR_FILTER_NAMES,
        )

        self._fields: dict[str, SearchMappingFieldType] = {
            "summary": BASIC_TEXT,
            "title": FILTERABLE_TEXT,
            "subtitle": FILTERABLE_TEXT,
            "series": FILTERABLE_TEXT,
            "classifications.term": FILTERABLE_TEXT,
            "author": FILTERABLE_TEXT,
            "publisher": FILTERABLE_TEXT,
            "imprint": FILTERABLE_TEXT,
            "presentation_ready": BOOLEAN,
            "sort_title": icu_collation_keyword(),
            "sort_author": sort_author_keyword(),
            "series_position": INTEGER,
            "work_id": INTEGER,
            "last_update_time": LONG,
            "published": LONG,
            "audience": keyword(),
            "language": keyword(),
        }

        contributors = nested()
        contributors.add_property("display_name", FILTERABLE_TEXT)
        contributors.add_property("sort_name", FILTERABLE_TEXT)
        contributors.add_property("family_name", FILTERABLE_TEXT)
        contributors.add_property("role", keyword())
        contributors.add_property("lc", keyword())
        contributors.add_property("viaf", keyword())
        self._fields["contributors"] = contributors

        licensepools = nested()
        licensepools.add_property("collection_id", INTEGER)
        licensepools.add_property("data_source_id", INTEGER)
        licensepools.add_property("availability_time", LONG)
        licensepools.add_property("available", BOOLEAN)
        licensepools.add_property("open_access", BOOLEAN)
        licensepools.add_property("suppressed", BOOLEAN)
        licensepools.add_property("licensed", BOOLEAN)
        licensepools.add_property("medium", keyword())
        self._fields["licensepools"] = licensepools

        identifiers = nested()
        identifiers.add_property("type", keyword())
        identifiers.add_property("identifier", keyword())
        self._fields["identifiers"] = identifiers

        genres = nested()
        genres.add_property("scheme", keyword())
        genres.add_property("name", keyword())
        genres.add_property("term", keyword())
        genres.add_property("weight", FLOAT)
        self._fields["genres"] = genres

        customlists = nested()
        customlists.add_property("list_id", INTEGER)
        customlists.add_property("first_appearance", LONG)
        customlists.add_property("featured", BOOLEAN)
        self._fields["customlists"] = customlists

    def mapping_document(self) -> SearchMappingDocument:
        document = SearchMappingDocument()
        document.settings["analysis"] = dict(
            filter=dict(self._filters),
            char_filter=dict(self._char_filters),
            normalizer=dict(self._normalizers),
            analyzer=dict(self._analyzers),
        )
        document.properties = self._fields
        document.scripts[
            self.script_name("work_last_update")
        ] = SearchV5.WORK_LAST_UPDATE_SCRIPT
        return document
