import re

from palace.manager.search.revision import SearchSchemaRevision
from palace.manager.search.v8 import SearchV8


class TestSearchV8:
    def test_version(self):
        assert SearchV8().version == 8

    def test_self_contained(self):
        """v8 must not chain off any previous revision, so old revisions can be
        deleted once nothing in production uses them."""
        assert SearchV8.__bases__ == (SearchSchemaRevision,)

    def test_pins_index_settings(self):
        """v8 sets the expected index settings."""
        index = SearchV8().mapping_document().settings["index"]
        for setting in [
            "number_of_shards",
            "number_of_replicas",
            "search.slowlog.threshold.query.warn",
            "search.slowlog.threshold.query.info",
            "search.slowlog.threshold.fetch.warn",
            "search.slowlog.threshold.fetch.info",
        ]:
            assert setting in index

    def test_character_filters(self):
        # Verify the functionality of the regular expressions we tell
        # Opensearch to use when normalizing fields that will be used
        # for searching.
        filters = []
        for filter_name in SearchV8.AUTHOR_CHAR_FILTER_NAMES:
            configuration = SearchV8.CHAR_FILTERS[filter_name]
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
