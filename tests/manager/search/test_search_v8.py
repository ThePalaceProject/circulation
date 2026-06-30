from palace.manager.search.revision import SearchSchemaRevision
from palace.manager.search.v7 import SearchV7
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

    def test_mapping_matches_v7(self):
        """v8 is a faithful, self-contained copy of the v7 schema: same fields
        and same analysis settings. Only the index settings differ (v8 pins the
        shard count)."""
        v8_document = SearchV8().mapping_document()
        v7_document = SearchV7().mapping_document()

        assert v8_document.serialize_properties() == v7_document.serialize_properties()
        assert v8_document.settings["analysis"] == v7_document.settings["analysis"]
        # v7 left the shard count to an inherited default; v8 sets it explicitly.
        assert "index" not in v7_document.settings
        assert "index" in v8_document.settings
