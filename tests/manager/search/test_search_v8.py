from palace.manager.search.document import INTEGER, LONG
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

    def test_includes_fields_from_prior_revisions(self):
        v8 = SearchV8()
        # Added in v6.
        assert v8._fields["lane_priority_level"] == INTEGER
        # Added in v7.
        assert v8._fields["licensepools"].properties["last_updated"] == LONG

    def test_pins_number_of_shards(self):
        """The primary shard count is pinned explicitly (it is immutable after
        index creation and must not be left to an inherited default)."""
        document = SearchV8().mapping_document()
        assert document.settings["index"]["number_of_shards"] == 1

    def test_pins_number_of_replicas(self):
        """The replica count is pinned explicitly so indexes are created with a
        deterministic count rather than an inherited cluster default."""
        document = SearchV8().mapping_document()
        assert document.settings["index"]["number_of_replicas"] == 1

    def test_sets_search_slowlog_thresholds(self):
        """Slow-query-log thresholds are seeded so slow queries surface in the
        cluster slow log (and onward to CloudWatch) on every index v8 builds."""
        index = SearchV8().mapping_document().settings["index"]
        assert index["search.slowlog.threshold.query.warn"] == "2s"
        assert index["search.slowlog.threshold.query.info"] == "1s"
        assert index["search.slowlog.threshold.query.debug"] == "500ms"
        assert index["search.slowlog.threshold.fetch.warn"] == "1s"
        assert index["search.slowlog.threshold.fetch.info"] == "500ms"

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
