from core.search.document import (
    BASIC_TEXT,
    BINARY,
    BOOLEAN,
    BYTE,
    CUSTOM_KEYWORD,
    DOUBLE,
    FILTERABLE_TEXT,
    FLOAT,
    INTEGER,
    IP,
    LONG,
    SHORT,
    UNSIGNED_LONG,
    SearchMappingDocument,
    date,
    icu_collation_keyword,
    keyword,
    nested,
    sort_author_keyword,
)


class TestDocuments:
    def test_binary(self):
        assert {"type": "binary"} == BINARY.serialize()

    def test_boolean(self):
        assert {"type": "boolean"} == BOOLEAN.serialize()

    def test_byte(self):
        assert {"type": "byte"} == BYTE.serialize()

    def test_double(self):
        assert {"type": "double"} == DOUBLE.serialize()

    def test_float(self):
        assert {"type": "float"} == FLOAT.serialize()

    def test_ip(self):
        assert {"type": "ip"} == IP.serialize()

    def test_integer(self):
        assert {"type": "integer"} == INTEGER.serialize()

    def test_long(self):
        assert {"type": "long"} == LONG.serialize()

    def test_unsigned_long(self):
        assert {"type": "unsigned_long"} == UNSIGNED_LONG.serialize()

    def test_short(self):
        assert {"type": "short"} == SHORT.serialize()

    def test_date(self):
        t = date()
        t.parameters["x"] = "a"
        t.parameters["y"] = "b"
        t.parameters["z"] = "c"
        assert {"type": "date", "x": "a", "y": "b", "z": "c"} == t.serialize()

    def test_keyword(self):
        t = keyword()
        t.parameters["x"] = "a"
        t.parameters["y"] = "b"
        t.parameters["z"] = "c"
        assert {
            "type": "keyword",
            "normalizer": "filterable_string",
            "x": "a",
            "y": "b",
            "z": "c",
        } == t.serialize()

    def test_icu_collation_keyword(self):
        t = icu_collation_keyword()
        t.parameters["x"] = "a"
        t.parameters["y"] = "b"
        t.parameters["z"] = "c"
        assert {
            "type": "icu_collation_keyword",
            "x": "a",
            "y": "b",
            "z": "c",
        } == t.serialize()

    def test_sort_author_keyword(self):
        t = sort_author_keyword()
        t.parameters["x"] = "a"
        t.parameters["y"] = "b"
        t.parameters["z"] = "c"
        assert {
            "type": "text",
            "analyzer": "en_sort_author_analyzer",
            "fielddata": "true",
            "x": "a",
            "y": "b",
            "z": "c",
        } == t.serialize()

    def test_nested(self):
        u = nested()
        u.add_property("a", INTEGER)

        t = nested()
        t.add_property("x", INTEGER)
        t.add_property("y", LONG)
        t.add_property("z", u)

        assert {
            "type": "nested",
            "properties": {
                "x": {"type": "integer"},
                "y": {"type": "long"},
                "z": {"type": "nested", "properties": {"a": {"type": "integer"}}},
            },
        } == t.serialize()

    def test_basic_text(self):
        assert {
            "type": "text",
            "analyzer": "en_default_text_analyzer",
            "fields": {
                "minimal": {"type": "text", "analyzer": "en_minimal_text_analyzer"},
                "with_stopwords": {
                    "type": "text",
                    "analyzer": "en_with_stopwords_text_analyzer",
                },
            },
        } == BASIC_TEXT.serialize()

    def test_filterable(self):
        assert {
            "type": "text",
            "analyzer": "en_default_text_analyzer",
            "fields": {
                "minimal": {"type": "text", "analyzer": "en_minimal_text_analyzer"},
                "keyword": {
                    "type": "keyword",
                    "normalizer": "filterable_string",
                    "store": False,
                    "index": True,
                },
                "with_stopwords": {
                    "type": "text",
                    "analyzer": "en_with_stopwords_text_analyzer",
                },
            },
        } == FILTERABLE_TEXT.serialize()

    def test_custom_keyword(self):
        assert {
            "type": "keyword",
            "normalizer": "filterable_string",
        } == CUSTOM_KEYWORD.serialize()

    def test_document(self):
        doc = SearchMappingDocument()
        doc.properties["a"] = INTEGER
        doc.properties["b"] = LONG
        doc.settings["c"] = {"z": "x"}

        assert {
            "settings": {"c": {"z": "x"}},
            "mappings": {
                "properties": {"a": {"type": "integer"}, "b": {"type": "long"}}
            },
        } == doc.serialize()
