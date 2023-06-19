from typing import Iterable

from opensearchpy import OpenSearch

from core.search.document import LONG, SearchMappingDocument
from core.search.revision import SearchSchemaRevision
from core.search.service import SearchServiceOpensearch1
from tests.fixtures.search import ExternalSearchFixture


class BasicMutableRevision(SearchSchemaRevision):
    def __init__(self, version: int):
        super().__init__(version)
        self.document = SearchMappingDocument()

    def mapping_document(self) -> SearchMappingDocument:
        return self.document


class TestService:
    """
    Tests to verify that the Opensearch service implementation has the semantics we expect.
    """

    @staticmethod
    def get_client(external_search_fixture: ExternalSearchFixture) -> OpenSearch:
        indices = external_search_fixture.search.indices  # type: ignore
        assert indices is not None
        return indices.client

    def test_create_empty_idempotent(
        self, external_search_fixture: ExternalSearchFixture
    ):
        """Creating the empty index is idempotent."""
        service = SearchServiceOpensearch1(
            client=TestService.get_client(external_search_fixture)
        )
        service.create_empty_index("base")

        # Log the index so that the fixture cleans it up afterward.
        external_search_fixture.indexes.append("base-empty")

        service.create_empty_index("base")

        indices = external_search_fixture.search.indices.client.indices  # type: ignore
        assert indices is not None
        assert indices.exists("base-empty")

    def test_create_index_idempotent(
        self, external_search_fixture: ExternalSearchFixture
    ):
        """Creating any index is idempotent."""
        service = SearchServiceOpensearch1(
            client=TestService.get_client(external_search_fixture)
        )
        revision = BasicMutableRevision(23)
        service.create_index("base", revision)
        service.create_index("base", revision)

        # Log the index so that the fixture cleans it up afterward.
        external_search_fixture.indexes.append("base-v23")

        indices = external_search_fixture.search.indices.client.indices  # type: ignore
        assert indices is not None
        assert indices.exists(revision.name_for_index("base"))

    def test_read_pointer_none(self, external_search_fixture: ExternalSearchFixture):
        """The read pointer is initially unset."""
        service = SearchServiceOpensearch1(
            client=TestService.get_client(external_search_fixture)
        )
        assert None == service.read_pointer("nonexistent")

    def test_write_pointer_none(self, external_search_fixture: ExternalSearchFixture):
        """The write pointer is initially unset."""
        service = SearchServiceOpensearch1(
            client=TestService.get_client(external_search_fixture)
        )
        assert None == service.write_pointer("nonexistent")

    def test_read_pointer_set(self, external_search_fixture: ExternalSearchFixture):
        """Setting the read pointer works."""
        service = SearchServiceOpensearch1(
            client=TestService.get_client(external_search_fixture)
        )
        revision = BasicMutableRevision(23)
        service.create_index("base", revision)

        # Log the index so that the fixture cleans it up afterward.
        external_search_fixture.indexes.append("base-v23")

        service.read_pointer_set("base", revision)
        assert "base-v23" == service.read_pointer("base")

    def test_read_pointer_set_empty(
        self, external_search_fixture: ExternalSearchFixture
    ):
        """Setting the read pointer to the empty index works."""
        service = SearchServiceOpensearch1(
            client=TestService.get_client(external_search_fixture)
        )
        service.create_empty_index("base")

        # Log the index so that the fixture cleans it up afterward.
        external_search_fixture.indexes.append("base-empty")

        service.read_pointer_set_empty("base")
        assert "base-empty" == service.read_pointer("base")

    def test_write_pointer_set(self, external_search_fixture: ExternalSearchFixture):
        """Setting the write pointer works."""
        service = SearchServiceOpensearch1(
            client=TestService.get_client(external_search_fixture)
        )
        revision = BasicMutableRevision(23)
        service.create_index("base", revision)

        # Log the index so that the fixture cleans it up afterward.
        external_search_fixture.indexes.append("base-v23")

        service.write_pointer_set("base", revision)

        pointer = service.write_pointer("base")
        assert pointer is not None
        assert "base-v23" == pointer.target_name

    def test_populate_index_idempotent(
        self, external_search_fixture: ExternalSearchFixture
    ):
        """Populating an index is idempotent."""
        service = SearchServiceOpensearch1(
            client=TestService.get_client(external_search_fixture)
        )
        revision = BasicMutableRevision(23)

        mappings = revision.mapping_document()
        mappings.properties["x"] = LONG
        mappings.properties["y"] = LONG

        # The format expected by the opensearch bulk helper is completely undocumented.
        # It does, however, appear to use mostly the same format as the Elasticsearch equivalent.
        # See: https://elasticsearch-py.readthedocs.io/en/v7.13.1/helpers.html#bulk-helpers
        documents: Iterable[dict] = [
            {
                "_index": revision.name_for_index("base"),
                "_type": "_doc",
                "_id": 1,
                "_source": {"x": 23, "y": 24},
            },
            {
                "_index": revision.name_for_index("base"),
                "_type": "_doc",
                "_id": 2,
                "_source": {"x": 25, "y": 26},
            },
            {
                "_index": revision.name_for_index("base"),
                "_type": "_doc",
                "_id": 3,
                "_source": {"x": 27, "y": 28},
            },
        ]

        service.create_index("base", revision)

        # Log the index so that the fixture cleans it up afterward.
        external_search_fixture.indexes.append("base-v23")

        service.populate_index("base", revision, lambda: documents)
        service.populate_index("base", revision, lambda: documents)

        indices = external_search_fixture.search.indices.client.indices  # type: ignore
        assert indices is not None
        assert indices.exists(revision.name_for_index("base"))
        assert indices.get(revision.name_for_index("base"))["base-v23"]["mappings"] == {
            "properties": mappings.serialize_properties()
        }
