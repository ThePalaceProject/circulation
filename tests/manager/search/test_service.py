from palace.manager.search.document import LONG, SearchMappingDocument
from palace.manager.search.revision import SearchSchemaRevision
from palace.manager.search.service import SearchDocument
from tests.fixtures.search import ExternalSearchFixture


class BasicMutableRevision(SearchSchemaRevision):
    SEARCH_VERSION = 0

    def __init__(self, version: int):
        self.SEARCH_VERSION = version
        super().__init__()
        self.document = SearchMappingDocument()

    def mapping_document(self) -> SearchMappingDocument:
        return self.document


class TestService:
    """
    Tests to verify that the Opensearch service implementation has the semantics we expect.
    """

    def test_create_empty_idempotent(
        self, external_search_fixture: ExternalSearchFixture
    ):
        """Creating the empty index is idempotent."""
        service = external_search_fixture.service
        service.create_empty_index()
        service.create_empty_index()

        indices = external_search_fixture.client.indices.client.indices
        assert indices is not None
        assert indices.exists(f"{external_search_fixture.index_prefix}-empty")

    def test_create_index_idempotent(
        self, external_search_fixture: ExternalSearchFixture
    ):
        """Creating any index is idempotent."""
        service = external_search_fixture.service
        revision = BasicMutableRevision(23)
        service.index_create(revision)
        service.index_create(revision)

        indices = external_search_fixture.client.indices.client.indices
        assert indices is not None
        assert indices.exists(
            revision.name_for_index(external_search_fixture.index_prefix)
        )

    def test_read_pointer_none(self, external_search_fixture: ExternalSearchFixture):
        """The read pointer is initially unset."""
        service = external_search_fixture.service
        assert service.read_pointer() is None

    def test_write_pointer_none(self, external_search_fixture: ExternalSearchFixture):
        """The write pointer is initially unset."""
        service = external_search_fixture.service
        assert service.write_pointer() is None

    def test_read_pointer_set(self, external_search_fixture: ExternalSearchFixture):
        """Setting the read pointer works."""
        service = external_search_fixture.service
        revision = BasicMutableRevision(23)
        service.index_create(revision)

        service.read_pointer_set(revision)
        assert service.read_pointer() == f"{external_search_fixture.index_prefix}-v23"

    def test_read_pointer_set_empty(
        self, external_search_fixture: ExternalSearchFixture
    ):
        """Setting the read pointer to the empty index works."""
        service = external_search_fixture.service
        service.create_empty_index()

        service.read_pointer_set_empty()
        assert service.read_pointer() == f"{external_search_fixture.index_prefix}-empty"

    def test_write_pointer_set(self, external_search_fixture: ExternalSearchFixture):
        """Setting the write pointer works."""
        service = external_search_fixture.service
        revision = BasicMutableRevision(23)
        service.index_create(revision)

        service.write_pointer_set(revision)

        pointer = service.write_pointer()
        assert pointer is not None
        assert pointer.target_name == f"{external_search_fixture.index_prefix}-v23"

    def test_populate_index_idempotent(
        self, external_search_fixture: ExternalSearchFixture
    ):
        """Populating an index is idempotent."""
        service = external_search_fixture.service
        revision = BasicMutableRevision(23)

        mappings = revision.mapping_document()
        mappings.properties["x"] = LONG
        mappings.properties["y"] = LONG

        # The format expected by the opensearch bulk helper is completely undocumented.
        # It does, however, appear to use mostly the same format as the Elasticsearch equivalent.
        # See: https://elasticsearch-py.readthedocs.io/en/v7.13.1/helpers.html#bulk-helpers
        documents: list[SearchDocument] = [
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

        service.index_create(revision)

        service.index_submit_documents(
            f"{external_search_fixture.index_prefix}-v23", documents
        )
        service.index_submit_documents(
            f"{external_search_fixture.index_prefix}-v23", documents
        )

        indices = external_search_fixture.client.indices.client.indices
        assert indices is not None
        assert indices.exists(
            revision.name_for_index(external_search_fixture.index_prefix)
        )
        assert indices.get(
            revision.name_for_index(external_search_fixture.index_prefix)
        )[f"{external_search_fixture.index_prefix}-v23"]["mappings"] == {
            "properties": mappings.serialize_properties()
        }
