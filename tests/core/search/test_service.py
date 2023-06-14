from core.search.document import SearchMappingDocument
from core.search.revision import SearchSchemaRevision
from core.search.service import SearchServiceOpensearch1
from tests.fixtures.search import ExternalSearchFixture


class EmptyRevision(SearchSchemaRevision):
    def __init__(self, version: int):
        super().__init__(version)

    def mapping_document(self) -> SearchMappingDocument:
        return SearchMappingDocument()


class TestService:
    """
    Tests to verify that the Opensearch service implementation has the semantics we expect.
    Note: The 'type: ignore' lines are because the real code takes an Opensearch client, and
          the test fixture provides something that is compatible but that mypy doesn't recognize.
    """

    def test_create_empty_idempotent(self, external_search_fixture: ExternalSearchFixture):
        """Creating the empty index is idempotent."""
        service = SearchServiceOpensearch1(client=external_search_fixture.search) # type: ignore
        service.create_empty_index('base')
        service.create_empty_index('base')

        indices = external_search_fixture.search.indices # type: ignore
        assert indices is not None
        assert indices.exists('base-empty')

    def test_create_index_idempotent(self, external_search_fixture: ExternalSearchFixture):
        """Creating any index is idempotent."""
        service = SearchServiceOpensearch1(client=external_search_fixture.search) # type: ignore
        revision = EmptyRevision(23)
        service.create_index('base', revision)
        service.create_index('base', revision)

        indices = external_search_fixture.search.indices # type: ignore
        assert indices is not None
        assert indices.exists(revision.name_for_index('base'))

    def test_read_pointer_none(self, external_search_fixture: ExternalSearchFixture):
        """The read pointer is initially unset."""
        service = SearchServiceOpensearch1(client=external_search_fixture.search) # type: ignore
        assert None == service.read_pointer('nonexistent')

    def test_write_pointer_none(self, external_search_fixture: ExternalSearchFixture):
        """The write pointer is initially unset."""
        service = SearchServiceOpensearch1(client=external_search_fixture.search) # type: ignore
        assert None == service.write_pointer('nonexistent')

    def test_read_pointer_set(self, external_search_fixture: ExternalSearchFixture):
        """Setting the read pointer works."""
        service = SearchServiceOpensearch1(client=external_search_fixture.search) # type: ignore
        revision = EmptyRevision(23)
        service.create_index('base', revision)
        service.read_pointer_set('base', revision)
        assert 'base-v23' == service.read_pointer('base')

    def test_read_pointer_set_empty(self, external_search_fixture: ExternalSearchFixture):
        """Setting the read pointer to the empty index works."""
        service = SearchServiceOpensearch1(client=external_search_fixture.search) # type: ignore
        service.create_empty_index('base')
        service.read_pointer_set_empty('base')
        assert 'base-empty' == service.read_pointer('base')

    def test_write_pointer_set(self, external_search_fixture: ExternalSearchFixture):
        """Setting the write pointer works."""
        service = SearchServiceOpensearch1(client=external_search_fixture.search) # type: ignore
        revision = EmptyRevision(23)
        service.create_index('base', revision)
        service.write_pointer_set('base', revision)

        pointer = service.write_pointer('base')
        assert pointer is not None
        assert 'base-v23' == pointer.target_name
