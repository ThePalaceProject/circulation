"""Explicitly test the different states of migration, and ensure we are adhering to the principles set out.
These tests do have some overlap with the unit tests for the search migration, but these are specific to the migration use cases.
Initial Case
- No pointers or indices are available
- The System comes online for the first time and some prep work must be done
- The initial versioned indices and pointers should be prepped by the init_instance script
- The ExternalSearchIndex should not be hindered by this
Migration Case
- Pointers exist, indices exist
- The migration contains a new version for the index
- The search_index_refresh script, when run, should create and populate the indices, and move the red/write pointers
- The ExternalSearchIndex should not be hindered by this, and should continue to work with the pointers, regardless of where they point
"""

import pytest

from core.external_search import ExternalSearchIndex
from core.scripts import RunWorkCoverageProviderScript
from core.search.coverage_provider import SearchIndexCoverageProvider
from core.search.document import SearchMappingDocument
from core.search.revision import SearchSchemaRevision
from core.search.revision_directory import SearchRevisionDirectory
from scripts import InstanceInitializationScript
from tests.fixtures.search import ExternalSearchFixture


class TestMigrationStates:
    def test_initial_migration_case(
        self, external_search_fixture: ExternalSearchFixture
    ):
        fx = external_search_fixture
        db = fx.db
        index = fx.index
        service = fx.service

        # Ensure we are in the initial state, no test indices and pointer available
        prefix = fx.search_config.index_prefix
        all_indices = fx.client.indices.get("*")
        for index_name in all_indices.keys():
            if prefix in index_name:
                fx.client.indices.delete(index_name)

        # We cannot make any requests before we intitialize
        with pytest.raises(Exception) as raised:
            index.query_works("")
        assert "index_not_found" in str(raised.value)

        # When a new sytem comes up the first code to run is the InstanceInitailization script
        # This preps the DB and the search indices/pointers
        InstanceInitializationScript().initialize(db.session.connection())

        # Ensure we have created the index and pointers
        new_index_name = index._revision.name_for_index(prefix)
        empty_index_name = service._empty(prefix)
        all_indices = fx.client.indices.get("*")

        assert prefix in new_index_name
        assert new_index_name in all_indices.keys()
        assert empty_index_name in all_indices.keys()
        assert fx.client.indices.exists_alias(
            index._search_read_pointer, index=new_index_name
        )
        assert fx.client.indices.exists_alias(
            index._search_write_pointer, index=new_index_name
        )

        # The same client should work without issue once the pointers are setup
        assert index.query_works("").hits == []

    def test_migration_case(self, external_search_fixture: ExternalSearchFixture):
        fx = external_search_fixture
        db = fx.db

        # The initial indices setup
        InstanceInitializationScript().initialize(db.session.connection())

        MOCK_VERSION = 1000001

        class MockSchema(SearchSchemaRevision):
            def __init__(self, v: int):
                self.SEARCH_VERSION = v
                super().__init__()

            def mapping_document(self) -> SearchMappingDocument:
                return SearchMappingDocument()

        client = ExternalSearchIndex(
            fx.search_container.service(),
            revision_directory=SearchRevisionDirectory(
                {MOCK_VERSION: MockSchema(MOCK_VERSION)}
            ),
        )

        # The search client works just fine
        assert client.query_works("") is not None
        receiver = client.start_updating_search_documents()
        receiver.add_documents([{"work_id": 123}])
        receiver.finish()

        mock_index_name = client._revision.name_for_index(fx.service.base_revision_name)
        assert str(MOCK_VERSION) in mock_index_name

        # The mock index does not exist yet
        with pytest.raises(Exception) as raised:
            fx.client.indices.get(mock_index_name)
        assert "index_not_found" in str(raised.value)

        # This should run the migration
        RunWorkCoverageProviderScript(
            SearchIndexCoverageProvider, db.session, search_index_client=client
        ).run()

        # The new version is created, and the aliases point to the right index
        assert fx.client.indices.get(mock_index_name) is not None
        assert mock_index_name in fx.client.indices.get_alias(
            name=client._search_read_pointer
        )
        assert mock_index_name in fx.client.indices.get_alias(
            name=client._search_write_pointer
        )
