from unittest.mock import MagicMock, Mock, call

import pytest

from core.search.document import SearchMappingDocument
from core.search.migrator import SearchMigrationException, SearchMigrator
from core.search.revision import SearchSchemaRevision
from core.search.revision_directory import SearchRevisionDirectory
from core.search.service import SearchWritePointer


class EmptyRevision(SearchSchemaRevision):
    SEARCH_VERSION = 0

    def __init__(self, version: int):
        self.SEARCH_VERSION = version
        super().__init__()

    def mapping_document(self) -> SearchMappingDocument:
        return SearchMappingDocument()


class TestMigrator:
    def test_migrate_no_revisions(self):
        """If a revision isn't available, the migration fails fast."""
        service = Mock()
        revisions = SearchRevisionDirectory.empty()
        migrator = SearchMigrator(revisions, service)
        with pytest.raises(SearchMigrationException):
            migrator.migrate(base_name="any", version=23)

    def test_migrate_from_empty(self):
        """With an empty search state, migrating to a supported version works."""
        service = Mock()
        service.read_pointer = MagicMock(return_value=None)
        service.write_pointer = MagicMock(return_value=None)
        service.index_is_populated = MagicMock(return_value=False)
        service.index_set_populated = MagicMock()

        revision = EmptyRevision(3)
        revisions = SearchRevisionDirectory({revision.version: revision})
        migrator = SearchMigrator(revisions, service)

        migration = migrator.migrate(base_name="works", version=revision.version)
        migration.finish()

        # The sequence of expected calls.
        service.create_empty_index.assert_called_with()
        service.read_pointer.assert_called_with()
        # The read pointer didn't exist, so it's set to the empty index
        service.read_pointer_set_empty.assert_called_with()
        service.write_pointer.assert_called_with()
        # The new index is created and populated.
        service.index_create.assert_called_with(revision)
        service.populate_index.assert_not_called()
        # Both the read and write pointers are set.
        service.write_pointer_set.assert_called_with(revision)
        service.read_pointer_set.assert_called_with(revision)
        service.index_set_populated.assert_called_with(revision)

    def test_migrate_upgrade(self):
        """Index 2 exists, and we can migrate to 3."""
        service = Mock()
        service.read_pointer = MagicMock(return_value="works-v2")
        service.write_pointer = MagicMock(return_value=None)
        service.index_is_populated = MagicMock(return_value=False)
        service.index_set_mapping = MagicMock()
        service.index_submit_documents = MagicMock()
        service.index_set_populated = MagicMock()

        revision = EmptyRevision(3)
        revisions = SearchRevisionDirectory({revision.version: revision})
        migrator = SearchMigrator(revisions, service)

        docs = migrator.migrate(base_name="works", version=revision.version)
        docs.add_documents([{"_id": "1"}, {"_id": "2"}, {"_id": "3"}])
        docs.add_documents([{"_id": "4"}, {"_id": "5"}, {"_id": "6"}])
        docs.add_documents([{"_id": "7"}, {"_id": "8"}])
        docs.finish()

        # The sequence of expected calls.
        service.create_empty_index.assert_called_with()
        # The read pointer existed, so it's left alone for now.
        service.read_pointer.assert_called_with()
        service.write_pointer.assert_called_with()
        # The index for version 3 is created and populated.
        service.index_create.assert_called_with(revision)
        service.index_set_mapping.assert_called_with(revision)
        service.index_submit_documents.assert_has_calls(
            [
                call(
                    pointer="works-v3",
                    documents=[{"_id": "1"}, {"_id": "2"}, {"_id": "3"}],
                ),
                call(
                    pointer="works-v3",
                    documents=[{"_id": "4"}, {"_id": "5"}, {"_id": "6"}],
                ),
                call(
                    pointer="works-v3",
                    documents=[{"_id": "7"}, {"_id": "8"}],
                ),
            ]
        )
        # Both the read and write pointers are set.
        service.write_pointer_set.assert_called_with(revision)
        service.read_pointer_set.assert_called_with(revision)
        service.index_set_populated.assert_called_with(revision)

    def test_migrate_upgrade_cancel(self):
        """Cancelling a migration leaves the pointers untouched."""
        service = Mock()
        service.read_pointer = MagicMock(return_value="works-v2")
        service.write_pointer = MagicMock(return_value=None)
        service.index_is_populated = MagicMock(return_value=False)
        service.index_set_mapping = MagicMock()
        service.index_submit_documents = MagicMock()
        service.index_set_populated = MagicMock()

        revision = EmptyRevision(3)
        revisions = SearchRevisionDirectory({revision.version: revision})
        migrator = SearchMigrator(revisions, service)

        docs = migrator.migrate(base_name="works", version=revision.version)
        docs.add_documents([{"_id": "1"}, {"_id": "2"}, {"_id": "3"}])
        docs.add_documents([{"_id": "4"}, {"_id": "5"}, {"_id": "6"}])
        docs.add_documents([{"_id": "7"}, {"_id": "8"}])
        docs.cancel()

        # The sequence of expected calls.
        service.create_empty_index.assert_called_with()
        # The read pointer existed, so it's left alone for now.
        service.read_pointer.assert_called_with()
        service.write_pointer.assert_called_with()
        # The index for version 3 is created and populated.
        service.index_create.assert_called_with(revision)
        service.index_set_mapping.assert_called_with(revision)
        service.index_submit_documents.assert_has_calls(
            [
                call(
                    pointer="works-v3",
                    documents=[{"_id": "1"}, {"_id": "2"}, {"_id": "3"}],
                ),
                call(
                    pointer="works-v3",
                    documents=[{"_id": "4"}, {"_id": "5"}, {"_id": "6"}],
                ),
                call(
                    pointer="works-v3",
                    documents=[{"_id": "7"}, {"_id": "8"}],
                ),
            ]
        )
        # Both the read and write pointers are left untouched.
        service.write_pointer_set.assert_not_called()
        service.read_pointer_set.assert_not_called()
        service.index_set_populated.assert_not_called()

    def test_migrate_no_op(self):
        """Index 3 already exists, so migrating to 3 is a no-op."""
        service = Mock()
        service.read_pointer = MagicMock(return_value="works-v3")
        service.write_pointer = MagicMock(return_value=SearchWritePointer("works", 3))
        service.index_is_populated = MagicMock(return_value=True)
        service.index_set_populated = MagicMock()

        revision = EmptyRevision(3)
        revisions = SearchRevisionDirectory({revision.version: revision})
        migrator = SearchMigrator(revisions, service)
        docs = migrator.migrate("works", revision.version)
        assert docs is None

        # The sequence of expected calls.
        service.create_empty_index.assert_called_with()
        # The read pointer existed, so it's left alone for now.
        service.read_pointer.assert_called_with()
        service.write_pointer.assert_called_with()
        # The index for version 3 already exists and is populated, so nothing happens.
        service.index_create.assert_not_called()
        service.index_set_mapping.assert_not_called()
        # The write pointer is set redundantly.
        service.write_pointer_set.assert_called_with(revision)
        # The read pointer is set redundantly.
        service.read_pointer_set.assert_called_with(revision)
        # The "indexed" flag is set redundantly.
        service.index_set_populated.assert_called_with(revision)

    def test_migrate_from_indexed_2_to_3_unpopulated(self):
        """Index 3 exists but is not populated. Migrating involves populating it."""
        service = Mock()
        service.read_pointer = MagicMock(return_value="works-v2")
        service.write_pointer = MagicMock(return_value=SearchWritePointer("works", 2))
        service.index_is_populated = MagicMock(return_value=False)
        service.index_set_populated = MagicMock()

        revision = EmptyRevision(3)
        revisions = SearchRevisionDirectory({revision.version: revision})
        migrator = SearchMigrator(revisions, service)
        migration = migrator.migrate("works", revision.version)
        migration.add_documents([])
        migration.finish()

        # The sequence of expected calls.
        service.create_empty_index.assert_called_with()
        # The read pointer existed, so it's left alone for now.
        service.read_pointer.assert_called_with()
        service.write_pointer.assert_called_with()
        # The index for version 3 exists but isn't populated, so it is populated.
        service.index_create.assert_called_with(revision)
        service.index_set_mapping.assert_called_with(revision)
        service.index_submit_documents.assert_has_calls(
            [
                call(
                    pointer="works-v3",
                    documents=[],
                )
            ]
        )
        # Both the read and write pointers are updated.
        service.write_pointer_set.assert_called_with(revision)
        service.read_pointer_set.assert_called_with(revision)
        service.index_set_populated.assert_called_with(revision)

    def test_migrate_from_indexed_2_to_3_write_unset(self):
        """Index 3 exists and is populated, but the write pointer is unset."""
        service = Mock()
        service.read_pointer = MagicMock(return_value="works-v2")
        service.write_pointer = MagicMock(return_value=None)
        service.index_is_populated = MagicMock(return_value=True)
        service.index_set_populated = MagicMock()

        revision = EmptyRevision(3)
        revisions = SearchRevisionDirectory({revision.version: revision})
        migrator = SearchMigrator(revisions, service)
        docs = migrator.migrate("works", revision.version)
        assert docs is None

        # The sequence of expected calls.
        service.create_empty_index.assert_called_with()
        # The read pointer existed, so it's left alone for now.
        service.read_pointer.assert_called_with()
        # The write pointer is completely unset.
        service.write_pointer.assert_called_with()
        # The index for version 3 exists and is populated. The create call is redundant but harmless.
        service.index_create.assert_called_with(revision)
        service.populate_index.assert_not_called()
        # Both the read and write pointers are updated.
        service.write_pointer_set.assert_called_with(revision)
        service.read_pointer_set.assert_called_with(revision)
        service.index_set_populated.assert_called_with(revision)
