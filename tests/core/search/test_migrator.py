from unittest.mock import MagicMock, Mock

import pytest

from core.search.document import SearchMappingDocument
from core.search.migrator import SearchMigrationException, SearchMigrator
from core.search.revision import SearchSchemaRevision
from core.search.revision_directory import SearchRevisionDirectory
from core.search.service import SearchWritePointer


class EmptyRevision(SearchSchemaRevision):
    def __init__(self, version: int):
        super().__init__(version)

    def mapping_document(self) -> SearchMappingDocument:
        return SearchMappingDocument()


class TestMigrator:
    def test_no_revisions(self):
        """If a revision isn't available, the migration fails fast."""
        service = Mock()
        revisions = SearchRevisionDirectory.empty()
        migrator = SearchMigrator(revisions, service)
        with pytest.raises(SearchMigrationException):
            migrator.migrate("any", 23, [])

    def test_migrate_empty(self):
        """With an empty search state, migrating to a supported version works."""
        service = Mock()
        service.read_pointer = MagicMock(return_value=None)
        service.write_pointer = MagicMock(return_value=None)
        service.index_is_populated = MagicMock(return_value=False)

        revision = EmptyRevision(3)
        revisions = SearchRevisionDirectory({revision.version: revision})
        migrator = SearchMigrator(revisions, service)
        migrator.migrate("works", revision.version, [])

        # The sequence of expected calls.
        service.create_empty_index.assert_called_with("works")
        service.read_pointer.assert_called_with("works")
        # The read pointer didn't exist, so it's set to the empty index
        service.read_pointer_set_empty.assert_called_with("works")
        service.write_pointer.assert_called_with("works")
        # The new index is created and populated.
        service.create_index.assert_called_with("works", revision)
        service.populate_index.assert_called_with("works", revision, [])
        # Both the read and write pointers are set.
        service.write_pointer_set.assert_called_with("works", revision)
        service.read_pointer_set.assert_called_with("works", revision)

    def test_migrate_from_indexed_2_to_3_nonexistent(self):
        """Index 2 exists, and we can migrate to 3."""
        service = Mock()
        service.read_pointer = MagicMock(return_value="works-v2")
        service.write_pointer = MagicMock(return_value=None)
        service.index_is_populated = MagicMock(return_value=False)

        revision = EmptyRevision(3)
        revisions = SearchRevisionDirectory({revision.version: revision})
        migrator = SearchMigrator(revisions, service)
        migrator.migrate("works", revision.version, [])

        # The sequence of expected calls.
        service.create_empty_index.assert_called_with("works")
        # The read pointer existed, so it's left alone for now.
        service.read_pointer.assert_called_with("works")
        service.write_pointer.assert_called_with("works")
        # The index for version 3 is created and populated.
        service.create_index.assert_called_with("works", revision)
        service.populate_index.assert_called_with("works", revision, [])
        # Both the read and write pointers are set.
        service.write_pointer_set.assert_called_with("works", revision)
        service.read_pointer_set.assert_called_with("works", revision)

    def test_migrate_from_indexed_3_to_3(self):
        """Index 3 already exists, so migrating is a no-op."""
        service = Mock()
        service.read_pointer = MagicMock(return_value="works-v3")
        service.write_pointer = MagicMock(return_value=SearchWritePointer("works", 3))
        service.index_is_populated = MagicMock(return_value=True)

        revision = EmptyRevision(3)
        revisions = SearchRevisionDirectory({revision.version: revision})
        migrator = SearchMigrator(revisions, service)
        migrator.migrate("works", revision.version, [])

        # The sequence of expected calls.
        service.create_empty_index.assert_called_with("works")
        # The read pointer existed, so it's left alone for now.
        service.read_pointer.assert_called_with("works")
        service.write_pointer.assert_called_with("works")
        # The index for version 3 already exists and is populated, so nothing happens.
        service.create_index.assert_not_called()
        service.populate_index.assert_not_called()
        service.write_pointer_set.assert_not_called()
        # The read pointer is set redundantly.
        service.read_pointer_set.assert_called_with("works", revision)

    def test_migrate_from_indexed_2_to_3_unpopulated(self):
        """Index 3 exists but is not populated. Migrating involves populating it."""
        service = Mock()
        service.read_pointer = MagicMock(return_value="works-v2")
        service.write_pointer = MagicMock(return_value=SearchWritePointer("works", 2))
        service.index_is_populated = MagicMock(return_value=False)

        revision = EmptyRevision(3)
        revisions = SearchRevisionDirectory({revision.version: revision})
        migrator = SearchMigrator(revisions, service)
        migrator.migrate("works", revision.version, [])

        # The sequence of expected calls.
        service.create_empty_index.assert_called_with("works")
        # The read pointer existed, so it's left alone for now.
        service.read_pointer.assert_called_with("works")
        service.write_pointer.assert_called_with("works")
        # The index for version 3 exists but isn't populated, so it is populated.
        service.create_index.assert_called_with("works", revision)
        service.populate_index.assert_called_with("works", revision, [])
        # Both the read and write pointers are updated.
        service.write_pointer_set.assert_called_with("works", revision)
        service.read_pointer_set.assert_called_with("works", revision)

    def test_migrate_from_indexed_2_to_3_write_unset(self):
        """Index 3 exists and is populated, but the write pointer is unset."""
        service = Mock()
        service.read_pointer = MagicMock(return_value="works-v2")
        service.write_pointer = MagicMock(return_value=None)
        service.index_is_populated = MagicMock(return_value=True)

        revision = EmptyRevision(3)
        revisions = SearchRevisionDirectory({revision.version: revision})
        migrator = SearchMigrator(revisions, service)
        migrator.migrate("works", revision.version, [])

        # The sequence of expected calls.
        service.create_empty_index.assert_called_with("works")
        # The read pointer existed, so it's left alone for now.
        service.read_pointer.assert_called_with("works")
        # The write pointer is completely unset.
        service.write_pointer.assert_called_with("works")
        # The index for version 3 exists and is populated. The create call is redundant but harmless.
        service.create_index.assert_called_with("works", revision)
        service.populate_index.assert_not_called()
        # Both the read and write pointers are updated.
        service.write_pointer_set.assert_called_with("works", revision)
        service.read_pointer_set.assert_called_with("works", revision)
