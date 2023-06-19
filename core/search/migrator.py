from typing import Callable, Iterable

from core.search.revision_directory import SearchRevisionDirectory
from core.search.service import SearchService


class SearchMigrationException(Exception):
    """The type of exceptions raised by the search migrator."""

    def __init__(self, message: str):
        super().__init__(message)


class SearchMigrator:
    """A search migrator. This moves a search service to the targeted schema version."""

    def __init__(self, revisions: SearchRevisionDirectory, service: SearchService):
        self._revisions = revisions
        self._service = service

    def migrate(
        self, base_name: str, version: int, documents: Callable[[], Iterable[dict]]
    ):
        """
        Migrate to the given version using the given base name (such as 'circulation-works').

        :arg base_name: The base name used for indices (such as 'circulation-works').
        :arg version: The version number to which we are migrating
        :arg documents: A function that returns documents to be indexed. This is used to populate an index when
             upgrading to a new version. The function will only be called if an index actually needs to be
             populated with documents.
        """

        target = self._revisions.available.get(version)
        if target is None:
            raise SearchMigrationException(
                f"No support is available for schema version {version}"
            )

        # Does the empty index exist? Create it if not.
        self._service.create_empty_index(base_name)

        # Does the read pointer exist? Point it at the empty index if not.
        read = self._service.read_pointer(base_name)
        if read is None:
            self._service.read_pointer_set_empty(base_name)

        # Does the write pointer exist?
        write = self._service.write_pointer(base_name)
        if write is None or (not write.version == version):
            # Either the write pointer didn't exist, or it's pointing at a version
            # other than the one we want. Create a new index for the version we want.
            self._service.create_index(base_name, target)

            # The index now definitely exists, but it might not be populated. Populate it if necessary.
            if not self._service.index_is_populated(base_name, target):
                self._service.populate_index(base_name, target, documents)

            # Update the write pointer to point to the now-populated index.
            self._service.write_pointer_set(base_name, target)

        # Set the read pointer to point at the now-populated index
        self._service.read_pointer_set(base_name, target)
