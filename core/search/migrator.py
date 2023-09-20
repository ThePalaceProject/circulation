import logging
from abc import ABC, abstractmethod
from typing import Iterable, List, Optional

from core.search.revision import SearchSchemaRevision
from core.search.revision_directory import SearchRevisionDirectory
from core.search.service import SearchService, SearchServiceFailedDocument


class SearchMigrationException(Exception):
    """The type of exceptions raised by the search migrator."""

    def __init__(self, fatal: bool, message: str):
        super().__init__(message)
        self.fatal = fatal


class SearchDocumentReceiverType(ABC):
    """A receiver of search documents."""

    @abstractmethod
    def add_documents(
        self, documents: Iterable[dict]
    ) -> List[SearchServiceFailedDocument]:
        """Submit documents to be indexed."""

    @abstractmethod
    def finish(self) -> None:
        """Make sure all changes are committed."""


class SearchDocumentReceiver(SearchDocumentReceiverType):
    """A receiver of search documents."""

    def __init__(self, pointer: str, service: SearchService):
        self._logger = logging.getLogger(SearchDocumentReceiver.__name__)
        self._pointer = pointer
        self._service = service

    @property
    def pointer(self) -> str:
        """The name of the index that will receive search documents."""
        return self._pointer

    def add_documents(
        self, documents: Iterable[dict]
    ) -> List[SearchServiceFailedDocument]:
        """Submit documents to be indexed."""
        return self._service.index_submit_documents(
            pointer=self._pointer, documents=documents
        )

    def finish(self) -> None:
        """Make sure all changes are committed."""
        self._logger.info("Finishing search documents.")
        self._service.refresh()
        self._logger.info("Finished search documents.")


class SearchMigrationInProgress(SearchDocumentReceiverType):
    """A migration in progress. Documents are being submitted, and the migration must be
    explicitly finished or cancelled to take effect (or not!)."""

    def __init__(
        self,
        base_name: str,
        revision: SearchSchemaRevision,
        service: SearchService,
    ):
        self._logger = logging.getLogger(SearchMigrationInProgress.__name__)
        self._base_name = base_name
        self._revision = revision
        self._service = service
        self._receiver = SearchDocumentReceiver(
            pointer=self._revision.name_for_index(base_name), service=self._service
        )

    def add_documents(
        self, documents: Iterable[dict]
    ) -> List[SearchServiceFailedDocument]:
        """Submit documents to be indexed."""
        return self._receiver.add_documents(documents)

    def finish(self) -> None:
        """Finish the migration."""
        self._logger.info(f"Completing migration to {self._revision.version}")
        # Make sure all changes are committed.
        self._receiver.finish()
        # Create the "indexed" alias.
        self._service.index_set_populated(self._revision)
        # Update the write pointer to point to the now-populated index.
        self._service.write_pointer_set(self._revision)
        # Set the read pointer to point at the now-populated index
        self._service.read_pointer_set(self._revision)
        self._service.refresh()
        self._logger.info(f"Completed migration to {self._revision.version}")

    def cancel(self) -> None:
        """Cancel the migration, leaving the read and write pointers untouched."""
        self._logger.info(f"Cancelling migration to {self._revision.version}")
        return None


class SearchMigrator:
    """A search migrator. This moves a search service to the targeted schema version."""

    def __init__(self, revisions: SearchRevisionDirectory, service: SearchService):
        self._logger = logging.getLogger(SearchMigrator.__name__)
        self._revisions = revisions
        self._service = service

    def migrate(
        self, base_name: str, version: int
    ) -> Optional[SearchMigrationInProgress]:
        """
        Migrate to the given version using the given base name (such as 'circulation-works'). The function returns
        an object that expects to receive batches of search documents used to populate any new index. When all
        the batches of documents have been sent to the object, callers must call 'finish' to indicate to the search
        migrator that no more documents are coming. Only at this point will the migrator consider the new index to be
        "populated".

        :arg base_name: The base name used for indices (such as 'circulation-works').
        :arg version: The version number to which we are migrating

        :raises SearchMigrationException: On errors, but always leaves the system in a usable state.
        """

        self._logger.info(f"starting migration to {base_name} {version}")

        try:
            target = self._revisions.available.get(version)
            if target is None:
                raise SearchMigrationException(
                    fatal=True,
                    message=f"No support is available for schema version {version}",
                )

            # Does the empty index exist? Create it if not.
            self._service.create_empty_index()

            # Does the read pointer exist? Point it at the empty index if not.
            read = self._service.read_pointer()
            if read is None:
                self._logger.info("Read pointer did not exist.")
                self._service.read_pointer_set_empty()

            # We're probably going to have to do a migration. We might end up returning
            # this instance so that users can submit documents for indexing.
            in_progress = SearchMigrationInProgress(
                base_name=base_name, revision=target, service=self._service
            )

            # Does the write pointer exist?
            write = self._service.write_pointer()
            if write is None or (not write.version == version):
                self._logger.info(
                    f"Write pointer does not point to the desired version: {write} != {version}."
                )
                # Either the write pointer didn't exist, or it's pointing at a version
                # other than the one we want. Create a new index for the version we want.
                self._service.index_create(target)
                self._service.index_set_mapping(target)

                # The index now definitely exists, but it might not be populated. Populate it if necessary.
                if not self._service.index_is_populated(target):
                    self._logger.info("Write index is not populated.")
                    return in_progress

            # If we didn't need to return the migration, finish it here. This will
            # update the read and write pointers appropriately.
            in_progress.finish()
            return None
        except SearchMigrationException:
            raise
        except Exception as e:
            raise SearchMigrationException(
                fatal=True, message=f"Service raised exception: {repr(e)}"
            ) from e
