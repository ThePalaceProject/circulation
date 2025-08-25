from sqlalchemy.orm import Session

from palace.manager.celery.tasks import opds_for_distributors
from palace.manager.integration.license.opds.base.scripts import OpdsTaskScript


class OpdsForDistributorsImportScript(OpdsTaskScript):
    """Import all books from the feed associated with a collection."""

    def __init__(
        self,
        db: Session | None = None,
    ):
        super().__init__(
            "import",
            collection_task=opds_for_distributors.import_collection,
            all_task=opds_for_distributors.import_all,
            db=db,
        )


class OpdsForDistributorsReaperScript(OpdsTaskScript):
    """Mark all items in CM that are not in the feed as unavailable."""

    def __init__(
        self,
        db: Session | None = None,
    ):
        super().__init__(
            "reap",
            collection_task=opds_for_distributors.import_and_reap_not_found_chord,
            all_task=opds_for_distributors.reap_all,
            db=db,
        )
