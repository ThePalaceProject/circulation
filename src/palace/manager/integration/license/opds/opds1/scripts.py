from sqlalchemy.orm import Session

from palace.manager.celery.tasks import opds1
from palace.manager.integration.license.opds.base.scripts import OpdsTaskScript


class Opds1ImportScript(OpdsTaskScript):
    """Import all books from the feed associated with a collection."""

    def __init__(
        self,
        db: Session | None = None,
    ):
        super().__init__(
            "import",
            collection_task=opds1.import_collection,
            all_task=opds1.import_all,
            db=db,
        )


class Opds1ReaperScript(OpdsTaskScript):
    """Mark all items in CM that are not in the feed as unavailable."""

    def __init__(
        self,
        db: Session | None = None,
    ):
        super().__init__(
            "reap",
            collection_task=opds1.import_and_reap_not_found_chord,
            db=db,
        )
