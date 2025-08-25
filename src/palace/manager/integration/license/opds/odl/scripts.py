from sqlalchemy.orm import Session

from palace.manager.celery.tasks import opds_odl
from palace.manager.integration.license.opds.base.scripts import OpdsTaskScript


class OPDS2WithODLImportScript(OpdsTaskScript):
    """Import all books from the OPDS2+ODL feed associated with a collection."""

    def __init__(
        self,
        db: Session | None = None,
    ):
        super().__init__(
            "import",
            collection_task=opds_odl.import_collection,
            all_task=opds_odl.import_all,
            db=db,
        )
