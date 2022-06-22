from typing import Union

from api.admin.problem_details import (
    CUSTOMLIST_ENTRY_NOT_VALID_FOR_LIBRARY,
    CUSTOMLIST_SOURCE_COLLECTION_MISSING,
)
from core.model.customlist import CustomList, CustomListEntry
from core.model.library import Library
from core.model.licensing import LicensePool
from core.util.problem_detail import ProblemDetail


class CustomListQueries:
    @classmethod
    def share_locally_with_library(
        cls, _db, customlist: CustomList, library: Library
    ) -> Union[ProblemDetail, bool]:
        # All customlist collections must be present in the library
        for collection in customlist.collections:
            if collection not in library.collections:
                return CUSTOMLIST_SOURCE_COLLECTION_MISSING

        # All entries must be valid for the library
        library_collection_ids = [c.id for c in library.collections]
        entry: CustomListEntry
        for entry in customlist.entries:
            valid_license = (
                _db.query(LicensePool)
                .filter(
                    LicensePool.work_id == entry.work_id,
                    LicensePool.collection_id.in_(library_collection_ids),
                )
                .first()
            )
            if valid_license is None:
                return CUSTOMLIST_ENTRY_NOT_VALID_FOR_LIBRARY

        customlist.shared_locally_with_libraries.append(library)
        return True
