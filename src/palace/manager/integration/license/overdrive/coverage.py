from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session

from palace.manager.core.coverage import BibliographicCoverageProvider, CoverageFailure
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.integration.license.overdrive.constants import OVERDRIVE_LABEL
from palace.manager.integration.license.overdrive.representation import (
    OverdriveRepresentationExtractor,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier

if TYPE_CHECKING:
    from palace.manager.integration.license.overdrive.api import OverdriveAPI


class OverdriveBibliographicCoverageProvider(BibliographicCoverageProvider):
    """Fill in bibliographic metadata for Overdrive records.

    This will occasionally fill in some availability information for a
    single Collection, but we rely on Monitors to keep availability
    information up to date for all Collections.
    """

    SERVICE_NAME = "Overdrive Bibliographic Coverage Provider"
    DATA_SOURCE_NAME = DataSource.OVERDRIVE
    PROTOCOL = OVERDRIVE_LABEL
    INPUT_IDENTIFIER_TYPES = Identifier.OVERDRIVE_ID

    def __init__(
        self,
        collection: Collection,
        api: OverdriveAPI | None = None,
        **kwargs: Any,
    ) -> None:
        """Constructor.

        :param collection: Provide bibliographic coverage to all
            Overdrive books in the given Collection.
        :param api: API class, if none it will be initialized with OverdriveAPI.
        """
        super().__init__(collection, **kwargs)
        if api is None:
            # A web application should not use this option because it
            # will put a non-scoped session in the mix.
            _db = Session.object_session(collection)
            from palace.manager.integration.license.overdrive.api import OverdriveAPI

            self.api = OverdriveAPI(_db, collection)
        else:
            self.api = api

    def process_item(self, identifier: Identifier) -> Identifier | CoverageFailure:
        info = self.api.metadata_lookup(identifier)
        error = None
        if info.get("errorCode") == "NotFound":
            error = "ID not recognized by Overdrive: %s" % identifier.identifier
        elif info.get("errorCode") == "InvalidGuid":
            error = "Invalid Overdrive ID: %s" % identifier.identifier

        if error:
            return self.failure(identifier, error, transient=False)  # type: ignore[no-any-return]

        bibliographic = OverdriveRepresentationExtractor.book_info_to_bibliographic(
            info
        )

        if not bibliographic:
            e = "Could not extract bibliographic data from Overdrive data: %r" % info
            return self.failure(identifier, e)  # type: ignore[no-any-return]

        self.bibliographic_data_pre_hook(bibliographic)
        return self.set_bibliographic(identifier, bibliographic)  # type: ignore[no-any-return]

    def bibliographic_data_pre_hook(
        self, bibliographic: BibliographicData
    ) -> BibliographicData:
        """A hook method that allows subclasses to modify a BibliographicData
        object derived from Overdrive before it's applied.
        """
        return bibliographic
