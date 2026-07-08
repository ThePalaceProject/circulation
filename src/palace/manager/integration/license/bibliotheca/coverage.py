from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from palace.manager.core.coverage import BibliographicCoverageProvider, CoverageFailure
from palace.manager.integration.license.bibliotheca.api import (
    BibliothecaAPI,
    BibliothecaApiClassT,
)
from palace.manager.integration.license.bibliotheca.constants import BIBLIOTHECA_LABEL
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier


class BibliothecaBibliographicCoverageProvider(BibliographicCoverageProvider):
    """Fill in bibliographic metadata for Bibliotheca records.

    This will occasionally fill in some availability information for a
    single Collection, but we rely on Monitors to keep availability
    information up to date for all Collections.
    """

    SERVICE_NAME = "Bibliotheca Bibliographic Coverage Provider"
    DATA_SOURCE_NAME = DataSource.BIBLIOTHECA
    PROTOCOL = BIBLIOTHECA_LABEL
    INPUT_IDENTIFIER_TYPES = Identifier.BIBLIOTHECA_ID

    # 25 is the maximum batch size for the Bibliotheca API.
    DEFAULT_BATCH_SIZE = 25

    def __init__(
        self,
        collection: Collection,
        api_class: BibliothecaApiClassT = BibliothecaAPI,
        **kwargs: Any,
    ) -> None:
        """Constructor.

        :param collection: Provide bibliographic coverage to all
            Bibliotheca books in the given Collection.
        :param api_class: Instantiate this class with the given Collection,
            rather than instantiating BibliothecaAPI.
        :param input_identifiers: Passed in by RunCoverageProviderScript.
            A list of specific identifiers to get coverage for.
        """
        super().__init__(collection, **kwargs)
        if isinstance(api_class, BibliothecaAPI):
            # This is an already instantiated API object. Use it
            # instead of creating a new one.
            self.api = api_class
        else:
            # A web application should not use this option because it
            # will put a non-scoped session in the mix.
            _db = Session.object_session(collection)
            self.api = api_class(_db, collection)

    def process_item(self, identifier: Identifier) -> Identifier | CoverageFailure:
        list_bibliographic = self.api.bibliographic_lookup(identifier)
        if not list_bibliographic:
            return self.failure(identifier, "Bibliotheca bibliographic lookup failed.")  # type: ignore[no-any-return]
        [bibliographic] = list_bibliographic
        return self.set_bibliographic(identifier, bibliographic)  # type: ignore[no-any-return]
