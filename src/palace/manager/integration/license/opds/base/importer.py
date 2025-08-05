from __future__ import annotations

import logging
import traceback
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
from typing import Generic, TypeVar, overload

from feedparser import FeedParserDict
from sqlalchemy.orm import Session

from palace.manager.core.coverage import CoverageFailure
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.integration.base import integration_settings_load
from palace.manager.integration.license.opds.opds1.settings import OPDSImporterSettings
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.log import LoggerMixin

SettingsType = TypeVar("SettingsType", bound=OPDSImporterSettings, covariant=True)


class BaseOPDSImporter(
    Generic[SettingsType],
    LoggerMixin,
    ABC,
):
    def __init__(
        self,
        _db: Session,
        collection: Collection,
        data_source_name: str | None,
    ):
        self._db = _db
        if collection.id is None:
            raise ValueError(
                f"Unable to create importer for Collection with id = None. Collection: {collection.name}."
            )
        self._collection_id = collection.id
        self._integration_configuration_id = collection.integration_configuration_id
        if data_source_name is None:
            # Use the Collection data_source for OPDS import.
            data_source = self.collection.data_source
            if data_source:
                data_source_name = data_source.name
            else:
                raise ValueError(
                    "Cannot perform an OPDS import on a Collection that has no associated DataSource!"
                )
        self.data_source_name = data_source_name
        self.settings = integration_settings_load(
            self.settings_class(), collection.integration_configuration
        )

    @classmethod
    @abstractmethod
    def settings_class(cls) -> type[SettingsType]: ...

    @abstractmethod
    def extract_feed_data(
        self, feed: str | bytes, feed_url: str | None = None
    ) -> tuple[dict[str, BibliographicData], dict[str, list[CoverageFailure]]]: ...

    @abstractmethod
    def extract_last_update_dates(
        self, feed: str | bytes | FeedParserDict
    ) -> list[tuple[str | None, datetime | None]]: ...

    @abstractmethod
    def extract_next_links(self, feed: str | bytes) -> list[str]: ...

    @overload
    def parse_identifier(self, identifier: str) -> Identifier: ...

    @overload
    def parse_identifier(self, identifier: str | None) -> Identifier | None: ...

    def parse_identifier(self, identifier: str | None) -> Identifier | None:
        """Parse the identifier and return an Identifier object representing it.

        :param identifier: String containing the identifier

        :return: Identifier object
        """
        parsed_identifier = None

        try:
            result = Identifier.parse_urn(self._db, identifier)
            if result is not None:
                parsed_identifier, _ = result
        except Exception as e:
            self.log.exception(
                f"An unexpected exception occurred during parsing identifier '{identifier}': {e}"
            )

        return parsed_identifier

    @property
    def data_source(self) -> DataSource:
        """Look up or create a DataSource object representing the
        source of this OPDS feed.
        """
        offers_licenses = self.collection is not None
        return DataSource.lookup(
            self._db,
            self.data_source_name,
            autocreate=True,
            offers_licenses=offers_licenses,
        )

    @property
    def collection(self) -> Collection:
        collection = Collection.by_id(self._db, self._collection_id)
        if collection is None:
            raise ValueError("Unable to load collection.")
        return collection

    def import_edition_from_bibliographic(
        self, bibliographic: BibliographicData
    ) -> Edition:
        """For the passed-in BibliographicData object, see if can find or create an Edition
        in the database. Also create a LicensePool if the BibliographicData has
        CirculationData in it.
        """
        # Locate or create an Edition for this book.
        edition, is_new_edition = bibliographic.edition(self._db)

        policy = ReplacementPolicy(
            subjects=True,
            links=True,
            contributions=True,
            rights=True,
            link_content=True,
            formats=True,
            even_if_not_apparently_updated=True,
        )
        bibliographic.apply(
            self._db,
            edition=edition,
            collection=self.collection,
            replace=policy,
        )

        return edition

    def update_work_for_edition(
        self,
        edition: Edition,
        is_open_access: bool = True,
    ) -> tuple[LicensePool | None, Work | None]:
        """If possible, ensure that there is a presentation-ready Work for the
        given edition's primary identifier.

        :param edition: The edition whose license pool and work we're interested in.
        :param is_open_access: Whether this is an open access edition.
        :return: 2-Tuple of license pool (optional) and work (optional) for edition.
        """

        work = None

        # Looks up a license pool for the primary identifier associated with
        # the given edition. If this is not an open access book, then the
        # collection is also used as criteria for the lookup. Open access
        # books don't require a collection match, according to this explanation
        # from prior work:
        #   Find a LicensePool for the primary identifier. Any LicensePool will
        #   do--the collection doesn't have to match, since all
        #   LicensePools for a given identifier have the same Work.
        #
        # If we have CirculationData, a pool was created when we
        # imported the edition. If there was already a pool from a
        # different data source or a different collection, that's fine
        # too.
        collection_criteria = {} if is_open_access else {"collection": self.collection}
        pool = get_one(
            self._db,
            LicensePool,
            identifier=edition.primary_identifier,
            on_multiple="interchangeable",
            **collection_criteria,
        )

        if pool:
            if not pool.work or not pool.work.presentation_ready:
                # There is no presentation-ready Work for this
                # LicensePool. Try to create one.
                work, ignore = pool.calculate_work()
            else:
                # There is a presentation-ready Work for this LicensePool.
                # Use it.
                work = pool.work

        # If a presentation-ready Work already exists, there's no
        # rush. We might have new BibliographicData that will change the Work's
        # presentation, but when we called BibliographicData.apply() the work
        # was set up to have its presentation recalculated in the
        # background, and that's good enough.
        return pool, work

    def import_from_feed(self, feed: str | bytes, feed_url: str | None = None) -> tuple[
        list[Edition],
        list[LicensePool],
        list[Work],
        dict[str, list[CoverageFailure]],
    ]:
        # Keep track of editions that were imported. Pools and works
        # for those editions may be looked up or created.
        imported_editions = {}
        pools = {}
        works = {}

        # If parsing the overall feed throws an exception, we should address that before
        # moving on. Let the exception propagate.
        bibliographic_objs, extracted_failures = self.extract_feed_data(feed, feed_url)
        failures = defaultdict(list, extracted_failures)
        # make editions.  if have problem, make sure associated pool and work aren't created.
        for key, bibliographic in bibliographic_objs.items():
            # key is identifier.urn here

            # If there's a status message about this item, don't try to import it.
            if key in list(failures.keys()):
                continue

            try:
                # Create an edition. This will also create a pool if there's circulation data.
                edition = self.import_edition_from_bibliographic(bibliographic)
                if edition:
                    imported_editions[key] = edition
            except Exception as e:
                # Rather than scratch the whole import, treat this as a failure that only applies
                # to this item.
                self.log.error("Error importing an OPDS item", exc_info=e)
                data_source = self.data_source
                identifier = bibliographic.load_primary_identifier(self._db)
                failure = CoverageFailure(
                    identifier,
                    traceback.format_exc(),
                    data_source=data_source,
                    transient=False,
                    collection=self.collection,
                )
                failures[key].append(failure)
                # clean up any edition might have created
                if key in imported_editions:
                    del imported_editions[key]
                # Move on to the next item, don't create a work.
                continue

            try:
                pool, work = self.update_work_for_edition(edition)
                if pool:
                    pools[key] = pool
                if work:
                    works[key] = work
            except Exception as e:
                collection_name = self.collection.name if self.collection else "None"
                logging.warning(
                    f"Non-fatal exception: Failed to import item - import will continue: "
                    f"identifier={key}; collection={collection_name}/{self._collection_id}; "
                    f"data_source={self.data_source}; exception={e}",
                    stack_info=True,
                )
                identifier, ignore = Identifier.parse_urn(self._db, key)
                data_source = self.data_source
                failure = CoverageFailure(
                    identifier,
                    traceback.format_exc(),
                    data_source=data_source,
                    transient=False,
                    collection=self.collection,
                )
                failures[key].append(failure)

        return (
            list(imported_editions.values()),
            list(pools.values()),
            list(works.values()),
            failures,
        )
