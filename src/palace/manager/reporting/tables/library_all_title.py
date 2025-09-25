from collections.abc import Sequence, Set
from typing import TypeVar

import sqlalchemy as sa
from sqlalchemy import bindparam, case, func, lateral, select, true
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql import Select

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.reporting.model import (
    ReportTable,
    TabularQueryDefinition,
    TTabularDataProcessor,
    TTabularHeadings,
    TTabularRows,
)
from palace.manager.sqlalchemy.model.classification import Genre
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Equivalency, Identifier
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
)
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work, WorkGenre
from palace.manager.sqlalchemy.util import get_one


def collections_all_titles_query() -> Select:
    """A query for all titles for the designated collections."""

    work_genre_alias = aliased(WorkGenre)
    genre_alias = aliased(Genre)
    id_isbn = aliased(Identifier)
    equivalency_alias = aliased(Equivalency)

    isbn = lateral(
        select(id_isbn.identifier)
        .join(equivalency_alias, equivalency_alias.output_id == id_isbn.id)
        .where(
            equivalency_alias.input_id == Identifier.id,
            id_isbn.type == Identifier.ISBN,
            id_isbn.identifier.is_not(None),
            equivalency_alias.strength > 0.5,
            equivalency_alias.enabled == true(),
        )
        .order_by(equivalency_alias.strength.desc())
        .limit(1)
    )

    wg_subquery = (
        select(
            work_genre_alias.work_id,
            func.string_agg(genre_alias.name, ", ").label("genres"),
        )
        .join(genre_alias, genre_alias.id == work_genre_alias.genre_id)
        .group_by(work_genre_alias.work_id)
        .subquery()
    )

    return (
        select(
            Edition.title,
            Edition.author,
            Identifier.type.label("identifier_type"),
            Identifier.identifier.label("identifier"),
            func.coalesce(
                case(
                    (Identifier.type == Identifier.ISBN, Identifier.identifier),
                    else_=sa.cast(isbn.c.identifier, sa.String),
                ),
                "",
            ).label("isbn"),
            Edition.language,
            func.coalesce(Edition.publisher, "").label("publisher"),
            Edition.medium.label("format"),
            func.coalesce(Work.audience, "").label("audience"),
            func.coalesce(wg_subquery.c.genres, "").label("genres"),
            DataSource.name.label("data_source"),
            IntegrationConfiguration.name.label("collection"),
        )
        .join(LicensePool, LicensePool.presentation_edition_id == Edition.id)
        .join(Work, LicensePool.work_id == Work.id)
        .outerjoin(wg_subquery, Work.id == wg_subquery.c.work_id)
        .join(Identifier, LicensePool.identifier_id == Identifier.id)
        .outerjoin(isbn, Identifier.type != Identifier.ISBN)
        .join(DataSource, LicensePool.data_source_id == DataSource.id)
        .join(Collection, LicensePool.collection_id == Collection.id)
        .join(
            IntegrationConfiguration,
            Collection.integration_configuration_id == IntegrationConfiguration.id,
        )
        .where(
            IntegrationConfiguration.id.in_(
                bindparam("integration_ids", expanding=True)
            ),
        )
        .order_by(
            Edition.sort_title,
            Edition.author,
            DataSource.name,
            IntegrationConfiguration.name,
        )
    )


TReturn = TypeVar("TReturn")


class LibraryAllTitleReportTable(ReportTable):
    """A report table with all titles in a library's collections."""

    DEFINITION = TabularQueryDefinition(
        key="all-title",
        title="All Title",
        statement=collections_all_titles_query(),
    )

    @property
    def definition(self) -> TabularQueryDefinition:
        """Get the report definition."""
        return self.DEFINITION

    @property
    def headings(self) -> TTabularHeadings:
        return self.DEFINITION.headings

    def __call__(
        self,
        processor: TTabularDataProcessor[TReturn],
    ) -> TReturn:
        """Process the tabular data."""
        return processor(rows=self.rows, headings=self.headings)

    def __init__(
        self,
        *,
        session: Session,
        library_id: int,
        collection_ids: Sequence[int] | None = None,
    ) -> None:
        self.session = session
        self.library_id = library_id
        self.collections = self.included_collections(
            session=session, library_id=library_id, collection_ids=collection_ids
        )

    @staticmethod
    def included_collections(
        *,
        session: Session,
        library_id: int,
        collection_ids: Sequence[int] | Set[int] | None = None,
    ) -> list[Collection]:
        """Return the collections to be included in the results.

        :param session: A database session.
        :param library_id: The id of the library.
        :param collection_ids: IDs requested for inclusion. If provided, all
            requested collections must be among the given library's *associated*
            collections to be considered eligible. Otherwise (i.e., not provided),
            all *active* collections for the library are considered eligible.
        :return: The list of collections.
        :raises PalaceValueError: If any of the requested collections are not among
            the library's associated collections.
        """
        library = get_one(session, Library, id=library_id)
        if library is None:
            raise PalaceValueError(f"Library '{library_id}' not found.")

        # If no collections are specified, the active collections are used.
        if collection_ids is None:
            return list(library.active_collections)

        # Otherwise, if collections are specified, all associated collections are eligible.
        eligible_collections_ids = {c.id for c in library.associated_collections}
        requested_collections_ids = set(collection_ids)
        if ineligible_collection_ids := requested_collections_ids.difference(
            eligible_collections_ids
        ):
            ineligible = ", ".join(map(str, sorted(ineligible_collection_ids)))
            raise PalaceValueError(
                f"Ineligible report collection id(s) for library '{library.name}' (id={library_id}): {ineligible}"
            )
        return [
            c
            for c in library.associated_collections
            if c.id in requested_collections_ids
        ]

    @property
    def rows(self) -> TTabularRows:
        """Run the query to get the rows."""
        integration_ids: list[int] = [
            c.integration_configuration_id for c in self.collections
        ]
        return self.definition.rows(
            session=self.session,
            integration_ids=integration_ids,
        )
