from __future__ import annotations

import datetime

from sqlalchemy import and_, exists, not_, or_, select
from sqlalchemy.orm import Session, aliased, contains_eager, joinedload, query

from palace.manager.core.classifier import Classifier
from palace.manager.core.config import ConfigurationAttributeValue
from palace.manager.feed.worklist.base import WorkList
from palace.manager.sqlalchemy.util import tuple_to_numericrange
from palace.manager.util.datetime_helpers import utc_now


class DatabaseBackedWorkList(WorkList):
    """A WorkList that can get its works from the database in addition to
    (or possibly instead of) the search index.

    Even when works _are_ obtained through the search index, a
    DatabaseBackedWorkList is then created to look up the Work objects
    for use in an OPDS feed.
    """

    def works_from_database(self, _db, facets=None, pagination=None, **kwargs):
        """Create a query against the `works` table that finds Work objects
        corresponding to all the Works that belong in this WorkList.

        The apply_filters() implementation defines which Works qualify
        for membership in a WorkList of this type.

        This tends to be slower than WorkList.works, but not all
        lanes can be generated through search engine queries.

        :param _db: A database connection.
        :param facets: A faceting object, which may place additional
           constraints on WorkList membership.
        :param pagination: A Pagination object indicating which part of
           the WorkList the caller is looking at.
        :param kwargs: Ignored -- only included for compatibility with works().
        :return: A Query.
        """
        # Local import to avoid circular dependency
        from palace.manager.sqlalchemy.model.work import Work

        qu = self.base_query(_db)

        # In general, we only show books that are present in one of
        # the WorkList's collections and ready to be delivered to
        # patrons.
        qu = self.only_show_ready_deliverable_works(_db, qu)
        qu = self._restrict_query_for_no_hold_collections(_db, qu)
        # Apply to the database the bibliographic restrictions with
        # which this WorkList was initialized -- genre, audience, and
        # whatnot.
        qu, bibliographic_clauses = self.bibliographic_filter_clauses(_db, qu)
        if bibliographic_clauses:
            bibliographic_clause = and_(*bibliographic_clauses)
            qu = qu.filter(bibliographic_clause)

        # Allow the faceting object to modify the database query.
        if facets is not None:
            qu = facets.modify_database_query(_db, qu)

        # Allow a subclass to modify the database query.
        qu = self.modify_database_query_hook(_db, qu)

        if qu._distinct is False:
            # This query must always be made distinct, since a Work
            # can have more than one LicensePool. If no one else has
            # taken the opportunity to make it distinct (e.g. the
            # faceting object, while setting sort order), we'll make
            # it distinct based on work ID.
            qu = qu.distinct(Work.id)

        # Allow the pagination object to modify the database query.
        if pagination is not None:
            qu = pagination.modify_database_query(_db, qu)

        return qu

    def _restrict_query_for_no_hold_collections(
        self, _db: Session, qu: query.Query
    ) -> query.Query:
        """Restrict query to available books, if holds not allowed.

        Holds are not allowed if the collection's `hold_limit` is set to 0
        or if the library's `dont_display_reserves` is set to True.
        """
        # Local imports to avoid circular dependency
        from palace.manager.sqlalchemy.model.collection import Collection
        from palace.manager.sqlalchemy.model.integration import (
            IntegrationConfiguration,
            IntegrationLibraryConfiguration,
        )
        from palace.manager.sqlalchemy.model.licensing import LicensePool

        restricted_collections = _db.execute(
            select(Collection.id)
            .join(IntegrationConfiguration)
            .join(IntegrationLibraryConfiguration)
            .where(
                or_(
                    IntegrationConfiguration.settings_dict.contains({"hold_limit": 0}),
                    and_(
                        IntegrationLibraryConfiguration.library_id == self.library_id,
                        IntegrationLibraryConfiguration.settings_dict.contains(
                            {
                                "dont_display_reserves": ConfigurationAttributeValue.NOVALUE.value
                            }
                        ),
                    ),
                ),
            )
        ).all()
        restricted_collection_ids = (r.id for r in restricted_collections)

        # If a licensepool is from a collection that restricts holds
        # and has no available copies, then we don't want to see it
        # Should this be a configurable feature?
        qu = qu.filter(
            not_(
                and_(
                    LicensePool.collection_id.in_(restricted_collection_ids),
                    LicensePool.licenses_available == 0,
                )
            )
        )

        return qu

    @classmethod
    def base_query(cls, _db):
        """Return a query that contains the joins set up as necessary to
        create OPDS feeds.
        """
        # Local import to avoid circular dependency
        from palace.manager.sqlalchemy.model.work import Work

        qu = _db.query(Work).join(Work.license_pools).join(Work.presentation_edition)

        # Apply optimizations.
        qu = cls._modify_loading(qu)
        return qu

    @classmethod
    def _modify_loading(cls, qu):
        """Optimize a query for use in generating OPDS feeds, by modifying
        which related objects get pulled from the database.
        """
        # Local imports to avoid circular dependency
        from palace.manager.sqlalchemy.model.licensing import (
            LicensePool,
            LicensePoolDeliveryMechanism,
        )
        from palace.manager.sqlalchemy.model.resource import Resource
        from palace.manager.sqlalchemy.model.work import Work

        # Avoid eager loading of objects that are already being loaded.
        qu = qu.options(
            contains_eager(Work.presentation_edition),
            contains_eager(Work.license_pools),
        )

        # Load some objects that wouldn't normally be loaded, but
        # which are necessary when generating OPDS feeds.

        # TODO: Strictly speaking, these joinedload calls are
        # only needed by the circulation manager. This code could
        # be moved to circulation and everyone else who uses this
        # would be a little faster. (But right now there is no one
        # else who uses this.)
        qu = qu.options(
            # These speed up the process of generating acquisition links.
            joinedload(Work.license_pools).joinedload(
                LicensePool.available_delivery_mechanisms
            ),
            joinedload(Work.license_pools)
            .joinedload(LicensePool.available_delivery_mechanisms)
            .joinedload(LicensePoolDeliveryMechanism.delivery_mechanism),
            joinedload(Work.license_pools).joinedload(LicensePool.identifier),
            # These speed up the process of generating the open-access link
            # for open-access works.
            joinedload(Work.license_pools)
            .joinedload(LicensePool.available_delivery_mechanisms)
            .joinedload(LicensePoolDeliveryMechanism.resource),
            joinedload(Work.license_pools)
            .joinedload(LicensePool.available_delivery_mechanisms)
            .joinedload(LicensePoolDeliveryMechanism.resource)
            .joinedload(Resource.representation),
        )
        return qu

    def only_show_ready_deliverable_works(self, _db, query, show_suppressed=False):
        """Restrict a query to show only presentation-ready works present in
        an appropriate collection which the default client can
        fulfill.

        Note that this assumes the query has an active join against
        LicensePool.
        """
        # Local imports to avoid circular dependency
        from palace.manager.sqlalchemy.model.collection import Collection
        from palace.manager.sqlalchemy.model.work import Work

        query = Collection.restrict_to_ready_deliverable_works(
            query, show_suppressed=show_suppressed, collection_ids=self.collection_ids
        )

        if not show_suppressed and self.library_id is not None:
            query = query.filter(
                not_(Work.suppressed_for.contains(self.get_library(_db)))
            )

        return query

    def bibliographic_filter_clauses(self, _db, qu):
        """Create a SQLAlchemy filter that excludes books whose bibliographic
        metadata doesn't match what we're looking for.

        query is either `qu`, or a new query that has been modified to
        join against additional tables.

        :return: A 2-tuple (query, clauses).

        """
        # Local imports to avoid circular dependency
        from palace.manager.sqlalchemy.model.classification import Genre
        from palace.manager.sqlalchemy.model.edition import Edition
        from palace.manager.sqlalchemy.model.licensing import LicensePool
        from palace.manager.sqlalchemy.model.work import Work, WorkGenre

        # Audience language, and genre restrictions are allowed on all
        # WorkLists. (So are collection restrictions, but those are
        # applied by only_show_ready_deliverable_works().
        clauses = self.audience_filter_clauses(_db, qu)
        if self.languages:
            clauses.append(Edition.language.in_(self.languages))
        if self.media:
            clauses.append(Edition.medium.in_(self.media))
        if self.fiction is not None:
            clauses.append(Work.fiction == self.fiction)
        if self.license_datasource_id:
            clauses.append(LicensePool.data_source_id == self.license_datasource_id)

        if self.genre_ids:
            qu, clause = self.genre_filter_clause(qu)
            if clause is not None:
                clauses.append(clause)

        if self.customlist_ids:
            qu, customlist_clauses = self.customlist_filter_clauses(qu)
            clauses.extend(customlist_clauses)

        library = self.get_library(_db)
        if library:
            settings = library.settings
            if settings.filtered_audiences:
                clauses.append(
                    or_(
                        Work.audience.is_(None),
                        not_(Work.audience.in_(settings.filtered_audiences)),
                    )
                )
            if settings.filtered_genres:
                genre_filter = (
                    select(1)
                    .select_from(WorkGenre)
                    .join(Genre, WorkGenre.genre_id == Genre.id)
                    .where(
                        WorkGenre.work_id == Work.id,
                        Genre.name.in_(settings.filtered_genres),
                    )
                )
                clauses.append(not_(exists(genre_filter)))

        clauses.extend(self.age_range_filter_clauses())

        if self.parent and self.inherit_parent_restrictions:
            # In addition to the other any other restrictions, books
            # will show up here only if they would also show up in the
            # parent WorkList.
            qu, parent_clauses = self.parent.bibliographic_filter_clauses(_db, qu)
            if parent_clauses:
                clauses.extend(parent_clauses)

        return qu, clauses

    def audience_filter_clauses(self, _db, qu):
        """Create a SQLAlchemy filter that excludes books whose intended
        audience doesn't match what we're looking for.
        """
        # Local import to avoid circular dependency
        from palace.manager.sqlalchemy.model.work import Work

        if not self.audiences:
            return []
        return [Work.audience.in_(self.audiences)]

    def customlist_filter_clauses(self, qu):
        """Create a filter clause that only books that are on one of the
        CustomLists allowed by Lane configuration.

        :return: A 3-tuple (query, clauses).

        `query` is the same query as `qu`, possibly extended with
        additional table joins.

        `clauses` is a list of SQLAlchemy statements for use in a
        filter() or case() statement.
        """
        # Local imports to avoid circular dependency
        from palace.manager.sqlalchemy.model.customlist import (
            CustomList,
            CustomListEntry,
        )
        from palace.manager.sqlalchemy.model.work import Work

        if not self.uses_customlists:
            # This lane does not require that books be on any particular
            # CustomList.
            return qu, []

        # We will be joining against CustomListEntry at least
        # once. For a lane derived from the intersection of two or
        # more custom lists, we may be joining CustomListEntry
        # multiple times. To avoid confusion, we make a new alias for
        # the table every time.
        a_entry = aliased(CustomListEntry)

        clause = a_entry.work_id == Work.id
        qu = qu.join(a_entry, clause)

        # Actually build the restriction clauses.
        clauses = []
        customlist_ids = None
        if self.list_datasource_id:
            # Use a subquery to obtain the CustomList IDs of all
            # CustomLists from this DataSource. This is significantly
            # simpler than adding a join against CustomList.
            customlist_ids = select(CustomList.id).where(
                CustomList.data_source_id == self.list_datasource_id
            )
        else:
            customlist_ids = self.customlist_ids
        if customlist_ids is not None:
            clauses.append(a_entry.list_id.in_(customlist_ids))
        if self.list_seen_in_previous_days:
            cutoff = utc_now() - datetime.timedelta(self.list_seen_in_previous_days)
            clauses.append(a_entry.most_recent_appearance >= cutoff)

        return qu, clauses

    def genre_filter_clause(self, qu):
        # Local import to avoid circular dependency
        from palace.manager.sqlalchemy.model.work import Work, WorkGenre

        wg = aliased(WorkGenre)
        qu = qu.join(wg, wg.work_id == Work.id)
        return qu, wg.genre_id.in_(self.genre_ids)

    def age_range_filter_clauses(self):
        """Create a clause that filters out all books not classified as
        suitable for this DatabaseBackedWorkList's age range.
        """
        # Local import to avoid circular dependency
        from palace.manager.sqlalchemy.model.work import Work

        if self.target_age is None:
            return []

        # self.target_age will be a NumericRange for Lanes and a tuple for
        # most other WorkLists. Make sure it's always a NumericRange.
        target_age = self.target_age
        if isinstance(target_age, tuple):
            target_age = tuple_to_numericrange(target_age)

        audiences = self.audiences or []
        adult_audiences = [Classifier.AUDIENCE_ADULT, Classifier.AUDIENCE_ADULTS_ONLY]
        if target_age.upper >= 18 or (any(x in audiences for x in adult_audiences)):
            # Books for adults don't have target ages. If we're
            # including books for adults, either due to the audience
            # setting or the target age setting, allow the target age
            # to be empty.
            audience_has_no_target_age = Work.target_age == None
        else:
            audience_has_no_target_age = False

        # The lane's target age is an inclusive NumericRange --
        # set_target_age makes sure of that. The work's target age
        # must overlap that of the lane.

        return [or_(Work.target_age.overlaps(target_age), audience_has_no_target_age)]

    def modify_database_query_hook(self, _db, qu):
        """A hook method allowing subclasses to modify a database query
        that's about to find all the works in this WorkList.

        This can avoid the need for complex subclasses of
        DatabaseBackedFacets.
        """
        return qu
