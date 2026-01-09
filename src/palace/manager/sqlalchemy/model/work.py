# WorkGenre, Work

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from functools import cache
from typing import TYPE_CHECKING, Any, Self, cast

import opensearchpy
import pytz
from dependency_injector.wiring import Provide, inject
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    Numeric,
    Table,
    Unicode,
)
from sqlalchemy.dialects.postgresql import INT4RANGE
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Mapped, contains_eager, joinedload, relationship
from sqlalchemy.orm.base import NO_VALUE
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import and_, case, literal_column, select
from sqlalchemy.sql.functions import func

from palace.manager.core.classifier import Classifier
from palace.manager.core.classifier.work import WorkClassifier
from palace.manager.core.exceptions import BasePalaceException
from palace.manager.data_layer.policy.presentation import (
    PresentationCalculationPolicy,
)
from palace.manager.search.service import SearchDocument
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.constants import (
    DataSourceConstants,
    IntegrationConfigurationConstants,
)
from palace.manager.sqlalchemy.model.base import Base
from palace.manager.sqlalchemy.model.classification import (
    Classification,
    Genre,
    Subject,
)
from palace.manager.sqlalchemy.model.contributor import Contribution
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import (
    Identifier,
    RecursiveEquivalencyCache,
)
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.util import (
    flush,
    get_one_or_create,
    numericrange_to_string,
    numericrange_to_tuple,
    tuple_to_numericrange,
)
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.languages import LanguageCodes
from palace.manager.util.log import LoggerMixin

if TYPE_CHECKING:
    from palace.manager.search.external_search import ExternalSearchIndex
    from palace.manager.sqlalchemy.model.customlist import CustomList, CustomListEntry
    from palace.manager.sqlalchemy.model.library import Library
    from palace.manager.sqlalchemy.model.resource import Resource


class WorkGenre(Base):
    """An assignment of a genre to a work."""

    __tablename__ = "workgenres"
    id: Mapped[int] = Column(Integer, primary_key=True)
    genre_id: Mapped[int] = Column(
        Integer, ForeignKey("genres.id"), index=True, nullable=False
    )
    genre: Mapped[Genre] = relationship("Genre", back_populates="work_genres")

    work_id: Mapped[int] = Column(
        Integer, ForeignKey("works.id"), index=True, nullable=False
    )
    work: Mapped[Work] = relationship("Work", back_populates="work_genres")

    affinity: Mapped[float] = Column(Float, index=True, default=0, nullable=False)

    @classmethod
    def from_genre(cls, genre):
        wg = WorkGenre()
        wg.genre = genre
        return wg

    def __repr__(self):
        return "%s (%d%%)" % (self.genre.name, self.affinity * 100)


class Work(Base, LoggerMixin):
    APPEALS_URI = "http://librarysimplified.org/terms/appeals/"

    CHARACTER_APPEAL = "Character"
    LANGUAGE_APPEAL = "Language"
    SETTING_APPEAL = "Setting"
    STORY_APPEAL = "Story"
    UNKNOWN_APPEAL = "Unknown"
    NOT_APPLICABLE_APPEAL = "Not Applicable"
    NO_APPEAL = "None"

    CURRENTLY_AVAILABLE = "currently_available"
    ALL = "all"

    # If no quality data is available for a work, it will be assigned
    # a default quality based on where we got it.
    #
    # The assumption is that a librarian would not have ordered a book
    # if it didn't meet a minimum level of quality.
    #
    # For data sources where librarians tend to order big packages of
    # books instead of selecting individual titles, the default
    # quality is lower. For data sources where there is no curation at
    # all, the default quality is zero.
    #
    # If there is absolutely no way to get quality data for a curated
    # data source, each work is assigned the minimum level of quality
    # necessary to show up in featured feeds.
    default_quality_by_data_source = {
        DataSourceConstants.GUTENBERG: 0,
        DataSourceConstants.OVERDRIVE: 0.4,
        DataSourceConstants.BIBLIOTHECA: 0.65,
        DataSourceConstants.BOUNDLESS: 0.65,
        DataSourceConstants.STANDARD_EBOOKS: 0.8,
        DataSourceConstants.UNGLUE_IT: 0.4,
        DataSourceConstants.PLYMPTON: 0.5,
    }

    __tablename__ = "works"
    id: Mapped[int] = Column(Integer, primary_key=True)

    # One Work may have copies scattered across many LicensePools.
    license_pools: Mapped[list[LicensePool]] = relationship(
        "LicensePool",
        back_populates="work",
        lazy="joined",
        uselist=True,
        order_by="asc(LicensePool.id)",
    )

    # A Work takes its presentation metadata from a single Edition.
    # But this Edition is a composite of provider, admin interface, etc.-derived Editions.
    presentation_edition_id = Column(Integer, ForeignKey("editions.id"), index=True)
    presentation_edition: Mapped[Edition | None] = relationship(
        "Edition", back_populates="work"
    )

    # One Work may be associated with many CustomListEntries.
    # However, a CustomListEntry may lose its Work without
    # ceasing to exist.
    custom_list_entries: Mapped[list[CustomListEntry]] = relationship(
        "CustomListEntry", back_populates="work"
    )

    # One Work may participate in many WorkGenre assignments.
    genres = association_proxy("work_genres", "genre", creator=WorkGenre.from_genre)
    work_genres: Mapped[list[WorkGenre]] = relationship(
        "WorkGenre", back_populates="work", cascade="all, delete-orphan"
    )
    audience = Column(Unicode, index=True)
    target_age = Column(INT4RANGE, index=True)
    fiction = Column(Boolean, index=True)

    summary_id = Column(
        Integer,
        ForeignKey("resources.id", use_alter=True, name="fk_works_summary_id"),
        index=True,
    )
    summary: Mapped[Resource | None] = relationship(
        "Resource", foreign_keys=[summary_id], back_populates="summary_works"
    )
    # This gives us a convenient place to store a cleaned-up version of
    # the content of the summary Resource.
    summary_text = Column(Unicode)

    # The overall suitability of this work for unsolicited
    # presentation to a patron. This is a calculated value taking both
    # rating and popularity into account.
    quality = Column(Numeric(4, 3), index=True)

    # The overall rating given to this work.
    rating = Column(Float, index=True)

    # The overall current popularity of this work.
    popularity = Column(Float, index=True)

    appeal_type = Enum(
        CHARACTER_APPEAL,
        LANGUAGE_APPEAL,
        SETTING_APPEAL,
        STORY_APPEAL,
        NOT_APPLICABLE_APPEAL,
        NO_APPEAL,
        UNKNOWN_APPEAL,
        name="appeal",
    )

    primary_appeal = Column(appeal_type, default=None, index=True)
    secondary_appeal = Column(appeal_type, default=None, index=True)

    appeal_character = Column(Float, default=None, index=True)
    appeal_language = Column(Float, default=None, index=True)
    appeal_setting = Column(Float, default=None, index=True)
    appeal_story = Column(Float, default=None, index=True)

    # The last time the availability or metadata changed for this Work.
    last_update_time = Column(DateTime(timezone=True), index=True)

    # This is set to True once all metadata and availability
    # information has been obtained for this Work. Until this is True,
    # the work will not show up in feeds.
    presentation_ready: Mapped[bool] = Column(
        Boolean, default=False, index=True, nullable=False
    )

    # This is the last time we tried to make this work presentation ready.
    presentation_ready_attempt = Column(
        DateTime(timezone=True), default=None, index=True
    )

    # This is the error that occured while trying to make this Work
    # presentation ready. Until this is cleared, no further attempt
    # will be made to make the Work presentation ready.
    presentation_ready_exception = Column(Unicode, default=None, index=True)

    # Supress this work from appearing in any feeds for a specific library.
    suppressed_for: Mapped[list[Library]] = relationship(
        "Library", secondary="work_library_suppressions", passive_deletes=True
    )

    # These fields are potentially large and can be deferred if you
    # don't need all the data in a Work.
    LARGE_FIELDS = [
        "summary_text",
    ]

    @property
    def title(self):
        if self.presentation_edition:
            return self.presentation_edition.title
        return None

    @property
    def sort_title(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.sort_title or self.presentation_edition.title

    @property
    def subtitle(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.subtitle

    @property
    def series(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.series

    @property
    def series_position(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.series_position

    @property
    def author(self):
        if self.presentation_edition:
            return self.presentation_edition.author
        return None

    @property
    def sort_author(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.sort_author or self.presentation_edition.author

    @property
    def language(self) -> str | None:
        if self.presentation_edition:
            return self.presentation_edition.language
        return None

    @property
    def publisher(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.publisher

    @property
    def imprint(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.imprint

    @property
    def cover_full_url(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.cover_full_url

    @property
    def cover_thumbnail_url(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.cover_thumbnail_url

    @property
    def target_age_string(self):
        return numericrange_to_string(self.target_age)

    @property
    def has_open_access_license(self):
        return any(x.open_access for x in self.license_pools)

    def __repr__(self):
        return '<Work #{} "{}" (by {}) {} lang={} ({} lp)>'.format(
            self.id,
            self.title,
            self.author,
            ", ".join([g.name for g in self.genres]),
            self.language,
            len(self.license_pools),
        )

    @classmethod
    def for_unchecked_subjects(cls, _db):
        from palace.manager.sqlalchemy.model.classification import (
            Classification,
            Subject,
        )
        from palace.manager.sqlalchemy.model.licensing import LicensePool

        """Find all Works whose LicensePools have an Identifier that
        is classified under an unchecked Subject.
        This is a good indicator that the Work needs to be
        reclassified.
        """
        qu = (
            _db.query(Work)
            .join(Work.license_pools)
            .join(LicensePool.identifier)
            .join(Identifier.classifications)
            .join(Classification.subject)
        )
        return qu.filter(Subject.checked == False).order_by(Subject.id)

    @classmethod
    def _potential_open_access_works_for_permanent_work_id(
        cls, _db, pwid, medium, language
    ):
        """Find all Works that might be suitable for use as the
        canonical open-access Work for the given `pwid`, `medium`,
        and `language`.
        :return: A 2-tuple (pools, counts_by_work). `pools` is a set
        containing all affected LicensePools; `counts_by_work is a
        Counter tallying the number of affected LicensePools
        associated with a given work.
        """
        from palace.manager.sqlalchemy.model.licensing import LicensePool

        qu = (
            _db.query(LicensePool)
            .join(LicensePool.presentation_edition)
            .filter(LicensePool.open_access == True)
            .filter(Edition.permanent_work_id == pwid)
            .filter(Edition.medium == medium)
            .filter(Edition.language == language)
        )
        pools = set(qu.all())

        # Build the Counter of Works that are eligible to represent
        # this pwid/medium/language combination.
        affected_licensepools_for_work = Counter()
        for lp in pools:
            work = lp.work
            if not lp.work:
                continue
            if affected_licensepools_for_work[lp.work]:
                # We already got this information earlier in the loop.
                continue
            pe = work.presentation_edition
            if pe and (
                pe.language != language
                or pe.medium != medium
                or pe.permanent_work_id != pwid
            ):
                # This Work's presentation edition doesn't match
                # this LicensePool's presentation edition.
                # It would be better to create a brand new Work and
                # remove this LicensePool from its current Work.
                continue
            affected_licensepools_for_work[lp.work] = len(
                [x for x in pools if x.work == lp.work]
            )
        return pools, affected_licensepools_for_work

    @classmethod
    def open_access_for_permanent_work_id(cls, _db, pwid, medium, language):
        """Find or create the Work encompassing all open-access LicensePools
        whose presentation Editions have the given permanent work ID,
        the given medium, and the given language.
        This may result in the consolidation or splitting of Works, if
        a book's permanent work ID has changed without
        calculate_work() being called, or if the data is in an
        inconsistent state for any other reason.
        """
        is_new = False

        (
            licensepools,
            licensepools_for_work,
        ) = cls._potential_open_access_works_for_permanent_work_id(
            _db, pwid, medium, language
        )
        if not licensepools:
            # There is no work for this PWID/medium/language combination
            # because no LicensePools offer it.
            return None, is_new

        work = None
        if len(licensepools_for_work) == 0:
            # None of these LicensePools have a Work. Create a new one.
            work = Work()
            is_new = True
        else:
            # Pick the Work with the most LicensePools.
            work, count = licensepools_for_work.most_common(1)[0]

            # In the simple case, there will only be the one Work.
            if len(licensepools_for_work) > 1:
                # But in this case, for whatever reason (probably bad
                # data caused by a bug) there's more than one
                # Work. Merge the other Works into the one we chose
                # earlier.  (This is why we chose the work with the
                # most LicensePools--it minimizes the disruption
                # here.)

                # First, make sure this Work is the exclusive
                # open-access work for its permanent work ID.
                # Otherwise the merge may fail.
                work.make_exclusive_open_access_for_permanent_work_id(
                    pwid, medium, language
                )
                for needs_merge in list(licensepools_for_work.keys()):
                    if needs_merge != work:
                        # Make sure that Work we're about to merge has
                        # nothing but LicensePools whose permanent
                        # work ID matches the permanent work ID of the
                        # Work we're about to merge into.
                        needs_merge.make_exclusive_open_access_for_permanent_work_id(
                            pwid, medium, language
                        )
                        needs_merge.merge_into(work)

        # At this point we have one, and only one, Work for this
        # permanent work ID. Assign it to every LicensePool whose
        # presentation Edition has that permanent work ID/medium/language
        # combination.
        for lp in licensepools:
            lp.work = work
        return work, is_new

    def make_exclusive_open_access_for_permanent_work_id(self, pwid, medium, language):
        """Ensure that every open-access LicensePool associated with this Work
        has the given PWID and medium. Any non-open-access
        LicensePool, and any LicensePool with a different PWID or a
        different medium, is kicked out and assigned to a different
        Work. LicensePools with no presentation edition or no PWID
        are kicked out.
        In most cases this Work will be the _only_ work for this PWID,
        but inside open_access_for_permanent_work_id this is called as
        a preparatory step for merging two Works, and after the call
        (but before the merge) there may be two Works for a given PWID.
        """
        _db = Session.object_session(self)
        for pool in list(self.license_pools):
            other_work = is_new = None
            if not pool.open_access:
                # This needs to have its own Work--we don't mix
                # open-access and commercial versions of the same book.
                pool.work = None
                if pool.presentation_edition:
                    pool.presentation_edition.work = None
                other_work, is_new = pool.calculate_work()
            elif not pool.presentation_edition:
                # A LicensePool with no presentation edition
                # cannot have an associated Work.
                self.log.warning(
                    "LicensePool %r has no presentation edition, setting .work to None.",
                    pool,
                )
                pool.work = None
            else:
                e = pool.presentation_edition
                this_pwid = e.permanent_work_id
                if not this_pwid:
                    # A LicensePool with no permanent work ID
                    # cannot have an associated Work.
                    self.log.warning(
                        "Presentation edition for LicensePool %r has no PWID, setting .work to None.",
                        pool,
                    )
                    e.work = None
                    pool.work = None
                    continue
                if this_pwid != pwid or e.medium != medium or e.language != language:
                    # This LicensePool should not belong to this Work.
                    # Make sure it gets its own Work, creating a new one
                    # if necessary.
                    pool.work = None
                    pool.presentation_edition.work = None
                    other_work, is_new = Work.open_access_for_permanent_work_id(
                        _db, this_pwid, e.medium, e.language
                    )
            if other_work and is_new:
                other_work.calculate_presentation()

    @property
    def pwids(self):
        """Return the set of permanent work IDs associated with this Work.
        There should only be one permanent work ID associated with a
        given work, but if there is more than one, this will find all
        of them.
        """
        pwids = set()
        for pool in self.license_pools:
            if (
                pool.presentation_edition
                and pool.presentation_edition.permanent_work_id
            ):
                pwids.add(pool.presentation_edition.permanent_work_id)
        return pwids

    def merge_into(self, other_work):
        """Merge this Work into another Work and delete it."""

        # Neither the source nor the destination work may have any
        # non-open-access LicensePools.
        for w in self, other_work:
            for pool in w.license_pools:
                if not pool.open_access:
                    raise ValueError(
                        "Refusing to merge %r into %r because it would put an open-access LicensePool into the same work as a non-open-access LicensePool."
                        % (self, other_work)
                    )

        my_pwids = self.pwids
        other_pwids = other_work.pwids
        if not my_pwids == other_pwids:
            raise ValueError(
                "Refusing to merge %r into %r because permanent work IDs don't match: %s vs. %s"
                % (
                    self,
                    other_work,
                    ",".join(sorted(my_pwids)),
                    ",".join(sorted(other_pwids)),
                )
            )

        # Every LicensePool associated with this work becomes
        # associated instead with the other work.
        for pool in self.license_pools:
            other_work.license_pools.append(pool)

        _db = Session.object_session(self)
        _db.delete(self)

        other_work.calculate_presentation()

    @classmethod
    @cache
    def _xml_text_sanitization_regex(cls) -> re.Pattern[str]:
        # Source: https://stackoverflow.com/questions/8733233/filtering-out-certain-bytes-in-python
        return re.compile(
            r"[^\u0020-\uD7FF\u0009\u000A\u000D\uE000-\uFFFD\U00010000-\U0010FFFF]+"
        )

    def set_summary(self, resource: Resource) -> None:
        new_summary = resource
        if self.summary != resource:
            self.summary = resource

        if resource and resource.representation:
            # Make sure that the summary text only contains characters that are XML compatible.

            new_summary_text = self._xml_text_sanitization_regex().sub(
                "", resource.representation.unicode_content
            )
        else:
            new_summary_text = ""

        if new_summary_text != self.summary_text:
            self.summary_text = new_summary_text

    @classmethod
    def with_genre(cls, _db, genre):
        """Find all Works classified under the given genre."""
        from palace.manager.sqlalchemy.model.classification import Genre

        if isinstance(genre, (bytes, str)):
            genre, ignore = Genre.lookup(_db, genre)
        return _db.query(Work).join(WorkGenre).filter(WorkGenre.genre == genre)

    @classmethod
    def with_no_genres(self, q):
        """Modify a query so it finds only Works that are not classified under
        any genre."""
        q = q.outerjoin(Work.work_genres)
        q = q.options(contains_eager(Work.work_genres))
        q = q.filter(WorkGenre.genre == None)
        return q

    @classmethod
    def from_identifiers(cls, _db, identifiers, base_query=None, policy=None):
        """Returns all of the works that have one or more license_pools
        associated with either an identifier in the given list or an
        identifier considered equivalent to one of those listed.

        :param policy: A PresentationCalculationPolicy, used to
           determine how far to go when looking for equivalent
           Identifiers. By default, this method will be very strict
           about equivalencies.
        """
        from palace.manager.sqlalchemy.model.licensing import LicensePool

        identifier_ids = [identifier.id for identifier in identifiers]
        if not identifier_ids:
            return None

        if not base_query:
            # A raw base query that makes no accommodations for works that are
            # suppressed or otherwise undeliverable.
            base_query = (
                _db.query(Work).join(Work.license_pools).join(LicensePool.identifier)
            )

        if policy is None:
            policy = PresentationCalculationPolicy(
                equivalent_identifier_levels=1, equivalent_identifier_threshold=0.999
            )

        identifier_ids_subquery = (
            Identifier.recursively_equivalent_identifier_ids_query(
                Identifier.id, policy=policy
            )
        )
        identifier_ids_subquery = identifier_ids_subquery.where(
            Identifier.id.in_(identifier_ids)
        )

        query = base_query.filter(Identifier.id.in_(identifier_ids_subquery))
        return query

    @classmethod
    def reject_covers(cls, _db, works_or_identifiers):
        """Suppresses the currently visible covers of a number of Works"""
        from palace.manager.sqlalchemy.model.licensing import LicensePool
        from palace.manager.sqlalchemy.model.resource import Hyperlink, Resource

        works = list(set(works_or_identifiers))
        if not isinstance(works[0], cls):
            # This assumes that everything in the provided list is the
            # same class: either Work or Identifier.
            works = cls.from_identifiers(_db, works_or_identifiers).all()
        work_ids = [w.id for w in works]

        if len(works) == 1:
            cls.logger().info("Suppressing cover for %r", works[0])
        else:
            cls.logger().info("Supressing covers for %i Works", len(works))

        cover_urls = list()
        for work in works:
            # Create a list of the URLs of the works' active cover images.
            edition = work.presentation_edition
            if edition:
                if edition.cover_full_url:
                    cover_urls.append(edition.cover_full_url)
                if edition.cover_thumbnail_url:
                    cover_urls.append(edition.cover_thumbnail_url)

        if not cover_urls:
            # All of the target Works have already had their
            # covers suppressed. Nothing to see here.
            return

        covers = (
            _db.query(Resource)
            .join(Hyperlink, Hyperlink.resource_id == Resource.id)
            .join(Identifier, Hyperlink.identifier_id == Identifier.id)
            .join(LicensePool, Identifier.id == LicensePool.identifier_id)
            .filter(Resource.url.in_(cover_urls), LicensePool.work_id.in_(work_ids))
        )

        editions = list()
        for cover in covers:
            # Record a downvote that will dismiss the Resource.
            cover.reject()
            if len(cover.cover_editions) > 1:
                editions += cover.cover_editions
        flush(_db)

        editions = list(set(editions))
        if editions:
            # More Editions and Works have been impacted by this cover
            # suppression.
            works += [ed.work for ed in editions if ed.work]
            editions = [ed for ed in editions if not ed.work]

        # Remove the cover from the Work and its Edition and reset
        # cached OPDS entries.
        policy = PresentationCalculationPolicy.reset_cover()
        for work in works:
            work.calculate_presentation(policy=policy)
        for edition in editions:
            edition.calculate_presentation(policy=policy)
        _db.commit()

    def reject_cover(self, search_index_client=None):
        """Suppresses the current cover of the Work"""
        _db = Session.object_session(self)
        self.suppress_covers(_db, [self], search_index_client=search_index_client)

    def all_editions(self, policy=None):
        """All Editions identified by an Identifier equivalent to
        the identifiers of this Work's license pools.

        :param policy: A PresentationCalculationPolicy, used to
           determine how far to go when looking for equivalent
           Identifiers.
        """
        from palace.manager.sqlalchemy.model.licensing import LicensePool

        _db = Session.object_session(self)
        identifier_ids_subquery = (
            Identifier.recursively_equivalent_identifier_ids_query(
                LicensePool.identifier_id, policy=policy
            )
        )
        identifier_ids_subquery = identifier_ids_subquery.where(
            LicensePool.work_id == self.id
        )

        q = _db.query(Edition).filter(
            Edition.primary_identifier_id.in_(identifier_ids_subquery)
        )
        return q

    @property
    def _direct_identifier_ids(self):
        """Return all Identifier IDs associated with one of this
        Work's LicensePools.
        """
        return [lp.identifier.id for lp in self.license_pools if lp.identifier]

    def all_identifier_ids(self, policy=None):
        """Return all Identifier IDs associated with this Work.

        :param policy: A `PresentationCalculationPolicy`.
        :return: A set containing all Identifier IDs associated
             with this Work (as per the rules set down in `policy`).
        """
        _db = Session.object_session(self)
        # Get a dict that maps identifier ids to lists of their equivalents.
        equivalent_lists = Identifier.recursively_equivalent_identifier_ids(
            _db, self._direct_identifier_ids, policy=policy
        )

        all_identifier_ids = set()
        for equivs in list(equivalent_lists.values()):
            all_identifier_ids.update(equivs)
        return all_identifier_ids

    @property
    def language_code(self):
        """A single BCP47 language code for display purposes."""
        if not self.language:
            return None
        return LanguageCodes.bcp47_for_locale(self.language, default=self.language)

    def age_appropriate_for_patron(self, patron):
        """Is this Work age-appropriate for the given Patron?

        :param patron: A Patron.
        :return: A boolean
        """
        if patron is None:
            return True
        return patron.work_is_age_appropriate(self.audience, self.target_age)

    def is_filtered_by(
        self, filtered_audiences: Sequence[str], filtered_genres: Sequence[str]
    ) -> bool:
        """Return whether this Work should be filtered (hidden) by the provided filters.

        A work is filtered if its audience is in ``filtered_audiences`` or any of its
        genres are in ``filtered_genres``.

        :param filtered_audiences: Audiences that should be excluded.
        :param filtered_genres: Genres that should be excluded.
        :return: True if the work should be filtered (hidden), False otherwise.
        """

        # Check audience filtering
        if self.audience and filtered_audiences:
            if self.audience in filtered_audiences:
                return True

        # Check genre filtering
        if filtered_genres:
            work_genre_names = {wg.genre.name for wg in self.work_genres}
            if work_genre_names & set(filtered_genres):
                return True

        return False

    def is_filtered_for_library(self, library: Library) -> bool:
        """Return whether this Work should be filtered (hidden) for the given Library.

        A work is filtered if its audience or any of its genres match the
        library's configured filtered_audiences or filtered_genres settings.

        :param library: The Library to check filtering for.
        :return: True if the work should be filtered (hidden), False otherwise.
        """
        settings = library.settings
        return self.is_filtered_by(
            settings.filtered_audiences, settings.filtered_genres
        )

    def set_presentation_edition(self, new_presentation_edition):
        """Sets presentation edition and lets owned pools and editions know.
        Raises exception if edition to set to is None.
        """
        # only bother if something changed, or if were explicitly told to
        # set (useful for setting to None)
        if not new_presentation_edition:
            error_message = (
                "Trying to set presentation_edition to None on Work [%s]" % self.id
            )
            raise ValueError(error_message)

        trigger_customlists_update = (
            not self.presentation_edition or self.presentation_edition is NO_VALUE
        )
        self.presentation_edition = new_presentation_edition

        # if the edition is the presentation edition for any license
        # pools, let them know they have a Work.
        for pool in self.presentation_edition.is_presentation_for:
            pool.work = self

        if trigger_customlists_update:
            add_work_to_customlists_for_collection(self)

    def calculate_presentation_edition(self, policy=None):
        """Which of this Work's Editions should be used as the default?
        First, every LicensePool associated with this work must have
        its presentation edition set.
        Then, we go through the pools, see which has the best presentation edition,
        and make it our presentation edition.
        """
        changed = False
        policy = policy or PresentationCalculationPolicy()
        if not policy.choose_edition:
            return changed

        # For each owned edition, see if its LicensePool was suppressed.
        # if yes, the edition is unlikely to be the best.
        edition_metadata_changed = False
        old_presentation_edition = self.presentation_edition
        new_presentation_edition = None

        for pool in self.license_pools:
            # Note:  making the assumption here that we won't have a situation
            # where we marked all of the work's pools as suppressed.
            if pool.suppressed:
                continue

            # make sure the pool has most up-to-date idea of its presentation edition,
            # and then ask what it is.
            pool_edition_changed = pool.set_presentation_edition()
            edition_metadata_changed = edition_metadata_changed or pool_edition_changed
            potential_presentation_edition = pool.presentation_edition

            # TODO: I'm leaving this next old comment behind. We may need to deal with
            #  this, since we've eliminated the "superceded" [sic] (superseded) concept:
            # We currently have no real way to choose between
            # competing presentation editions. But it doesn't matter much
            # because in the current system there should never be more
            # than one non-superceded license pool per Work.
            #
            # So basically we pick the first available edition and
            # make it the presentation edition.
            if not new_presentation_edition or (
                potential_presentation_edition is old_presentation_edition
                and old_presentation_edition
            ):
                # We would prefer not to change the Work's presentation
                # edition unnecessarily, so if the current presentation
                # edition is still an option, choose it.
                new_presentation_edition = potential_presentation_edition

        if (
            self.presentation_edition != new_presentation_edition
        ) and new_presentation_edition != None:
            # did we find a pool whose presentation edition was better than the work's?
            self.set_presentation_edition(new_presentation_edition)

        changed = (
            edition_metadata_changed
            or old_presentation_edition != self.presentation_edition
        )
        return changed

    def _get_default_audience(self) -> str | None:
        """Return the default audience.

        :return: Default audience
        """
        for license_pool in self.license_pools:
            if license_pool.collection.default_audience:
                return license_pool.collection.default_audience

        return None

    def calculate_presentation(
        self,
        policy=None,
        exclude_search=False,
        default_fiction=None,
        default_audience=None,
    ):
        """Make a Work ready to show to patrons.
        Call calculate_presentation_edition() to find the best-quality presentation edition
        that could represent this work.
        Then determine the following information, global to the work:
        * Subject-matter classifications for the work.
        * Whether or not the work is fiction.
        * The intended audience for the work.
        * The best available summary for the work.
        * The overall popularity of the work.
        """
        if not default_audience:
            default_audience = self._get_default_audience()

        # Gather information up front so we can see if anything
        # actually changed.
        changed = False
        edition_changed = False
        classification_changed = False

        policy = policy or PresentationCalculationPolicy()

        edition_changed = self.calculate_presentation_edition(policy)

        if not self.presentation_edition:
            # Without a presentation edition, we can't calculate presentation
            # for the work.
            return

        if policy.choose_cover or policy.set_edition_metadata:
            cover_changed = self.presentation_edition.calculate_presentation(policy)
            edition_changed = edition_changed or cover_changed

        old_summary = self.summary
        old_summary_text = self.summary_text
        old_quality = self.quality

        # If we find a cover or description that comes direct from a
        # license source, it may short-circuit the process of finding
        # a good cover or description.
        licensed_data_sources = set()
        for pool in self.license_pools:
            # Descriptions from Gutenberg are useless, so we
            # specifically exclude it from being a privileged data
            # source.
            if pool.data_source.name != DataSourceConstants.GUTENBERG:
                licensed_data_sources.add(pool.data_source)

        if policy.classify or policy.choose_summary or policy.calculate_quality:
            # Find all related IDs that might have associated descriptions,
            # classifications, or measurements.
            _db = Session.object_session(self)

            direct_identifier_ids = self._direct_identifier_ids
            all_identifier_ids = self.all_identifier_ids(policy=policy)
        else:
            # Don't bother.
            direct_identifier_ids = all_identifier_ids = []

        if policy.classify:
            classification_changed = self.assign_genres(
                all_identifier_ids,
                default_fiction=default_fiction,
                default_audience=default_audience,
            )

        if policy.choose_summary:
            self._choose_summary(
                direct_identifier_ids, all_identifier_ids, licensed_data_sources
            )

        if policy.calculate_quality:
            # In the absense of other data, we will make a rough
            # judgement as to the quality of a book based on the
            # license source. Commercial data sources have higher
            # default quality, because it's presumed that a librarian
            # put some work into deciding which books to buy.
            default_quality = None
            for source in licensed_data_sources:
                q = self.default_quality_by_data_source.get(source.name, None)
                if q is None:
                    continue
                if default_quality is None or q > default_quality:
                    default_quality = q

            if not default_quality:
                # if we still haven't found anything of a quality measurement,
                # then at least make it an integer zero, not none.
                default_quality = 0
            self.calculate_quality(all_identifier_ids, default_quality)

        if self.summary_text:
            if isinstance(self.summary_text, str):
                new_summary_text = self.summary_text
            else:
                new_summary_text = self.summary_text.decode("utf8")
        else:
            new_summary_text = self.summary_text

        changed = (
            edition_changed
            or classification_changed
            or old_summary != self.summary
            or old_summary_text != new_summary_text
            or (
                policy.calculate_quality
                and float(old_quality or default_quality)
                != float(self.quality or default_quality)
            )
        )

        if changed:
            # last_update_time tracks the last time the data actually
            # changed, not the last time we checked whether or not to
            # change it.
            self.last_update_time = utc_now()

        if (changed or policy.update_search_index) and not exclude_search:
            self.external_index_needs_updating()

        # Now that everything's calculated, print it out.
        if policy.verbose:
            if changed:
                changed = "changed"
                representation = self.detailed_representation
            else:
                # TODO: maybe change changed to a boolean, and return it as method result
                changed = "unchanged"
                representation = repr(self)
            self.log.info("Presentation %s for work: %s", changed, representation)

        # We want works to be presentation-ready as soon as possible,
        # unless they are missing crucial information like language or
        # title.
        self.set_presentation_ready_based_on_content()

    def _choose_summary(
        self, direct_identifier_ids, all_identifier_ids, licensed_data_sources
    ):
        """Helper method for choosing a summary as part of presentation
        calculation.

        Summaries closer to a LicensePool, or from a more trusted source
        will be preferred.

        :param direct_identifier_ids: All IDs of Identifiers of LicensePools
            directly associated with this Work. Summaries associated with
            these IDs will be preferred. In the real world, this will happen
            almost all the time.

        :param all_identifier_ids: All IDs of Identifiers of
            LicensePools associated (directly or indirectly) with this
            Work. Summaries associated with these IDs will be
            used only if none are found from direct_identifier_ids.

        :param licensed_data_sources: A list of DataSources that should be
            given priority -- either because they provided the books or because
            they are trusted sources such as library staff.
        """
        _db = Session.object_session(self)
        staff_data_source = DataSource.lookup(_db, DataSourceConstants.LIBRARY_STAFF)
        data_sources = [staff_data_source, licensed_data_sources]
        summary = None
        for id_set in (direct_identifier_ids, all_identifier_ids):
            summary, summaries = Identifier.evaluate_summary_quality(
                _db, id_set, data_sources
            )
            if summary:
                # We found a summary.
                break
        self.set_summary(summary)

    @property
    def detailed_representation(self):
        """A description of this work more detailed than repr()"""
        l = [f"{self.title} (by {self.author})"]
        l.append(" language=%s" % self.language)
        l.append(" quality=%s" % self.quality)

        if self.presentation_edition and self.presentation_edition.primary_identifier:
            primary_identifier = self.presentation_edition.primary_identifier
        else:
            primary_identifier = None
        l.append(" primary id=%s" % primary_identifier)
        if self.fiction:
            fiction = "Fiction"
        elif self.fiction == False:
            fiction = "Nonfiction"
        else:
            fiction = "???"
        if self.target_age and (self.target_age.upper or self.target_age.lower):
            target_age = " age=" + self.target_age_string
        else:
            target_age = ""
        l.append(
            " %(fiction)s a=%(audience)s%(target_age)r"
            % (dict(fiction=fiction, audience=self.audience, target_age=target_age))
        )
        l.append(" " + ", ".join(repr(wg) for wg in self.work_genres))

        if self.cover_full_url:
            l.append(" Full cover: %s" % self.cover_full_url)
        else:
            l.append(" No full cover.")

        if self.cover_thumbnail_url:
            l.append(" Cover thumbnail: %s" % self.cover_thumbnail_url)
        else:
            l.append(" No thumbnail cover.")

        downloads = []
        expect_downloads = False
        for pool in self.license_pools:
            if pool.open_access:
                expect_downloads = True
            for lpdm in pool.available_delivery_mechanisms:
                if lpdm.resource and lpdm.resource.final_url:
                    downloads.append(lpdm.resource)

        if downloads:
            l.append(" Open-access downloads:")
            for r in downloads:
                l.append("  " + r.final_url)
        elif expect_downloads:
            l.append(" Expected open-access downloads but found none.")

        def _ensure(s):
            if not s:
                return ""
            elif isinstance(s, str):
                return s
            else:
                return s.decode("utf8", "replace")

        if self.summary and self.summary.representation:
            snippet = _ensure(self.summary.representation.content)[:100]
            d = f" Description ({self.summary.quality:.2f}) {snippet}"
            l.append(d)

        l = [_ensure(s) for s in l]
        return "\n".join(l)

    def active_license_pool(self, library: Library | None = None) -> LicensePool | None:
        """
        Select the most appropriate license pool for this work.

        Pools are prioritized in the following order:
        1. Open access pools (unlimited free access)
        2. Unlimited access pools (unlimited licensed access)
        3. Regular pools sorted by licenses_available (highest first)

        When multiple pools have the same priority and availability,
        the pool with the lowest ID is selected for determinism.

        :param library: If provided, only consider pools from this library's collections
        :return: The selected license pool, or None if no suitable pool exists
        """
        # Filter pools by library's collections if specified
        collections = [] if not library else [c for c in library.active_collections]
        eligible_pools = []

        for p in self.license_pools:
            # Skip pools not in the library's collections
            if collections and p.collection not in collections:
                continue

            if not p.active_status:
                # Licensepools that are not active should not be considered
                continue

            edition = p.presentation_edition

            # Unlimited access pools are always eligible
            if p.unlimited_type:
                eligible_pools.append(p)

            # Regular pools need valid edition and owned licenses
            elif edition and edition.title and p.licenses_owned > 0:
                eligible_pools.append(p)

        if not eligible_pools:
            return None

        # Sort by priority (lower sort key = higher priority):
        # 1. open_access first (False=0 sorts before True=1, so negate)
        # 2. unlimited_access second (same logic)
        # 3. licenses_available third (negate for descending order)
        # 4. id fourth (ascending for determinism)
        eligible_pools.sort(
            key=lambda p: (
                not p.open_access,
                not p.unlimited_type,
                -p.licenses_available,
                p.id,
            )
        )

        return eligible_pools[0]

    def external_index_needs_updating(self) -> None:
        """Mark this work as needing to have its search document reindexed."""
        return self.queue_indexing(self.id)

    @staticmethod
    @inject
    def queue_indexing(
        work_id: int | None, *, redis_client: Redis = Provide["redis.client"]
    ):
        """
        Add a work to the set of works in redis waiting to be indexed.
        """
        from palace.manager.service.redis.models.search import WaitingForIndexing

        waiting = WaitingForIndexing(redis_client)
        if work_id is not None:
            waiting.add(work_id)

    def set_presentation_ready(self, as_of=None, exclude_search=False):
        """Set this work as presentation-ready, no matter what.

        This assumes that we know the work has the minimal information
        necessary to be found with typical queries and that patrons
        will be able to understand what work we're talking about.

        In most cases you should call set_presentation_ready_based_on_content
        instead, which runs those checks.
        """
        changed = False

        if not self.presentation_ready:
            self.presentation_ready = True
            changed = True

        if self.presentation_ready_exception is not None:
            self.presentation_ready_exception = None

        if as_of is not None:
            if self.presentation_ready_attempt != as_of:
                self.presentation_ready_attempt = as_of
        else:
            if self.presentation_ready_attempt is None or changed:
                self.presentation_ready_attempt = utc_now()

        if not exclude_search:
            self.external_index_needs_updating()

    def set_presentation_ready_based_on_content(self):
        """Set this work as presentation ready, if it appears to
        be ready based on its data.

        Presentation ready means the book is ready to be shown to
        patrons and (pending availability) checked out. It doesn't
        necessarily mean the presentation is complete.

        The absolute minimum data necessary is a title, a language,
        and a medium. We don't need a cover or an author -- we can
        fill in that info later if it exists.
        """
        if (
            not self.presentation_edition
            or not self.license_pools
            or not self.title
            or not self.language
            or not self.presentation_edition.medium
        ):
            if self.presentation_ready:
                self.presentation_ready = False
            self.external_index_needs_updating()
            self.log.warning("Work is not presentation ready: %r", self)
        else:
            self.set_presentation_ready()

    def calculate_quality(self, identifier_ids, default_quality=0):
        _db = Session.object_session(self)
        # Relevant Measurements are direct measurements of popularity
        # and quality, plus any quantity that might be mapppable to the 0..1
        # range -- ratings, and measurements with an associated percentile
        # score.
        quantities = {Measurement.POPULARITY, Measurement.QUALITY, Measurement.RATING}
        quantities = quantities.union(list(Measurement.PERCENTILE_SCALES.keys()))
        measurements = (
            _db.query(Measurement)
            .filter(Measurement.identifier_id.in_(identifier_ids))
            .filter(Measurement.is_most_recent == True)
            .filter(Measurement.quantity_measured.in_(quantities))
            .all()
        )

        new_quality = Measurement.overall_quality(
            measurements, default_value=default_quality
        )

        if new_quality != self.quality:
            self.quality = new_quality

    def assign_genres(
        self,
        identifier_ids,
        default_fiction=False,
        default_audience=Classifier.AUDIENCE_ADULT,
    ):
        """Set classification information for this work based on the
        subquery to get equivalent identifiers.
        :return: A boolean explaining whether or not any data actually
        changed.
        """
        classifier = WorkClassifier(self)

        old_fiction = self.fiction
        old_audience = self.audience
        old_target_age = self.target_age

        _db = Session.object_session(self)
        classifications = Identifier.classifications_for_identifier_ids(
            _db, identifier_ids
        )
        for classification in classifications:
            classifier.add(classification)

        (genre_weights, new_fiction, new_audience, target_age) = classifier.classify(
            default_fiction=default_fiction, default_audience=default_audience
        )

        new_target_age = tuple_to_numericrange(target_age)
        if self.target_age != new_target_age:
            self.target_age = new_target_age

        if new_fiction != old_fiction:
            self.fiction = new_fiction
        if new_audience != old_audience:
            self.audience = new_audience

        workgenres, workgenres_changed = self.assign_genres_from_weights(genre_weights)

        classification_changed = (
            workgenres_changed
            or old_fiction != self.fiction
            or old_audience != self.audience
            or numericrange_to_tuple(old_target_age) != target_age
        )

        return classification_changed

    def assign_genres_from_weights(self, genre_weights):
        # Assign WorkGenre objects to the remainder.
        from palace.manager.sqlalchemy.model.classification import Genre

        changed = False
        _db = Session.object_session(self)
        total_genre_weight = float(sum(genre_weights.values()))
        workgenres = []
        current_workgenres = _db.query(WorkGenre).filter(WorkGenre.work == self)
        by_genre = dict()
        for wg in current_workgenres:
            by_genre[wg.genre] = wg
        for g, score in list(genre_weights.items()):
            affinity = score / total_genre_weight
            if not isinstance(g, Genre):
                g, ignore = Genre.lookup(_db, g.name)
            if g in by_genre:
                wg = by_genre[g]
                is_new = False
                del by_genre[g]
            else:
                wg, is_new = get_one_or_create(_db, WorkGenre, work=self, genre=g)
            if is_new or round(wg.affinity, 2) != round(affinity, 2):
                changed = True
            wg.affinity = affinity
            workgenres.append(wg)

        # Any WorkGenre objects left over represent genres the Work
        # was once classified under, but is no longer. Delete them.
        for wg in list(by_genre.values()):
            _db.delete(wg)
            changed = True

        # ensure that work_genres is up to date without having to read from database again
        if self.work_genres != workgenres:
            self.work_genres = workgenres

        return workgenres, changed

    def assign_appeals(self, character, language, setting, story, cutoff=0.20):
        """Assign the given appeals to the corresponding database fields,
        as well as calculating the primary and secondary appeal.
        """
        self.appeal_character = character
        self.appeal_language = language
        self.appeal_setting = setting
        self.appeal_story = story

        c = Counter()
        c[self.CHARACTER_APPEAL] = character
        c[self.LANGUAGE_APPEAL] = language
        c[self.SETTING_APPEAL] = setting
        c[self.STORY_APPEAL] = story
        primary, secondary = c.most_common(2)
        if primary[1] > cutoff:
            self.primary_appeal = primary[0]
        else:
            self.primary_appeal = self.UNKNOWN_APPEAL

        if secondary[1] > cutoff:
            self.secondary_appeal = secondary[0]
        else:
            self.secondary_appeal = self.NO_APPEAL

    # This can be used in func.to_char to convert a SQL datetime into a string
    # that Opensearch can parse as a date.
    OPENSEARCH_TIME_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS"."MS'

    @classmethod
    def to_search_documents(
        cls, session: Session, work_ids: Sequence[int]
    ) -> Sequence[SearchDocument]:
        """In app to search documents needed to ease off the burden
        of complex queries from the DB cluster
        No recursive identifier policy is taken here as using the
        RecursiveEquivalentsCache implicitly has that set
        """
        if not work_ids:
            return []

        qu = session.query(Work).filter(Work.id.in_(work_ids))
        qu = qu.options(
            joinedload(Work.presentation_edition)
            .joinedload(Edition.contributions)
            .joinedload(Contribution.contributor),
            joinedload(Work.suppressed_for),
            joinedload(Work.work_genres).joinedload(WorkGenre.genre),
            joinedload(Work.custom_list_entries),
        )

        rows: list[Work] = qu.all()

        ## IDENTIFIERS START
        ## Identifiers is a house of cards, it comes crashing down if anything is changed here
        ## We need a WITH statement selecting the procedural function on works_alias
        ## then proceed to select the Identifiers for each of those outcomes
        ## but must match works to the equivalent ids for which they were chosen
        ## The same nonsense will be required for classifications
        ## TODO: move this equivalence code into another job based on its required frequency
        ## Add it to another table so it becomes faster to just query the pre-computed table

        equivalent_identifiers = (
            session.query(RecursiveEquivalencyCache)
            .join(
                Edition,
                Edition.primary_identifier_id
                == RecursiveEquivalencyCache.parent_identifier_id,
            )
            .join(Work, Work.presentation_edition_id == Edition.id)
            .filter(Work.id.in_(work_ids))
            .with_entities(
                Work.id.label("work_id"),
                RecursiveEquivalencyCache.identifier_id.label("equivalent_id"),
            )
            .cte("equivalent_cte")
        )

        identifiers_query = select(
            equivalent_identifiers.c.work_id,
            Identifier.identifier,
            Identifier.type,
        ).join_from(
            Identifier,
            equivalent_identifiers,
            Identifier.id == literal_column("equivalent_cte.equivalent_id"),
        )

        identifiers = list(session.execute(identifiers_query))
        ## IDENTIFIERS END

        ## CLASSIFICATION START
        ## Copied almost exactly from the previous implementation
        ## Through trials in the local environment we find this to add
        ## about 30% to the SQL CPU usage, this section holds back improvements
        ## for the entire script
        ## TODO: Improve this, maybe only run this section once a day???
        # Map our constants for Subject type to their URIs.
        scheme_column: Any = case(
            *[
                (Subject.type == key, literal_column("'%s'" % val))
                for key, val in list(Subject.uri_lookup.items())
            ]
        )

        # If the Subject has a name, use that, otherwise use the Subject's identifier.
        # Also, 3M's classifications have slashes, e.g. "FICTION/Adventure". Make sure
        # we get separated words for search.
        term_column = func.replace(
            case((Subject.name != None, Subject.name), else_=Subject.identifier),
            "/",
            " ",
        )

        # Normalize by dividing each weight by the sum of the weights for that Identifier's Classifications.
        weight_column = (
            func.sum(Classification.weight)
            / func.sum(func.sum(Classification.weight)).over()
        )

        subjects = (
            select(
                equivalent_identifiers.c.work_id,
                scheme_column.label("scheme"),
                term_column.label("term"),
                weight_column.label("weight"),
            )
            .where(
                # Only include Subjects with terms that are useful for search.
                and_(Subject.type.in_(Subject.TYPES_FOR_SEARCH), term_column != None),
            )
            .group_by(scheme_column, term_column, equivalent_identifiers.c.work_id)
            .join_from(
                Classification,
                equivalent_identifiers,
                Classification.identifier_id
                == literal_column("equivalent_cte.equivalent_id"),
            )
            .join_from(Classification, Subject, Classification.subject_id == Subject.id)
        )

        all_subjects = list(session.execute(subjects))

        ## CLASSIFICATION END

        # Create JSON
        results = []
        for item in rows:
            item.identifiers = list(filter(lambda idx: idx[0] == item.id, identifiers))  # type: ignore
            item.classifications = list(  # type: ignore
                filter(lambda idx: idx[0] == item.id, all_subjects)
            )

            try:
                search_doc = cls.search_doc_as_dict(cast(Self, item))
                results.append(search_doc)
            except:
                cls.logger().exception(f"Could not create search document for {item}")

        return results

    @classmethod
    def search_doc_as_dict(cls, doc: Self) -> dict[str, Any]:
        columns = {
            "work": [
                "fiction",
                "audience",
                "quality",
                "rating",
                "popularity",
                "presentation_ready",
                "last_update_time",
            ],
            "edition": [
                "title",
                "subtitle",
                "series",
                "series_position",
                "language",
                "sort_title",
                "author",
                "sort_author",
                "medium",
                "publisher",
                "imprint",
                "permanent_work_id",
                "published",
            ],
            "contribution": ["role"],
            "contributor": ["display_name", "sort_name", "family_name", "lc", "viaf"],
            "licensepools": [
                "data_source_id",
                "collection_id",
                "open_access",
                "suppressed",
                "availability_time",
                "type",
                "status",
                "unlimited_type",
                "metered_or_equivalent_type",
                "active_status",
            ],
            "identifiers": ["type", "identifier"],
            "classifications": ["scheme", "term", "weight"],
            "custom_list_entries": ["list_id", "featured", "first_appearance"],
        }

        result: dict = {}

        def _convert(value):
            if isinstance(value, Decimal):
                return float(value)
            elif isinstance(value, datetime):
                try:
                    # If we do not have a timezone, force UTC
                    if value.tzinfo is None:
                        value = value.replace(tzinfo=pytz.UTC)
                    return value.timestamp()
                except (ValueError, OverflowError) as e:
                    cls.logger().error(
                        f"Could not convert date value {value} for document {doc.id}: {e}"
                    )
                    return 0
            elif isinstance(value, date):
                try:
                    return datetime(
                        value.year, value.month, value.day, tzinfo=pytz.UTC
                    ).timestamp()
                except (ValueError, OverflowError) as e:
                    cls.logger().error(
                        f"Could not convert date value {value} for document {doc.id}: {e}"
                    )
                    return 0
            return value

        def _set_value(parent, key, target):
            if parent:
                for c in columns[key]:
                    val = getattr(parent, c)
                    target[c] = _convert(val)

        _set_value(doc, "work", result)
        result["_id"] = getattr(doc, "id")
        result["work_id"] = getattr(doc, "id")
        result["summary"] = getattr(doc, "summary_text")
        result["suppressed_for"] = [int(l.id) for l in getattr(doc, "suppressed_for")]
        result["fiction"] = (
            "fiction" if getattr(doc, "fiction") is True else "nonfiction"
        )
        if result["audience"]:
            result["audience"] = result["audience"].replace(" ", "")

        target_age = doc.target_age
        result["target_age"] = {"lower": None, "upper": None}
        if target_age and target_age.lower is not None:
            result["target_age"]["lower"] = target_age.lower + (
                0 if target_age.lower_inc else 1
            )
        if target_age and target_age.upper is not None:
            result["target_age"]["upper"] = target_age.upper - (
                0 if target_age.upper_inc else 1
            )

        if doc.presentation_edition:
            _set_value(doc.presentation_edition, "edition", result)

        result["contributors"] = []
        if doc.presentation_edition and doc.presentation_edition.contributions:
            for contribution in doc.presentation_edition.contributions:
                contributor: dict = {}
                _set_value(contribution.contributor, "contributor", contributor)
                _set_value(contribution, "contribution", contributor)
                result["contributors"].append(contributor)

        result["licensepools"] = []
        if doc.license_pools:
            for license_pool in doc.license_pools:
                if not license_pool.active_status:
                    continue

                lc: dict = {}
                _set_value(license_pool, "licensepools", lc)
                lc["available"] = license_pool.unlimited_type or (
                    license_pool.metered_or_equivalent_type
                    and license_pool.licenses_available > 0
                )
                lc["licensed"] = (
                    license_pool.metered_or_equivalent_type
                    or license_pool.unlimited_non_open_access_type
                )
                if doc.presentation_edition:
                    lc["medium"] = doc.presentation_edition.medium
                lc["licensepool_id"] = license_pool.id
                lc["quality"] = doc.quality
                collection_settings = (
                    license_pool.collection.integration_configuration.settings_dict
                )
                lc["lane_priority_level"] = collection_settings.get(
                    "lane_priority_level",
                    IntegrationConfigurationConstants.DEFAULT_LANE_PRIORITY_LEVEL,
                )
                result["licensepools"].append(lc)
            # use the maximum lane priority level associated with the work.
            if result["licensepools"]:
                result["lane_priority_level"] = max(
                    [lc["lane_priority_level"] for lc in result["licensepools"]]
                )
            else:
                result["lane_priority_level"] = (
                    IntegrationConfigurationConstants.DEFAULT_LANE_PRIORITY_LEVEL
                )

        # Extra special genre massaging
        result["genres"] = []
        if doc.work_genres:
            for work_genre in doc.work_genres:
                genre = {
                    "scheme": Subject.SIMPLIFIED_GENRE,
                    "term": work_genre.genre.id,
                    "name": work_genre.genre.name,
                    "weight": work_genre.affinity,
                }
                result["genres"].append(genre)

        result["identifiers"] = []
        if doc.identifiers:  # type: ignore[attr-defined]
            for ident in doc.identifiers:  # type: ignore[attr-defined]
                identifier: dict = {}
                _set_value(ident, "identifiers", identifier)
                result["identifiers"].append(identifier)

        result["classifications"] = []
        if doc.classifications:  # type: ignore[attr-defined]
            for _classification in doc.classifications:  # type: ignore[attr-defined]
                classification: dict = {}
                _set_value(_classification, "classifications", classification)
                result["classifications"].append(classification)

        result["customlists"] = []
        if doc.custom_list_entries:
            for custom_list_entry in doc.custom_list_entries:
                customlist: dict = {}
                _set_value(custom_list_entry, "custom_list_entries", customlist)
                result["customlists"].append(customlist)

        # No empty lists, they should be null
        for key, val in result.items():
            if val == []:
                result[key] = None

        return result

    @classmethod
    def target_age_query(self, foreign_work_id_field):
        # If the upper limit of the target age is inclusive, we leave
        # it alone. Otherwise, we subtract one to make it inclusive.
        upper_field = func.upper(Work.target_age)
        upper = case(
            (func.upper_inc(Work.target_age), upper_field), else_=upper_field - 1
        ).label("upper")

        # If the lower limit of the target age is inclusive, we leave
        # it alone. Otherwise, we add one to make it inclusive.
        lower_field = func.lower(Work.target_age)
        lower = case(
            (func.lower_inc(Work.target_age), lower_field), else_=lower_field + 1
        ).label("lower")

        # Subquery for target age. This has to be a subquery so it can
        # become a nested object in the final json.
        target_age = select(upper, lower).where(Work.id == foreign_work_id_field)
        return target_age

    def to_search_document(self) -> dict[str, Any]:
        """Generate a search document for this Work."""
        db = Session.object_session(self)
        if self.id is None:
            raise BasePalaceException(
                "Work has no ID. Cannot generate search document."
            )

        return Work.to_search_documents(db, [self.id])[0]

    @classmethod
    def restrict_to_custom_lists_from_data_source(
        cls, _db, base_query, data_source, on_list_as_of=None
    ):
        """Annotate a query that joins Work against Edition to match only
        Works that are on a custom list from the given data source."""

        from palace.manager.sqlalchemy.model.customlist import CustomList

        condition = CustomList.data_source == data_source
        return cls._restrict_to_customlist_subquery_condition(
            _db, base_query, condition, on_list_as_of
        )

    @classmethod
    def restrict_to_custom_lists(
        cls, _db, base_query, custom_lists, on_list_as_of=None
    ):
        """Annotate a query that joins Work against Edition to match only
        Works that are on one of the given custom lists."""
        from palace.manager.sqlalchemy.model.customlist import CustomList

        condition = CustomList.id.in_([x.id for x in custom_lists])
        return cls._restrict_to_customlist_subquery_condition(
            _db, base_query, condition, on_list_as_of
        )

    @classmethod
    def _restrict_to_customlist_subquery_condition(
        cls, _db, base_query, condition, on_list_as_of=None
    ):
        """Annotate a query that joins Work against Edition to match only
        Works that are on a custom list from the given data source."""
        # Find works that are on a list that meets the given condition.
        from palace.manager.sqlalchemy.model.customlist import CustomListEntry

        qu = base_query.join(LicensePool.custom_list_entries).join(
            CustomListEntry.customlist
        )
        if on_list_as_of:
            qu = qu.filter(CustomListEntry.most_recent_appearance >= on_list_as_of)
        qu = qu.filter(condition)
        return qu

    def classifications_with_genre(self):
        from palace.manager.sqlalchemy.model.classification import (
            Classification,
            Subject,
        )

        _db = Session.object_session(self)
        identifier = self.presentation_edition.primary_identifier
        return (
            _db.query(Classification)
            .join(Subject)
            .filter(Classification.identifier_id == identifier.id)
            .filter(Subject.genre_id != None)
            .order_by(Classification.weight.desc())
        )

    def top_genre(self):
        from palace.manager.sqlalchemy.model.classification import Genre

        _db = Session.object_session(self)
        genre = (
            _db.query(Genre)
            .join(WorkGenre)
            .filter(WorkGenre.work_id == self.id)
            .order_by(WorkGenre.affinity.desc())
            .first()
        )
        return genre.name if genre else None

    @inject
    def delete(
        self, *, search_index: ExternalSearchIndex = Provide["search.index"]
    ) -> None:
        """Delete the work from both the DB and search index."""
        _db = Session.object_session(self)
        try:
            search_index.remove_work(self)
        except opensearchpy.exceptions.NotFoundError:
            self.log.warning(
                f"Work {self.id} not found in search index while attempting to delete it."
            )
        _db.delete(self)


work_library_suppressions = Table(
    "work_library_suppressions",
    Base.metadata,
    Column("work_id", ForeignKey("works.id", ondelete="CASCADE"), primary_key=True),
    Column(
        "library_id", ForeignKey("libraries.id", ondelete="CASCADE"), primary_key=True
    ),
)


def add_work_to_customlists_for_collection(pool_or_work: LicensePool | Work) -> None:
    """Add a work to all customlists associated with its collection(s).

    This function collects all customlists from all license pools and processes
    them in a consistent order (by customlist ID) to prevent deadlocks when
    multiple concurrent Celery tasks are adding different works to overlapping
    customlists.
    """
    work: Work | None
    if isinstance(pool_or_work, Work):
        work = pool_or_work
        pools = work.license_pools
    else:
        work = pool_or_work.work
        pools = [pool_or_work]

    if work and work.presentation_edition:
        # Collect all unique customlists from all pools first, then sort by ID.
        # This ensures a consistent lock acquisition order across all concurrent
        # workers, preventing deadlocks. Without this, workers processing works
        # with pools from different collections might iterate customlists in
        # different orders, causing circular lock dependencies.
        all_customlists: dict[int, CustomList] = {}
        for pool in pools:
            if not pool.collection:
                # This shouldn't happen, but don't crash if it does --
                # the correct behavior is that the work not be added to
                # any CustomLists.
                continue
            for customlist in pool.collection.customlists:
                all_customlists[customlist.id] = customlist

        # Process customlists in ascending ID order for consistent lock ordering
        for customlist_id in sorted(all_customlists.keys()):
            customlist = all_customlists[customlist_id]
            # This function is intended to be called during initial work setup,
            # when an index update will already be triggered by the work's
            # creation or configuration. We skip the redundant index update here.
            customlist.add_entry(work, featured=True, update_external_index=False)
