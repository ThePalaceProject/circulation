from __future__ import annotations

import datetime
from collections import defaultdict
from typing import Any

from pydantic import Field, field_validator, model_validator
from sqlalchemy import and_
from sqlalchemy.orm import Query, Session
from typing_extensions import Self

from palace.manager.core.classifier import NO_NUMBER, NO_VALUE
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.base.mutable import BaseMutableData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.link import LinkData
from palace.manager.data_layer.measurement import MeasurementData
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.data_layer.subject import SubjectData
from palace.manager.sqlalchemy.constants import LinkRelations
from palace.manager.sqlalchemy.model.classification import Classification
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.coverage import CoverageRecord
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool, RightsStatus
from palace.manager.sqlalchemy.model.resource import Hyperlink, Resource
from palace.manager.sqlalchemy.util import get_one, get_one_or_create
from palace.manager.util.languages import LanguageCodes
from palace.manager.util.median import median

_BASIC_EDITION_FIELDS: list[str] = [
    "title",
    "sort_title",
    "subtitle",
    "language",
    "medium",
    "duration",
    "series",
    "series_position",
    "publisher",
    "imprint",
    "issued",
    "published",
]

_REL_REQUIRES_NEW_PRESENTATION_EDITION: list[str] = [
    LinkRelations.IMAGE,
    LinkRelations.THUMBNAIL_IMAGE,
]
_REL_REQUIRES_FULL_RECALCULATION: list[str] = [LinkRelations.DESCRIPTION]


class BibliographicData(BaseMutableData):
    """A (potentially partial) set of bibliographic data for a published work."""

    title: str | None = None
    subtitle: str | None = None
    sort_title: str | None = None
    language: str | None = None
    medium: str | None = None
    series: str | None = None
    series_position: int | None = None
    publisher: str | None = None
    imprint: str | None = None
    issued: datetime.date | None = None
    published: datetime.date | None = None
    identifiers: list[IdentifierData] = Field(default_factory=list)
    subjects: list[SubjectData] = Field(default_factory=list)
    contributors: list[ContributorData] = Field(default_factory=list)
    measurements: list[MeasurementData] = Field(default_factory=list)
    links: list[LinkData] = Field(default_factory=list)
    data_source_last_updated: datetime.datetime | None = None
    duration: float | None = None
    permanent_work_id: str | None = None
    # Note: brought back to keep callers of bibliographic extraction process_one() methods simple.
    circulation: CirculationData | None = None

    @field_validator("language")
    @classmethod
    def _convert_langage_alpha3(cls, value: str | None) -> str | None:
        if value is not None:
            value = LanguageCodes.string_to_alpha_3(value)
        return value

    @field_validator("links")
    @classmethod
    def _filter_links(cls, links: list[LinkData]) -> list[LinkData]:
        return [link for link in links if link.rel in Hyperlink.BIBLIOGRAPHIC_ALLOWED]

    @model_validator(mode="after")
    def _primary_identifier_in_identifiers(self) -> Self:
        if (
            self.primary_identifier_data
            and self.primary_identifier_data not in self.identifiers
        ):
            self.identifiers.append(self.primary_identifier_data)

        return self

    @classmethod
    def from_edition(cls, edition: Edition) -> BibliographicData:
        """Create a basic BibliographicData object for the given Edition.

        This doesn't contain everything but it contains enough
        information to run guess_license_pools.
        """
        kwargs: dict[str, Any] = dict()
        for field in _BASIC_EDITION_FIELDS:
            kwargs[field] = getattr(edition, field)

        contributors: list[ContributorData] = []
        for contribution in edition.contributions:
            contributor = ContributorData.from_contribution(contribution)
            contributors.append(contributor)

        if not edition.contributions:
            # This should only happen for low-quality data sources such as
            # the NYT best-seller API.
            if edition.sort_author and edition.sort_author != Edition.UNKNOWN_AUTHOR:
                contributors.append(
                    ContributorData(
                        sort_name=edition.sort_author,
                        display_name=edition.author,
                        roles=[Contributor.Role.PRIMARY_AUTHOR],
                    )
                )

        i = edition.primary_identifier
        primary_identifier = IdentifierData.from_identifier(i)

        links: list[LinkData] = []
        for link in i.links:
            link_data = LinkData(rel=link.rel, href=link.resource.url)
            links.append(link_data)

        return BibliographicData(
            data_source_name=edition.data_source.name,
            primary_identifier_data=primary_identifier,
            contributors=contributors,
            links=links,
            **kwargs,
        )

    @property
    def primary_author(self) -> ContributorData | None:
        primary_author = None
        for tier in Contributor.author_contributor_tiers():
            for c in self.contributors:
                for role in tier:
                    if role in c.roles:
                        primary_author = c
                        break
                if primary_author:
                    break
            if primary_author:
                break
        return primary_author

    def update(self, bibliographic: BibliographicData) -> None:
        """Update this BibliographicData object with values from the
        given BibliographicData object.

        TODO: We might want to take a policy object as an argument.
        """

        fields = _BASIC_EDITION_FIELDS
        for field in fields:
            new_value = getattr(bibliographic, field)
            if new_value != None and new_value != "":
                setattr(self, field, new_value)

        new_value = getattr(bibliographic, "contributors")
        if new_value and isinstance(new_value, list):
            old_value = getattr(self, "contributors")
            # if we already have a better value, don't override it with a "missing info" placeholder value
            if not (old_value and new_value[0].sort_name == Edition.UNKNOWN_AUTHOR):
                setattr(self, "contributors", new_value)

    def calculate_permanent_work_id(self, _db: Session) -> str | None:
        """Try to calculate a permanent work ID from this BibliographicData."""
        primary_author = self.primary_author

        if not primary_author:
            return None

        sort_author = primary_author.sort_name
        pwid = Edition.calculate_permanent_work_id_for_title_and_author(
            self.title, sort_author, "book"
        )
        self.permanent_work_id = pwid
        return pwid  # type: ignore[no-any-return]

    def associate_with_identifiers_based_on_permanent_work_id(
        self, _db: Session
    ) -> None:
        """Try to associate this object's primary identifier with
        the primary identifiers of Editions in the database which share
        a permanent work ID.
        """
        if not self.primary_identifier_data or not self.permanent_work_id:
            # We don't have the information necessary to carry out this
            # task.
            return

        if not self.medium:
            # We don't know the medium of this item, and we only want
            # to associate it with other items of the same type.
            return

        primary_identifier = self.load_primary_identifier(_db)

        # Try to find the primary identifiers of other Editions with
        # the same permanent work ID and the same medium, representing
        # books already in our collection.
        qu = (
            _db.query(Identifier)
            .join(Identifier.primarily_identifies)
            .filter(Edition.permanent_work_id == self.permanent_work_id)
            .filter(Identifier.type.in_(Identifier.LICENSE_PROVIDING_IDENTIFIER_TYPES))
            .filter(Edition.medium == self.medium)
        )
        identifiers_same_work_id = qu.all()
        for same_work_id in identifiers_same_work_id:
            if (
                same_work_id.type != self.primary_identifier_data.type
                or same_work_id.identifier != self.primary_identifier_data.identifier
            ):
                self.log.info(
                    "Discovered that %r is equivalent to %r because of matching permanent work ID %s",
                    same_work_id,
                    primary_identifier,
                    self.permanent_work_id,
                )
                primary_identifier.equivalent_to(
                    self.load_data_source(_db), same_work_id, 0.85
                )

    def edition(self, _db: Session) -> tuple[Edition, bool]:
        """Find or create the edition described by this BibliographicData object."""
        if not self.primary_identifier_data:
            raise PalaceValueError(
                "Cannot find edition: BibliographicData has no primary identifier."
            )

        data_source = self.load_data_source(_db)

        return Edition.for_foreign_id(
            _db,
            data_source,
            self.primary_identifier_data.type,
            self.primary_identifier_data.identifier,
        )

    def consolidate_identifiers(self) -> None:
        by_weight: defaultdict[tuple[str, str], list[float]] = defaultdict(list)
        for i in self.identifiers:
            by_weight[(i.type, i.identifier)].append(i.weight)
        new_identifiers: list[IdentifierData] = []
        for (type, identifier), weights in list(by_weight.items()):
            new_identifiers.append(
                IdentifierData(type=type, identifier=identifier, weight=median(weights))
            )
        self.identifiers = new_identifiers

    def guess_license_pools(self, _db: Session) -> dict[LicensePool, float]:
        """Try to find existing license pools for this BibliographicData."""
        potentials: dict[LicensePool, float] = {}
        for contributor in self.contributors:
            if not any(
                x in contributor.roles
                for x in (Contributor.Role.AUTHOR, Contributor.Role.PRIMARY_AUTHOR)
            ):
                continue
            contributor_sort_name = contributor.find_sort_name(_db)

            base = (
                _db.query(Edition)
                .filter(Edition.title.ilike(self.title))
                .filter(Edition.medium == Edition.BOOK_MEDIUM)
            )

            # A match based on work ID is the most reliable.
            pwid = self.calculate_permanent_work_id(_db)
            clause = and_(
                Edition.data_source_id == LicensePool.data_source_id,
                Edition.primary_identifier_id == LicensePool.identifier_id,
            )
            qu = base.filter(Edition.permanent_work_id == pwid).join(
                LicensePool, clause
            )
            success = self._run_query(qu, potentials, 0.95)
            if not success and contributor_sort_name:
                qu = base.filter(Edition.sort_author == contributor_sort_name)
                success = self._run_query(qu, potentials, 0.9)
            if not success and contributor.display_name:
                qu = base.filter(Edition.author == contributor.display_name)
                success = self._run_query(qu, potentials, 0.8)
            if not success:
                # Look for the book by an unknown author (our mistake)
                qu = base.filter(Edition.author == Edition.UNKNOWN_AUTHOR)
                success = self._run_query(qu, potentials, 0.45)
            if not success:
                # See if there is any book with this title at all.
                success = self._run_query(base, potentials, 0.3)
        return potentials

    def _run_query(
        self,
        qu: Query[Edition],
        potentials: dict[LicensePool, float],
        confidence: float,
    ) -> bool:
        success = False
        for i in qu:
            pools = i.license_pools
            for lp in pools:
                if lp and lp.deliverable and potentials.get(lp, 0) < confidence:
                    potentials[lp] = confidence
                    success = True
        return success

    def apply(
        self,
        db: Session,
        edition: Edition,
        collection: Collection | None,
        replace: ReplacementPolicy | None = None,
    ) -> tuple[Edition, bool]:
        """Apply this BibliographicData to the given edition.

        :return: (edition, made_core_changes), where edition is the newly-updated object, and made_core_changes
            answers the question: were any edition core fields harmed in the making of this update?
            So, if title changed, return True.
            New: If contributors changed, this is now considered a core change,
            so work.simple_opds_feed refresh can be triggered.
        """
        # If summary, subjects, or measurements change, then any Work
        # associated with this edition will need a full presentation
        # recalculation.
        work_requires_full_recalculation = False

        # If any other data changes, then any Work associated with
        # this edition will need to have its presentation edition
        # regenerated, but we can do it on the cheap.
        work_requires_new_presentation_edition = False

        if replace is None:
            replace = ReplacementPolicy()

        # We were given an Edition, so either this BibliographicData's
        # primary_identifier must be missing or it must match the
        # Edition's primary identifier.
        if self.primary_identifier_data:
            if (
                self.primary_identifier_data.type != edition.primary_identifier.type
                or self.primary_identifier_data.identifier
                != edition.primary_identifier.identifier
            ):
                raise PalaceValueError(
                    "BibliographicData's primary identifier (%s/%s) does not match edition's primary identifier (%r)"
                    % (
                        self.primary_identifier_data.type,
                        self.primary_identifier_data.identifier,
                        edition.primary_identifier,
                    )
                )

        # Check whether we should do any work at all.
        data_source = self.load_data_source(db)

        if self.data_source_last_updated and not replace.even_if_not_apparently_updated:
            coverage_record = CoverageRecord.lookup(edition, data_source)
            if coverage_record:
                check_time = coverage_record.timestamp
                last_time = self.data_source_last_updated
                if check_time >= last_time:
                    # The BibliographicData has not changed since last time. Do nothing.
                    return edition, False

        identifier = edition.primary_identifier

        self.log.info("APPLYING BIBLIOGRAPHIC DATA TO EDITION: %s", self.title)
        fields = _BASIC_EDITION_FIELDS + ["permanent_work_id"]
        for field in fields:
            old_edition_value = getattr(edition, field)
            new_bibliographic_value = getattr(self, field)
            if (
                new_bibliographic_value != None
                and new_bibliographic_value != ""
                and (new_bibliographic_value != old_edition_value)
            ):
                if new_bibliographic_value in [NO_VALUE, NO_NUMBER]:
                    new_bibliographic_value = None
                setattr(edition, field, new_bibliographic_value)
                work_requires_new_presentation_edition = True

        # Create equivalencies between all given identifiers and
        # the edition's primary identifier.
        contributors_changed = self.update_contributions(
            db, edition, replace.contributions
        )
        if contributors_changed:
            work_requires_new_presentation_edition = True

        # TODO: remove equivalencies when replace.identifiers is True.
        if self.identifiers is not None:
            for identifier_data in self.identifiers:
                if not identifier_data.identifier:
                    continue
                if (
                    identifier_data.identifier == identifier.identifier
                    and identifier_data.type == identifier.type
                ):
                    # These are the same identifier.
                    continue
                new_identifier, ignore = Identifier.for_foreign_id(
                    db, identifier_data.type, identifier_data.identifier
                )
                identifier.equivalent_to(
                    data_source, new_identifier, identifier_data.weight
                )

        new_subjects = set(self.subjects if self.subjects else [])
        if replace.subjects:
            # Remove any old Subjects from this data source, unless they
            # are also in the list of new subjects.
            surviving_classifications = []

            def _key(
                classification: Classification,
            ) -> SubjectData:
                s = classification.subject
                return SubjectData(
                    type=s.type,
                    identifier=s.identifier,
                    name=s.name,
                    weight=classification.weight,
                )

            for classification in identifier.classifications:
                if classification.data_source == data_source:
                    key = _key(classification)
                    if not key in new_subjects:
                        # The data source has stopped claiming that
                        # this classification should exist.
                        db.delete(classification)
                        work_requires_full_recalculation = True
                    else:
                        # The data source maintains that this
                        # classification is a good idea. We don't have
                        # to do anything.
                        new_subjects.remove(key)
                        surviving_classifications.append(classification)
                else:
                    # This classification comes from some other data
                    # source.  Don't mess with it.
                    surviving_classifications.append(classification)
            identifier.classifications = surviving_classifications

        # Apply all new subjects to the identifier.
        for subject in new_subjects:
            try:
                identifier.classify(
                    data_source,
                    subject.type,
                    subject.identifier,
                    subject.name,
                    weight=subject.weight,
                )
                work_requires_full_recalculation = True
            except ValueError as e:
                self.log.error(
                    f"Error classifying subject: {subject} for identifier {identifier}: {e}"
                )

        # Associate all links with the primary identifier.
        if replace.links and self.links is not None:
            surviving_hyperlinks = []
            dirty = False
            for hyperlink in identifier.links:
                if hyperlink.data_source == data_source:
                    db.delete(hyperlink)
                    dirty = True
                else:
                    surviving_hyperlinks.append(hyperlink)
            if dirty:
                identifier.links = surviving_hyperlinks

        link_objects = {}

        for link in self.links:
            if link.rel in Hyperlink.BIBLIOGRAPHIC_ALLOWED:
                original_resource = None
                if link.original:
                    rights_status = RightsStatus.lookup(db, link.original.rights_uri)
                    original_resource, ignore = get_one_or_create(
                        db,
                        Resource,
                        url=link.original.href,
                    )
                    if not original_resource.data_source:
                        original_resource.data_source = data_source
                    original_resource.rights_status = rights_status
                    original_resource.rights_explanation = (
                        link.original.rights_explanation
                    )
                    if link.original.content:
                        original_resource.set_fetched_content(
                            link.original.guessed_media_type,
                            link.original.content,
                            None,
                        )

                link_obj, ignore = identifier.add_link(
                    rel=link.rel,
                    href=link.href,
                    data_source=data_source,
                    media_type=link.guessed_media_type,
                    content=link.content,
                    rights_status_uri=link.rights_uri,
                    rights_explanation=link.rights_explanation,
                    original_resource=original_resource,
                    transformation_settings=link.transformation_settings,
                    db=db,
                )
                if link.rel in _REL_REQUIRES_NEW_PRESENTATION_EDITION:
                    work_requires_new_presentation_edition = True
                elif link.rel in _REL_REQUIRES_FULL_RECALCULATION:
                    work_requires_full_recalculation = True

            link_objects[link] = link_obj
            if link.thumbnail:
                thumbnail = link.thumbnail
                thumbnail_obj, ignore = identifier.add_link(
                    rel=thumbnail.rel,
                    href=thumbnail.href,
                    data_source=data_source,
                    media_type=thumbnail.guessed_media_type,
                    content=thumbnail.content,
                )
                work_requires_new_presentation_edition = True
                if thumbnail_obj.resource and thumbnail_obj.resource.representation:
                    thumbnail_obj.resource.representation.thumbnail_of = (
                        link_obj.resource.representation
                    )
                else:
                    self.log.error(
                        "Thumbnail link %r cannot be marked as a thumbnail of %r because it has no Representation, probably due to a missing media type."
                        % (link.thumbnail, link)
                    )

        # Apply all measurements to the primary identifier
        for measurement in self.measurements:
            work_requires_full_recalculation = True
            identifier.add_measurement(
                data_source,
                measurement.quantity_measured,
                measurement.value,
                measurement.weight,
                measurement.taken_at,
            )

        if not edition.sort_author:
            # This may be a situation like the NYT best-seller list where
            # we know the display name of the author but weren't able
            # to normalize that name.
            primary_author = self.primary_author
            if primary_author:
                self.log.info(
                    "In the absence of Contributor objects, setting Edition author name to %s/%s",
                    primary_author.sort_name,
                    primary_author.display_name,
                )
                edition.sort_author = primary_author.sort_name
                work_requires_new_presentation_edition = True

        # The BibliographicData object may include a CirculationData object which
        # contains information about availability such as open-access
        # links. Make sure
        # that that Collection has a LicensePool for this book and that
        # its information is up-to-date.
        if self.circulation:
            self.circulation.apply(db, collection, replace)

        # obtains a presentation_edition for the title
        has_image = any([link.rel == Hyperlink.IMAGE for link in self.links])
        for link in self.links:
            link_obj = link_objects[link]

            if link_obj.rel == Hyperlink.THUMBNAIL_IMAGE and has_image:
                # This is a thumbnail but we also have a full-sized image link
                continue

            elif link.thumbnail:
                # We need to make sure that its thumbnail exists locally and
                # is associated with the original image.
                self.make_thumbnail(db, data_source, link, link_obj)

        # Make sure the work we just did shows up.
        made_changes = edition.calculate_presentation(
            policy=replace.presentation_calculation_policy
        )
        if made_changes:
            work_requires_new_presentation_edition = True

        # Update the coverage record for this edition and data
        # source. We omit the collection information, even if we know
        # which collection this is, because we only changed bibliographic data.
        CoverageRecord.add_for(
            edition,
            data_source,
            timestamp=self.data_source_last_updated,
            collection=None,
        )

        if work_requires_full_recalculation or work_requires_new_presentation_edition:
            # If there is a Work associated with the Edition's primary
            # identifier, mark it for recalculation.

            # Any LicensePool will do here, since all LicensePools for
            # a given Identifier have the same Work.
            pool = get_one(
                db,
                LicensePool,
                identifier=edition.primary_identifier,
                on_multiple="interchangeable",
            )
            if pool and pool.work:
                work = pool.work
                if work_requires_full_recalculation:
                    work.needs_full_presentation_recalculation()
                else:
                    work.needs_new_presentation_edition()

        return edition, work_requires_new_presentation_edition

    def make_thumbnail(
        self, _db: Session, data_source: DataSource, link: LinkData, link_obj: Hyperlink
    ) -> Hyperlink | None:
        """Make sure a Hyperlink representing an image is connected
        to its thumbnail.
        """
        thumbnail = link.thumbnail
        if not thumbnail:
            return None

        if thumbnail.href == link.href:
            # The image serves as its own thumbnail. This is a
            # hacky way to represent this in the database.
            if link_obj.resource.representation:
                link_obj.resource.representation.image_height = (
                    Edition.MAX_THUMBNAIL_HEIGHT
                )
            return link_obj

        # The thumbnail and image are different. Make sure there's a
        # separate link to the thumbnail.
        thumbnail_obj, ignore = link_obj.identifier.add_link(
            rel=thumbnail.rel,
            href=thumbnail.href,
            data_source=data_source,
            media_type=thumbnail.media_type,
            content=thumbnail.content,
            db=_db,
        )
        # And make sure the thumbnail knows it's a thumbnail of the main
        # image.
        if thumbnail_obj.resource.representation:
            thumbnail_obj.resource.representation.thumbnail_of = (
                link_obj.resource.representation
            )
        return thumbnail_obj

    def update_contributions(
        self, _db: Session, edition: Edition, replace: bool = True
    ) -> bool:
        contributors_changed = False
        old_contributors = []
        new_contributors = []

        if not replace and self.contributors:
            # we've chosen to append new contributors, which exist
            # this means the edition's contributor list will, indeed, change
            contributors_changed = True

        if replace and self.contributors:
            # Remove any old Contributions from this data source --
            # we're about to add a new set
            for contribution in edition.contributions:
                old_contributors.append(contribution.contributor.id)
                _db.delete(contribution)
            edition.contributions = []

        for contributor_data in self.contributors:
            contributor_sort_name = contributor_data.find_sort_name(_db)
            if contributor_sort_name or contributor_data.lc or contributor_data.viaf:
                contributor = edition.add_contributor(
                    name=contributor_sort_name,
                    roles=contributor_data.roles,
                    lc=contributor_data.lc,
                    viaf=contributor_data.viaf,
                )
                new_contributors.append(contributor.id)
                if contributor_data.display_name:
                    contributor.display_name = contributor_data.display_name
                if contributor_data.biography:
                    contributor.biography = contributor_data.biography
                if contributor_data.aliases:
                    contributor.aliases = contributor_data.aliases
                if contributor_data.lc:
                    contributor.lc = contributor_data.lc
                if contributor_data.viaf:
                    contributor.viaf = contributor_data.viaf
                if contributor_data.wikipedia_name:
                    contributor.wikipedia_name = contributor_data.wikipedia_name
            else:
                self.log.info(
                    "Not registering %s because no sort name, LC, or VIAF",
                    contributor_data.display_name,
                )

        if sorted(old_contributors) != sorted(new_contributors):
            contributors_changed = True

        return contributors_changed
