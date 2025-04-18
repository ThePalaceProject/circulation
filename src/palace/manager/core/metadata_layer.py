from __future__ import annotations

import dataclasses

from sqlalchemy import Boolean
from sqlalchemy.orm import Query
from sqlalchemy.sql import ColumnElement

from palace.manager.sqlalchemy.model.patron import Loan
from palace.manager.util.sentinel import SentinelType

"""An abstract way of representing incoming metadata and applying it
to Identifiers and Editions.

This acts as an intermediary between the third-party integrations
(which have this information in idiosyncratic formats) and the
model. Doing a third-party integration should be as simple as putting
the information into this format.
"""

import csv
import datetime
import logging
from collections import defaultdict
from collections.abc import Generator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from dateutil.parser import parse
from dependency_injector.wiring import Provide, inject
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import and_, or_
from typing_extensions import Self, TypedDict, Unpack

from palace.manager.core.classifier import NO_NUMBER, NO_VALUE
from palace.manager.opds.odl.info import LicenseStatus
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.sqlalchemy.constants import LinkRelations
from palace.manager.sqlalchemy.model.classification import Classification, Subject
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.contributor import Contribution, Contributor
from palace.manager.sqlalchemy.model.coverage import CoverageRecord, Timestamp
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    License,
    LicenseFunctions,
    LicensePool,
    LicensePoolDeliveryMechanism,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation, Resource
from palace.manager.sqlalchemy.presentation import PresentationCalculationPolicy
from palace.manager.sqlalchemy.util import get_one, get_one_or_create
from palace.manager.util.datetime_helpers import to_utc, utc_now
from palace.manager.util.languages import LanguageCodes
from palace.manager.util.log import LoggerMixin
from palace.manager.util.median import median
from palace.manager.util.personal_names import display_name_to_sort_name


class _ReplacementPolicyKwargs(TypedDict, total=False):
    even_if_not_apparently_updated: bool
    link_content: bool
    presentation_calculation_policy: PresentationCalculationPolicy


@dataclass(kw_only=True)
class ReplacementPolicy:
    """How serious should we be about overwriting old metadata with
    this new metadata?
    """

    identifiers: bool = False
    subjects: bool = False
    contributions: bool = False
    links: bool = False
    formats: bool = False
    rights: bool = False
    link_content: bool = False
    analytics: Analytics | None = None
    even_if_not_apparently_updated: bool = False
    presentation_calculation_policy: PresentationCalculationPolicy = field(
        default_factory=PresentationCalculationPolicy
    )

    @classmethod
    @inject
    def from_license_source(
        cls,
        _db: Session,
        analytics: Analytics = Provide["analytics.analytics"],
        **kwargs: Unpack[_ReplacementPolicyKwargs],
    ) -> Self:
        """When gathering data from the license source, overwrite all old data
        from this source with new data from the same source. Also
        overwrite an old rights status with an updated status and update
        the list of available formats. Log availability changes to the
        configured analytics services.
        """
        return cls(
            identifiers=True,
            subjects=True,
            contributions=True,
            links=True,
            rights=True,
            formats=True,
            analytics=analytics,
            **kwargs,
        )

    @classmethod
    def from_metadata_source(cls, **kwargs: Unpack[_ReplacementPolicyKwargs]) -> Self:
        """When gathering data from a metadata source, overwrite all old data
        from this source, but do not overwrite the rights status or
        the available formats. License sources are the authority on rights
        and formats, and metadata sources have no say in the matter.
        """
        return cls(
            identifiers=True,
            subjects=True,
            contributions=True,
            links=True,
            rights=False,
            formats=False,
            **kwargs,
        )

    @classmethod
    def append_only(cls, **kwargs: Unpack[_ReplacementPolicyKwargs]) -> Self:
        """Don't overwrite any information, just append it.

        This should probably never be used.
        """
        return cls(
            identifiers=False,
            subjects=False,
            contributions=False,
            links=False,
            rights=False,
            formats=False,
            **kwargs,
        )


class SubjectData:
    def __init__(
        self,
        type: str,
        identifier: str | None,
        name: str | None = None,
        weight: int = 1,
    ) -> None:
        self.type = type

        # Because subjects are sometimes evaluated according to keyword
        # matching, it's important that any leading or trailing white
        # space is removed during import.
        self.identifier = identifier
        if identifier:
            self.identifier = identifier.strip()

        self.name = name
        if name:
            self.name = name.strip()

        self.weight = weight

    @property
    def key(self) -> tuple[str, str | None, str | None, int]:
        return self.type, self.identifier, self.name, self.weight

    def __repr__(self) -> str:
        return '<SubjectData type="%s" identifier="%s" name="%s" weight=%d>' % (
            self.type,
            self.identifier,
            self.name,
            self.weight,
        )


class ContributorData(LoggerMixin):
    def __init__(
        self,
        sort_name: str | None = None,
        display_name: str | None = None,
        family_name: str | None = None,
        wikipedia_name: str | None = None,
        roles: str | Sequence[str] | None = None,
        lc: str | None = None,
        viaf: str | None = None,
        biography: str | None = None,
        aliases: Sequence[str] | None = None,
        extra: dict[str, str] | None = None,
    ) -> None:
        self.sort_name = sort_name
        self.display_name = display_name
        self.family_name = family_name
        self.wikipedia_name = wikipedia_name
        if roles is None:
            roles = [Contributor.Role.AUTHOR]
        if isinstance(roles, str):
            roles = [roles]
        self.roles = list(roles)
        self.lc = lc
        self.viaf = viaf
        self.biography = biography
        self.aliases = list(aliases) if aliases is not None else []
        # extra is a dictionary of stuff like birthdates
        self.extra = extra or dict()
        # TODO:  consider if it's time for ContributorData to connect back to Contributions

    def __repr__(self) -> str:
        return (
            '<ContributorData sort="%s" display="%s" family="%s" wiki="%s" roles=%r lc=%s viaf=%s>'
            % (
                self.sort_name,
                self.display_name,
                self.family_name,
                self.wikipedia_name,
                self.roles,
                self.lc,
                self.viaf,
            )
        )

    @classmethod
    def from_contribution(cls, contribution: Contribution) -> Self:
        """Create a ContributorData object from a data-model Contribution
        object.
        """
        c = contribution.contributor
        return cls(
            sort_name=c.sort_name,
            display_name=c.display_name,
            family_name=c.family_name,
            wikipedia_name=c.wikipedia_name,
            lc=c.lc,
            viaf=c.viaf,
            biography=c.biography,
            aliases=c.aliases,
            roles=[contribution.role],
        )

    @classmethod
    def lookup(
        cls,
        _db: Session,
        sort_name: str | None = None,
        display_name: str | None = None,
        lc: str | None = None,
        viaf: str | None = None,
    ) -> Self | None:
        """Create a (potentially synthetic) ContributorData based on
        the best available information in the database.

        :return: A ContributorData.
        """
        clauses: list[ColumnElement[Boolean]] = []
        if sort_name:
            # Mypy doesn't like this one because Contributor.sort_name is a HybridProperty, so we just
            # ignore the type check here, since it is a valid comparison.
            clauses.append(Contributor.sort_name == sort_name)  # type: ignore[arg-type, comparison-overlap]
        if display_name:
            clauses.append(Contributor.display_name == display_name)
        if lc:
            clauses.append(Contributor.lc == lc)
        if viaf:
            clauses.append(Contributor.viaf == viaf)

        if not clauses:
            raise ValueError("No Contributor information provided!")

        or_clause = or_(*clauses)
        contributors = _db.query(Contributor).filter(or_clause).all()
        if len(contributors) == 0:
            # We have no idea who this person is.
            return None

        # We found at least one matching Contributor. Let's try to
        # build a composite ContributorData for the person.
        sort_name_values = set()
        display_name_values = set()
        lc_values = set()
        viaf_values = set()

        # If all the people we found share (e.g.) a VIAF field, then
        # we can use that as a clue when doing a search -- anyone with
        # that VIAF number is probably this person, even if their display
        # name doesn't match.
        for c in contributors:
            if c.sort_name:
                sort_name_values.add(c.sort_name)
            if c.display_name:
                display_name_values.add(c.display_name)
            if c.lc:
                lc_values.add(c.lc)
            if c.viaf:
                viaf_values.add(c.viaf)

        # Use any passed-in values as default values for the
        # ContributorData. If all the Contributors we found have the
        # same value for a field, we can use it to supplement the
        # default values.
        if len(sort_name_values) == 1:
            sort_name = sort_name_values.pop()
        if len(display_name_values) == 1:
            display_name = display_name_values.pop()
        if len(lc_values) == 1:
            lc = lc_values.pop()
        if len(viaf_values) == 1:
            viaf = viaf_values.pop()

        return cls(
            roles=[], sort_name=sort_name, display_name=display_name, lc=lc, viaf=viaf
        )

    def apply(self, destination: Contributor) -> tuple[Contributor, bool]:
        """Update the passed-in Contributor-type object with this
        ContributorData's information.

        :param: destination -- the Contributor or ContributorData object to
            write this ContributorData object's metadata to.

        :return: the possibly changed Contributor object and a flag of whether it's been changed.
        """
        self.log.debug(
            "Applying %r (%s) into %r (%s)",
            self,
            self.viaf,
            destination,
            destination.viaf,
        )

        made_changes = False

        if self.sort_name and self.sort_name != destination.sort_name:
            destination.sort_name = self.sort_name
            made_changes = True

        existing_aliases = set(destination.aliases or [])
        new_aliases = list(destination.aliases or [])
        for name in [self.sort_name] + self.aliases:
            if name != destination.sort_name and name not in existing_aliases:
                new_aliases.append(name)
                made_changes = True
        if new_aliases != destination.aliases:
            destination.aliases = new_aliases
            made_changes = True

        for k, v in list(self.extra.items()):
            if not k in destination.extra:
                destination.extra[k] = v

        if self.lc and self.lc != destination.lc:
            destination.lc = self.lc
            made_changes = True
        if self.viaf and self.viaf != destination.viaf:
            destination.viaf = self.viaf
            made_changes = True
        if self.family_name and self.family_name != destination.family_name:
            destination.family_name = self.family_name
            made_changes = True
        if self.display_name and self.display_name != destination.display_name:
            destination.display_name = self.display_name
            made_changes = True
        if self.wikipedia_name and self.wikipedia_name != destination.wikipedia_name:
            destination.wikipedia_name = self.wikipedia_name
            made_changes = True

        if self.biography and self.biography != destination.biography:
            destination.biography = self.biography
            made_changes = True

        # TODO:  Contributor.merge_into also looks at
        # contributions.  Could maybe extract contributions from roles,
        # but not sure it'd be useful.

        return destination, made_changes

    def find_sort_name(self, _db: Session) -> bool:
        """Try as hard as possible to find this person's sort name."""
        if self.sort_name:
            return True

        if not self.display_name:
            raise ValueError(
                "Cannot find sort name for a contributor with no display name!"
            )

        # Is there a contributor already in the database with this
        # exact sort name? If so, use their display name.
        # If not, take our best guess based on the display name.
        sort_name = self.display_name_to_sort_name_from_existing_contributor(
            _db, self.display_name
        )
        if sort_name:
            self.sort_name = sort_name
            return True

        # If there's still no sort name, take our best guess based
        # on the display name.
        self.sort_name = display_name_to_sort_name(self.display_name)

        return self.sort_name is not None

    @classmethod
    def display_name_to_sort_name_from_existing_contributor(
        self, _db: Session, display_name: str
    ) -> str | None:
        """Find the sort name for this book's author, assuming it's easy.

        'Easy' means we already have an established sort name for a
        Contributor with this exact display name.

        If we have a copy of this book in our collection (the only
        time an external list item is relevant), this will probably be
        easy.
        """
        contributors = (
            _db.query(Contributor)
            .filter(Contributor.display_name == display_name)
            .filter(Contributor.sort_name != None)
            .all()
        )
        if contributors:
            log = logging.getLogger("Abstract metadata layer")
            log.debug(
                "Determined that sort name of %s is %s based on previously existing contributor",
                display_name,
                contributors[0].sort_name,
            )
            return contributors[0].sort_name  # type: ignore[no-any-return]
        return None


@dataclass(frozen=True)
class IdentifierData:
    type: str
    identifier: str
    weight: float = 1

    def __repr__(self) -> str:
        return '<IdentifierData type="{}" identifier="{}" weight="{}">'.format(
            self.type,
            self.identifier,
            self.weight,
        )

    def load(self, _db: Session) -> tuple[Identifier, bool]:
        return Identifier.for_foreign_id(_db, self.type, self.identifier)


class LinkData:
    def __init__(
        self,
        rel: str | None,
        href: str | None = None,
        media_type: str | None = None,
        content: bytes | str | None = None,
        thumbnail: LinkData | None = None,
        rights_uri: str | None = None,
        rights_explanation: str | None = None,
        original: LinkData | None = None,
        transformation_settings: dict[str, str] | None = None,
    ) -> None:
        if not rel:
            raise ValueError("rel is required")

        if not href and not content:
            raise ValueError("Either href or content is required")
        self.rel = rel
        self.href = href
        self.media_type = media_type
        self.content = content
        self.thumbnail = thumbnail
        # This handles content sources like unglue.it that have rights for each link
        # rather than each edition, and rights for cover images.
        self.rights_uri = rights_uri
        self.rights_explanation = rights_explanation
        # If this LinkData is a derivative, it may also contain the original link
        # and the settings used to transform the original into the derivative.
        self.original = original
        self.transformation_settings = transformation_settings or {}

    @property
    def guessed_media_type(self) -> str | None:
        """If the media type of a link is unknown, take a guess."""
        if self.media_type:
            # We know.
            return self.media_type

        if self.href:
            # Take a guess.
            return Representation.guess_url_media_type_from_path(self.href)  # type: ignore[no-any-return]

        # No idea.
        # TODO: We might be able to take a further guess based on the
        # content and the link relation.
        return None

    def __repr__(self) -> str:
        if self.content:
            content = ", %d bytes content" % len(self.content)
        else:
            content = ""
        if self.thumbnail:
            thumbnail = ", has thumbnail"
        else:
            thumbnail = ""
        return '<LinkData: rel="{}" href="{}" media_type={!r}{}{}>'.format(
            self.rel,
            self.href,
            self.media_type,
            thumbnail,
            content,
        )


class MeasurementData:
    def __init__(
        self,
        quantity_measured: str,
        value: float | int | str,
        weight: float = 1,
        taken_at: datetime.datetime | None = None,
    ):
        if not quantity_measured:
            raise ValueError("quantity_measured is required.")
        if value is None:
            raise ValueError("measurement value is required.")
        self.quantity_measured = quantity_measured
        if not isinstance(value, float) and not isinstance(value, int):
            value = float(value)
        self.value = value
        self.weight = weight
        self.taken_at = taken_at or utc_now()

    def __repr__(self) -> str:
        return '<MeasurementData quantity="%s" value=%f weight=%d taken=%s>' % (
            self.quantity_measured,
            self.value,
            self.weight,
            self.taken_at,
        )


@dataclass(frozen=True, kw_only=True)
class FormatData(LoggerMixin):
    content_type: str | None
    drm_scheme: str | None
    link: LinkData | None = None
    rights_uri: str | None = None
    available: bool = True
    # By default, we don't update a formats availability, we only set it when
    # creating a new one, this can be overridden by setting this flag to True.
    update_available: bool = False

    def __post_init__(self) -> None:
        if self.link and not isinstance(self.link, LinkData):
            raise TypeError("Expected LinkData object, got %s" % type(self.link))

        # We can't use direct assignment because of the frozen=True flag, so
        # we have to use object.__setattr__.
        # https://stackoverflow.com/questions/53756788/how-to-set-the-value-of-dataclass-field-in-post-init-when-frozen-true
        if self.link:
            if not self.rights_uri and self.link.rights_uri:
                object.__setattr__(self, "rights_uri", self.link.rights_uri)

            if not self.content_type and self.link.media_type:
                object.__setattr__(self, "content_type", self.link.media_type)

    def apply(
        self,
        db: Session,
        data_source: DataSource,
        identifier: Identifier,
        resource: Resource | None = None,
        default_rights_uri: str | None = None,
    ) -> LicensePoolDeliveryMechanism:
        """Apply this FormatData. Creating a new LicensePoolDeliveryMechanism
        if necessary.

        :param db: Use this database connection. If this is not supplied
            the database connection will be taken from the data_source.
        :param data_source: A DataSource identifying the distributor.
        :param identifier: An Identifier identifying the title.
        :param resource: A Resource representing the book itself in
            a freely redistributable form, if any.
        :param default_rights_uri: The default rights URI to use if none is
            specified in the FormatData.

        :return: A LicensePoolDeliveryMechanism.
        """
        return LicensePoolDeliveryMechanism.set(
            data_source,
            identifier,
            rights_uri=self.rights_uri or default_rights_uri,
            resource=resource,
            content_type=self.content_type,
            drm_scheme=self.drm_scheme,
            available=self.available,
            update_available=self.update_available,
            db=db,
        )

    def apply_to_loan(
        self,
        db: Session,
        loan: Loan,
    ) -> LicensePoolDeliveryMechanism | None:
        """Set an appropriate LicensePoolDeliveryMechanism on the given
        `Loan`, creating the DeliveryMechanism and LicensePoolDeliveryMechanism
         if necessary.

        :param db: A database session.
        :param loan: A Loan object.
        :return: A LicensePoolDeliveryMechanism if one could be set on the
            given Loan; None otherwise.
        """

        # Create or update the DeliveryMechanism.
        delivery_mechanism, _ = DeliveryMechanism.lookup(
            db, self.content_type, self.drm_scheme
        )

        if (
            loan.fulfillment
            and loan.fulfillment.delivery_mechanism == delivery_mechanism
        ):
            # The work has already been done. Do nothing.
            return None

        # At this point we know we need to update the local delivery
        # mechanism.
        pool = loan.license_pool
        if not pool:
            # This shouldn't happen, but bail out if it does.
            self.log.warning(
                f"No license pool for loan (id:{loan.id}), can't set delivery mechanism."
            )
            return None

        # Apply this FormatData, looking up or creating a LicensePoolDeliveryMechanism.
        lpdm = self.apply(
            db,
            pool.data_source,
            pool.identifier,
        )
        loan.fulfillment = lpdm
        return lpdm


class LicenseData(LicenseFunctions):
    def __init__(
        self,
        identifier: str,
        checkout_url: str | None,
        status_url: str,
        status: LicenseStatus,
        checkouts_available: int,
        expires: datetime.datetime | None = None,
        checkouts_left: int | None = None,
        terms_concurrency: int | None = None,
        content_types: list[str] | None = None,
    ):
        self.identifier = identifier
        self.checkout_url = checkout_url
        self.status_url = status_url
        self.status = status
        self.expires = expires
        self.checkouts_left = checkouts_left
        self.checkouts_available = checkouts_available
        self.terms_concurrency = terms_concurrency
        self.content_types = content_types

    def add_to_pool(self, db: Session, pool: LicensePool) -> License:
        license_obj, _ = get_one_or_create(
            db,
            License,
            identifier=self.identifier,
            license_pool=pool,
        )
        for key, value in vars(self).items():
            if key != "content_types":
                setattr(license_obj, key, value)
        return license_obj


class TimestampData:
    def __init__(
        self,
        start: datetime.datetime | None | Literal[SentinelType.ClearValue] = None,
        finish: datetime.datetime | None | Literal[SentinelType.ClearValue] = None,
        achievements: str | None | Literal[SentinelType.ClearValue] = None,
        counter: int | None | Literal[SentinelType.ClearValue] = None,
        exception: str | None | Literal[SentinelType.ClearValue] = None,
    ) -> None:
        """A constructor intended to be used by a service to customize its
        eventual Timestamp.

        service, service_type, and collection cannot be set through
        this constructor, because they are generally not under the
        control of the code that runs the service. They are set
        afterwards, in finalize().

        :param start: The time that the service should be considered to
           have started running.
        :param finish: The time that the service should be considered
           to have stopped running.
        :param achievements: A string describing what was achieved by the
           service.
        :param counter: A single integer item of state representing the point
           at which the service left off.
        :param exception: A traceback representing an exception that stopped
           the progress of the service.
        """

        # These are set by finalize().
        self.service: str | None = None
        self.service_type: str | None = None
        self.collection_id: int | None = None

        self.start = start
        self.finish = finish
        self.achievements = achievements
        self.counter = counter
        self.exception = exception

    @property
    def is_failure(self) -> bool:
        """Does this TimestampData represent an unrecoverable failure?"""
        return self.exception not in (None, SentinelType.ClearValue)

    @property
    def is_complete(self) -> bool:
        """Does this TimestampData represent an operation that has
        completed?

        An operation is completed if it has failed, or if the time of its
        completion is known.
        """
        return self.is_failure or self.finish not in (None, SentinelType.ClearValue)

    def finalize(
        self,
        service: str,
        service_type: str,
        collection: Collection | None,
        start: datetime.datetime | None = None,
        finish: datetime.datetime | None = None,
        counter: int | None = None,
        exception: str | None = None,
    ) -> None:
        """Finalize any values that were not set during the constructor.

        This is intended to be run by the code that originally ran the
        service.

        The given values for `start`, `finish`,
        `counter`, and `exception` will be used only if the service
        did not specify its own values for those fields.
        """
        self.service = service
        self.service_type = service_type
        if collection is None:
            self.collection_id = None
        else:
            self.collection_id = collection.id
        if self.start is None:
            self.start = start
        if self.finish is None:
            if finish is None:
                finish = utc_now()
            self.finish = finish
        if self.start is None:
            self.start = self.finish
        if self.counter is None:
            self.counter = counter
        if self.exception is None:
            self.exception = exception

    def collection(self, _db: Session) -> Collection | None:
        return get_one(_db, Collection, id=self.collection_id)

    def apply(self, _db: Session) -> Timestamp:
        if self.service is None or self.service_type is None:
            raise ValueError(
                "Not enough information to write TimestampData to the database."
            )

        return Timestamp.stamp(
            _db,
            self.service,
            self.service_type,
            self.collection(_db),
            self.start,
            self.finish,
            self.achievements,
            self.counter,
            self.exception,
        )


class CirculationData(LoggerMixin):
    """Information about actual copies of a book that can be delivered to
    patrons.

    As distinct from Metadata, which is a container for information
    about a book.

    Basically,
        Metadata : Edition :: CirculationData : Licensepool
    """

    def __init__(
        self,
        data_source: str | DataSource,
        primary_identifier: Identifier | IdentifierData | None,
        licenses_owned: int | None = None,
        licenses_available: int | None = None,
        licenses_reserved: int | None = None,
        patrons_in_hold_queue: int | None = None,
        formats: list[FormatData] | None = None,
        default_rights_uri: str | None = None,
        links: list[LinkData] | None = None,
        licenses: list[LicenseData] | None = None,
        last_checked: datetime.datetime | None = None,
        should_track_playtime: bool = False,
    ) -> None:
        """Constructor.

        :param data_source: The authority providing the lending licenses.
            This may be a DataSource object or the name of the data source.
        :param primary_identifier: An Identifier or IdentifierData representing
            how the lending authority distinguishes this book from others.
        """
        self._data_source = data_source
        if isinstance(self._data_source, DataSource):
            self.data_source_obj: DataSource | None = self._data_source
            self.data_source_name = self.data_source_obj.name
        else:
            self.data_source_obj = None
            self.data_source_name = self._data_source

        if isinstance(primary_identifier, Identifier):
            self.primary_identifier_obj: Identifier | None = primary_identifier
            self._primary_identifier: IdentifierData | None = IdentifierData(
                primary_identifier.type, primary_identifier.identifier
            )
        else:
            self.primary_identifier_obj = None
            self._primary_identifier = primary_identifier
        self.licenses_owned = licenses_owned
        self.licenses_available = licenses_available
        self.licenses_reserved = licenses_reserved
        self.patrons_in_hold_queue = patrons_in_hold_queue

        # If no 'last checked' data was provided, assume the data was
        # just gathered.
        self.last_checked: datetime.datetime = last_checked or utc_now()

        # format contains pdf/epub, drm, link
        self.formats: list[FormatData] = formats or []

        self.default_rights_uri: str | None = None
        self.set_default_rights_uri(
            data_source_name=self.data_source_name,
            default_rights_uri=default_rights_uri,
        )

        self.__links: list[LinkData] | None = None
        # The type ignore here is necessary because mypy does not like when a property setter and
        # getter have different types. A PR just went in to fix this in mypy, so this should be able
        # to be removed once mypy 1.16 is released.
        # See: https://github.com/python/mypy/pull/18510
        self.links = links  # type: ignore[assignment]

        # Information about individual terms for each license in a pool. If we are
        # given licenses then they are used to calculate values for the LicensePool
        # instead of directly using the values that are given to CirculationData.
        self.licenses: list[LicenseData] | None = licenses

        # Whether the license should contain a playtime tracking link
        self.should_track_playtime: bool = should_track_playtime

    @property
    def links(self) -> Sequence[LinkData]:
        return self.__links or []

    @links.setter
    def links(self, arg_links: list[LinkData] | None) -> None:
        """If got passed all links, indiscriminately, filter out to only those relevant to
        pools (the rights-related links).
        """
        # start by deleting any old links
        self.__links = []

        if not arg_links:
            return

        for link in arg_links:
            if link.rel in Hyperlink.CIRCULATION_ALLOWED:
                # TODO:  what about Hyperlink.SAMPLE?
                # only accept the types of links relevant to pools
                self.__links.append(link)

                # An open-access link or open-access rights implies a FormatData object.
                open_access_link = (
                    link.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD and link.href
                )
                # try to deduce if the link is open-access, even if it doesn't explicitly say it is
                rights_uri = link.rights_uri or self.default_rights_uri
                open_access_rights_link = (
                    link.media_type in Representation.BOOK_MEDIA_TYPES
                    and link.href
                    and rights_uri in RightsStatus.OPEN_ACCESS
                )

                if open_access_link or open_access_rights_link:
                    if (
                        open_access_link
                        and rights_uri != RightsStatus.IN_COPYRIGHT
                        and not rights_uri in RightsStatus.OPEN_ACCESS
                    ):
                        # We don't know exactly what's going on here but
                        # the link said it was an open-access book
                        # and the rights URI doesn't contradict it,
                        # so treat it as a generic open-access book.
                        rights_uri = RightsStatus.GENERIC_OPEN_ACCESS
                    format_found = False
                    format = None
                    for format in self.formats:
                        if format and format.link and format.link.href == link.href:
                            format_found = True
                            break
                    if format_found and format and not format.rights_uri:
                        self.formats.remove(format)
                        self.formats.append(
                            dataclasses.replace(format, rights_uri=rights_uri)
                        )
                    if not format_found:
                        self.formats.append(
                            FormatData(
                                content_type=link.media_type,
                                drm_scheme=DeliveryMechanism.NO_DRM,
                                link=link,
                                rights_uri=rights_uri,
                            )
                        )

    def __repr__(self) -> str:
        description_string = "<CirculationData primary_identifier=%(primary_identifier)r| licenses_owned=%(licenses_owned)s|"
        description_string += " licenses_available=%(licenses_available)s| default_rights_uri=%(default_rights_uri)s|"
        description_string += (
            " links=%(links)r| formats=%(formats)r| data_source=%(data_source)s|>"
        )

        description_data: dict[str, Any] = {"licenses_owned": self.licenses_owned}
        if self._primary_identifier:
            description_data["primary_identifier"] = self._primary_identifier
        else:
            description_data["primary_identifier"] = self.primary_identifier_obj
        description_data["licenses_available"] = self.licenses_available
        description_data["default_rights_uri"] = self.default_rights_uri
        description_data["links"] = self.links
        description_data["formats"] = self.formats
        description_data["data_source"] = self.data_source_name

        return description_string % description_data

    def data_source(self, _db: Session) -> DataSource:
        """Find the DataSource associated with this circulation information."""
        if not self.data_source_obj:
            obj = DataSource.lookup(_db, self.data_source_name, autocreate=True)
            self.data_source_obj = obj
        return self.data_source_obj

    def primary_identifier(self, _db: Session) -> Identifier:
        """Find the Identifier associated with this circulation information."""
        if not self.primary_identifier_obj:
            if self._primary_identifier:
                obj, ignore = self._primary_identifier.load(_db)
            else:
                raise ValueError("No primary identifier provided!")
            self.primary_identifier_obj = obj
        return self.primary_identifier_obj

    def license_pool(
        self, _db: Session, collection: Collection | None
    ) -> tuple[LicensePool, bool]:
        """Find or create a LicensePool object for this CirculationData.

        :param collection: The LicensePool object will be associated with
            the given Collection.
        """
        if not collection:
            raise ValueError("Cannot find license pool: no collection provided.")
        identifier = self.primary_identifier(_db)
        if not identifier:
            raise ValueError(
                "Cannot find license pool: CirculationData has no primary identifier."
            )

        data_source_obj = self.data_source(_db)
        license_pool, is_new = LicensePool.for_foreign_id(
            _db,
            data_source=data_source_obj,
            foreign_id_type=identifier.type,
            foreign_id=identifier.identifier,
            collection=collection,
        )

        if is_new:
            license_pool.open_access = self.has_open_access_link
            license_pool.availability_time = self.last_checked
            license_pool.last_checked = self.last_checked
            license_pool.should_track_playtime = self.should_track_playtime

        return license_pool, is_new

    @property
    def has_open_access_link(self) -> bool:
        """Does this Circulation object have an associated open-access link?"""
        return any(
            [
                x
                for x in self.links
                if x.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD
                and x.href
                and x.rights_uri != RightsStatus.IN_COPYRIGHT
            ]
        )

    def set_default_rights_uri(
        self, data_source_name: str | None, default_rights_uri: str | None = None
    ) -> None:
        if default_rights_uri:
            self.default_rights_uri = default_rights_uri

        elif data_source_name:
            # We didn't get rights passed in, so use the default rights for the data source if any.
            default = RightsStatus.DATA_SOURCE_DEFAULT_RIGHTS_STATUS.get(
                data_source_name, None
            )
            if default:
                self.default_rights_uri = default

        if not self.default_rights_uri:
            # We still haven't determined rights, so it's unknown.
            self.default_rights_uri = RightsStatus.UNKNOWN

    def apply(
        self,
        _db: Session,
        collection: Collection | None,
        replace: ReplacementPolicy | None = None,
    ) -> tuple[LicensePool | None, bool]:
        """Update the title with this CirculationData's information.

        :param collection: A Collection representing actual copies of
            this title. Availability information (e.g. number of copies)
            will be associated with a LicensePool in this Collection. If
            this is not present, only delivery information (e.g. format
            information and open-access downloads) will be processed.

        """
        # Immediately raise an exception if there is information that
        # can only be stored in a LicensePool, but we have no
        # Collection to tell us which LicensePool to use. This is
        # indicative of an error in programming.
        if not collection and (
            self.licenses_owned is not None
            or self.licenses_available is not None
            or self.licenses_reserved is not None
            or self.patrons_in_hold_queue is not None
        ):
            raise ValueError(
                "Cannot store circulation information because no "
                "Collection was provided."
            )

        made_changes = False
        if replace is None:
            replace = ReplacementPolicy()

        pool = None
        if collection:
            pool, ignore = self.license_pool(_db, collection)

        data_source = self.data_source(_db)
        identifier = self.primary_identifier(_db)
        # First, make sure all links in self.links are associated with the book's identifier.

        # TODO: be able to handle the case where the URL to a link changes or
        # a link disappears.
        link_objects: dict[LinkData, Hyperlink] = {}
        for link in self.links:
            if link.rel in Hyperlink.CIRCULATION_ALLOWED and identifier is not None:
                link_obj, ignore = identifier.add_link(
                    rel=link.rel,
                    href=link.href,
                    data_source=data_source,
                    media_type=link.media_type,
                    content=link.content,
                    db=_db,
                )
                link_objects[link] = link_obj

        # Next, make sure the DeliveryMechanisms associated
        # with the book reflect the formats in self.formats.
        old_lpdms: list[LicensePoolDeliveryMechanism] = []
        new_lpdms: list[LicensePoolDeliveryMechanism] = []
        if pool:
            pool.should_track_playtime = self.should_track_playtime
            old_lpdms = list(pool.delivery_mechanisms)

        # Before setting and unsetting delivery mechanisms, which may
        # change the open-access status of the work, see what it the
        # status currently is.
        pools = identifier.licensed_through if identifier is not None else []
        old_open_access = any(pool.open_access for pool in pools)

        for format in self.formats:
            if format.link:
                link_obj = link_objects[format.link]
                resource = link_obj.resource
            else:
                resource = None
            # This can cause a non-open-access LicensePool to go open-access.
            lpdm = format.apply(
                _db,
                data_source,
                identifier,
                resource,
                default_rights_uri=self.default_rights_uri,
            )
            new_lpdms.append(lpdm)

        if replace.formats:
            # If any preexisting LicensePoolDeliveryMechanisms were
            # not mentioned in self.formats, remove the corresponding
            # LicensePoolDeliveryMechanisms.
            for lpdm in old_lpdms:
                if lpdm not in new_lpdms:
                    for loan in lpdm.fulfills:
                        self.log.info(
                            "Loan %i is associated with a format that is no longer available. Deleting its delivery mechanism."
                            % loan.id
                        )
                        loan.fulfillment = None
                    # This can cause an open-access LicensePool to go
                    # non-open-access.
                    lpdm.delete()

        new_open_access = any(pool.open_access for pool in pools)
        open_access_status_changed = old_open_access != new_open_access

        # Finally, if we have data for a specific Collection's license
        # for this book, find its LicensePool and update it.
        changed_availability = False
        if pool and self._availability_needs_update(pool):
            # Update availability information. This may result in
            # the issuance of additional circulation events.
            if self.licenses is not None:
                # If we have licenses set, use those to set our availability
                old_licenses = list(pool.licenses or [])
                new_licenses = [
                    license.add_to_pool(_db, pool) for license in self.licenses
                ]
                for license in old_licenses:
                    if license not in new_licenses:
                        self.log.warning(
                            f"License {license.identifier} has been removed from feed."
                        )
                changed_availability = pool.update_availability_from_licenses(
                    as_of=self.last_checked,
                )
            else:
                # Otherwise update the availability directly
                changed_availability = pool.update_availability(
                    new_licenses_owned=self.licenses_owned,
                    new_licenses_available=self.licenses_available,
                    new_licenses_reserved=self.licenses_reserved,
                    new_patrons_in_hold_queue=self.patrons_in_hold_queue,
                    as_of=self.last_checked,
                )

        # If this is the first time we've seen this pool, or we never
        # made a Work for it, make one now.
        work_changed = False
        if pool and not pool.work:
            work, work_changed = pool.calculate_work()
            if work:
                work.set_presentation_ready()
                work_changed = True

        made_changes = (
            made_changes
            or changed_availability
            or open_access_status_changed
            or work_changed
        )

        return pool, made_changes

    def _availability_needs_update(self, pool: LicensePool) -> bool:
        """Does this CirculationData represent information more recent than
        what we have for the given LicensePool?
        """
        if not self.last_checked:
            # Assume that our data represents the state of affairs
            # right now.
            return True
        if not pool.last_checked:
            # It looks like the LicensePool has never been checked.
            return True
        return self.last_checked >= pool.last_checked


class Metadata(LoggerMixin):
    """A (potentially partial) set of metadata for a published work."""

    BASIC_EDITION_FIELDS: list[str] = [
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

    def __init__(
        self,
        data_source: str | DataSource | None,
        *,
        title: str | None = None,
        subtitle: str | None = None,
        sort_title: str | None = None,
        language: str | None = None,
        medium: str | None = None,
        series: str | None = None,
        series_position: int | None = None,
        publisher: str | None = None,
        imprint: str | None = None,
        issued: datetime.date | None = None,
        published: datetime.date | None = None,
        primary_identifier: IdentifierData | Identifier | None = None,
        identifiers: list[IdentifierData] | None = None,
        recommendations: list[IdentifierData | Identifier] | None = None,
        subjects: list[SubjectData] | None = None,
        contributors: list[ContributorData] | None = None,
        measurements: list[MeasurementData] | None = None,
        links: list[LinkData] | None = None,
        data_source_last_updated: datetime.datetime | None = None,
        duration: float | None = None,
        # Note: brought back to keep callers of bibliographic extraction process_one() methods simple.
        circulation: CirculationData | None = None,
    ) -> None:
        # data_source is where the data comes from (e.g. overdrive, admin interface),
        # and not necessarily where the associated Identifier's LicencePool's lending licenses are coming from.
        self._data_source = data_source
        if isinstance(self._data_source, DataSource):
            self.data_source_obj: DataSource | None = self._data_source
            self.data_source_name: str | None = self.data_source_obj.name
        else:
            self.data_source_obj = None
            self.data_source_name = self._data_source

        self.title = title
        self.sort_title = sort_title
        self.subtitle = subtitle
        if language:
            language = LanguageCodes.string_to_alpha_3(language)
        self.language = language
        # medium is book/audio/video, etc.
        self.medium = medium
        self.series = series
        self.series_position = series_position
        self.publisher = publisher
        self.imprint = imprint
        self.issued = issued
        self.published = published
        self.duration = duration

        if isinstance(primary_identifier, Identifier):
            primary_identifier = IdentifierData(
                primary_identifier.type, primary_identifier.identifier
            )
        self.primary_identifier = primary_identifier
        self.identifiers = identifiers or []
        self.permanent_work_id: str | None = None
        if self.primary_identifier and self.primary_identifier not in self.identifiers:
            self.identifiers.append(self.primary_identifier)
        self.recommendations = recommendations or []
        self.subjects = subjects or []
        self.contributors = contributors or []
        self.measurements = measurements or []

        self.circulation = circulation

        # renamed last_update_time to data_source_last_updated
        self.data_source_last_updated = data_source_last_updated

        self.__links: list[LinkData] = []
        # The type ignore here is necessary because mypy does not like when a property setter and
        # getter have different types. A PR just went in to fix this in mypy, so this should be able
        # to be removed once mypy 1.16 is released.
        # See: https://github.com/python/mypy/pull/18510
        self.links = links  # type: ignore[assignment]

    @property
    def links(self) -> list[LinkData]:
        return self.__links

    @links.setter
    def links(self, arg_links: list[LinkData] | None) -> None:
        """If got passed all links, undiscriminately, filter out to only those relevant to
        editions (the image/cover/etc links).
        """
        # start by deleting any old links
        self.__links = []

        if not arg_links:
            return

        for link in arg_links:
            if link.rel in Hyperlink.METADATA_ALLOWED:
                # only accept the types of links relevant to editions
                self.__links.append(link)

    @classmethod
    def from_edition(cls, edition: Edition) -> Metadata:
        """Create a basic Metadata object for the given Edition.

        This doesn't contain everything but it contains enough
        information to run guess_license_pools.
        """
        kwargs: dict[str, Any] = dict()
        for field in cls.BASIC_EDITION_FIELDS:
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
        primary_identifier = IdentifierData(
            type=i.type, identifier=i.identifier, weight=1
        )

        links: list[LinkData] = []
        for link in i.links:
            link_data = LinkData(link.rel, link.resource.url)
            links.append(link_data)

        return Metadata(
            data_source=edition.data_source,
            primary_identifier=primary_identifier,
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

    def update(self, metadata: Metadata) -> None:
        """Update this Metadata object with values from the given Metadata
        object.

        TODO: We might want to take a policy object as an argument.
        """

        fields = self.BASIC_EDITION_FIELDS
        for field in fields:
            new_value = getattr(metadata, field)
            if new_value != None and new_value != "":
                setattr(self, field, new_value)

        new_value = getattr(metadata, "contributors")
        if new_value and isinstance(new_value, list):
            old_value = getattr(self, "contributors")
            # if we already have a better value, don't override it with a "missing info" placeholder value
            if not (old_value and new_value[0].sort_name == Edition.UNKNOWN_AUTHOR):
                setattr(self, "contributors", new_value)

    def calculate_permanent_work_id(self, _db: Session) -> str | None:
        """Try to calculate a permanent work ID from this metadata."""
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
        if not self.primary_identifier or not self.permanent_work_id:
            # We don't have the information necessary to carry out this
            # task.
            return

        if not self.medium:
            # We don't know the medium of this item, and we only want
            # to associate it with other items of the same type.
            return

        primary_identifier_obj, ignore = self.primary_identifier.load(_db)

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
                same_work_id.type != self.primary_identifier.type
                or same_work_id.identifier != self.primary_identifier.identifier
            ):
                self.log.info(
                    "Discovered that %r is equivalent to %r because of matching permanent work ID %s",
                    same_work_id,
                    primary_identifier_obj,
                    self.permanent_work_id,
                )
                primary_identifier_obj.equivalent_to(
                    self.data_source(_db), same_work_id, 0.85
                )

    def data_source(self, _db: Session) -> DataSource:
        if not self.data_source_obj:
            if not self.data_source_name:
                raise ValueError("No data source specified!")
            self.data_source_obj = DataSource.lookup(_db, self.data_source_name)
        if not self.data_source_obj:
            raise ValueError("Data source %s not found!" % self.data_source_name)
        return self.data_source_obj

    def edition(self, _db: Session) -> tuple[Edition, bool]:
        """Find or create the edition described by this Metadata object."""
        if not self.primary_identifier:
            raise ValueError("Cannot find edition: metadata has no primary identifier.")

        data_source = self.data_source(_db)

        return Edition.for_foreign_id(
            _db,
            data_source,
            self.primary_identifier.type,
            self.primary_identifier.identifier,
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
        """Try to find existing license pools for this Metadata."""
        potentials: dict[LicensePool, float] = {}
        for contributor in self.contributors:
            if not any(
                x in contributor.roles
                for x in (Contributor.Role.AUTHOR, Contributor.Role.PRIMARY_AUTHOR)
            ):
                continue
            contributor.find_sort_name(_db)
            confidence = 0

            base = (
                _db.query(Edition)
                .filter(Edition.title.ilike(self.title))
                .filter(Edition.medium == Edition.BOOK_MEDIUM)
            )
            success = False

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
            if not success and contributor.sort_name:
                qu = base.filter(Edition.sort_author == contributor.sort_name)
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

    REL_REQUIRES_NEW_PRESENTATION_EDITION: list[str] = [
        LinkRelations.IMAGE,
        LinkRelations.THUMBNAIL_IMAGE,
    ]
    REL_REQUIRES_FULL_RECALCULATION: list[str] = [LinkRelations.DESCRIPTION]

    # TODO: We need to change all calls to apply() to use a ReplacementPolicy
    # instead of passing in individual `replace` arguments. Once that's done,
    # we can get rid of the `replace` arguments.
    def apply(
        self,
        edition: Edition,
        collection: Collection | None,
        replace: ReplacementPolicy | None = None,
        replace_identifiers: bool = False,
        replace_subjects: bool = False,
        replace_contributions: bool = False,
        replace_links: bool = False,
        replace_formats: bool = False,
        replace_rights: bool = False,
        force: bool = False,
        db: Session | None = None,
    ) -> tuple[Edition, bool]:
        """Apply this metadata to the given edition.

        :return: (edition, made_core_changes), where edition is the newly-updated object, and made_core_changes
            answers the question: were any edition core fields harmed in the making of this update?
            So, if title changed, return True.
            New: If contributors changed, this is now considered a core change,
            so work.simple_opds_feed refresh can be triggered.
        """
        if not db:
            _db = Session.object_session(edition)
        else:
            _db = db
        # If summary, subjects, or measurements change, then any Work
        # associated with this edition will need a full presentation
        # recalculation.
        work_requires_full_recalculation = False

        # If any other data changes, then any Work associated with
        # this edition will need to have its presentation edition
        # regenerated, but we can do it on the cheap.
        work_requires_new_presentation_edition = False

        if replace is None:
            replace = ReplacementPolicy(
                identifiers=replace_identifiers,
                subjects=replace_subjects,
                contributions=replace_contributions,
                links=replace_links,
                formats=replace_formats,
                rights=replace_rights,
                even_if_not_apparently_updated=force,
            )

        # We were given an Edition, so either this metadata's
        # primary_identifier must be missing or it must match the
        # Edition's primary identifier.
        if self.primary_identifier:
            if (
                self.primary_identifier.type != edition.primary_identifier.type
                or self.primary_identifier.identifier
                != edition.primary_identifier.identifier
            ):
                raise ValueError(
                    "Metadata's primary identifier (%s/%s) does not match edition's primary identifier (%r)"
                    % (
                        self.primary_identifier.type,
                        self.primary_identifier.identifier,
                        edition.primary_identifier,
                    )
                )

        # Check whether we should do any work at all.
        data_source = self.data_source(_db)

        if self.data_source_last_updated and not replace.even_if_not_apparently_updated:
            coverage_record = CoverageRecord.lookup(edition, data_source)
            if coverage_record:
                check_time = coverage_record.timestamp
                last_time = self.data_source_last_updated
                if check_time >= last_time:
                    # The metadata has not changed since last time. Do nothing.
                    return edition, False

        identifier = edition.primary_identifier

        self.log.info("APPLYING METADATA TO EDITION: %s", self.title)
        fields = self.BASIC_EDITION_FIELDS + ["permanent_work_id"]
        for field in fields:
            old_edition_value = getattr(edition, field)
            new_metadata_value = getattr(self, field)
            if (
                new_metadata_value != None
                and new_metadata_value != ""
                and (new_metadata_value != old_edition_value)
            ):
                if new_metadata_value in [NO_VALUE, NO_NUMBER]:
                    new_metadata_value = None
                setattr(edition, field, new_metadata_value)
                work_requires_new_presentation_edition = True

        # Create equivalencies between all given identifiers and
        # the edition's primary identifier.
        contributors_changed = self.update_contributions(
            _db, edition, replace.contributions
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
                    _db, identifier_data.type, identifier_data.identifier
                )
                identifier.equivalent_to(
                    data_source, new_identifier, identifier_data.weight
                )

        new_subjects = {}
        if self.subjects:
            new_subjects = {subject.key: subject for subject in self.subjects}
        if replace.subjects:
            # Remove any old Subjects from this data source, unless they
            # are also in the list of new subjects.
            surviving_classifications = []

            def _key(
                classification: Classification,
            ) -> tuple[str, str | None, str | None, int]:
                s = classification.subject
                return s.type, s.identifier, s.name, classification.weight

            for classification in identifier.classifications:
                if classification.data_source == data_source:
                    key = _key(classification)
                    if not key in new_subjects:
                        # The data source has stopped claiming that
                        # this classification should exist.
                        _db.delete(classification)
                        work_requires_full_recalculation = True
                    else:
                        # The data source maintains that this
                        # classification is a good idea. We don't have
                        # to do anything.
                        del new_subjects[key]
                        surviving_classifications.append(classification)
                else:
                    # This classification comes from some other data
                    # source.  Don't mess with it.
                    surviving_classifications.append(classification)
            identifier.classifications = surviving_classifications

        # Apply all new subjects to the identifier.
        for subject in list(new_subjects.values()):
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
                    _db.delete(hyperlink)
                    dirty = True
                else:
                    surviving_hyperlinks.append(hyperlink)
            if dirty:
                identifier.links = surviving_hyperlinks

        link_objects = {}

        for link in self.links:
            if link.rel in Hyperlink.METADATA_ALLOWED:
                original_resource = None
                if link.original:
                    rights_status = RightsStatus.lookup(_db, link.original.rights_uri)
                    original_resource, ignore = get_one_or_create(
                        _db,
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
                    db=_db,
                )
                if link.rel in self.REL_REQUIRES_NEW_PRESENTATION_EDITION:
                    work_requires_new_presentation_edition = True
                elif link.rel in self.REL_REQUIRES_FULL_RECALCULATION:
                    work_requires_full_recalculation = True

            link_objects[link] = link_obj
            if link.thumbnail:
                if link.thumbnail.rel == Hyperlink.THUMBNAIL_IMAGE:
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
                else:
                    self.log.error(
                        "Thumbnail link %r does not have the thumbnail link relation! Not acceptable as a thumbnail of %r."
                        % (link.thumbnail, link)
                    )
                    link.thumbnail = None

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

        # The Metadata object may include a CirculationData object which
        # contains information about availability such as open-access
        # links. Make sure
        # that that Collection has a LicensePool for this book and that
        # its information is up-to-date.
        if self.circulation:
            self.circulation.apply(_db, collection, replace)

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
                self.make_thumbnail(_db, data_source, link, link_obj)

        # Make sure the work we just did shows up.
        made_changes = edition.calculate_presentation(
            policy=replace.presentation_calculation_policy
        )
        if made_changes:
            work_requires_new_presentation_edition = True

        # Update the coverage record for this edition and data
        # source. We omit the collection information, even if we know
        # which collection this is, because we only changed metadata.
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
                _db,
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
            contributor_data.find_sort_name(_db)
            if (
                contributor_data.sort_name
                or contributor_data.lc
                or contributor_data.viaf
            ):
                contributor = edition.add_contributor(
                    name=contributor_data.sort_name,
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

    def filter_recommendations(self, _db: Session) -> None:
        """Filters out recommended identifiers that don't exist in the db.
        Any IdentifierData objects will be replaced with Identifiers.
        """

        by_type: defaultdict[str, list[str]] = defaultdict(list)
        for identifier in self.recommendations:
            by_type[identifier.type].append(identifier.identifier)

        recommendations = set()
        for type, identifiers in list(by_type.items()):
            existing_identifiers = (
                _db.query(Identifier)
                .filter(Identifier.type == type)
                .filter(Identifier.identifier.in_(identifiers))
            )
            recommendations.update(existing_identifiers.all())

        if self.primary_identifier:
            primary_identifier_obj, _ = self.primary_identifier.load(_db)
            if primary_identifier_obj in recommendations:
                recommendations.remove(primary_identifier_obj)

        self.recommendations = list(recommendations)


class CSVFormatError(csv.Error):
    pass


class CSVMetadataImporter(LoggerMixin):
    """Turn a CSV file into a list of Metadata objects."""

    IDENTIFIER_PRECEDENCE = [
        Identifier.AXIS_360_ID,
        Identifier.OVERDRIVE_ID,
        Identifier.THREEM_ID,
        Identifier.ISBN,
    ]

    DEFAULT_IDENTIFIER_FIELD_NAMES = {
        Identifier.OVERDRIVE_ID: ("overdrive id", 0.75),
        Identifier.THREEM_ID: ("3m id", 0.75),
        Identifier.AXIS_360_ID: ("axis 360 id", 0.75),
        Identifier.ISBN: ("isbn", 0.75),
    }

    # When classifications are imported from a CSV file, we treat
    # them as though they came from a trusted distributor.
    DEFAULT_SUBJECT_FIELD_NAMES = {
        "tags": (Subject.TAG, Classification.TRUSTED_DISTRIBUTOR_WEIGHT),
        "age": (Subject.AGE_RANGE, Classification.TRUSTED_DISTRIBUTOR_WEIGHT),
        "audience": (
            Subject.FREEFORM_AUDIENCE,
            Classification.TRUSTED_DISTRIBUTOR_WEIGHT,
        ),
    }

    def __init__(
        self,
        data_source_name: str,
        title_field: str = "title",
        language_field: str = "language",
        default_language: str = "eng",
        medium_field: str = "medium",
        default_medium: str = Edition.BOOK_MEDIUM,
        series_field: str = "series",
        publisher_field: str = "publisher",
        imprint_field: str = "imprint",
        issued_field: str = "issued",
        published_field: Sequence[str] | str = ["published", "publication year"],
        identifier_fields: Mapping[
            str, tuple[str, float]
        ] = DEFAULT_IDENTIFIER_FIELD_NAMES,
        subject_fields: Mapping[str, tuple[str, int]] = DEFAULT_SUBJECT_FIELD_NAMES,
        sort_author_field: str = "file author as",
        display_author_field: Sequence[str] | str = ["author", "display author as"],
    ) -> None:
        self.data_source_name = data_source_name
        self.title_field = title_field
        self.language_field = language_field
        self.default_language = default_language
        self.medium_field = medium_field
        self.default_medium = default_medium
        self.series_field = series_field
        self.publisher_field = publisher_field
        self.imprint_field = imprint_field
        self.issued_field = issued_field
        self.published_field = published_field
        self.identifier_fields = identifier_fields
        self.subject_fields = subject_fields
        self.sort_author_field = sort_author_field
        self.display_author_field = display_author_field

    def to_metadata(self, dictreader: csv.DictReader[str]) -> Generator[Metadata]:
        """Turn the CSV file in `dictreader` into a sequence of Metadata.

        :yield: A sequence of Metadata objects.
        """
        fields = dictreader.fieldnames
        if fields is None:
            # fields is none if the CSV file is empty, so we just return
            return

        # Make sure this CSV file has some way of identifying books.
        found_identifier_field = False
        possibilities = []
        for field_name, weight in self.identifier_fields.values():
            possibilities.append(field_name)
            if field_name in fields:
                found_identifier_field = True
                break
        if not found_identifier_field:
            raise CSVFormatError(
                "Could not find a primary identifier field. Possibilities: %r. Actualities: %r."
                % (possibilities, fields)
            )

        for row in dictreader:
            yield self.row_to_metadata(row)

    def row_to_metadata(self, row: dict[str, str]) -> Metadata:
        title = self._field(row, self.title_field)
        language = self._field(row, self.language_field, self.default_language)
        medium = self._field(row, self.medium_field, self.default_medium)
        if medium not in Edition.medium_to_additional_type.keys():
            self.log.warning("Ignored unrecognized medium %s" % medium)
            medium = Edition.BOOK_MEDIUM
        series = self._field(row, self.series_field)
        publisher = self._field(row, self.publisher_field)
        imprint = self._field(row, self.imprint_field)
        issued = self._date_field(row, self.issued_field)
        published = self._date_field(row, self.published_field)

        primary_identifier = None
        identifiers = []
        # TODO: This is annoying and could use some work.
        for identifier_type in self.IDENTIFIER_PRECEDENCE:
            correct_type = False
            for target_type, (field_name, weight) in self.identifier_fields.items():
                if target_type == identifier_type:
                    correct_type = True
                    break
            if not correct_type:
                continue

            if field_name in row:
                value = self._field(row, field_name)
                if value:
                    identifier = IdentifierData(identifier_type, value, weight=weight)
                    identifiers.append(identifier)
                    if not primary_identifier:
                        primary_identifier = identifier

        subjects = []
        for field_name, (subject_type, weight) in list(self.subject_fields.items()):
            values = self.list_field(row, field_name)
            for value in values:
                subjects.append(
                    SubjectData(type=subject_type, identifier=value, weight=weight)
                )

        contributors = []
        sort_author = self._field(row, self.sort_author_field)
        display_author = self._field(row, self.display_author_field)
        if sort_author or display_author:
            contributors.append(
                ContributorData(
                    sort_name=sort_author,
                    display_name=display_author,
                    roles=[Contributor.Role.AUTHOR],
                )
            )

        metadata = Metadata(
            data_source=self.data_source_name,
            title=title,
            language=language,
            medium=medium,
            series=series,
            publisher=publisher,
            imprint=imprint,
            issued=issued,
            published=published,
            primary_identifier=primary_identifier,
            identifiers=identifiers,
            subjects=subjects,
            contributors=contributors,
        )
        return metadata

    def list_field(self, row: dict[str, str], names: str | Sequence[str]) -> list[str]:
        """Parse a string into a list by splitting on commas."""
        value = self._field(row, names)
        if not value:
            return []
        return [item.strip() for item in value.split(",")]

    def _field(
        self,
        row: dict[str, str],
        names: str | Sequence[str],
        default: str | None = None,
    ) -> str | None:
        """Get a value from one of the given fields and ensure it comes in as
        Unicode.
        """
        if isinstance(names, (bytes, str)):
            return self.__field(row, names, default)
        for name in names:
            v = self.__field(row, name)
            if v:
                return v
        return default

    def __field(
        self, row: dict[str, str], name: str, default: str | None = None
    ) -> str | None:
        """Get a value from the given field and ensure it comes in as
        Unicode.
        """
        value = row.get(name, default)
        if isinstance(value, bytes):
            value = value.decode("utf8")  # type: ignore[unreachable]
        return value

    def _date_field(
        self, row: dict[str, str], field_name: str | Sequence[str]
    ) -> datetime.datetime | None:
        """Attempt to parse a field as a date."""
        value = self._field(row, field_name)
        if value:
            try:
                return to_utc(parse(value))
            except ValueError:
                self.log.warning('Could not parse date "%s"' % value)
        return None
