from __future__ import annotations

import datetime

# Identifier, Equivalency
import random
import re
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable, Mapping
from functools import total_ordering
from typing import TYPE_CHECKING, Literal, NamedTuple, overload
from urllib.parse import quote, unquote

import isbnlib
from sqlalchemy import (
    Boolean,
    Column,
    Computed,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.orm import Mapped, Query, joinedload, relationship, selectinload
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import Select, select
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.expression import and_, or_

from palace.manager.core.exceptions import BasePalaceException, PalaceValueError
from palace.manager.data_layer.policy.presentation import (
    PresentationCalculationPolicy,
)
from palace.manager.sqlalchemy.constants import IdentifierConstants, LinkRelations
from palace.manager.sqlalchemy.model.base import Base
from palace.manager.sqlalchemy.model.classification import Classification, Subject
from palace.manager.sqlalchemy.model.coverage import CoverageRecord
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolDeliveryMechanism,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.util import create, get_one, get_one_or_create
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.log import LoggerMixin
from palace.manager.util.summary import SummaryEvaluator

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.collection import Collection
    from palace.manager.sqlalchemy.model.edition import Edition
    from palace.manager.sqlalchemy.model.patron import Annotation
    from palace.manager.sqlalchemy.model.resource import Hyperlink, Resource
    from palace.manager.sqlalchemy.model.work import Work


class IdentifierParser(ABC, LoggerMixin):
    """Interface for identifier parsers."""

    @abstractmethod
    def parse(self, identifier_string: str) -> tuple[str, str] | None:
        """Parse a string containing an identifier, extract it and determine its type.

        :param identifier_string: String containing an identifier
        :return: 2-tuple containing the identifier's type and identifier itself or None
            if the string contains an incorrect identifier
        """
        raise NotImplementedError()


class ISBNURNIdentifierParser(IdentifierParser):
    """Parser for ISBN URN IDs."""

    def parse(self, identifier_string: str) -> tuple[str, str] | None:
        self.log.debug(f'Started parsing identifier string "{identifier_string}"')

        if identifier_string.lower().startswith(Identifier.ISBN_URN_SCHEME_PREFIX):
            identifier_string = identifier_string[
                len(Identifier.ISBN_URN_SCHEME_PREFIX) :
            ]
            identifier_string = unquote(identifier_string)
            # Make sure this is a valid ISBN, and convert it to an ISBN-13.
            if not (
                isbnlib.is_isbn10(identifier_string)
                or isbnlib.is_isbn13(identifier_string)
            ):
                raise ValueError("%s is not a valid ISBN." % identifier_string)
            if isbnlib.is_isbn10(identifier_string):
                identifier_string = isbnlib.to_isbn13(identifier_string)

            return (Identifier.ISBN, identifier_string)

        self.log.debug(
            'Finished parsing identifier string "{}". It does not contain a '
            "ISBN URN ID".format(identifier_string)
        )

        return None


class URNIdentifierParser(IdentifierParser):
    """Parser for URN IDs."""

    def parse(self, identifier_string: str) -> tuple[str, str] | None:
        self.log.debug(f'Started parsing identifier string "{identifier_string}"')

        if identifier_string.startswith(Identifier.URN_SCHEME_PREFIX):
            identifier_string = identifier_string[len(Identifier.URN_SCHEME_PREFIX) :]
            type, identifier_string = list(
                map(unquote, identifier_string.split("/", 1))
            )
            return (type, identifier_string)

        self.log.debug(
            'Finished parsing identifier string "{}". It does not contain a '
            "URN ID".format(identifier_string)
        )

        return None


class GenericURNIdentifierParser(IdentifierParser):
    """Parser for Generic URN IDs."""

    def parse(self, identifier_string: str) -> tuple[str, str] | None:
        self.log.debug(f'Started parsing identifier string "{identifier_string}"')

        if identifier_string.startswith(Identifier.OTHER_URN_SCHEME_PREFIX):
            return (Identifier.URI, identifier_string)

        self.log.debug(
            'Finished parsing identifier string "{}". It does not contain a '
            "Generic URN ID".format(identifier_string)
        )

        return None


class URIIdentifierParser(IdentifierParser):
    """Parser for URI IDs."""

    def parse(self, identifier_string: str) -> tuple[str, str] | None:
        self.log.debug(f'Started parsing identifier string "{identifier_string}"')

        if identifier_string.startswith("http:") or identifier_string.startswith(
            "https:"
        ):
            return (Identifier.URI, identifier_string)

        self.log.debug(
            'Finished parsing identifier string "{}". It does not contain a '
            "URI ID".format(identifier_string)
        )

        return None


class GutenbergIdentifierParser(IdentifierParser):
    """Parser for Gutenberg Doc IDs."""

    ID_REGEX = IdentifierConstants.GUTENBERG_URN_SCHEME_RE

    def parse(self, identifier_string: str) -> tuple[str, str] | None:
        self.log.debug(f'Started parsing identifier string "{identifier_string}"')

        match = self.ID_REGEX.match(identifier_string)

        if match:
            document_id = match.groups()[0]
            result = (Identifier.GUTENBERG_ID, document_id)

            self.log.debug(
                'Finished parsing identifier string "{}". Result: {}'.format(
                    document_id, result
                )
            )

            return result

        self.log.debug(
            'Finished parsing identifier string "{}". It does not contain a ProQuest Doc ID'.format(
                identifier_string
            )
        )

        return None


class ProQuestIdentifierParser(IdentifierParser):
    """Parser for ProQuest Doc IDs."""

    ID_REGEX = re.compile(r"urn:proquest.com/document-id/(\d+)")

    def parse(self, identifier_string: str) -> tuple[str, str] | None:
        self.log.debug(f'Started parsing identifier string "{identifier_string}"')

        match = self.ID_REGEX.match(identifier_string)

        if match:
            document_id = match.groups()[0]
            result = (Identifier.PROQUEST_ID, document_id)

            self.log.debug(
                'Finished parsing identifier string "{}". Result={}'
                "".format(identifier_string, result)
            )

            return result

        self.log.debug(
            'Finished parsing identifier string "{}". It does not contain a ProQuest Doc ID'.format(
                identifier_string
            )
        )

        return None


@total_ordering
class Identifier(Base, IdentifierConstants, LoggerMixin):
    """A way of uniquely referring to a particular edition."""

    __tablename__ = "identifiers"
    id: Mapped[int] = Column(Integer, primary_key=True)
    type: Mapped[str] = Column(String(64), index=True, nullable=False)

    @property
    def active_type(self) -> str:
        """
        The type of this Identifier, normalized to replace any
        deprecated types with their current equivalents.
        """
        return self.get_active_type(self.type)

    identifier: Mapped[str] = Column(String, index=True, nullable=False)

    collections: Mapped[list[Collection]] = relationship(
        "Collection", secondary="collections_identifiers", back_populates="catalog"
    )

    equivalencies: Mapped[list[Equivalency]] = relationship(
        "Equivalency",
        foreign_keys="Equivalency.input_id",
        back_populates="input",
        cascade="all, delete-orphan",
        uselist=True,
    )

    inbound_equivalencies: Mapped[list[Equivalency]] = relationship(
        "Equivalency",
        foreign_keys="Equivalency.output_id",
        back_populates="output",
        cascade="all, delete-orphan",
        uselist=True,
    )

    # One Identifier may have many associated CoverageRecords.
    coverage_records: Mapped[list[CoverageRecord]] = relationship(
        "CoverageRecord", back_populates="identifier"
    )

    def __repr__(self) -> str:
        records = self.primarily_identifies
        if records and records[0].title:
            title = ' prim_ed=%d ("%s")' % (records[0].id, records[0].title)
        else:
            title = ""
        return f"{self.type}/{self.identifier} ID={self.id}{title}"

    # One Identifier may serve as the primary identifier for
    # several Editions.
    primarily_identifies: Mapped[list[Edition]] = relationship(
        "Edition", back_populates="primary_identifier"
    )

    # One Identifier may serve as the identifier for many
    # LicensePools, through different Collections.
    licensed_through: Mapped[list[LicensePool]] = relationship(
        "LicensePool",
        back_populates="identifier",
        lazy="joined",
        overlaps="delivery_mechanisms",
    )

    # One Identifier may have many Links.
    links: Mapped[list[Hyperlink]] = relationship(
        "Hyperlink", back_populates="identifier", uselist=True
    )

    # One Identifier may be the subject of many Measurements.
    measurements: Mapped[list[Measurement]] = relationship(
        "Measurement", back_populates="identifier"
    )

    # One Identifier may participate in many Classifications.
    classifications: Mapped[list[Classification]] = relationship(
        "Classification", back_populates="identifier"
    )

    # One identifier may participate in many Annotations.
    annotations: Mapped[list[Annotation]] = relationship(
        "Annotation", back_populates="identifier"
    )

    # One Identifier can have many LicensePoolDeliveryMechanisms.
    delivery_mechanisms: Mapped[list[LicensePoolDeliveryMechanism]] = relationship(
        "LicensePoolDeliveryMechanism", back_populates="identifier"
    )

    # Type + identifier is unique.
    __table_args__ = (UniqueConstraint("type", "identifier"),)

    @classmethod
    def get_active_type(cls, identifier_type: str) -> str:
        """Convert a deprecated identifier type to its current equivalent."""
        return cls.DEPRECATED_NAMES.get(identifier_type, identifier_type)

    @classmethod
    @overload
    def for_foreign_id(
        cls,
        _db: Session,
        foreign_identifier_type: str,
        foreign_id: str,
        autocreate: Literal[True] = ...,
    ) -> tuple[Identifier, bool]: ...

    @classmethod
    @overload
    def for_foreign_id(
        cls,
        _db: Session,
        foreign_identifier_type: str | None,
        foreign_id: str | None,
        autocreate: bool = ...,
    ) -> tuple[Identifier | None, bool]: ...

    @classmethod
    def for_foreign_id(
        cls,
        _db: Session,
        foreign_identifier_type: str | None,
        foreign_id: str | None,
        autocreate: bool = True,
    ) -> tuple[Identifier | None, bool]:
        """Turn a foreign ID into an Identifier."""
        if not foreign_identifier_type or not foreign_id:
            return None, False

        primary_type, secondary_type, foreign_id = (
            cls.prepare_foreign_type_and_identifier(foreign_identifier_type, foreign_id)
        )

        constraint = (
            or_(cls.type == primary_type, cls.type == secondary_type)
            if secondary_type
            else cls.type == primary_type
        )
        result: Identifier | None
        if autocreate:
            result, is_new = get_one_or_create(
                _db,
                cls,
                identifier=foreign_id,
                constraint=constraint,
                create_method_kwargs={
                    "type": primary_type,
                },
            )
        else:
            is_new = False
            result = get_one(_db, cls, identifier=foreign_id, constraint=constraint)

        return result, is_new

    class _ForeignIdentifierTuple(NamedTuple):
        """A tuple representing a foreign identifier and its type."""

        primary_type: str
        secondary_type: str | None
        identifier: str

    @classmethod
    def prepare_foreign_type_and_identifier(
        cls, foreign_type: str, foreign_identifier: str
    ) -> _ForeignIdentifierTuple:
        # Turn a deprecated identifier type into a current one.
        if foreign_type in cls.DEPRECATED_NAMES:
            primary_type = cls.DEPRECATED_NAMES[foreign_type]
            secondary_type = foreign_type
        elif foreign_type in cls.DEPRECATED_NAMES.inverse:
            primary_type = foreign_type
            secondary_type = cls.DEPRECATED_NAMES.inverse[foreign_type]
        else:
            primary_type = foreign_type
            secondary_type = None

        if primary_type in (Identifier.OVERDRIVE_ID, Identifier.BIBLIOTHECA_ID):
            foreign_identifier = foreign_identifier.lower()

        if not cls.valid_as_foreign_identifier(primary_type, foreign_identifier):
            raise ValueError(f'"{foreign_identifier}" is not a valid {primary_type}.')

        return cls._ForeignIdentifierTuple(
            primary_type, secondary_type, foreign_identifier
        )

    @classmethod
    def valid_as_foreign_identifier(cls, type: str, id: str) -> bool:
        """Return True if the given `id` can be an Identifier of the given
        `type`.
        This is not a complete implementation; we will add to it as
        necessary.
        In general we err on the side of allowing IDs that look
        invalid (e.g. all Overdrive IDs look like UUIDs, but we
        currently don't enforce that). We only reject an ID out of
        hand if it will cause problems with a third-party API.
        """
        forbidden_characters = ""
        if type == Identifier.BIBLIOTHECA_ID:
            # IDs are joined with commas and provided as a URL path
            # element.  Embedded commas or slashes will confuse the
            # Bibliotheca API.
            forbidden_characters = ",/"
        elif type == Identifier.AXIS_360_ID:
            # IDs are joined with commas during a lookup. Embedded
            # commas will confuse the Boundless API.
            forbidden_characters = ","
        if any(x in id for x in forbidden_characters):
            return False
        return True

    @property
    def urn(self) -> str:
        return self._urn_from_type_and_value(self.type, self.identifier)

    @classmethod
    def _urn_from_type_and_value(cls, id_type: str, id_value: str) -> str:
        identifier_text = quote(id_value)
        if id_type == Identifier.ISBN:
            return cls.ISBN_URN_SCHEME_PREFIX + identifier_text
        elif id_type == Identifier.URI:
            return id_value
        elif id_type == Identifier.GUTENBERG_ID:
            return cls.GUTENBERG_URN_SCHEME_PREFIX + identifier_text
        else:
            identifier_type = quote(id_type)
            return f"{cls.URN_SCHEME_PREFIX}{identifier_type}/{identifier_text}"

    @property
    def work(self) -> Work | None:
        """Find the Work, if any, associated with this Identifier.
        Although one Identifier may be associated with multiple LicensePools,
        all of them must share a Work.
        """
        for lp in self.licensed_through:
            if lp.work:
                return lp.work
        return None

    class UnresolvableIdentifierException(BasePalaceException):
        # Raised when an identifier that can't be resolved into a LicensePool
        # is provided in a context that requires a resolvable identifier
        pass

    PARSERS: list[IdentifierParser] = [
        GutenbergIdentifierParser(),
        URIIdentifierParser(),
        ISBNURNIdentifierParser(),
        URNIdentifierParser(),
        ProQuestIdentifierParser(),
        GenericURNIdentifierParser(),
    ]

    @classmethod
    def type_and_identifier_for_urn(cls, identifier_string: str) -> tuple[str, str]:
        for parser in Identifier.PARSERS:
            result = parser.parse(identifier_string)
            if result:
                identifier_type, identifier = result
                return identifier_type, identifier

        raise PalaceValueError(
            "Could not turn %s into a recognized identifier." % identifier_string
        )

    @classmethod
    def parse_urns(
        cls,
        _db: Session,
        identifier_strings: Iterable[str],
        autocreate: bool = True,
        allowed_types: list[str] | None = None,
    ) -> tuple[dict[str, Identifier], list[str]]:
        """Converts a batch of URNs into Identifier objects.

        :param _db: A database connection
        :param identifier_strings: A list of strings, each a URN
            identifying some identifier.
        :param autocreate: Create an Identifier for a URN if none
            presently exists.
        :param allowed_types: If this is a list of Identifier
            types, only identifiers of those types may be looked
            up. All other identifier types will be treated as though
            they did not exist.
        :return: A 2-tuple (identifiers, failures). `identifiers` is a
            list of Identifiers. `failures` is a list of URNs that
            did not become Identifiers.
        """
        allowed_types_set = set(allowed_types) if allowed_types is not None else None
        failures = list()
        identifier_details = dict()
        for urn in identifier_strings:
            try:
                primary_type, secondary_type, identifier = (
                    cls.prepare_foreign_type_and_identifier(
                        *cls.type_and_identifier_for_urn(urn)
                    )
                )
                if (
                    primary_type
                    and identifier
                    and (allowed_types_set is None or primary_type in allowed_types_set)
                ):
                    identifier_details[urn] = (primary_type, identifier)
                else:
                    failures.append(urn)
            except ValueError:
                failures.append(urn)

        identifiers_by_urn = dict()

        def find_existing_identifiers(
            identifier_details: list[tuple[str, str]],
        ) -> None:
            if not identifier_details:
                return
            and_clauses = list()
            for type, identifier in identifier_details:
                and_clauses.append(and_(cls.type == type, cls.identifier == identifier))

            identifiers = _db.query(cls).filter(or_(*and_clauses)).all()
            for identifier_obj in identifiers:
                identifiers_by_urn[identifier_obj.urn] = identifier_obj

        # Find identifiers that are already in the database.
        find_existing_identifiers(list(identifier_details.values()))

        # Remove the existing identifiers from the identifier_details list,
        # regardless of whether the provided URN was accurate.
        existing_details = [
            (i.type, i.identifier) for i in list(identifiers_by_urn.values())
        ]
        identifier_details = {
            k: v
            for k, v in list(identifier_details.items())
            if v not in existing_details and k not in list(identifiers_by_urn.keys())
        }

        if not autocreate:
            # Don't make new identifiers. Send back unfound urns as failures.
            failures.extend(list(identifier_details.keys()))
            return identifiers_by_urn, failures

        # Find any identifier details that don't correspond to an existing
        # identifier. Try to create them.
        new_identifiers = list()
        new_identifiers_details = set()
        for urn, details in list(identifier_details.items()):
            if details in new_identifiers_details:
                # For some reason, this identifier is here twice.
                # Don't try to insert it twice.
                continue
            new_identifiers.append(dict(type=details[0], identifier=details[1]))
            new_identifiers_details.add(details)

        # Insert new identifiers into the database, then add them to the
        # results.
        if new_identifiers:
            _db.bulk_insert_mappings(cls, new_identifiers)
            _db.commit()
        find_existing_identifiers(list(identifier_details.values()))

        return identifiers_by_urn, failures

    @classmethod
    @overload
    def _parse_urn(
        cls,
        _db: Session,
        identifier_string: str,
        identifier_type: str,
        must_support_license_pools: bool = ...,
        autocreate: Literal[True] = ...,
    ) -> tuple[Identifier, bool]: ...

    @classmethod
    @overload
    def _parse_urn(
        cls,
        _db: Session,
        identifier_string: str,
        identifier_type: str,
        must_support_license_pools: bool = ...,
        autocreate: bool = ...,
    ) -> tuple[Identifier | None, bool]: ...

    @classmethod
    def _parse_urn(
        cls,
        _db: Session,
        identifier_string: str,
        identifier_type: str,
        must_support_license_pools: bool = False,
        autocreate: bool = True,
    ) -> tuple[Identifier | None, bool]:
        """Parse identifier string.

        :param _db: Database session
        :param identifier_string: Identifier itself
        :param identifier_type: Identifier's type
        :param must_support_license_pools: Boolean value indicating whether there should be a DataSource that provides
            licenses for books identified by the given identifier
        :param autocreate: Boolean value indicating whether an identifier should be created if it's not found.
        :return: 2-tuple containing Identifier object and a boolean value indicating whether it's new
        """
        if must_support_license_pools:
            try:
                _ = DataSource.license_source_for(_db, identifier_type)
            except NoResultFound:
                raise Identifier.UnresolvableIdentifierException()
            except MultipleResultsFound:
                # This is fine.
                pass

        return cls.for_foreign_id(
            _db, identifier_type, identifier_string, autocreate=autocreate
        )

    @classmethod
    @overload
    def parse_urn(
        cls,
        _db: Session,
        identifier_string: str,
        must_support_license_pools: bool = False,
        autocreate: Literal[True] = ...,
    ) -> tuple[Identifier, bool]: ...

    @classmethod
    @overload
    def parse_urn(
        cls,
        _db: Session,
        identifier_string: str | None,
        must_support_license_pools: bool = False,
        autocreate: bool = ...,
    ) -> tuple[Identifier | None, bool | None]: ...

    @classmethod
    def parse_urn(
        cls,
        _db: Session,
        identifier_string: str | None,
        must_support_license_pools: bool = False,
        autocreate: bool = True,
    ) -> tuple[Identifier | None, bool | None]:
        """Parse identifier string.

        :param _db: Database session
        :param identifier_string: String containing an identifier
        :param must_support_license_pools: Boolean value indicating whether there should be a DataSource that provides
            licenses for books identified by the given identifier
        :param autocreate: Boolean value indicating whether an identifier should be created if it's not found.
        :return: 2-tuple containing Identifier object and a boolean value indicating whether it's new
        """
        # I added this in here in my refactoring because there is a test
        # that tests for this case. I'm not sure that it is necessary, but
        # I've assumed there was a reason that the test_identifier.test_parse_urn
        # tests for this case and thus ensure that it remains valid here.
        # test
        if identifier_string is None:
            return None, None

        identifier_type, identifier_string = cls.type_and_identifier_for_urn(
            identifier_string
        )

        return cls._parse_urn(
            _db,
            identifier_string,
            identifier_type,
            must_support_license_pools,
            autocreate=autocreate,
        )

    @classmethod
    def parse(
        cls,
        _db: Session,
        identifier_string: str,
        parser: IdentifierParser,
        must_support_license_pools: bool = False,
    ) -> tuple[Identifier, bool]:
        """Parse identifier string.

        :param _db: Database session
        :param identifier_string: String containing an identifier
        :param parser: Identifier parser
        :param must_support_license_pools: Boolean value indicating whether there should be a DataSource that provides
            licenses for books identified by the given identifier
        :return: 2-tuple containing Identifier object and a boolean value indicating whether it's new
        """
        identifier = parser.parse(identifier_string)
        if identifier is None:
            raise ValueError(f"Unable to parse identifier {identifier_string}.")
        identifier_type, identifier_string = identifier

        return cls._parse_urn(
            _db, identifier_string, identifier_type, must_support_license_pools
        )

    def equivalent_to(
        self, data_source: DataSource | None, identifier: Identifier, strength: float
    ) -> Equivalency | None:
        """Make one Identifier equivalent to another.
        `data_source` is the DataSource that believes the two
        identifiers are equivalent.
        """
        _db = Session.object_session(self)
        if self == identifier:
            # That an identifier is equivalent to itself is tautological.
            # Do nothing.
            return None
        eq, new = get_one_or_create(
            _db,
            Equivalency,
            data_source=data_source,
            input=self,
            output=identifier,
            on_multiple="interchangeable",
        )
        eq.strength = strength
        if new:
            self.log.info(
                "Identifier equivalency: %r==%r p=%.2f", self, identifier, strength
            )
        return eq

    @classmethod
    def recursively_equivalent_identifier_ids_query(
        cls,
        identifier_id_column: str,
        policy: PresentationCalculationPolicy | None = None,
    ) -> Select:
        """Get a SQL statement that will return all Identifier IDs
        equivalent to a given ID at the given confidence threshold.
        `identifier_id_column` can be a single Identifier ID, or a column
        like `Edition.primary_identifier_id` if the query will be used as
        a subquery.
        This uses the function defined in resources/sqlalchemy/recursive_equivalents.sql.
        """
        fn = cls._recursively_equivalent_identifier_ids_query(
            identifier_id_column, policy
        )
        return select(fn)

    @classmethod
    def _recursively_equivalent_identifier_ids_query(
        cls,
        identifier_id_column: str,
        policy: PresentationCalculationPolicy | None = None,
    ) -> ClauseElement:
        policy = policy or PresentationCalculationPolicy()
        levels = policy.equivalent_identifier_levels
        threshold = policy.equivalent_identifier_threshold
        cutoff = policy.equivalent_identifier_cutoff

        return func.fn_recursive_equivalents(
            identifier_id_column, levels, threshold, cutoff
        )

    @classmethod
    def recursively_equivalent_identifier_ids(
        cls,
        _db: Session,
        identifier_ids: list[int],
        policy: PresentationCalculationPolicy | None = None,
    ) -> dict[int, list[int]]:
        """All Identifier IDs equivalent to the given set of Identifier
        IDs at the given confidence threshold.
        This uses the function defined in resources/sqlalchemy/recursive_equivalents.sql.
        Four levels is enough to go from a Gutenberg text to an ISBN.
        Gutenberg ID -> OCLC Work IS -> OCLC Number -> ISBN
        Returns a dictionary mapping each ID in the original to a
        list of equivalent IDs.

        :param policy: A PresentationCalculationPolicy that explains
           how you've chosen to make the tradeoff between performance,
           data quality, and sheer number of equivalent identifiers.
        """
        fn = cls._recursively_equivalent_identifier_ids_query(Identifier.id, policy)
        query = select(Identifier.id, fn).where(Identifier.id.in_(identifier_ids))
        results = _db.execute(query)
        equivalents = defaultdict(list)
        for r in results:
            original = r[0]
            equivalent = r[1]
            equivalents[original].append(equivalent)
        return equivalents

    def equivalent_identifier_ids(
        self, policy: PresentationCalculationPolicy | None = None
    ) -> dict[int, list[int]]:
        _db = Session.object_session(self)
        return Identifier.recursively_equivalent_identifier_ids(_db, [self.id], policy)

    def licensed_through_collection(
        self, collection: Collection | None
    ) -> LicensePool | None:
        """Find the LicensePool, if any, for this Identifier
        in the given Collection.
        :return: At most one LicensePool.
        """
        for lp in self.licensed_through:
            if lp.collection == collection:
                return lp
        return None

    def add_link(
        self,
        rel: str,
        href: str | None,
        data_source: DataSource | None,
        media_type: str | None = None,
        content: bytes | str | None = None,
        content_path: str | None = None,
        rights_status_uri: str | None = None,
        rights_explanation: str | None = None,
        original_resource: Resource | None = None,
        transformation_settings: Mapping[str, str] | None = None,
        db: Session | None = None,
    ) -> tuple[Hyperlink, bool]:
        """Create a link between this Identifier and a (potentially new)
        Resource.
        TODO: There's some code in data_layer for automatically
        fetching, mirroring and scaling Representations as links are
        created. It might be good to move that code into here.
        """
        from palace.manager.sqlalchemy.model.resource import (
            Hyperlink,
            Representation,
            Resource,
        )

        if not db:
            _db = Session.object_session(self)
        else:
            _db = db
        # Find or create the Resource.
        if not href:
            href = Hyperlink.generic_uri(data_source, self, rel, content)
        rights_status = None
        if rights_status_uri:
            rights_status = RightsStatus.lookup(_db, rights_status_uri)
        resource, new_resource = get_one_or_create(
            _db,
            Resource,
            url=href,
            create_method_kwargs=dict(
                data_source=data_source,
                rights_status=rights_status,
                rights_explanation=rights_explanation,
            ),
        )

        # Find or create the Hyperlink.
        link, new_link = get_one_or_create(
            _db,
            Hyperlink,
            rel=rel,
            data_source=data_source,
            identifier=self,
            resource=resource,
        )

        if content or content_path:
            # We have content for this resource.
            resource.set_fetched_content(media_type, content, content_path)
        elif media_type and not resource.representation:
            # We know the type of the resource, so make a
            # Representation for it.
            rep, is_new = get_one_or_create(
                _db, Representation, url=resource.url, media_type=media_type
            )

            if resource.representation != rep:
                resource.representation = rep
        elif (
            media_type
            and resource.representation
            and resource.representation.media_type != media_type
        ):
            # Ensure we do not violate unique constraints
            representation_exists = (
                _db.query(Representation)
                .filter(
                    Representation.url == resource.url,
                    Representation.media_type == media_type,
                )
                .count()
            )
            if not representation_exists:
                # We have a representation that is not the same media type we previously knew of
                resource.representation.media_type = media_type
                resource.representation.url = resource.url

        if original_resource:
            original_resource.add_derivative(link.resource, transformation_settings)

        # TODO: This is where we would mirror the resource if we
        # wanted to.
        return link, new_link

    def add_measurement(
        self,
        data_source: DataSource,
        quantity_measured: str,
        value: float,
        weight: float = 1,
        taken_at: datetime.datetime | None = None,
    ) -> Measurement:
        """Associate a new Measurement with this Identifier."""
        _db = Session.object_session(self)

        self.log.debug(
            "MEASUREMENT: %s on %s/%s: %s == %s (wt=%d)",
            data_source.name,
            self.type,
            self.identifier,
            quantity_measured,
            value,
            weight,
        )

        now = utc_now()
        if taken_at is None:
            taken_at = now
        # Is there an existing most recent measurement?
        most_recent = get_one(
            _db,
            Measurement,
            identifier=self,
            data_source=data_source,
            quantity_measured=quantity_measured,
            is_most_recent=True,
            on_multiple="interchangeable",
        )
        if most_recent and most_recent.value == value and taken_at == now:
            # The value hasn't changed since last time. Just update
            # the timestamp of the existing measurement.
            self.taken_at = taken_at

        if most_recent and most_recent.taken_at and most_recent.taken_at < taken_at:
            most_recent.is_most_recent = False

        return create(
            _db,
            Measurement,
            identifier=self,
            data_source=data_source,
            quantity_measured=quantity_measured,
            taken_at=taken_at,
            value=value,
            weight=weight,
            is_most_recent=True,
        )[0]

    def classify(
        self,
        data_source: DataSource,
        subject_type: str,
        subject_identifier: str | None,
        subject_name: str | None = None,
        weight: int = 1,
    ) -> Classification:
        """Classify this Identifier under a Subject.

        :param type: Classification scheme; one of the constants from Subject.
        :param subject_identifier: Internal ID of the subject according to that classification scheme.
        :param subject_name: Human-readable description of the subject, if different
            from the ID.
        :param weight: How confident the data source is in classifying a
            book under this subject. The meaning of this
            number depends entirely on the source of the
            information.
        """
        _db = Session.object_session(self)
        # Turn the subject type and identifier into a Subject.
        subject, is_new = Subject.lookup(
            _db,
            subject_type,
            subject_identifier,
            subject_name,
        )

        self.log.debug(
            "CLASSIFICATION: %s on %s/%s: %s %s/%s (wt=%d)",
            data_source.name,
            self.type,
            self.identifier,
            subject.type,
            subject.identifier,
            subject.name,
            weight,
        )

        # Use a Classification to connect the Identifier to the
        # Subject.
        try:
            classification, is_new = get_one_or_create(
                _db,
                Classification,
                identifier=self,
                subject=subject,
                data_source=data_source,
                create_method_kwargs={"weight": weight},
            )
        except MultipleResultsFound as e:
            # TODO: This is a hack.
            all_classifications_query = _db.query(Classification).filter(
                Classification.identifier == self,
                Classification.subject == subject,
                Classification.data_source == data_source,
            )
            all_classifications = all_classifications_query.all()
            classification = all_classifications[0]
            for i in all_classifications[1:]:
                _db.delete(i)

        classification.weight = weight
        return classification

    @classmethod
    def resources_for_identifier_ids(
        self,
        _db: Session,
        identifier_ids: list[int],
        rel: str | list[str] | None = None,
        data_source: DataSource | list[DataSource] | None = None,
    ) -> Query[Resource]:
        from palace.manager.sqlalchemy.model.resource import Hyperlink, Resource

        resources = (
            _db.query(Resource)
            .join(Resource.links)
            .filter(Hyperlink.identifier_id.in_(identifier_ids))
        )
        if data_source:
            if isinstance(data_source, DataSource):
                data_source = [data_source]
            resources = resources.filter(
                Hyperlink.data_source_id.in_([d.id for d in data_source])
            )
        if rel:
            if isinstance(rel, list):
                resources = resources.filter(Hyperlink.rel.in_(rel))
            else:
                resources = resources.filter(Hyperlink.rel == rel)
        resources = resources.options(joinedload(Resource.representation))
        return resources

    @classmethod
    def classifications_for_identifier_ids(
        self, _db: Session, identifier_ids: list[int]
    ) -> Query[Classification]:
        classifications = _db.query(Classification).filter(
            Classification.identifier_id.in_(identifier_ids)
        )
        return classifications.options(joinedload(Classification.subject))

    @classmethod
    def best_cover_for(
        cls, _db: Session, identifier_ids: list[int], rel: str | None = None
    ) -> tuple[Resource | None, list[Resource]]:
        # Find all image resources associated with any of
        # these identifiers.
        from palace.manager.sqlalchemy.model.resource import Hyperlink, Resource

        rel = rel or Hyperlink.IMAGE
        images_query = cls.resources_for_identifier_ids(_db, identifier_ids, rel)
        images_query = images_query.join(Resource.representation)
        images = images_query.all()

        champions = Resource.best_covers_among(images)
        if not champions:
            champion = None
        elif len(champions) == 1:
            [champion] = champions
        else:
            champion = random.choice(champions)

        return champion, images

    @classmethod
    def evaluate_summary_quality(
        cls,
        _db: Session,
        identifier_ids: list[int],
        privileged_data_sources: list[DataSource] | None = None,
    ) -> tuple[Resource | None, list[Resource]]:
        """Evaluate the summaries for the given group of Identifier IDs.
        This is an automatic evaluation based solely on the content of
        the summaries. It will be combined with human-entered ratings
        to form an overall quality score.
        We need to evaluate summaries from a set of Identifiers
        (typically those associated with a single work) because we
        need to see which noun phrases are most frequently used to
        describe the underlying work.
        :param privileged_data_sources: If present, a summary from one
        of these data source will be instantly chosen, short-circuiting the
        decision process. Data sources are in order of priority.
        :return: The single highest-rated summary Resource.
        """
        evaluator = SummaryEvaluator()

        if privileged_data_sources and len(privileged_data_sources) > 0:
            privileged_data_source = privileged_data_sources[0]
        else:
            privileged_data_source = None

        # Find all rel="description" resources associated with any of
        # these records.
        rels = [LinkRelations.DESCRIPTION, LinkRelations.SHORT_DESCRIPTION]
        descriptions = cls.resources_for_identifier_ids(
            _db, identifier_ids, rels, privileged_data_source
        ).all()

        champion = None
        # Add each resource's content to the evaluator's corpus.
        for r in descriptions:
            if r.representation and r.representation.content:
                evaluator.add(r.representation.content)
        evaluator.ready()

        # Then have the evaluator rank each resource.
        for r in descriptions:
            if r.representation and r.representation.content:
                content = r.representation.content
                quality = evaluator.score(content)
                r.set_estimated_quality(quality)
            # If there's no champion yet, or if the current resource has a quality
            # and it's higher than the champion's quality (if the champion has one)
            if not champion or (
                r.quality is not None
                and (champion.quality is None or r.quality > champion.quality)
            ):
                champion = r

        if (
            privileged_data_sources
            and len(privileged_data_sources) > 0
            and not champion
        ):
            # We could not find any descriptions from the privileged
            # data source. Try relaxing that restriction.
            return cls.evaluate_summary_quality(
                _db, identifier_ids, privileged_data_sources[1:]
            )
        return champion, descriptions

    @classmethod
    def missing_coverage_from(
        cls,
        _db: Session,
        identifier_types: list[str] | None,
        coverage_data_source: DataSource | None,
        operation: str | None = None,
        count_as_covered: list[str] | None = None,
        count_as_missing_before: datetime.datetime | None = None,
        identifiers: list[Identifier] | None = None,
        collection: Collection | None = None,
    ) -> Query[Identifier]:
        """Find identifiers of the given types which have no CoverageRecord
        from `coverage_data_source`.
        :param count_as_covered: Identifiers will be counted as
        covered if their CoverageRecords have a status in this list.
        :param identifiers: Restrict search to a specific set of identifier objects.
        """
        if collection:
            collection_id = collection.id
        else:
            collection_id = None

        data_source_id = None
        if coverage_data_source:
            data_source_id = coverage_data_source.id

        clause = and_(
            Identifier.id == CoverageRecord.identifier_id,
            CoverageRecord.data_source_id == data_source_id,
            CoverageRecord.operation == operation,
            CoverageRecord.collection_id == collection_id,
        )
        qu = _db.query(Identifier).outerjoin(CoverageRecord, clause)
        if identifier_types:
            qu = qu.filter(Identifier.type.in_(identifier_types))
        missing = CoverageRecord.not_covered(count_as_covered, count_as_missing_before)
        qu = qu.filter(missing)

        if identifiers:
            qu = qu.filter(Identifier.id.in_([x.id for x in identifiers]))

        return qu

    def __eq__(self, other: object) -> bool:
        """Equality implementation for total_ordering."""
        # We don't want an Identifier to be == an IdentifierData
        # with the same data.
        if other is None or not isinstance(other, Identifier):
            return False
        return (self.type, self.identifier) == (other.type, other.identifier)

    def __hash__(self) -> int:
        return hash((self.type, self.identifier))

    def __lt__(self, other: object) -> bool:
        """Comparison implementation for total_ordering."""
        if other is None or not isinstance(other, Identifier):
            return False
        return (self.type, self.identifier) < (other.type, other.identifier)


class Equivalency(Base):
    """An assertion that two Identifiers identify the same work.
    This assertion comes with a 'strength' which represents how confident
    the data source is in the assertion.
    """

    __tablename__ = "equivalents"

    # 'input' is the ID that was used as input to the datasource.
    # 'output' is the output
    id: Mapped[int] = Column(Integer, primary_key=True)
    input_id: Mapped[int] = Column(
        Integer, ForeignKey("identifiers.id"), index=True, nullable=False
    )
    input: Mapped[Identifier] = relationship(
        "Identifier", foreign_keys=input_id, back_populates="equivalencies"
    )
    output_id: Mapped[int] = Column(Integer, ForeignKey("identifiers.id"), index=True)
    output: Mapped[Identifier] = relationship(
        "Identifier", foreign_keys=output_id, back_populates="inbound_equivalencies"
    )

    # Who says?
    data_source_id = Column(Integer, ForeignKey("datasources.id"), index=True)
    data_source: Mapped[DataSource | None] = relationship(
        "DataSource", back_populates="id_equivalencies"
    )

    # How many distinct votes went into this assertion? This will let
    # us scale the change to the strength when additional votes come
    # in.
    votes: Mapped[int] = Column(Integer, default=1, nullable=False)

    # How strong is this assertion (-1..1)? A negative number is an
    # assertion that the two Identifiers do *not* identify the
    # same work.
    strength = Column(Float, index=True)

    # Should this equivalency actually be used in calculations? This
    # is not manipulated directly, but it gives us the ability to use
    # manual intervention to defuse large chunks of problematic code
    # without actually deleting the data.
    enabled: Mapped[bool] = Column(Boolean, default=True, index=True, nullable=False)

    def __repr__(self) -> str:
        strength = (
            f"strength={self.strength:.2f}"
            if self.strength is not None
            else "strength=None"
        )
        r = "[%s ->\n %s\n source=%s %s votes=%d)]" % (
            repr(self.input),
            repr(self.output),
            self.data_source and self.data_source.name,
            strength,
            self.votes,
        )
        return r

    @classmethod
    def for_identifiers(
        self,
        _db: Session,
        identifiers: Iterable[Identifier | int],
        exclude_ids: list[int] | None = None,
    ) -> Iterable[Equivalency]:
        """Find all Equivalencies for the given Identifiers."""
        identifier_ids = [
            ident.id if isinstance(ident, Identifier) else ident
            for ident in identifiers
        ]
        if not identifier_ids:
            return []
        q = (
            _db.query(Equivalency)
            .distinct()
            .filter(
                or_(
                    Equivalency.input_id.in_(identifier_ids),
                    Equivalency.output_id.in_(identifier_ids),
                )
            )
        )
        if exclude_ids:
            q = q.filter(~Equivalency.id.in_(exclude_ids))
        return q


class RecursiveEquivalencyCache(Base):
    """A chain of identifiers linked to a starting "parent" identifier
    From equivalents if there exists (10,12),(12,19)
    then for parent 10 there should exist rows
    (10,12,order_no=1), (10,19,order_no=2)
    This allows for simple querying during different jobs
    rather than doing on-the-go dynamic recursive_equivalents
    """

    __tablename__ = "recursiveequivalentscache"

    id: Mapped[int] = Column(Integer, primary_key=True)

    # The "parent" or the start of the chain
    parent_identifier_id = Column(
        Integer, ForeignKey("identifiers.id", ondelete="CASCADE")
    )
    parent_identifier: Mapped[Identifier | None] = relationship(
        "Identifier", foreign_keys=parent_identifier_id
    )

    # The identifier chained to the parent
    identifier_id = Column(Integer, ForeignKey("identifiers.id", ondelete="CASCADE"))
    identifier: Mapped[Identifier | None] = relationship(
        "Identifier", foreign_keys=identifier_id
    )

    # Its always important to query for the parent id chain to self first
    # this can be easily accomplished by ORDER BY parent_identifier,is_parent DESC
    is_parent = Column(Boolean, Computed("parent_identifier_id = identifier_id"))

    __table_args__ = (UniqueConstraint(parent_identifier_id, identifier_id),)

    @staticmethod
    def equivalent_identifiers(
        session: Session, identifiers: set[Identifier], type: str | None = None
    ) -> dict[Identifier, Identifier]:
        """
        Find all equivalent identifiers for the given Identifiers.

        :param session: DB Session
        :param identifiers: Set of Identifiers that we need equivalencies for
        :param type: An optional type, if given only equivalent identifiers
                     of this type will be returned.
        :return: A dictionary mapping input identifiers to equivalent identifiers.
        """

        # Find identifiers that don't need to be looked up
        results = (
            {i: i for i in identifiers if i.type == type} if type is not None else {}
        )
        needs_lookup = {i.id: i for i in identifiers - results.keys()}
        if not needs_lookup:
            return results

        query = (
            select(RecursiveEquivalencyCache)
            .join(
                Identifier,
                RecursiveEquivalencyCache.identifier_id == Identifier.id,
            )
            .where(
                RecursiveEquivalencyCache.parent_identifier_id.in_(needs_lookup.keys()),
            )
            .order_by(
                RecursiveEquivalencyCache.parent_identifier_id,
                RecursiveEquivalencyCache.is_parent.desc(),
                RecursiveEquivalencyCache.identifier_id.desc(),
            )
            .options(
                selectinload(RecursiveEquivalencyCache.identifier),
            )
        )
        if type is not None:
            query = query.where(Identifier.type == Identifier.ISBN)

        equivalents = session.execute(query).scalars().all()

        for equivalent in equivalents:
            parent_identifier = needs_lookup[equivalent.parent_identifier_id]
            results[parent_identifier] = equivalent.identifier

        return results


def isbn_for_identifier(identifier: Identifier | None) -> str | None:
    """Find the strongest ISBN match for the given identifier.

    :param identifier: The identifier to match.
    :return: The ISBN string associated with the identifier or None, if no match is found.
    """
    if identifier is None:
        return None

    if identifier.type == Identifier.ISBN:
        return identifier.identifier

    # If our identifier is not an ISBN itself, we'll use our Recursive Equivalency
    # mechanism to find the next best one that is, if available.
    db = Session.object_session(identifier)
    eq_subquery = db.query(RecursiveEquivalencyCache.identifier_id).filter(
        RecursiveEquivalencyCache.parent_identifier_id == identifier.id
    )
    equivalent_identifiers = (
        db.query(Identifier)
        .filter(Identifier.id.in_(eq_subquery))
        .filter(Identifier.type == Identifier.ISBN)
    )

    return next(
        map(lambda id_: id_.identifier, equivalent_identifiers),
        None,
    )
