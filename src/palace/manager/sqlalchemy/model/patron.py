# LoanAndHoldMixin, Patron, Loan, Hold, Annotation, PatronProfileStorage
from __future__ import annotations

import datetime
import logging
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from dependency_injector.wiring import Provide, inject
from psycopg2.extras import NumericRange
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Unicode,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.orm.session import Session

from palace.manager.core.classifier import Classifier
from palace.manager.core.user_profile import ProfileStorage
from palace.manager.service.redis.key import RedisKeyMixin
from palace.manager.sqlalchemy.constants import LinkRelations
from palace.manager.sqlalchemy.hybrid import hybrid_property
from palace.manager.sqlalchemy.model.base import Base
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.credential import Credential
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.util import NumericRangeTuple, numericrange_to_tuple
from palace.manager.util.datetime_helpers import utc_now

if TYPE_CHECKING:
    from palace.manager.service.analytics.analytics import Analytics
    from palace.manager.sqlalchemy.model.devicetokens import DeviceToken
    from palace.manager.sqlalchemy.model.identifier import Identifier
    from palace.manager.sqlalchemy.model.lane import Lane
    from palace.manager.sqlalchemy.model.library import Library
    from palace.manager.sqlalchemy.model.licensing import (
        License,
        LicensePool,
        LicensePoolDeliveryMechanism,
    )
    from palace.manager.sqlalchemy.model.work import Work


class LoanAndHoldMixin:
    license_pool: LicensePool
    patron: Patron

    @property
    def work(self) -> Work | None:
        """Try to find the corresponding work for this Loan/Hold."""
        license_pool = self.license_pool
        if license_pool.work:
            return license_pool.work
        if license_pool.presentation_edition and license_pool.presentation_edition.work:
            return license_pool.presentation_edition.work
        return None

    @property
    def library(self) -> Library:
        """The corresponding library for this Loan/Hold."""
        return self.patron.library


class Patron(Base, RedisKeyMixin):
    __tablename__ = "patrons"
    id: Mapped[int] = Column(Integer, primary_key=True)

    # Each patron is the patron _of_ one particular library.  An
    # individual human being may patronize multiple libraries, but
    # they will have a different patron account at each one.
    library_id: Mapped[int] = Column(
        Integer, ForeignKey("libraries.id"), index=True, nullable=False
    )
    library: Mapped[Library] = relationship("Library", back_populates="patrons")

    # The patron's permanent unique identifier in an external library
    # system, probably never seen by the patron.
    #
    # This is not stored as a ForeignIdentifier because it corresponds
    # to the patron's identifier in the library responsible for the
    # Simplified instance, not a third party.
    external_identifier = Column(Unicode)

    # The patron's account type, as reckoned by an external library
    # system. Different account types may be subject to different
    # library policies.
    #
    # Depending on library policy it may be possible to automatically
    # derive the patron's account type from their authorization
    # identifier.
    external_type = Column(Unicode, index=True)

    # An identifier used by the patron that gives them the authority
    # to borrow books. This identifier may change over time.
    authorization_identifier = Column(Unicode)

    # An identifier used by the patron that authenticates them,
    # but does not give them the authority to borrow books. i.e. their
    # website username.
    username = Column(Unicode)

    # A universally unique identifier across all CMs used to track patron activity
    # in a way that allows users to disassociate their patron info
    # with account activity at any time.  When this UUID is reset it effectively
    # dissociates any patron activity history with this patron.
    uuid: Mapped[uuid.UUID] = Column(
        UUID(as_uuid=True), nullable=False, default=uuid.uuid4
    )

    # The last time this record was synced up with an external library
    # system such as an ILS.
    last_external_sync = Column(DateTime(timezone=True))

    # The time, if any, at which the user's authorization to borrow
    # books expires.
    authorization_expires = Column(Date, index=True)

    # Outstanding fines the user has, if any.
    fines = Column(Unicode)

    # If the patron's borrowing privileges have been blocked, this
    # field contains the library's reason for the block. If this field
    # is None, the patron's borrowing privileges have not been
    # blocked.
    #
    # Although we currently don't do anything with specific values for
    # this field, the expectation is that values will be taken from a
    # small controlled vocabulary (e.g. "banned", "incorrect personal
    # information", "unknown"), rather than freeform strings entered
    # by librarians.
    #
    # Common reasons for blocks are kept in circulation's PatronData
    # class.
    block_reason = Column(String(255), default=None)

    # Whether or not the patron wants their annotations synchronized
    # across devices (which requires storing those annotations on a
    # library server).
    _synchronize_annotations = Column("synchronize_annotations", Boolean, default=None)

    loans: Mapped[list[Loan]] = relationship(
        "Loan",
        back_populates="patron",
        cascade="delete",
        uselist=True,
        passive_deletes=True,
    )
    holds: Mapped[list[Hold]] = relationship(
        "Hold",
        back_populates="patron",
        cascade="delete",
        uselist=True,
        order_by="Hold.id",
        passive_deletes=True,
    )

    annotations: Mapped[list[Annotation]] = relationship(
        "Annotation",
        back_populates="patron",
        order_by="desc(Annotation.timestamp)",
        cascade="delete",
        passive_deletes=True,
    )

    # One Patron can have many associated Credentials.
    credentials: Mapped[list[Credential]] = relationship(
        "Credential", back_populates="patron", cascade="delete", passive_deletes=True
    )

    device_tokens: Mapped[list[DeviceToken]] = relationship(
        "DeviceToken", back_populates="patron", cascade="delete", passive_deletes=True
    )

    __table_args__ = (
        UniqueConstraint("library_id", "username"),
        UniqueConstraint("library_id", "authorization_identifier"),
        UniqueConstraint("library_id", "external_identifier"),
    )

    # A patron with borrowing privileges should have their local
    # metadata synced with their ILS record at intervals no greater
    # than this time.
    MAX_SYNC_TIME = datetime.timedelta(hours=12)

    def __repr__(self) -> str:
        def date(d: datetime.datetime | datetime.date | None) -> datetime.date | None:
            """Format an object that might be a datetime as a date.

            This keeps a patron representation short.
            """
            if d is None:
                return None
            if isinstance(d, datetime.datetime):
                return d.date()
            return d

        return "<Patron authentication_identifier={} expires={} sync={}>".format(
            self.authorization_identifier,
            date(self.authorization_expires),
            date(self.last_external_sync),
        )

    def identifier_to_remote_service(
        self,
        remote_data_source: DataSource | str,
        generator: Callable[[], str] | None = None,
    ) -> str:
        """Find or randomly create an identifier to use when identifying
        this patron to a remote service.
        :param remote_data_source: A DataSource object (or name of a
        DataSource) corresponding to the remote service.
        """
        _db = Session.object_session(self)

        def refresh(credential: Credential) -> None:
            if generator and callable(generator):
                identifier = generator()
            else:
                identifier = str(uuid.uuid1())
            credential.credential = identifier

        credential = Credential.lookup(
            _db,
            remote_data_source,
            Credential.IDENTIFIER_TO_REMOTE_SERVICE,
            self,
            refresh,
            allow_persistent_token=True,
        )
        # Any way that we create a credential should result in a result that does not
        # have credential.credential set to None. Mypy doesn't know that, so we assert
        # it here.
        assert credential.credential is not None
        return credential.credential

    def works_on_loan(self) -> list[Work]:
        return [loan.work for loan in self.loans if loan.work]

    def works_on_loan_or_on_hold(self) -> set[Work]:
        holds = [hold.work for hold in self.holds if hold.work]
        loans = self.works_on_loan()
        return set(holds + loans)

    @hybrid_property
    def synchronize_annotations(self) -> bool | None:
        return self._synchronize_annotations

    @synchronize_annotations.setter
    def synchronize_annotations(self, value: bool | None) -> None:
        """When a patron says they don't want their annotations to be stored
        on a library server, delete all their annotations.
        """
        if value is None:
            # A patron cannot decide to go back to the state where
            # they hadn't made a decision.
            raise ValueError("synchronize_annotations cannot be unset once set.")
        if value is False:
            _db = Session.object_session(self)
            qu = _db.query(Annotation).filter(Annotation.patron == self)
            for annotation in qu:
                _db.delete(annotation)
        self._synchronize_annotations = value

    @property
    def root_lane(self) -> Lane | None:
        """Find the Lane, if any, to be used as the Patron's root lane.

        A patron with a root Lane can only access that Lane and the
        Lanes beneath it. In addition, a patron with a root lane
        cannot conduct a transaction on a book intended for an older
        audience than the one defined by their root lane.
        """

        # Two ways of improving performance by short-circuiting this
        # logic.
        if not self.external_type:
            return None
        if not self.library.has_root_lanes:
            return None

        _db = Session.object_session(self)
        from palace.manager.sqlalchemy.model.lane import Lane

        qu = (
            _db.query(Lane)
            .filter(Lane.library == self.library)
            .filter(Lane.root_for_patron_type.any(self.external_type))
            .order_by(Lane.id)
        )
        lanes: list[Lane] = qu.all()
        if len(lanes) < 1:
            # The most common situation -- this patron has no special
            # root lane.
            return None
        if len(lanes) > 1:
            # Multiple root lanes for a patron indicates a
            # configuration problem, but we shouldn't make the patron
            # pay the price -- just pick the first one.
            logging.error(
                "Multiple root lanes found for patron type %s.", self.external_type
            )
        return lanes[0]

    def work_is_age_appropriate(
        self, work_audience: str, work_target_age: int | tuple[int, int]
    ) -> bool:
        """Is the given audience and target age an age-appropriate match for this Patron?

        NOTE: What "age-appropriate" means depends on some policy questions
        that have not been answered and may be library-specific. For
        now, it is determined by comparing audience and target age to that of the
        Patron's root lane.

        This is designed for use when reasoning about works in
        general. If you have a specific Work in mind, use
        `Work.age_appropriate_for_patron`.

        :param work_audience: One of the audience constants from
           Classifier, representing the general reading audience to
           which a putative work belongs.

        :param work_target_age: A number or 2-tuple representing the target age
           or age range of a putative work.

        :return: A boolean

        """
        root = self.root_lane
        if not root:
            # The patron has no root lane. They can interact with any
            # title.
            return True

        # The patron can interact with a title if any of the audiences
        # in their root lane (in conjunction with the root lane's target_age)
        # are a match for the title's audience and target age.
        return any(
            self.age_appropriate_match(
                work_audience, work_target_age, audience, root.target_age
            )
            for audience in root.audiences
        )

    @classmethod
    def age_appropriate_match(
        cls,
        work_audience: str,
        work_target_age: NumericRange | NumericRangeTuple | float,
        reader_audience: str | None,
        reader_age: NumericRange | NumericRangeTuple | float,
    ) -> bool:
        """Match the audience and target age of a work with that of a reader,
        and see whether they are an age-appropriate match.

        NOTE: What "age-appropriate" means depends on some policy
        questions that have not been answered and may be
        library-specific. For now, non-children's books are
        age-inappropriate for young children, and children's books are
        age-inappropriate for children too young to be in the book's
        target age range.

        :param reader_audience: One of the audience constants from
           Classifier, representing the general reading audience to
           which the reader belongs.

        :param reader_age: A number or 2-tuple representing the age or
           age range of the reader.
        """
        if reader_audience is None:
            # A patron with no particular audience restrictions
            # can see everything.
            #
            # This is by far the most common case, so we don't set up
            # logging until after running it.
            return True

        log = logging.getLogger("Age-appropriate match calculator")
        log.debug(
            "Matching work %s/%s to reader %s/%s"
            % (work_audience, work_target_age, reader_audience, reader_age)
        )

        if reader_audience not in Classifier.AUDIENCES_JUVENILE:
            log.debug("A non-juvenile patron can see everything.")
            return True

        if work_audience == Classifier.AUDIENCE_ALL_AGES:
            log.debug("An all-ages book is always age appropriate.")
            return True

        # At this point we know that the patron is a juvenile.

        def ensure_tuple(
            x: NumericRange | NumericRangeTuple | float,
        ) -> NumericRangeTuple | float:
            # Convert a potential NumericRange into a tuple.
            if isinstance(x, NumericRange):
                x = numericrange_to_tuple(x)
            return x

        reader_age = ensure_tuple(reader_age)
        if isinstance(reader_age, tuple):
            # A range was passed in rather than a specific age. Assume
            # the reader is at the top edge of the range.
            _, reader_age_max = reader_age
        else:
            reader_age_max = reader_age

        work_target_age = ensure_tuple(work_target_age)
        if isinstance(work_target_age, tuple):
            # Pick the _bottom_ edge of a work's target age range --
            # the work is appropriate for anyone _at least_ that old.
            work_target_age_min, _ = work_target_age
        else:
            work_target_age_min = work_target_age

        # A YA reader is treated as an adult (with no reading
        # restrictions) if they have no associated age range, or their
        # age range includes ADULT_AGE_CUTOFF.
        if reader_audience == Classifier.AUDIENCE_YOUNG_ADULT and (
            reader_age_max is None
            or (
                isinstance(reader_age_max, int)
                and reader_age_max >= Classifier.ADULT_AGE_CUTOFF
            )
        ):
            log.debug("YA reader to be treated as an adult.")
            return True

        # There are no other situations where a juvenile reader can access
        # non-juvenile titles.
        if work_audience not in Classifier.AUDIENCES_JUVENILE:
            log.debug("Juvenile reader cannot access non-juvenile title.")
            return False

        # At this point we know we have a juvenile reader and a
        # juvenile book.

        if reader_audience == Classifier.AUDIENCE_YOUNG_ADULT and work_audience in (
            Classifier.AUDIENCES_YOUNG_CHILDREN
        ):
            log.debug("YA reader can access any children's title.")
            return True

        if (
            reader_audience in (Classifier.AUDIENCES_YOUNG_CHILDREN)
            and work_audience == Classifier.AUDIENCE_YOUNG_ADULT
        ):
            log.debug("Child reader cannot access any YA title.")
            return False

        # At this point we either have a YA patron with a YA book, or
        # a child patron with a children's book. It comes down to a
        # question of the reader's age vs. the work's target age.

        if work_target_age_min is None:
            # This is a generic children's or YA book with no
            # particular target age. Assume it's age appropriate.
            log.debug("Juvenile book with no target age is presumed age-appropriate.")
            return True

        if reader_age_max is None:
            # We have no idea how old the patron is, so any work with
            # the appropriate audience is considered age-appropriate.
            log.debug(
                "Audience matches, and no specific patron age information available: presuming age-appropriate."
            )
            return True

        if reader_age_max < work_target_age_min:
            # The audience for this book matches the patron's
            # audience, but the book has a target age that is too high
            # for the reader.
            log.debug("Audience matches, but work's target age is too high for reader.")
            return False

        log.debug("Both audience and target age match; it's age-appropriate.")
        return True


Index(
    "ix_patron_library_id_external_identifier",
    Patron.library_id,
    Patron.external_identifier,
)
Index(
    "ix_patron_library_id_authorization_identifier",
    Patron.library_id,
    Patron.authorization_identifier,
)
Index("ix_patron_library_id_username", Patron.library_id, Patron.username)


class Loan(Base, LoanAndHoldMixin):
    __tablename__ = "loans"
    __mapper_args__ = {"confirm_deleted_rows": False}
    id: Mapped[int] = Column(Integer, primary_key=True)

    patron_id: Mapped[int] = Column(
        Integer,
        ForeignKey("patrons.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    patron: Mapped[Patron] = relationship("Patron", back_populates="loans")

    # A Loan is always associated with a LicensePool.
    license_pool_id: Mapped[int] = Column(
        Integer, ForeignKey("licensepools.id"), index=True, nullable=False
    )
    license_pool: Mapped[LicensePool] = relationship(
        "LicensePool", back_populates="loans"
    )

    # It may also be associated with an individual License if the source
    # provides information about individual licenses.
    license_id = Column(Integer, ForeignKey("licenses.id"), index=True, nullable=True)
    license: Mapped[License | None] = relationship("License", back_populates="loans")

    fulfillment_id = Column(Integer, ForeignKey("licensepooldeliveries.id"))
    fulfillment: Mapped[LicensePoolDeliveryMechanism | None] = relationship(
        "LicensePoolDeliveryMechanism", back_populates="fulfills"
    )
    start = Column(DateTime(timezone=True), index=True)
    end = Column(DateTime(timezone=True), index=True)
    # Some distributors (e.g. Feedbooks) may have an identifier that can
    # be used to check the status of a specific Loan.
    external_identifier = Column(Unicode, unique=True, nullable=True)
    patron_last_notified = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (UniqueConstraint("patron_id", "license_pool_id"),)

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, Loan) or self.id is None or other.id is None:
            return NotImplemented
        return self.id < other.id

    def until(
        self, default_loan_period: datetime.timedelta | None
    ) -> datetime.datetime | None:
        """Give or estimate the time at which the loan will end."""
        if self.end:
            return self.end
        if default_loan_period is None:
            # This loan will last forever.
            return None
        start = self.start or utc_now()
        return start + default_loan_period


class Hold(Base, LoanAndHoldMixin):
    """A patron is in line to check out a book."""

    __tablename__ = "holds"
    __mapper_args__ = {"confirm_deleted_rows": False}
    id: Mapped[int] = Column(Integer, primary_key=True)
    patron_id: Mapped[int] = Column(
        Integer,
        ForeignKey("patrons.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    patron: Mapped[Patron] = relationship(
        "Patron", back_populates="holds", lazy="joined"
    )
    license_pool_id: Mapped[int] = Column(
        Integer, ForeignKey("licensepools.id"), index=True, nullable=False
    )
    license_pool: Mapped[LicensePool] = relationship(
        "LicensePool", back_populates="holds"
    )
    start = Column(DateTime(timezone=True), index=True)
    end = Column(DateTime(timezone=True), index=True)
    position = Column(Integer, index=True)
    patron_last_notified = Column(DateTime(timezone=True), nullable=True)

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, Hold) or self.id is None or other.id is None:
            return NotImplemented
        return self.id < other.id

    @staticmethod
    def _calculate_until(
        start: datetime.datetime,
        queue_position: int,
        total_licenses: int,
        default_loan_period: datetime.timedelta,
        default_reservation_period: datetime.timedelta,
    ) -> datetime.datetime | None:
        """Helper method for `Hold.until` that can be tested independently.
        We have to wait for the available licenses to cycle a
        certain number of times before we get a turn.
        Example: 4 licenses, queue position 21
        After 1 cycle: queue position 17
              2      : queue position 13
              3      : queue position 9
              4      : queue position 5
              5      : queue position 1
              6      : available
        The worst-case cycle time is the loan period plus the reservation
        period.
        """
        if queue_position == 0:
            # The book is currently reserved to this patron--they need
            # to hurry up and check it out.
            return start + default_reservation_period

        if total_licenses == 0:
            # The book will never be available
            return None

        # If you are at the very front of the queue, the worst case
        # time to get the book is the time it takes for the person
        # in front of you to get a reservation notification, borrow
        # the book at the last minute, and keep the book for the
        # maximum allowable time.
        cycle_period = default_reservation_period + default_loan_period

        # This will happen at least once.
        cycles = 1

        if queue_position <= total_licenses:
            # But then the book will be available to you.
            pass
        else:
            # This will happen more than once. After the first cycle,
            # other people will be notified that it's their turn,
            # they'll wait a while, get a reservation, and then keep
            # the book for a while, and so on.
            cycles += queue_position // total_licenses
            if total_licenses > 1 and queue_position % total_licenses == 0:
                cycles -= 1
        return start + (cycle_period * cycles)

    def until(
        self,
        default_loan_period: datetime.timedelta | None,
        default_reservation_period: datetime.timedelta | None,
    ) -> datetime.datetime | None:
        """Give or estimate the time at which the book will be available
        to this patron.
        This is a *very* rough estimate that should be treated more or
        less as a worst case. (Though it could be even worse than
        this--the library's license might expire and then you'll
        _never_ get the book.)
        """
        if self.end and self.end > utc_now():
            # The license source provided their own estimate, and it's
            # not obviously wrong, so use it.
            return self.end

        if default_loan_period is None or default_reservation_period is None:
            # This hold has no definite end date, because there's no known
            # upper bound on how long someone in front of you can keep the
            # book.
            return None

        start = utc_now()
        licenses_available = self.license_pool.licenses_owned
        position = self.position
        if position is None:
            # We don't know where in line we are. Assume we're at the
            # end.
            position = self.license_pool.patrons_in_hold_queue
        return self._calculate_until(
            start,
            position,
            licenses_available,
            default_loan_period,
            default_reservation_period,
        )

    def update(
        self,
        start: datetime.datetime | None,
        end: datetime.datetime | None,
        position: int | None,
    ) -> None:
        """When the book becomes available, position will be 0 and end will be
        set to the time at which point the patron will lose their place in
        line.
        Otherwise, end is irrelevant and is set to None.
        """
        if start is not None:
            self.start = start
        if end is not None:
            self.end = end
        if position is not None:
            self.position = position

    @inject
    def collect_event_and_delete(
        self, *, analytics: Analytics | None = Provide["analytics.analytics"]
    ) -> None:
        """
        When a hold is converted to a loan, we log the event and delete
        the hold record.
        """
        session = Session.object_session(self)

        # Log the event
        if analytics is not None:
            analytics.collect_event(
                self.patron.library,
                self.license_pool,
                CirculationEvent.CM_HOLD_CONVERTED_TO_LOAN,
                patron=self.patron,
            )

        session.delete(self)

    __table_args__ = (UniqueConstraint("patron_id", "license_pool_id"),)


class Annotation(Base):
    # The Web Annotation Data Model defines a basic set of motivations.
    # https://www.w3.org/TR/annotation-model/#motivation-and-purpose
    OA_NAMESPACE = "http://www.w3.org/ns/oa#"

    # We need to define some terms of our own.
    LS_NAMESPACE = "http://librarysimplified.org/terms/annotation/"

    IDLING = LS_NAMESPACE + "idling"
    BOOKMARKING = OA_NAMESPACE + "bookmarking"

    MOTIVATIONS = [
        IDLING,
        BOOKMARKING,
    ]

    __tablename__ = "annotations"
    id: Mapped[int] = Column(Integer, primary_key=True)
    patron_id = Column(
        Integer, ForeignKey("patrons.id", ondelete="CASCADE"), index=True
    )
    patron: Mapped[Patron] = relationship("Patron", back_populates="annotations")

    identifier_id = Column(Integer, ForeignKey("identifiers.id"), index=True)
    identifier: Mapped[Identifier | None] = relationship(
        "Identifier", back_populates="annotations"
    )

    motivation = Column(Unicode, index=True)
    timestamp = Column(DateTime(timezone=True), index=True)
    active: Mapped[bool] = Column(Boolean, default=True, nullable=False)
    content = Column(Unicode)
    target = Column(Unicode)

    def set_inactive(self) -> None:
        self.active = False
        self.content = None
        self.timestamp = utc_now()


class PatronProfileStorage(ProfileStorage):
    """Interface between a Patron object and the User Profile Management
    Protocol.
    """

    def __init__(self, patron: Patron, url_for: Callable[..., str]) -> None:
        """Set up a storage interface for a specific Patron.
        :param patron: We are accessing the profile for this patron.
        """
        self.patron = patron
        self.url_for = url_for

    @property
    def writable_setting_names(self) -> set[str]:
        """Return the subset of settings that are considered writable."""
        return {self.SYNCHRONIZE_ANNOTATIONS}

    @property
    def profile_document(self) -> dict[str, Any]:
        """Create a Profile document representing the patron's current
        status.
        """
        doc: dict[str, Any] = dict()
        patron = self.patron
        doc[self.AUTHORIZATION_IDENTIFIER] = patron.authorization_identifier
        if patron.authorization_expires:
            doc[self.AUTHORIZATION_EXPIRES] = patron.authorization_expires.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        settings = {self.SYNCHRONIZE_ANNOTATIONS: patron.synchronize_annotations}
        doc[self.SETTINGS_KEY] = settings
        doc["links"] = [
            dict(
                rel=LinkRelations.DEVICE_REGISTRATION,
                type="application/json",
                href=self.url_for(
                    "put_patron_devices",
                    library_short_name=self.patron.library.short_name,
                    _external=True,
                ),
            )
        ]
        return doc

    def update(self, settable: dict[str, Any], full: dict[str, Any]) -> None:
        """Bring the Patron's status up-to-date with the given document.
        Right now this means making sure Patron.synchronize_annotations
        is up to date.
        """
        key = self.SYNCHRONIZE_ANNOTATIONS
        if key in settable:
            self.patron.synchronize_annotations = settable[key]
