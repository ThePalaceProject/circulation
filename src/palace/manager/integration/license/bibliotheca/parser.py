from __future__ import annotations

import html
import itertools
import logging
import re
from abc import ABC
from collections.abc import Generator
from datetime import datetime
from typing import Literal, Optional, overload

from frozendict import frozendict
from lxml.etree import _Element

from palace.util.datetime_helpers import strptime_utc, to_utc
from palace.util.log import LoggerMixin

from palace.manager.api.circulation.data import HoldInfo, LoanInfo
from palace.manager.api.circulation.exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    CannotHold,
    CannotLoan,
    CurrentlyAvailable,
    NoAvailableCopies,
    NoLicenses,
    NotCheckedOut,
    NotOnHold,
    PatronHoldLimitReached,
    PatronLoanLimitReached,
    RemoteInitiatedServerError,
)
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.link import LinkData
from palace.manager.data_layer.measurement import MeasurementData
from palace.manager.data_layer.subject import SubjectData
from palace.manager.integration.license.bibliotheca.constants import (
    BIBLIOTHECA_SERVICE_NAME,
    BIBLIOTHECA_TIME_FORMAT,
)
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.classification import Classification, Subject
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePoolStatus,
)
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.util.problem_detail import BaseProblemDetailException
from palace.manager.util.xmlparser import XMLParser, XMLProcessor


class ItemListParser(XMLProcessor[BibliographicData], LoggerMixin):
    DATE_FORMAT = "%Y-%m-%d"
    YEAR_FORMAT = "%Y"

    unescape_entity_references = html.unescape

    @property
    def xpath_expression(self) -> str:
        return "//Item"

    parenthetical = re.compile(r" \([^)]+\)$")

    format_data_for_bibliotheca_format = frozendict(
        {
            "EPUB": (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            "EPUB3": (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            "PDF": (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            "MP3": (None, DeliveryMechanism.FINDAWAY_DRM),
        }
    )

    @classmethod
    def contributors_from_string(
        cls, string: str | None, role: str = Contributor.Role.AUTHOR
    ) -> list[ContributorData]:
        contributors: list[ContributorData] = []
        if not string:
            return contributors

        # Contributors may have two levels of entity reference escaping,
        # one of which will have already been handled by the initial parse.
        # We handle the potential need for a second unescaping here.
        string = cls.unescape_entity_references(string)

        for sort_name in string.split(";"):
            sort_name = cls.parenthetical.sub("", sort_name.strip())
            contributors.append(
                ContributorData(sort_name=sort_name.strip(), roles=[role])
            )
        return contributors

    @classmethod
    def parse_genre_string(self, s: str | None) -> list[SubjectData]:
        genres: list[SubjectData] = []
        if not s:
            return genres
        for i in s.split(","):
            i = i.strip()
            if not i:
                continue
            i = (
                i.replace("&amp;amp;", "&amp;")
                .replace("&amp;", "&")
                .replace("&#39;", "'")
            )
            genres.append(
                SubjectData(
                    type=Subject.BISAC,
                    identifier=None,
                    name=i,
                    weight=Classification.TRUSTED_DISTRIBUTOR_WEIGHT,
                )
            )
        return genres

    def process_one(
        self, tag: _Element, namespaces: dict[str, str] | None
    ) -> BibliographicData:
        """Turn an <item> tag into a BibliographicData and an encompassed CirculationData
        objects, and return the BibliographicData."""

        def value(bibliotheca_key: str) -> str | None:
            return self.text_of_optional_subtag(tag, bibliotheca_key)

        primary_identifier = IdentifierData(
            type=Identifier.BIBLIOTHECA_ID, identifier=value("ItemId")
        )

        identifiers = []
        for key in ("ISBN13", "PhysicalISBN"):
            v = value(key)
            if v:
                identifiers.append(IdentifierData(type=Identifier.ISBN, identifier=v))

        subjects = self.parse_genre_string(value("Genre"))

        title = value("Title")
        subtitle = value("SubTitle")
        publisher = value("Publisher")
        language = value("Language")

        authors = list(self.contributors_from_string(value("Authors")))
        narrators = list(
            self.contributors_from_string(value("Narrator"), Contributor.Role.NARRATOR)
        )

        published_date = None
        published = value("PubDate")
        if published:
            formats = [self.DATE_FORMAT, self.YEAR_FORMAT]
        else:
            published = value("PubYear")
            formats = [self.YEAR_FORMAT]

        for format in formats:
            try:
                published_date = strptime_utc(published, format)  # type: ignore[arg-type]
            except ValueError as e:
                pass

        links = []
        description = value("Description")
        if description:
            links.append(LinkData(rel=Hyperlink.DESCRIPTION, content=description))

        # Presume all images from Bibliotheca are JPEG.
        media_type = Representation.JPEG_MEDIA_TYPE
        cover_url = self.text_of_subtag(tag, "CoverLinkURL").replace("&amp;", "&")

        # Unless the URL format has drastically changed, we should be
        # able to generate a thumbnail URL based on the full-size
        # cover URL found in the response document.
        #
        # NOTE: this is an undocumented feature of the Bibliotheca API
        # which was discovered by investigating the BookLinkURL.
        if "/delivery/img" in cover_url:
            thumbnail_url = cover_url + "&size=NORMAL"
            thumbnail = LinkData(
                rel=Hyperlink.THUMBNAIL_IMAGE, href=thumbnail_url, media_type=media_type
            )
        else:
            thumbnail = None
        cover_link = LinkData(
            rel=Hyperlink.IMAGE,
            href=cover_url,
            media_type=media_type,
            thumbnail=thumbnail,
        )
        links.append(cover_link)

        alternate_url = self.text_of_subtag(tag, "BookLinkURL").replace("&amp;", "&")
        links.append(LinkData(rel="alternate", href=alternate_url))

        measurements = []
        pages = value("NumberOfPages")
        if pages:
            pages_int = int(pages)
            measurements.append(
                MeasurementData(
                    quantity_measured=Measurement.PAGE_COUNT, value=pages_int
                )
            )

        circulation, medium = self._make_circulation_data(tag, primary_identifier)

        bibliographic = BibliographicData(
            data_source_name=DataSource.BIBLIOTHECA,
            title=title,
            subtitle=subtitle,
            language=language,
            medium=medium,
            publisher=publisher,
            published=published_date,
            primary_identifier_data=primary_identifier,
            identifiers=identifiers,
            subjects=subjects,
            contributors=authors + narrators,
            measurements=measurements,
            links=links,
            circulation=circulation,
        )
        return bibliographic

    def _make_circulation_data(
        self, tag: _Element, primary_identifier: IdentifierData
    ) -> tuple[CirculationData, str]:
        """Parse out a CirculationData containing current circulation
        and formatting information.
        """

        def value(bibliotheca_key: str) -> str:
            return self.text_of_subtag(tag, bibliotheca_key)

        def intvalue(key: str) -> int:
            return self.int_of_subtag(tag, key)

        book_format = value("BookFormat")
        medium, formats = self.internal_formats(book_format)

        licenses_owned = intvalue("TotalCopies")
        try:
            licenses_available = intvalue("AvailableCopies")
        except IndexError:
            self.log.warning(
                "No information on available copies for %s",
                primary_identifier.identifier,
            )
            licenses_available = 0

        patrons_in_hold_queue = intvalue("OnHoldCount")
        licenses_reserved = 0

        license_status = (
            LicensePoolStatus.ACTIVE
            if licenses_owned > 0
            else LicensePoolStatus.EXHAUSTED
        )

        circulation = CirculationData(
            data_source_name=DataSource.BIBLIOTHECA,
            primary_identifier_data=primary_identifier,
            licenses_owned=licenses_owned,
            licenses_available=licenses_available,
            licenses_reserved=licenses_reserved,
            patrons_in_hold_queue=patrons_in_hold_queue,
            formats=formats,
            status=license_status,
        )
        return circulation, medium

    @classmethod
    def internal_formats(cls, book_format: str) -> tuple[str, list[FormatData]]:
        """Convert the term Bibliotheca uses to refer to a book
        format into a (medium [formats]) 2-tuple.
        """
        if book_format not in cls.format_data_for_bibliotheca_format:
            cls.logger().error("Unrecognized BookFormat: %s", book_format)
            return Edition.BOOK_MEDIUM, []

        content_type, drm_scheme = cls.format_data_for_bibliotheca_format[book_format]

        format = FormatData(content_type=content_type, drm_scheme=drm_scheme)
        if book_format == "MP3":
            medium = Edition.AUDIO_MEDIUM
        else:
            medium = Edition.BOOK_MEDIUM
        return medium, [format]


class BibliothecaParser[T](XMLProcessor[T], ABC):
    INPUT_TIME_FORMAT = BIBLIOTHECA_TIME_FORMAT

    @classmethod
    def parse_date(cls, value: str | None) -> datetime | None:
        """Parse the string Bibliotheca sends as a date.

        Usually this is a string in INPUT_TIME_FORMAT, but it might be None.
        """
        if not value:
            parsed = None
        else:
            try:
                parsed = strptime_utc(value, cls.INPUT_TIME_FORMAT)
            except ValueError as e:
                logging.error(
                    'Unable to parse Bibliotheca date: "%s"', value, exc_info=e
                )
                parsed = None
        return to_utc(parsed)

    @overload
    def date_from_subtag(self, tag: _Element, key: str) -> datetime: ...

    @overload
    def date_from_subtag(
        self, tag: _Element, key: str, required: Literal[False]
    ) -> datetime | None: ...

    def date_from_subtag(
        self, tag: _Element, key: str, required: bool = True
    ) -> datetime | None:
        value = (
            self.text_of_subtag(tag, key)
            if required
            else self.text_of_optional_subtag(tag, key)
        )
        return self.parse_date(value)


class ErrorParser(BibliothecaParser[BaseProblemDetailException]):
    """Turn an error document from the Bibliotheca web service into a CheckoutException"""

    wrong_status = re.compile(
        "the patron document status was ([^ ]+) and not one of ([^ ]+)"
    )

    loan_limit_reached = re.compile("Patron cannot loan more than [0-9]+ document")

    hold_limit_reached = re.compile("Patron cannot have more than [0-9]+ hold")

    error_mapping = {
        "The patron does not have the book on hold": NotOnHold,
        "The patron has no eBooks checked out": NotCheckedOut,
    }

    @property
    def xpath_expression(self) -> str:
        return "//Error"

    def process_first(self, string: str | bytes) -> BaseProblemDetailException:
        try:
            return_val = super().process_first(string)
        except Exception as e:
            # The server sent us an error with an incorrect or
            # nonstandard syntax.
            if isinstance(string, bytes):
                try:
                    debug = string.decode("utf-8")
                except UnicodeDecodeError:
                    debug = "Unreadable error message (Unicode decode error)."
            else:
                debug = string
            return RemoteInitiatedServerError(debug, BIBLIOTHECA_SERVICE_NAME)

        if return_val is None:
            # We were not able to interpret the result as an error.
            # The most likely cause is that the Bibliotheca app server is down.
            return RemoteInitiatedServerError(
                "Unknown error",
                BIBLIOTHECA_SERVICE_NAME,
            )

        return return_val

    def process_one(
        self, error_tag: _Element, namespaces: dict[str, str] | None
    ) -> BaseProblemDetailException:
        message = self.text_of_optional_subtag(error_tag, "Message")
        if not message:
            return RemoteInitiatedServerError(
                "Unknown error",
                BIBLIOTHECA_SERVICE_NAME,
            )

        if message in self.error_mapping:
            return self.error_mapping[message](message)
        if message in ("Authentication failed", "Unknown error"):
            # 'Unknown error' is an unknown error on the Bibliotheca side.
            #
            # 'Authentication failed' could _in theory_ be an error on
            # our side, but if authentication is set up improperly we
            # actually get a 401 and no body. When we get a real error
            # document with 'Authentication failed', it's always a
            # transient error on the Bibliotheca side. Possibly some
            # authentication internal to Bibliotheca has failed? Anyway, it
            # happens relatively frequently.
            return RemoteInitiatedServerError(message, BIBLIOTHECA_SERVICE_NAME)

        m = self.loan_limit_reached.search(message)
        if m:
            return PatronLoanLimitReached(message)

        m = self.hold_limit_reached.search(message)
        if m:
            return PatronHoldLimitReached(message)

        m = self.wrong_status.search(message)
        if not m:
            return RemoteInitiatedServerError(message, BIBLIOTHECA_SERVICE_NAME)
        actual, expected = m.groups()
        expected = expected.split(",")

        if actual == "CAN_WISH":
            return NoLicenses(debug_info=message)

        if "CAN_LOAN" in expected and actual == "CAN_HOLD":
            return NoAvailableCopies(debug_info=message)

        if "CAN_LOAN" in expected and actual == "HOLD":
            return AlreadyOnHold(debug_info=message)

        if "CAN_LOAN" in expected and actual == "LOAN":
            return AlreadyCheckedOut(debug_info=message)

        if "CAN_HOLD" in expected and actual == "CAN_LOAN":
            return CurrentlyAvailable(debug_info=message)

        if "CAN_HOLD" in expected and actual == "HOLD":
            return AlreadyOnHold(debug_info=message)

        if "CAN_HOLD" in expected:
            return CannotHold(debug_info=message)

        if "CAN_LOAN" in expected:
            return CannotLoan(debug_info=message)

        return RemoteInitiatedServerError(message, BIBLIOTHECA_SERVICE_NAME)


class PatronCirculationParser(XMLParser):
    """Parse Bibliotheca's patron circulation status document into a list of
    LoanInfo and HoldInfo objects.
    """

    id_type = Identifier.BIBLIOTHECA_ID

    def __init__(self, collection: Collection) -> None:
        self.collection = collection

    def process_all(self, string: bytes | str) -> itertools.chain[LoanInfo | HoldInfo]:
        xml = self._load_xml(string)
        loans = self._process_all(
            xml, "//Checkouts/Item", namespaces={}, handler=self.process_one_loan
        )
        holds = self._process_all(
            xml, "//Holds/Item", namespaces={}, handler=self.process_one_hold
        )
        reserves = self._process_all(
            xml, "//Reserves/Item", namespaces={}, handler=self.process_one_reserve
        )
        return itertools.chain(loans, holds, reserves)

    def process_one_loan(
        self, tag: _Element, namespaces: dict[str, str]
    ) -> LoanInfo | None:
        return self.process_one(tag, namespaces, LoanInfo)

    def process_one_hold(
        self, tag: _Element, namespaces: dict[str, str]
    ) -> HoldInfo | None:
        return self.process_one(tag, namespaces, HoldInfo)

    def process_one_reserve(
        self, tag: _Element, namespaces: dict[str, str]
    ) -> HoldInfo | None:
        hold_info = self.process_one(tag, namespaces, HoldInfo)
        if hold_info is not None:
            hold_info.hold_position = 0
        return hold_info

    def process_one[T](
        self, tag: _Element, namespaces: dict[str, str], source_class: type[T]
    ) -> T | None:
        if not tag.xpath("ItemId"):
            # This happens for events associated with books
            # no longer in our collection.
            return None

        def datevalue(key: str) -> datetime:
            value = self.text_of_subtag(tag, key)
            return strptime_utc(value, BIBLIOTHECA_TIME_FORMAT)

        identifier = self.text_of_subtag(tag, "ItemId")
        start_date = datevalue("EventStartDateInUTC")
        end_date = datevalue("EventEndDateInUTC")
        kwargs = {
            "collection_id": self.collection.id,
            "identifier_type": self.id_type,
            "identifier": identifier,
            "start_date": start_date,
            "end_date": end_date,
        }
        if source_class is HoldInfo:
            kwargs["hold_position"] = self.int_of_subtag(tag, "Position")
        return source_class(**kwargs)


class DateResponseParser(BibliothecaParser[Optional[datetime]], ABC):
    """Extract a date from a response."""

    RESULT_TAG_NAME: str | None = None
    DATE_TAG_NAME: str | None = None

    @property
    def xpath_expression(self) -> str:
        return f"/{self.RESULT_TAG_NAME}/{self.DATE_TAG_NAME}"

    def process_one(
        self, tag: _Element, namespaces: dict[str, str] | None
    ) -> datetime | None:
        due_date = tag.text
        if not due_date:
            return None
        return strptime_utc(due_date, EventParser.INPUT_TIME_FORMAT)


class CheckoutResponseParser(DateResponseParser):
    """Extract due date from a checkout response."""

    @property
    def xpath_expression(self) -> str:
        return f"/CheckoutResult/DueDateInUTC"


class HoldResponseParser(DateResponseParser):
    """Extract availability date from a hold response."""

    @property
    def xpath_expression(self) -> str:
        return f"/PlaceHoldResult/AvailabilityDateInUTC"


class EventParser(
    BibliothecaParser[tuple[str, str, str | None, datetime, datetime | None, str]]
):
    """Parse Bibliotheca's event file format into our native event objects."""

    # Map Bibliotheca's event names to our names.
    EVENT_NAMES = {
        "CHECKOUT": CirculationEvent.DISTRIBUTOR_CHECKOUT,
        "CHECKIN": CirculationEvent.DISTRIBUTOR_CHECKIN,
        "HOLD": CirculationEvent.DISTRIBUTOR_HOLD_PLACE,
        "RESERVED": CirculationEvent.DISTRIBUTOR_AVAILABILITY_NOTIFY,
        "PURCHASE": CirculationEvent.DISTRIBUTOR_LICENSE_ADD,
        "REMOVED": CirculationEvent.DISTRIBUTOR_LICENSE_REMOVE,
    }

    @property
    def xpath_expression(self) -> str:
        return "//CloudLibraryEvent"

    def process_all(
        self, string: bytes | str, no_events_error: bool = False
    ) -> Generator[tuple[str, str, str | None, datetime, datetime | None, str]]:
        has_events = False
        # Bibliotheca occasionally returns an empty response body. An empty
        # document cannot be parsed as XML (lxml raises "Document is empty"
        # even in recovery mode), so treat a blank body the same as a
        # response that contained no events rather than letting the parse
        # error propagate as an unhandled exception.
        if string.strip():
            for i in super().process_all(string):
                yield i
                has_events = True

        # If we are catching up on events and we expect to have a time
        # period where there are no events, we don't want to consider that
        # action as an error. By default, not having events is not
        # considered to be an error.
        if not has_events and no_events_error:
            # An empty list of events may mean nothing happened, or it
            # may indicate an unreported server-side error. To be
            # safe, we'll treat this as a server-initiated error
            # condition. If this is just a slow day, normal behavior
            # will resume as soon as something happens.
            raise RemoteInitiatedServerError(
                "No events returned from server. This may not be an error, but treating it as one to be safe.",
                BIBLIOTHECA_SERVICE_NAME,
            )

    def process_one(
        self, tag: _Element, namespaces: dict[str, str] | None
    ) -> tuple[str, str, str | None, datetime, datetime | None, str]:
        isbn = self.text_of_subtag(tag, "ISBN")
        bibliotheca_id = self.text_of_subtag(tag, "ItemId")
        patron_id = self.text_of_optional_subtag(tag, "PatronId")

        start_time = self.date_from_subtag(tag, "EventStartDateTimeInUTC")
        end_time = self.date_from_subtag(tag, "EventEndDateTimeInUTC", required=False)

        bibliotheca_event_type = self.text_of_subtag(tag, "EventType")
        internal_event_type = self.EVENT_NAMES[bibliotheca_event_type]

        return (
            bibliotheca_id,
            isbn,
            patron_id,
            start_time,
            end_time,
            internal_event_type,
        )
