from __future__ import annotations

import datetime
import html
import json
import re
from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic, Literal, Optional, TypeVar, Union, cast

from lxml import etree
from lxml.etree import _Element, _ElementTree

from palace.manager.api.axis.constants import Axis360APIConstants
from palace.manager.api.axis.fulfillment import (
    Axis360AcsFulfillment,
    Axis360Fulfillment,
)
from palace.manager.api.axis.loan_info import AxisLoanInfo
from palace.manager.api.axis.manifest import AxisNowManifest
from palace.manager.api.circulation import Fulfillment, HoldInfo
from palace.manager.api.circulation_exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    CannotFulfill,
    CannotLoan,
    CurrentlyAvailable,
    InvalidInputException,
    LibraryAuthorizationFailedException,
    LibraryInvalidInputException,
    NoAcceptableFormat,
    NoActiveLoan,
    NoAvailableCopies,
    NotFoundOnRemote,
    NotOnHold,
    PatronAuthorizationFailedException,
    PatronLoanLimitReached,
    RemoteInitiatedServerError,
)
from palace.manager.api.web_publication_manifest import FindawayManifest, SpineItem
from palace.manager.core.exceptions import IntegrationException
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.link import LinkData
from palace.manager.data_layer.subject import SubjectData
from palace.manager.sqlalchemy.constants import LinkRelations, MediaTypes
from palace.manager.sqlalchemy.model.classification import Classification, Subject
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism, LicensePool
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.util.datetime_helpers import strptime_utc
from palace.manager.util.log import LoggerMixin
from palace.manager.util.xmlparser import XMLProcessor

if TYPE_CHECKING:
    from palace.manager.api.axis.api import Axis360API

T = TypeVar("T")


class Axis360Parser(XMLProcessor[T], ABC):
    SHORT_DATE_FORMAT = "%m/%d/%Y"
    FULL_DATE_FORMAT_IMPLICIT_UTC = "%m/%d/%Y %I:%M:%S %p"
    FULL_DATE_FORMAT_EXPLICIT_UTC = "%m/%d/%Y %I:%M:%S %p +00:00"

    NAMESPACES = {"axis": "http://axis360api.baker-taylor.com/vendorAPI"}

    def _pd(self, date: str | None) -> datetime.datetime | None:
        """Stupid function to parse a date."""
        if date is None:
            return date
        try:
            return strptime_utc(date, self.FULL_DATE_FORMAT_IMPLICIT_UTC)
        except ValueError:
            pass
        return strptime_utc(date, self.FULL_DATE_FORMAT_EXPLICIT_UTC)

    def _xpath1_boolean(
        self,
        e: _Element,
        target: str,
        ns: dict[str, str] | None,
        default: bool = False,
    ) -> bool:
        text = self.text_of_optional_subtag(e, target, ns)
        if text is None:
            return default
        if text == "true":
            return True
        else:
            return False

    def _xpath1_date(
        self, e: _Element, target: str, ns: dict[str, str] | None
    ) -> datetime.datetime | None:
        value = self.text_of_optional_subtag(e, target, ns)
        return self._pd(value)


class StatusResponseParser(Axis360Parser[tuple[int, str]]):
    @property
    def xpath_expression(self) -> str:
        # Sometimes the status tag is overloaded, so we want to only
        # look for the status tag that contains the code tag.
        return "//axis:status/axis:code/.."

    def process_one(
        self, tag: _Element, namespaces: dict[str, str] | None
    ) -> tuple[int, str] | None:
        status_code = self.int_of_subtag(tag, "axis:code", namespaces)
        message = self.text_of_subtag(tag, "axis:statusMessage", namespaces)
        return status_code, message

    def process_first(
        self,
        xml: str | bytes | _ElementTree | None,
    ) -> tuple[int, str] | None:
        if not xml:
            return None

        # Since this is being used to parse error codes, we want to generally be
        # very forgiving of errors in the XML, and return None if we can't parse it.
        try:
            return super().process_first(xml)
        except (etree.XMLSyntaxError, AssertionError, ValueError):
            return None


class BibliographicParser(
    Axis360Parser[tuple[BibliographicData, CirculationData]], LoggerMixin
):
    DELIVERY_DATA_FOR_AXIS_FORMAT = {
        Axis360APIConstants.BLIO: None,  # Legacy format, handled the same way as AxisNow
        "Acoustik": (None, DeliveryMechanism.FINDAWAY_DRM),  # Audiobooks
        Axis360APIConstants.AXISNOW: None,  # Handled specially, for ebooks only.
        "ePub": (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
        "PDF": (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
    }

    @classmethod
    def parse_list(cls, l: str) -> list[str]:
        """Turn strings like this into lists:

        FICTION / Thrillers; FICTION / Suspense; FICTION / General
        Ursu, Anne ; Fortune, Eric (ILT)
        """
        return [x.strip() for x in l.split(";")]

    @property
    def xpath_expression(self) -> str:
        return "//axis:title"

    def extract_availability(
        self,
        circulation_data: CirculationData | None,
        element: _Element,
        ns: dict[str, str] | None,
    ) -> CirculationData:
        identifier = self.text_of_subtag(element, "axis:titleId", ns)
        primary_identifier = IdentifierData(
            type=Identifier.AXIS_360_ID, identifier=identifier
        )
        if not circulation_data:
            circulation_data = CirculationData(
                data_source_name=DataSource.AXIS_360,
                primary_identifier_data=primary_identifier,
            )

        availability = self._xpath1(element, "axis:availability", ns)
        total_copies = self.int_of_subtag(availability, "axis:totalCopies", ns)
        available_copies = self.int_of_subtag(availability, "axis:availableCopies", ns)
        size_of_hold_queue = self.int_of_subtag(availability, "axis:holdsQueueSize", ns)

        circulation_data.licenses_owned = total_copies
        circulation_data.licenses_available = available_copies
        circulation_data.licenses_reserved = 0
        circulation_data.patrons_in_hold_queue = size_of_hold_queue

        return circulation_data

    # Axis authors with a special role have an abbreviation after their names,
    # e.g. "San Ruby (FRW)"
    role_abbreviation = re.compile(r"\(([A-Z][A-Z][A-Z])\)$")
    generic_author = object()
    role_abbreviation_to_role = dict(
        INT=Contributor.Role.INTRODUCTION,
        EDT=Contributor.Role.EDITOR,
        PHT=Contributor.Role.PHOTOGRAPHER,
        ILT=Contributor.Role.ILLUSTRATOR,
        TRN=Contributor.Role.TRANSLATOR,
        FRW=Contributor.Role.FOREWORD,
        ADP=generic_author,  # Author of adaptation
        COR=generic_author,  # Corporate author
    )

    @classmethod
    def parse_contributor(
        cls,
        author: str,
        primary_author_found: bool = False,
        force_role: Contributor.Role | None = None,
    ) -> ContributorData:
        """Parse an Axis 360 contributor string.

        The contributor string looks like "Butler, Octavia" or "Walt
        Disney Pictures (COR)" or "Rex, Adam (ILT)". The optional
        three-letter code describes the contributor's role in the
        book.

        :param author: The string to parse.

        :param primary_author_found: If this is false, then a
            contributor with no three-letter code will be treated as
            the primary author. If this is true, then a contributor
            with no three-letter code will be treated as just a
            regular author.

        :param force_role: If this is set, the contributor will be
            assigned this role, no matter what. This takes precedence
            over the value implied by primary_author_found.
        """
        if primary_author_found:
            default_author_role = Contributor.Role.AUTHOR
        else:
            default_author_role = Contributor.Role.PRIMARY_AUTHOR
        role = default_author_role
        match = cls.role_abbreviation.search(author)
        if match:
            role_type = match.groups()[0]
            mapped_role = cls.role_abbreviation_to_role.get(
                role_type, Contributor.Role.UNKNOWN
            )
            role = (
                default_author_role
                if mapped_role is cls.generic_author
                else cast(Contributor.Role, mapped_role)
            )
            author = author[:-5].strip()
        if force_role:
            role = force_role
        return ContributorData(sort_name=author, roles=[role])

    def extract_bibliographic(
        self, element: _Element, ns: dict[str, str] | None
    ) -> BibliographicData:
        """Turn bibliographic metadata into a BibliographicData and a CirculationData objects,
        and return them as a tuple."""

        # TODO: These are consistently empty (some are clearly for
        # audiobooks) so I don't know what they do and/or what format
        # they're in.
        #
        # edition
        # runtime

        identifier = self.text_of_subtag(element, "axis:titleId", ns)
        isbn = self.text_of_optional_subtag(element, "axis:isbn", ns)
        title = self.text_of_subtag(element, "axis:productTitle", ns)

        contributor = self.text_of_optional_subtag(element, "axis:contributor", ns)
        contributors = []
        found_primary_author = False
        if contributor:
            for c in self.parse_list(contributor):
                contributor_data = self.parse_contributor(c, found_primary_author)
                if Contributor.Role.PRIMARY_AUTHOR in contributor_data.roles:
                    found_primary_author = True
                contributors.append(contributor_data)

        narrator = self.text_of_optional_subtag(element, "axis:narrator", ns)
        if narrator:
            for n in self.parse_list(narrator):
                contributor_data = self.parse_contributor(
                    n, force_role=Contributor.Role.NARRATOR
                )
                contributors.append(contributor_data)

        links = []
        description = self.text_of_optional_subtag(element, "axis:annotation", ns)
        if description:
            links.append(
                LinkData(
                    rel=Hyperlink.DESCRIPTION,
                    content=description,
                    media_type=Representation.TEXT_PLAIN,
                )
            )

        subject = self.text_of_optional_subtag(element, "axis:subject", ns)
        subjects = []
        if subject:
            for subject_identifier in self.parse_list(subject):
                subjects.append(
                    SubjectData(
                        type=Subject.BISAC,
                        identifier=None,
                        name=subject_identifier,
                        weight=Classification.TRUSTED_DISTRIBUTOR_WEIGHT,
                    )
                )

        publication_date_str = self.text_of_optional_subtag(
            element, "axis:publicationDate", ns
        )
        if publication_date_str:
            publication_date = strptime_utc(
                publication_date_str, self.SHORT_DATE_FORMAT
            )

        series = self.text_of_optional_subtag(element, "axis:series", ns)
        publisher = self.text_of_optional_subtag(element, "axis:publisher", ns)
        imprint = self.text_of_optional_subtag(element, "axis:imprint", ns)

        audience = self.text_of_optional_subtag(element, "axis:audience", ns)
        if audience:
            subjects.append(
                SubjectData(
                    type=Subject.AXIS_360_AUDIENCE,
                    identifier=audience,
                    weight=Classification.TRUSTED_DISTRIBUTOR_WEIGHT,
                )
            )

        language = self.text_of_subtag(element, "axis:language", ns)

        thumbnail_url = self.text_of_optional_subtag(element, "axis:imageUrl", ns)
        if thumbnail_url:
            # We presume all images from this service are JPEGs.
            media_type = MediaTypes.JPEG_MEDIA_TYPE
            if "/Medium/" in thumbnail_url:
                # We know about a URL hack for this service that lets us
                # get a larger image.
                full_size_url = thumbnail_url.replace("/Medium/", "/Large/")
            else:
                # If the URL hack won't work, treat the image we got
                # as both the full-sized image and its thumbnail.
                # This won't happen unless B&T changes the service.
                full_size_url = thumbnail_url

            thumbnail = LinkData(
                rel=LinkRelations.THUMBNAIL_IMAGE,
                href=thumbnail_url,
                media_type=media_type,
            )
            image = LinkData(
                rel=LinkRelations.IMAGE,
                href=full_size_url,
                media_type=media_type,
                thumbnail=thumbnail,
            )
            links.append(image)

        # We don't use this for anything.
        # file_size = self.int_of_optional_subtag(element, 'axis:fileSize', ns)
        primary_identifier = IdentifierData(
            type=Identifier.AXIS_360_ID, identifier=identifier
        )
        identifiers = []
        if isbn:
            identifiers.append(IdentifierData(type=Identifier.ISBN, identifier=isbn))

        formats = []
        seen_formats = []

        # All of the formats we don't support, like Blio, are ebook
        # formats. If this is an audiobook format (Acoustik), we'll
        # hear about it below.
        medium = Edition.BOOK_MEDIUM

        # If AxisNow is mentioned as a format, and this turns out to be a book,
        # we'll be adding an extra delivery mechanism.
        axisnow_seen = False

        # Blio is an older ebook format now used as an alias for AxisNow.
        blio_seen = False

        for format_tag in self._xpath(
            element, "axis:availability/axis:availableFormats/axis:formatName", ns
        ):
            informal_name = format_tag.text
            seen_formats.append(informal_name)

            if informal_name == Axis360APIConstants.BLIO:
                # We will be adding an AxisNow FormatData.
                blio_seen = True
                continue
            elif informal_name == Axis360APIConstants.AXISNOW:
                # We will only be adding an AxisNow FormatData if this
                # turns out to be an ebook.
                axisnow_seen = True
                continue

            if informal_name not in self.DELIVERY_DATA_FOR_AXIS_FORMAT:
                self.log.warning(
                    "Unrecognized Axis format name for %s: %s"
                    % (identifier, informal_name)
                )
            elif delivery_data := self.DELIVERY_DATA_FOR_AXIS_FORMAT.get(informal_name):
                content_type, drm_scheme = delivery_data
                formats.append(
                    FormatData(content_type=content_type, drm_scheme=drm_scheme)
                )

                if drm_scheme == DeliveryMechanism.FINDAWAY_DRM:
                    medium = Edition.AUDIO_MEDIUM
                else:
                    medium = Edition.BOOK_MEDIUM
        if blio_seen or (axisnow_seen and medium == Edition.BOOK_MEDIUM):
            # This ebook is available through AxisNow. Add an
            # appropriate FormatData.
            #
            # Audiobooks may also be available through AxisNow, but we
            # currently ignore that fact.
            formats.append(
                FormatData(content_type=None, drm_scheme=DeliveryMechanism.AXISNOW_DRM)
            )

        if not formats:
            self.log.error(
                "No supported format for %s (%s)! Saw: %s",
                identifier,
                title,
                ", ".join(seen_formats),
            )

        bibliographic = BibliographicData(
            data_source_name=DataSource.AXIS_360,
            title=title,
            language=language,
            medium=medium,
            series=series,
            publisher=publisher,
            imprint=imprint,
            published=publication_date,
            primary_identifier_data=primary_identifier,
            identifiers=identifiers,
            subjects=subjects,
            contributors=contributors,
            links=links,
        )

        circulationdata = CirculationData(
            data_source_name=DataSource.AXIS_360,
            primary_identifier_data=primary_identifier,
            formats=formats,
        )

        bibliographic.circulation = circulationdata
        return bibliographic

    def process_one(
        self, element: _Element, ns: dict[str, str] | None
    ) -> tuple[BibliographicData, CirculationData]:
        bibliographic = self.extract_bibliographic(element, ns)

        passed_availability = None
        if bibliographic and bibliographic.circulation:
            passed_availability = bibliographic.circulation

        availability = self.extract_availability(
            circulation_data=passed_availability, element=element, ns=ns
        )

        return bibliographic, availability


class ResponseParser:
    id_type = Identifier.AXIS_360_ID

    SERVICE_NAME = "Axis 360"

    # Map Axis 360 error codes to our circulation exceptions.
    code_to_exception: Mapping[int | tuple[int, str], type[IntegrationException]] = {
        315: InvalidInputException,  # Bad password
        316: InvalidInputException,  # DRM account already exists
        1000: PatronAuthorizationFailedException,
        1001: PatronAuthorizationFailedException,
        1002: PatronAuthorizationFailedException,
        1003: PatronAuthorizationFailedException,
        2000: LibraryAuthorizationFailedException,
        2001: LibraryAuthorizationFailedException,
        2002: LibraryAuthorizationFailedException,
        2003: LibraryAuthorizationFailedException,  # "Encoded input parameters exceed limit", whatever that means
        2004: LibraryAuthorizationFailedException,  # Authorization string is not properly encoded
        2005: LibraryAuthorizationFailedException,  # Invalid credentials
        2006: LibraryAuthorizationFailedException,  # Library ID not associated with given vendor
        2007: LibraryAuthorizationFailedException,  # Invalid library ID
        2008: LibraryAuthorizationFailedException,  # Invalid library ID
        3100: LibraryInvalidInputException,  # Missing title ID
        3101: LibraryInvalidInputException,  # Missing patron ID
        3102: LibraryInvalidInputException,  # Missing email address (for hold notification)
        3103: NotFoundOnRemote,  # Invalid title ID
        3104: LibraryInvalidInputException,  # Invalid Email Address (for hold notification)
        3105: PatronAuthorizationFailedException,  # Invalid Account Credentials
        3106: InvalidInputException,  # Loan Period is out of bounds
        3108: InvalidInputException,  # DRM Credentials Required
        3109: InvalidInputException,  # Hold already exists or hold does not exist, depending.
        3110: AlreadyCheckedOut,
        3111: CurrentlyAvailable,
        3112: CannotFulfill,
        3113: CannotLoan,
        (3113, "Title ID is not available for checkout"): NoAvailableCopies,
        3114: PatronLoanLimitReached,
        3115: LibraryInvalidInputException,  # Missing DRM format
        3116: LibraryInvalidInputException,  # No patron session ID provided -- we don't use this
        3117: LibraryInvalidInputException,  # Invalid DRM format
        3118: LibraryInvalidInputException,  # Invalid Patron credentials
        3119: LibraryAuthorizationFailedException,  # No Blio account
        3120: LibraryAuthorizationFailedException,  # No Acoustikaccount
        3123: PatronAuthorizationFailedException,  # Patron Session ID expired
        3124: PatronAuthorizationFailedException,  # Patron SessionID is required
        3126: LibraryInvalidInputException,  # Invalid checkout format
        3127: InvalidInputException,  # First name is required
        3128: InvalidInputException,  # Last name is required
        3129: PatronAuthorizationFailedException,  # Invalid Patron Session Id
        3130: LibraryInvalidInputException,  # Invalid hold format (?)
        3131: RemoteInitiatedServerError,  # Custom error message (?)
        3132: LibraryInvalidInputException,  # Invalid delta datetime format
        3134: LibraryInvalidInputException,  # Delta datetime format must not be in the future
        3135: NoAcceptableFormat,
        3136: LibraryInvalidInputException,  # Missing checkout format
        4058: NoActiveLoan,  # No checkout is associated with patron for the title.
        5000: RemoteInitiatedServerError,
        5003: LibraryInvalidInputException,  # Missing TransactionID
        5004: LibraryInvalidInputException,  # Missing TransactionID
    }

    @classmethod
    def _raise_exception_on_error(
        cls,
        code: str | int,
        message: str,
        custom_error_classes: None | (
            Mapping[int | tuple[int, str], type[IntegrationException]]
        ) = None,
        ignore_error_codes: list[int] | None = None,
    ) -> tuple[int, str]:
        try:
            code = int(code)
        except ValueError:
            # Non-numeric code? Inconceivable!
            raise RemoteInitiatedServerError(
                "Invalid response code from Axis 360: %s" % code, cls.SERVICE_NAME
            )

        if ignore_error_codes and code in ignore_error_codes:
            return code, message

        if custom_error_classes is None:
            custom_error_classes = {}
        for d in custom_error_classes, cls.code_to_exception:
            if (code, message) in d:
                raise d[(code, message)]
            elif code in d:
                # Something went wrong and we know how to turn it into a
                # specific exception.
                error_class = d[code]
                if error_class is RemoteInitiatedServerError:
                    e = error_class(message, cls.SERVICE_NAME)
                else:
                    e = error_class(message)
                raise e
        return code, message


class XMLResponseParser(ResponseParser, Axis360Parser[T], ABC):
    def raise_exception_on_error(
        self,
        e: _Element,
        ns: dict[str, str] | None,
        custom_error_classes: None | (
            Mapping[int | tuple[int, str], type[IntegrationException]]
        ) = None,
        ignore_error_codes: list[int] | None = None,
    ) -> tuple[int, str]:
        """Raise an error if the given lxml node represents an Axis 360 error
        condition.

        :param e: An lxml Element
        :param ns: A dictionary of namespaces
        :param custom_error_classes: A dictionary of errors to map to custom
           classes rather than the defaults.
        :param ignore_error_codes: A list of error codes to treat as success
           rather than as cause to raise an exception.
        """
        code = self._xpath1(e, "//axis:status/axis:code", ns)
        message = self._xpath1(e, "//axis:status/axis:statusMessage", ns)
        if message is None:
            message = etree.tostring(e)
        else:
            message = message.text
        if code is None:
            # Something is so wrong that we don't know what to do.
            raise RemoteInitiatedServerError(message, self.SERVICE_NAME)
        return self._raise_exception_on_error(
            code.text, message, custom_error_classes, ignore_error_codes
        )


class CheckinResponseParser(XMLResponseParser[Literal[True]]):
    @property
    def xpath_expression(self) -> str:
        return "//axis:EarlyCheckinRestResult"

    def process_one(
        self, e: _Element, namespaces: dict[str, str] | None
    ) -> Literal[True]:
        """Either raise an appropriate exception, or do nothing."""
        self.raise_exception_on_error(e, namespaces, ignore_error_codes=[4058])
        return True


class CheckoutResponseParser(XMLResponseParser[datetime.datetime | None]):
    @property
    def xpath_expression(self) -> str:
        return "//axis:checkoutResult"

    def process_one(
        self, e: _Element, namespaces: dict[str, str] | None
    ) -> datetime.datetime | None:
        """Either turn the given document into a datetime representing the
        loan's expiration date, or raise an appropriate exception.
        """
        self.raise_exception_on_error(e, namespaces)

        # If we get to this point it's because the checkout succeeded.
        expiration_date = self._xpath1(e, "//axis:expirationDate", namespaces)

        if expiration_date is not None:
            expiration_date = expiration_date.text
            expiration_date = self._pd(expiration_date)

        return expiration_date


class HoldResponseParser(XMLResponseParser[int | None], LoggerMixin):
    @property
    def xpath_expression(self) -> str:
        return "//axis:addtoholdResult"

    def process_one(self, e: _Element, namespaces: dict[str, str] | None) -> int | None:
        """Either turn the given document into an int representing the hold position,
        or raise an appropriate exception.
        """
        self.raise_exception_on_error(e, namespaces, {3109: AlreadyOnHold})

        # If we get to this point it's because the hold place succeeded.
        queue_position = self._xpath1(e, "//axis:holdsQueuePosition", namespaces)
        if queue_position is None:
            queue_position = None
        else:
            try:
                queue_position = int(queue_position.text)
            except ValueError:
                self.log.warning("Invalid queue position: %s" % queue_position)
                queue_position = None

        return queue_position


class HoldReleaseResponseParser(XMLResponseParser[Literal[True]]):
    @property
    def xpath_expression(self) -> str:
        return "//axis:removeholdResult"

    def process_one(
        self, e: _Element, namespaces: dict[str, str] | None
    ) -> Literal[True]:
        # There's no data to gather here. Either there was an error
        # or we were successful.
        self.raise_exception_on_error(e, namespaces, {3109: NotOnHold})
        return True


class AvailabilityResponseParser(XMLResponseParser[Union[AxisLoanInfo, HoldInfo]]):
    def __init__(self, api: Axis360API, internal_format: str | None = None) -> None:
        """Constructor.

        :param api: An Axis360API instance, in case the parsing of an
           availability document triggers additional API requests.

        :param internal_format: The name Axis 360 gave to the format
           the user requested. Used to distinguish books
           checked out through the AxisNow Book Vault from books checked
           out through ACS.
        """
        self.api = api
        self.internal_format = internal_format
        if api.collection_id is None:
            raise ValueError(
                "Cannot use an Axis360AvailabilityResponseParser without a collection_id."
            )
        self.collection_id = api.collection_id
        super().__init__()

    @property
    def xpath_expression(self) -> str:
        return "//axis:title"

    def process_one(
        self, e: _Element, ns: dict[str, str] | None
    ) -> AxisLoanInfo | HoldInfo | None:
        # Figure out which book we're talking about.
        axis_identifier = self.text_of_subtag(e, "axis:titleId", ns)
        availability = self._xpath1(e, "axis:availability", ns)
        if availability is None:
            return None
        reserved = self._xpath1_boolean(availability, "axis:isReserved", ns)
        checked_out = self._xpath1_boolean(availability, "axis:isCheckedout", ns)
        on_hold = self._xpath1_boolean(availability, "axis:isInHoldQueue", ns)

        info: AxisLoanInfo | HoldInfo | None = None
        if checked_out:
            # When the item is checked out, it can be locked to a particular DRM format. So even though
            # the item supports other formats, it can only be fulfilled in the format that was checked out.
            # This format is returned in the checkoutFormat tag.
            checkout_format = self.text_of_optional_subtag(
                availability, "axis:checkoutFormat", ns
            )
            start_date = self._xpath1_date(availability, "axis:checkoutStartDate", ns)
            end_date = self._xpath1_date(availability, "axis:checkoutEndDate", ns)
            download_url = self.text_of_optional_subtag(
                availability, "axis:downloadUrl", ns
            )
            transaction_id = (
                self.text_of_optional_subtag(availability, "axis:transactionID", ns)
                or ""
            )

            if not self.internal_format and (
                checkout_format == self.api.AXISNOW or checkout_format == self.api.BLIO
            ):
                # If we didn't explicitly ask for a format, ignore any AxisNow or Blio formats, since
                # we can't fulfill them. If we add AxisNow and Blio support in the future, we can remove
                # this check.
                return None

            fulfillment: Fulfillment | None
            if download_url and self.internal_format != self.api.AXISNOW:
                # The patron wants a direct link to the book, which we can deliver
                # immediately, without making any more API requests.
                fulfillment = Axis360AcsFulfillment(
                    content_link=html.unescape(download_url),
                    content_type=DeliveryMechanism.ADOBE_DRM,
                    verify=self.api.verify_certificate,
                )
            elif transaction_id:
                # We will eventually need to make a request to the
                # "getfulfillmentInfo" endpoint, using this
                # transaction ID.
                #
                # For a book delivered in AxisNow format, this will give
                # us the Book Vault UUID and ISBN.
                #
                # For an audiobook, this will give us the Findaway
                # content ID, license ID, and session key. We'll also
                # need to make a second request to get the audiobook
                # metadata.
                #
                # Axis360Fulfillment can handle both cases.
                fulfillment = Axis360Fulfillment(
                    data_source_name=DataSource.AXIS_360,
                    identifier_type=self.id_type,
                    identifier=axis_identifier,
                    api=self.api,
                    key=transaction_id,
                )
            else:
                # We're out of luck -- we can't fulfill this loan.
                fulfillment = None
            info = AxisLoanInfo(
                collection_id=self.collection_id,
                identifier_type=self.id_type,
                identifier=axis_identifier,
                start_date=start_date,
                end_date=end_date,
                fulfillment=fulfillment,
            )

        elif reserved:
            end_date = self._xpath1_date(availability, "axis:reservedEndDate", ns)
            info = HoldInfo(
                collection_id=self.collection_id,
                identifier_type=self.id_type,
                identifier=axis_identifier,
                end_date=end_date,
                hold_position=0,
            )
        elif on_hold:
            position = self.int_of_optional_subtag(
                availability, "axis:holdsQueuePosition", ns
            )
            info = HoldInfo(
                collection_id=self.collection_id,
                identifier_type=self.id_type,
                identifier=axis_identifier,
                hold_position=position,
            )
        return info


class JSONResponseParser(Generic[T], ResponseParser, ABC):
    """Most ResponseParsers parse XML documents; subclasses of
    JSONResponseParser parse JSON documents.

    This only subclasses ResponseParser so it can reuse
    _raise_exception_on_error.
    """

    @classmethod
    def _required_key(cls, key: str, json_obj: Mapping[str, Any] | None) -> Any:
        """Raise an exception if the given key is not present in the given
        object.
        """
        if json_obj is None or key not in json_obj:
            raise RemoteInitiatedServerError(
                "Required key %s not present in Axis 360 fulfillment document: %s"
                % (
                    key,
                    json_obj,
                ),
                cls.SERVICE_NAME,
            )
        return json_obj[key]

    @classmethod
    def verify_status_code(cls, parsed: Mapping[str, Any] | None) -> None:
        """Assert that the incoming JSON document represents a successful
        response.
        """
        k = cls._required_key
        status = k("Status", parsed)
        code: int = k("Code", status)
        message = status.get("Message")

        # If the document describes an error condition, raise
        # an appropriate exception immediately.
        cls._raise_exception_on_error(code, message)

    def parse(self, data: dict[str, Any] | bytes | str, **kwargs: Any) -> T:
        """Parse a JSON document."""
        if isinstance(data, dict):
            parsed = data  # already parsed
        else:
            try:
                parsed = json.loads(data)
            except ValueError as e:
                # It's not JSON.
                raise RemoteInitiatedServerError(
                    f"Invalid response from Axis 360 (was expecting JSON): {data!r}",
                    self.SERVICE_NAME,
                )

        # If the response indicates an error condition, don't continue --
        # raise an exception immediately.
        self.verify_status_code(parsed)
        return self._parse(parsed, **kwargs)

    @abstractmethod
    def _parse(self, parsed: dict[str, Any], **kwargs: Any) -> T:
        """Parse a document we know to represent success on the
        API level. Called by parse() once the high-level details
        have been worked out.
        """
        ...


class Axis360FulfillmentInfoResponseParser(
    JSONResponseParser[
        tuple[Union[FindawayManifest, "AxisNowManifest"], datetime.datetime]
    ],
    LoggerMixin,
):
    """Parse JSON documents into Findaway audiobook manifests or AxisNow manifests."""

    def __init__(self, api: Axis360API):
        """Constructor.

        :param api: An Axis360API instance, in case the parsing of
        a fulfillment document triggers additional API requests.
        """
        self.api = api

    def _parse(
        self,
        parsed: dict[str, Any],
        license_pool: LicensePool | None = None,
        **kwargs: Any,
    ) -> tuple[FindawayManifest | AxisNowManifest, datetime.datetime]:
        """Extract all useful information from a parsed FulfillmentInfo
        response.

        :param parsed: A dictionary corresponding to a parsed JSON
        document.

        :param license_pool: The LicensePool for the book that's
        being fulfilled.

        :return: A 2-tuple (manifest, expiration_date). `manifest` is either
            a FindawayManifest (for an audiobook) or an AxisNowManifest (for an ebook).
        """
        if license_pool is None:
            raise TypeError("Must pass in a LicensePool")

        expiration_date = self._required_key("ExpirationDate", parsed)
        expiration_date = self.parse_date(expiration_date)

        manifest: FindawayManifest | AxisNowManifest
        if "FNDTransactionID" in parsed:
            manifest = self.parse_findaway(parsed, license_pool)
        else:
            manifest = self.parse_axisnow(parsed)

        return manifest, expiration_date

    def parse_date(self, date: str) -> datetime.datetime:
        if "." in date:
            # Remove 7(?!) decimal places of precision and
            # UTC timezone, which are more trouble to parse
            # than they're worth.
            date = date[: date.rindex(".")]

        try:
            date_parsed = strptime_utc(date, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise RemoteInitiatedServerError(
                "Could not parse expiration date: %s" % date, self.SERVICE_NAME
            )
        return date_parsed

    def parse_findaway(
        self, parsed: dict[str, Any], license_pool: LicensePool
    ) -> FindawayManifest:
        k = self._required_key
        fulfillmentId = k("FNDContentID", parsed)
        licenseId = k("FNDLicenseID", parsed)
        sessionKey = k("FNDSessionKey", parsed)
        checkoutId = k("FNDTransactionID", parsed)

        if sessionKey == "Expired":
            try:
                identifier_msg = f"{license_pool.identifier.type}/{license_pool.identifier.identifier}"
            except AttributeError:
                identifier_msg = f"LicensePool.id {license_pool.id}"

            message = f"Expired findaway session key for {identifier_msg}. Request data: {json.dumps(parsed)}"
            self.log.error(message)
            raise RemoteInitiatedServerError(
                message,
                self.SERVICE_NAME,
            )

        # Acquire the TOC information
        metadata_response = self.api.get_audiobook_metadata(fulfillmentId)
        parser = AudiobookMetadataParser()
        accountId, spine_items = parser.parse(metadata_response.content)

        return FindawayManifest(
            license_pool,
            accountId=accountId,
            checkoutId=checkoutId,
            fulfillmentId=fulfillmentId,
            licenseId=licenseId,
            sessionKey=sessionKey,
            spine_items=spine_items,
        )

    def parse_axisnow(self, parsed: dict[str, Any]) -> AxisNowManifest:
        k = self._required_key
        isbn = k("ISBN", parsed)
        book_vault_uuid = k("BookVaultUUID", parsed)
        return AxisNowManifest(book_vault_uuid, isbn)


class AudiobookMetadataParser(
    JSONResponseParser[tuple[Optional[str], list[SpineItem]]]
):
    """Parse the results of Axis 360's audiobook metadata API call."""

    @classmethod
    def _parse(
        cls, parsed: dict[str, Any], **kwargs: Any
    ) -> tuple[str | None, list[SpineItem]]:
        spine_items = []
        accountId = parsed.get("fndaccountid", None)
        for item in parsed.get("readingOrder", []):
            spine_item = cls._extract_spine_item(item)
            if spine_item:
                spine_items.append(spine_item)
        return accountId, spine_items

    @classmethod
    def _extract_spine_item(cls, part: dict[str, str | int | float]) -> SpineItem:
        """Convert an element of the 'readingOrder' list to a SpineItem."""
        title = part.get("title")
        # Incoming duration is measured in seconds.
        duration = part.get("duration", 0)
        part_number = int(part.get("fndpart", 0))
        sequence = int(part.get("fndsequence", 0))
        return SpineItem(title, duration, part_number, sequence)
