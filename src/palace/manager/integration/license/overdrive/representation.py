from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

import isbnlib

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.link import LinkData
from palace.manager.data_layer.measurement import MeasurementData
from palace.manager.data_layer.subject import SubjectData
from palace.manager.integration.license.overdrive.constants import (
    OVERDRIVE_MAIN_ACCOUNT_ID,
)
from palace.manager.integration.license.overdrive.util import _make_link_safe
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.classification import Classification, Subject
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
from palace.manager.util.datetime_helpers import strptime_utc
from palace.manager.util.log import LoggerMixin

if TYPE_CHECKING:
    from palace.manager.integration.license.overdrive.api import OverdriveAPI


class OverdriveRepresentationExtractor(LoggerMixin):
    """Extract useful information from Overdrive's JSON representations."""

    def __init__(self, api: OverdriveAPI) -> None:
        """Constructor.

        :param api: An OverdriveAPI object. This will be used when deciding
        which portions of a JSON representation are relevant to the active
        Overdrive collection.
        """
        self.library_id = api.advantage_library_id

    @classmethod
    def availability_link_list(cls, book_list: dict[str, Any]) -> list[dict[str, str]]:
        """:return: A list of dictionaries with keys `id`, `title`, `availability_link`."""
        l = []
        if not "products" in book_list:
            return []

        products = book_list["products"]
        for product in products:
            if not "id" in product:
                cls.logger().warning("No ID found in %r", product)
                continue
            book_id = product["id"]
            data = dict(
                id=book_id,
                title=product.get("title"),
                author_name=None,
                date_added=product.get("dateAdded"),
            )
            if "primaryCreator" in product:
                creator = product["primaryCreator"]
                if creator.get("role") == "Author":
                    data["author_name"] = creator.get("name")
            links = product.get("links", [])
            if "availability" in links:
                link = links["availability"]["href"]
                data["availability_link"] = _make_link_safe(link)
            else:
                logging.getLogger("Overdrive API").warning(
                    "No availability link for %s", book_id
                )
            l.append(data)
        return l

    @classmethod
    def link(cls, page: dict[str, Any], rel: str) -> str | None:
        if "links" in page and rel in page["links"]:
            raw_link = page["links"][rel]["href"]
            link = _make_link_safe(raw_link)
        else:
            link = None
        return link

    _format_data_for_overdrive_format: dict[str, list[FormatData]] = {
        "ebook-overdrive": [
            # When we have an Overdrive ebook, we don't actually know
            # 100% what format it's in, because Overdrive doesn't
            # give us that information though the API. We know that
            # ~95% of the time its available as an Adobe DRM EPUB,
            # so we'll use that as the default.
            #
            # When we go to fulfill the book, Overdrive gives us
            # more information about the format, so at that point,
            # if our assumption was wrong, we will mark the Adobe
            # DRM as unavailable, and add the correct format.
            FormatData(
                content_type=MediaTypes.EPUB_MEDIA_TYPE,
                drm_scheme=DeliveryMechanism.ADOBE_DRM,
                available=True,
            ),
            FormatData(
                content_type=MediaTypes.EPUB_MEDIA_TYPE,
                drm_scheme=DeliveryMechanism.NO_DRM,
                available=False,
            ),
            FormatData(
                content_type=MediaTypes.PDF_MEDIA_TYPE,
                drm_scheme=DeliveryMechanism.NO_DRM,
                available=False,
            ),
            FormatData(
                content_type=DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                drm_scheme=DeliveryMechanism.STREAMING_DRM,
                available=True,
            ),
        ],
        "audiobook-overdrive": [
            FormatData(
                content_type=MediaTypes.OVERDRIVE_AUDIOBOOK_MANIFEST_MEDIA_TYPE,
                drm_scheme=DeliveryMechanism.LIBBY_DRM,
                available=True,
            ),
            FormatData(
                content_type=DeliveryMechanism.STREAMING_AUDIO_CONTENT_TYPE,
                drm_scheme=DeliveryMechanism.STREAMING_DRM,
                available=True,
            ),
        ],
    }

    # A mapping of the overdrive format name to end sample content type
    # Overdrive samples are not DRM protected so the links should be
    # stored as the end sample content type
    _sample_format_to_content_type = {
        "ebook-overdrive": "text/html",
        "audiobook-overdrive": "text/html",
    }

    @classmethod
    def internal_formats(cls, overdrive_format: str) -> list[FormatData]:
        """Get all possible internal formats for the given Overdrive format.

        Some Overdrive formats become multiple internal formats.

        :return: A list of FormatData objects.
        """
        return cls._format_data_for_overdrive_format.get(overdrive_format, [])

    ignorable_overdrive_formats: set[str] = set()

    overdrive_role_to_simplified_role = {
        "actor": Contributor.Role.ACTOR,
        "artist": Contributor.Role.ARTIST,
        "book producer": Contributor.Role.PRODUCER,
        "associated name": Contributor.Role.ASSOCIATED,
        "author": Contributor.Role.AUTHOR,
        "author of introduction": Contributor.Role.INTRODUCTION,
        "author of foreword": Contributor.Role.FOREWORD,
        "author of afterword": Contributor.Role.AFTERWORD,
        "contributor": Contributor.Role.CONTRIBUTOR,
        "colophon": Contributor.Role.COLOPHON,
        "adapter": Contributor.Role.ADAPTER,
        "etc.": Contributor.Role.UNKNOWN,
        "cast member": Contributor.Role.ACTOR,
        "collaborator": Contributor.Role.COLLABORATOR,
        "compiler": Contributor.Role.COMPILER,
        "composer": Contributor.Role.COMPOSER,
        "copyright holder": Contributor.Role.COPYRIGHT_HOLDER,
        "director": Contributor.Role.DIRECTOR,
        "editor": Contributor.Role.EDITOR,
        "engineer": Contributor.Role.ENGINEER,
        "executive producer": Contributor.Role.EXECUTIVE_PRODUCER,
        "illustrator": Contributor.Role.ILLUSTRATOR,
        "musician": Contributor.Role.MUSICIAN,
        "narrator": Contributor.Role.NARRATOR,
        "other": Contributor.Role.UNKNOWN,
        "performer": Contributor.Role.PERFORMER,
        "producer": Contributor.Role.PRODUCER,
        "translator": Contributor.Role.TRANSLATOR,
        "photographer": Contributor.Role.PHOTOGRAPHER,
        "lyricist": Contributor.Role.LYRICIST,
        "transcriber": Contributor.Role.TRANSCRIBER,
        "designer": Contributor.Role.DESIGNER,
    }

    overdrive_medium_to_simplified_medium = {
        "eBook": Edition.BOOK_MEDIUM,
        "Video": Edition.VIDEO_MEDIUM,
        "Audiobook": Edition.AUDIO_MEDIUM,
        "Music": Edition.MUSIC_MEDIUM,
        "Periodicals": Edition.PERIODICAL_MEDIUM,
    }

    DATE_FORMAT = "%Y-%m-%d"

    @classmethod
    def parse_roles(cls, id: str, rolestring: str) -> list[Contributor.Role]:
        rolestring = rolestring.lower()
        roles = [x.strip() for x in rolestring.split(",")]
        if " and " in roles[-1]:
            roles = roles[:-1] + [x.strip() for x in roles[-1].split(" and ")]
        processed = []
        for x in roles:
            if x not in cls.overdrive_role_to_simplified_role:
                cls.logger().error("Could not process role %s for %s", x, id)
            else:
                processed.append(cls.overdrive_role_to_simplified_role[x])
        return processed

    def book_info_to_circulation(self, book: dict[str, Any]) -> CirculationData:
        """Note:  The json data passed into this method is from a different file/stream
        from the json data that goes into the book_info_to_metadata() method.
        """
        # In Overdrive, 'reserved' books show up as books on
        # hold. There is no separate notion of reserved books.
        licenses_reserved = 0

        licenses_owned = None
        licenses_available = None
        patrons_in_hold_queue = None

        # TODO: The only reason this works for a NotFound error is the
        # circulation code sticks the known book ID into `book` ahead
        # of time. That's a code smell indicating that this system
        # needs to be refactored.
        if "reserveId" in book and not "id" in book:
            book["id"] = book["reserveId"]
        if not "id" in book:
            self.log.error("Book has no ID: %r", book)
            raise PalaceValueError("Book must have an id to be processed")
        overdrive_id = book["id"]
        primary_identifier = IdentifierData(
            type=Identifier.OVERDRIVE_ID, identifier=overdrive_id
        )
        # TODO: We might be able to use this information to avoid the
        # need for explicit configuration of Advantage collections, or
        # at least to keep Advantage collections more up-to-date than
        # they would be otherwise, as a side effect of updating
        # regular Overdrive collections.

        # TODO: this would be the place to handle simultaneous use
        # titles -- these can be detected with
        # availabilityType="AlwaysAvailable" and have their
        # .licenses_owned set to LicensePool.UNLIMITED_ACCESS.
        # see http://developer.overdrive.com/apis/library-availability-new

        # TODO: Cost-per-circ titles
        # (availabilityType="LimitedAvailability") can be handled
        # similarly, though those can abruptly become unavailable, so
        # UNLIMITED_ACCESS is probably not appropriate.

        error_code = book.get("errorCode")
        # TODO: It's not clear what other error codes there might be.
        # The current behavior will respond to errors other than
        # NotFound by leaving the book alone, but this might not be
        # the right behavior.
        if error_code == "NotFound":
            licenses_owned = 0
            licenses_available = 0
            patrons_in_hold_queue = 0
        elif book.get("isOwnedByCollections") is not False:
            # We own this book.
            licenses_owned = 0
            licenses_available = 0

            for account in self._get_applicable_accounts(book.get("accounts", [])):
                licenses_owned += int(account.get("copiesOwned", 0))
                licenses_available += int(account.get("copiesAvailable", 0))

            if "numberOfHolds" in book:
                if patrons_in_hold_queue is None:
                    patrons_in_hold_queue = 0
                patrons_in_hold_queue += book["numberOfHolds"]

        if licenses_owned is None:
            license_status = None
        elif licenses_owned > 0:
            license_status = LicensePoolStatus.ACTIVE
        else:
            license_status = LicensePoolStatus.EXHAUSTED

        return CirculationData(
            data_source_name=DataSource.OVERDRIVE,
            primary_identifier_data=primary_identifier,
            licenses_owned=licenses_owned,
            licenses_available=licenses_available,
            licenses_reserved=licenses_reserved,
            patrons_in_hold_queue=patrons_in_hold_queue,
            status=license_status,
        )

    def _get_applicable_accounts(
        self, accounts: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Returns those accounts from the accounts array that apply the
        current overdrive collection context.

        If this is an overdrive parent collection, we want to return accounts
        associated with the main OverDrive "library" and any non-main account
        with sharing enabled.

        If this is a child OverDrive collection, then we return only the
        account associated with that child's OverDrive Advantage "library".
        Additionally, we want to exclude the account if it is "shared" since
        we will be counting it with the parent collection.
        """

        if self.library_id == OVERDRIVE_MAIN_ACCOUNT_ID:
            # this is a parent collection
            filtered_result = filter(
                lambda account: account.get("id") == OVERDRIVE_MAIN_ACCOUNT_ID
                or account.get("shared", False),
                accounts,
            )
        else:
            # this is child collection
            filtered_result = filter(
                lambda account: account.get("id") == self.library_id
                and not account.get("shared", False),
                accounts,
            )

        return list(filtered_result)

    @classmethod
    def image_link_to_linkdata(
        cls, link: dict[str, str], rel: str, thumbnail: LinkData | None = None
    ) -> LinkData | None:
        if not link or not "href" in link:
            return None
        href = link["href"]
        if "00000000-0000-0000-0000" in href:
            # This is a stand-in cover for preorders. It's better not
            # to have a cover at all -- we might be able to get one
            # later, or from another source.
            return None
        href = _make_link_safe(href)
        media_type = link.get("type", None)
        return LinkData(rel=rel, href=href, media_type=media_type, thumbnail=thumbnail)

    _SERIES_POSITION_REGEX = re.compile(r"^.*?(\d+)")

    @classmethod
    def _parse_series_position(
        cls, series_position: Any, overdrive_id: str
    ) -> int | None:
        """
        Parse the series position from Overdrive's JSON representation.

        Overdrive provides the series position as a string, and the format seems to be
        inconsistent from the examples we have seen. This method does its best to extract
        an integer from the string, since we store series position as an integer.
        """
        if not series_position:
            return None

        if not isinstance(series_position, str):
            series_position = str(series_position)
        match = cls._SERIES_POSITION_REGEX.match(series_position)
        if match:
            return int(match.groups()[0])

        cls.logger().error(
            f"Unable to parse series position '{series_position}' for OverDrive ID '{overdrive_id}'"
        )
        return None

    @classmethod
    def book_info_to_bibliographic(
        cls,
        book: dict[str, Any],
        include_bibliographic: bool = True,
        include_formats: bool = True,
    ) -> BibliographicData | None:
        """Turn Overdrive's JSON representation of a book into a BibliographicData
        object.

        Note:  The json data passed into this method is from a different file/stream
        from the json data that goes into the book_info_to_circulation() method.
        """
        if not "id" in book:
            return None
        overdrive_id = book["id"]
        primary_identifier = IdentifierData(
            type=Identifier.OVERDRIVE_ID, identifier=overdrive_id
        )

        # If we trust classification data, we'll give it this weight.
        # Otherwise we'll probably give it a fraction of this weight.
        trusted_weight = Classification.TRUSTED_DISTRIBUTOR_WEIGHT

        duration: int | None = None

        if include_bibliographic:
            title = book.get("title", None)
            sort_title = book.get("sortTitle")
            subtitle = book.get("subtitle", None)
            series = book.get("series", None)
            series_position = cls._parse_series_position(
                book.get("readingOrder"), overdrive_id
            )
            publisher = book.get("publisher", None)
            imprint = book.get("imprint", None)

            if "publishDate" in book:
                published = strptime_utc(book["publishDate"][:10], cls.DATE_FORMAT)
            else:
                published = None

            languages = [l["code"] for l in book.get("languages", [])]
            if "eng" in languages or not languages:
                language = "eng"
            else:
                language = sorted(languages)[0]

            contributors = []
            for creator in book.get("creators", []):
                sort_name = creator["fileAs"]
                display_name = creator["name"]
                role = creator["role"]
                roles = cls.parse_roles(overdrive_id, role) or [
                    Contributor.Role.UNKNOWN
                ]
                contributor = ContributorData(
                    sort_name=sort_name,
                    display_name=display_name,
                    roles=roles,
                    biography=creator.get("bioText", None),
                )
                contributors.append(contributor)

            subjects = []
            for sub in book.get("subjects", []):
                subject = SubjectData(
                    type=Subject.OVERDRIVE,
                    identifier=sub["value"],
                    weight=trusted_weight,
                )
                subjects.append(subject)

            for sub in book.get("keywords", []):
                subject = SubjectData(
                    type=Subject.TAG,
                    identifier=sub["value"],
                    # We don't use TRUSTED_DISTRIBUTOR_WEIGHT because
                    # we don't know where the tags come from --
                    # probably Overdrive users -- and they're
                    # frequently wrong.
                    weight=1,
                )
                subjects.append(subject)

            extra = dict()
            if "grade_levels" in book:
                # n.b. Grade levels are measurements of reading level, not
                # age appropriateness. We can use them as a measure of age
                # appropriateness in a pinch, but we weight them less
                # heavily than TRUSTED_DISTRIBUTOR_WEIGHT.
                for i in book["grade_levels"]:
                    subject = SubjectData(
                        type=Subject.GRADE_LEVEL,
                        identifier=i["value"],
                        weight=round(trusted_weight / 10),
                    )
                    subjects.append(subject)

            overdrive_medium = book.get("mediaType", None)
            if (
                overdrive_medium
                and overdrive_medium not in cls.overdrive_medium_to_simplified_medium
            ):
                cls.logger().error(
                    "Could not process medium %s for %s", overdrive_medium, overdrive_id
                )

            medium = (
                cls.overdrive_medium_to_simplified_medium.get(
                    overdrive_medium, Edition.BOOK_MEDIUM
                )
                if overdrive_medium is not None
                else Edition.BOOK_MEDIUM
            )

            measurements = []
            if "awards" in book:
                extra["awards"] = book.get("awards", [])
                num_awards = len(extra["awards"])
                measurements.append(
                    MeasurementData(
                        quantity_measured=Measurement.AWARDS, value=str(num_awards)
                    )
                )
            if popularity := book.get("popularity"):
                measurements.append(
                    MeasurementData(
                        quantity_measured=Measurement.POPULARITY,
                        value=popularity,
                    )
                )

            for name, subject_type in (
                ("ATOS", Subject.ATOS_SCORE),
                ("lexileScore", Subject.LEXILE_SCORE),
                ("interestLevel", Subject.INTEREST_LEVEL),
            ):
                if not name in book:
                    continue
                identifier = str(book[name])
                subjects.append(
                    SubjectData(
                        type=subject_type, identifier=identifier, weight=trusted_weight
                    )
                )

            for grade_level_info in book.get("gradeLevels", []):
                grade_level = grade_level_info.get("value")
                subjects.append(
                    SubjectData(
                        type=Subject.GRADE_LEVEL,
                        identifier=grade_level,
                        weight=trusted_weight,
                    )
                )

            identifiers = []
            links = []
            sample_hrefs = set()
            for format in book.get("formats", []):
                duration_str: str | None = format.get("duration")
                if duration_str is not None:
                    # Using this method only the last valid duration attribute is captured
                    # If there are multiple formats with different durations, the edition will ignore the rest
                    try:
                        hrs, mins, secs = duration_str.split(":")
                        duration = (int(hrs) * 3600) + (int(mins) * 60) + int(secs)
                    except Exception as ex:
                        cls.logger().error(
                            f"Duration ({duration_str}) could not be parsed: {ex}"
                        )

                for new_id in format.get("identifiers", []):
                    t = new_id["type"]
                    v = new_id["value"]
                    orig_v = v
                    type_key = None
                    if t == "ASIN":
                        type_key = Identifier.ASIN
                    elif t == "ISBN":
                        type_key = Identifier.ISBN
                        if len(v) == 10:
                            v = isbnlib.to_isbn13(v)
                        if v is None or not isbnlib.is_isbn13(v):
                            # Overdrive sometimes uses invalid values
                            # like "n/a" as placeholders. Ignore such
                            # values to avoid a situation where hundreds of
                            # books appear to have the same ISBN. ISBNs
                            # which fail check digit checks or are invalid
                            # also can occur. Log them for review.
                            cls.logger().info("Bad ISBN value provided: %s", orig_v)
                            continue
                    elif t == "DOI":
                        type_key = Identifier.DOI
                    elif t == "UPC":
                        type_key = Identifier.UPC
                    elif t == "PublisherCatalogNumber":
                        continue
                    if type_key and v:
                        identifiers.append(
                            IdentifierData(type=type_key, identifier=v, weight=1)
                        )

                # Samples become links.
                if "samples" in format:
                    for sample_info in format["samples"]:
                        href = sample_info["url"]
                        # Have we already parsed this sample? Overdrive repeats samples per format
                        if href in sample_hrefs:
                            continue

                        # Every sample has its own format type
                        overdrive_format_name = sample_info.get("formatType")
                        if not overdrive_format_name:
                            # Malformed sample
                            continue
                        content_type = cls._sample_format_to_content_type.get(
                            overdrive_format_name
                        )
                        if not content_type:
                            # Unusable by us.
                            cls.logger().warning(
                                f"Did not find a sample format mapping for '{overdrive_format_name}': {href}"
                            )
                            continue

                        if Representation.is_media_type(content_type):
                            links.append(
                                LinkData(
                                    rel=Hyperlink.SAMPLE,
                                    href=href,
                                    media_type=content_type,
                                )
                            )
                            sample_hrefs.add(href)

            # A cover and its thumbnail become a single LinkData.
            if "images" in book:
                images = book["images"]
                for name in ["cover300Wide", "cover150Wide", "thumbnail"]:
                    # Try to get a thumbnail that's as close as possible
                    # to the size we use.
                    image = images.get(name)
                    thumbnail_data = cls.image_link_to_linkdata(
                        image, Hyperlink.THUMBNAIL_IMAGE
                    )
                    if thumbnail_data:
                        break

                image_data = cls.image_link_to_linkdata(
                    images.get("cover"), Hyperlink.IMAGE, thumbnail_data
                )

                if not image_data:
                    image_data = cls.image_link_to_linkdata(
                        image, Hyperlink.IMAGE, thumbnail_data
                    )

                if image_data:
                    links.append(image_data)

            # Descriptions become links.
            short = book.get("shortDescription")
            full = book.get("fullDescription")
            if full:
                links.append(
                    LinkData(
                        rel=Hyperlink.DESCRIPTION,
                        content=full,
                        media_type="text/html",
                    )
                )

            if short and (not full or not full.startswith(short)):
                links.append(
                    LinkData(
                        rel=Hyperlink.SHORT_DESCRIPTION,
                        content=short,
                        media_type="text/html",
                    )
                )

            bibliographic = BibliographicData(
                data_source_name=DataSource.OVERDRIVE,
                title=title,
                subtitle=subtitle,
                sort_title=sort_title,
                language=language,
                medium=medium,
                series=series,
                series_position=series_position,
                publisher=publisher,
                imprint=imprint,
                published=published,
                primary_identifier_data=primary_identifier,
                identifiers=identifiers,
                subjects=subjects,
                contributors=contributors,
                measurements=measurements,
                links=links,
                duration=duration,
            )
        else:
            bibliographic = BibliographicData(
                data_source_name=DataSource.OVERDRIVE,
                primary_identifier_data=primary_identifier,
            )

        if include_formats:
            formats = []
            for format in book.get("formats", []):
                format_id = format["id"]
                internal_formats = list(cls.internal_formats(format_id))
                if internal_formats:
                    formats.extend(internal_formats)
                elif format_id not in cls.ignorable_overdrive_formats:
                    cls.logger().error(
                        "Could not process Overdrive format %s for %s",
                        format_id,
                        overdrive_id,
                    )

            # Also make a CirculationData so we can write the formats,
            circulationdata = CirculationData(
                data_source_name=DataSource.OVERDRIVE,
                primary_identifier_data=primary_identifier,
                formats=formats,
            )

            bibliographic.circulation = circulationdata

        return bibliographic
