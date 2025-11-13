from __future__ import annotations

from collections.abc import Generator, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from urllib.parse import urljoin

import dateutil
import feedparser
from lxml import etree
from lxml.etree import ElementTree, _Element as Element

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.link import LinkData
from palace.manager.data_layer.measurement import MeasurementData
from palace.manager.data_layer.subject import SubjectData
from palace.manager.integration.license.opds.bearer_token_drm import BearerTokenDrmMixin
from palace.manager.integration.license.opds.data import FailedPublication
from palace.manager.integration.license.opds.extractor import (
    OpdsExtractor,
)
from palace.manager.integration.license.opds.opds1.settings import (
    IdentifierSource,
)
from palace.manager.integration.license.opds.opds1.xml_parser import OPDSXMLParser
from palace.manager.opds.odl.info import LicenseInfo
from palace.manager.opds.odl.odl import License
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolType,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.model.resource import Hyperlink
from palace.manager.util import first_or_default
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from palace.manager.util.opds_writer import OPDSFeed


@dataclass
class OPDS1Feed:
    parser: OPDSXMLParser
    feed_parser: dict[str, Any]
    etree: ElementTree


@dataclass
class OPDS1Publication:
    parser: OPDSXMLParser
    entry_fp: dict[str, Any]
    entry_xml: Element


class Opds1Extractor(OpdsExtractor[OPDS1Feed, OPDS1Publication], BearerTokenDrmMixin):
    """
    Extractor for OPDS 1.x feeds.

    This extractor also handles extraction of data from OPDS for Distributors feeds if
    the opds_for_distributors flag is set to true in the constructor.
    """

    def __init__(
        self,
        base_url: str,
        data_source: str,
        primary_identifier_source: IdentifierSource = IdentifierSource.ID,
        xml_parser: OPDSXMLParser | None = None,
        opds_for_distributors: bool = False,
    ):
        self._base_url = base_url
        self._data_source = data_source
        self._primary_identifier_source = primary_identifier_source
        self._xml_parser = OPDSXMLParser() if xml_parser is None else xml_parser
        self._opds_for_distributors = opds_for_distributors

    @classmethod
    def _datetime(cls, entry: dict[str, str], key: str) -> datetime | None:
        value = entry.get(key, None)
        if not value:
            return None
        return datetime_utc(*value[:6])

    @classmethod
    def _lookup_rights_uri(cls, rights_string: str) -> str:
        """Determine the URI that best encapsulates the rights status of
        the downloads associated with this book.
        """
        return RightsStatus.rights_uri_from_string(rights_string)

    @staticmethod
    def _extract_feedparser_entries(feed: OPDS1Feed) -> Generator[dict[str, Any]]:
        yield from feed.feed_parser["entries"]

    @staticmethod
    def _extract_elementtree_entries(feed: OPDS1Feed) -> Generator[Element]:
        yield from feed.parser._xpath(feed.etree, "/atom:feed/atom:entry")

    @staticmethod
    def _extract_title(entry_fp: dict[str, Any]) -> str | None:
        title = entry_fp.get("title", None)
        if title == OPDSFeed.NO_TITLE:
            title = None
        return title

    @staticmethod
    def _extract_subtitle(entry_fp: dict[str, Any]) -> str | None:
        subtitle = entry_fp.get("schema_alternativeheadline", None)
        return subtitle

    @staticmethod
    def _extract_circulation_data_source(entry_fp: dict[str, Any], default: str) -> str:
        # Generally speaking, a data source will provide either
        # bibliographic data (e.g. the Simplified metadata wrangler) or both
        # bibliographic and circulation data (e.g. a publisher's ODL feed).
        #
        # However, there is at least one case (the Simplified
        # open-access content server) where one server provides
        # circulation data from a _different_ data source
        # (e.g. Project Gutenberg).
        #
        # In this case we want the data source of the LicensePool to
        # be Project Gutenberg, but the data source of the pool's
        # presentation to be the open-access content server.
        #
        # The open-access content server uses a
        # <bibframe:distribution> tag to keep track of which data
        # source provides the circulation data.
        circulation_data_source_tag = entry_fp.get("bibframe_distribution")
        if circulation_data_source_tag:
            return circulation_data_source_tag.get(  # type: ignore[no-any-return]
                "bibframe:providername", default
            )
        return default

    @classmethod
    def _extract_last_update_date(cls, entry_fp: dict[str, Any]) -> datetime | None:
        if "updated_parsed" in entry_fp:
            return cls._datetime(entry_fp, "updated_parsed")
        return cls._datetime(entry_fp, "published_parsed")

    @staticmethod
    def _extract_publisher(entry_fp: dict[str, Any]) -> str | None:
        publisher = entry_fp.get("publisher", None)
        if not publisher:
            publisher = entry_fp.get("dcterms_publisher", None)
        return publisher

    @staticmethod
    def _extract_language(entry_fp: dict[str, Any]) -> str | None:
        language = entry_fp.get("language", None)
        if not language:
            language = entry_fp.get("dcterms_language", None)
        return language

    @classmethod
    def _make_link_data(
        cls,
        rel: str,
        href: str | None = None,
        media_type: str | None = None,
        rights_uri: str | None = None,
        content: str | None = None,
    ) -> LinkData:
        """Hook method for creating a LinkData object.

        Intended to be overridden in subclasses.
        """
        return LinkData(
            rel=rel,
            href=href,
            media_type=media_type,
            rights_uri=rights_uri,
            content=content,
        )

    @classmethod
    def _summary_to_linkdata(cls, detail: dict[str, str] | None) -> LinkData | None:
        if not detail:
            return None
        if not "value" in detail or not detail["value"]:
            return None

        content = detail["value"]
        media_type = detail.get("type", "text/plain")
        return cls._make_link_data(
            rel=Hyperlink.DESCRIPTION, media_type=media_type, content=content
        )

    @classmethod
    def _extract_links_fp(cls, entry_fp: dict[str, Any]) -> list[LinkData]:
        links = []
        summary_detail = entry_fp.get("summary_detail", None)
        link = cls._summary_to_linkdata(summary_detail)
        if link:
            links.append(link)

        for content_detail in entry_fp.get("content", []):
            link = cls._summary_to_linkdata(content_detail)
            if link:
                links.append(link)

        return links

    @classmethod
    def _extract_link(
        cls,
        link_tag: Element,
        feed_url: str,
        entry_rights_uri: str | None,
    ) -> LinkData | None:
        """Convert a <link> tag into a LinkData object.

        :param feed_url: The URL to the enclosing feed, for use in resolving
            relative links.

        :param entry_rights_uri: A URI describing the rights advertised
            in the entry. Unless this specific link says otherwise, we
            will assume that the representation on the other end of the link
            if made available on these terms.
        """
        attr = link_tag.attrib
        rel = attr.get("rel")
        media_type = attr.get("type")
        href = attr.get("href")
        if not href or not rel:
            # The link exists but has no destination, or no specified
            # relationship to the entry.
            return None
        rights = attr.get("{%s}rights" % OPDSXMLParser.NAMESPACES["dcterms"])
        rights_uri = entry_rights_uri
        if rights:
            # Rights associated with the link override rights
            # associated with the entry.
            rights_uri = cls._lookup_rights_uri(rights)

        href = urljoin(feed_url, href)
        return cls._make_link_data(rel, href, media_type, rights_uri)

    @classmethod
    def _consolidate_xml_links(cls, links: Sequence[LinkData]) -> list[LinkData]:
        """Match up image links with their corresponding thumbnails.

        Scans through a list of links to find image and thumbnail pairs:
        - If an image link is followed by a thumbnail link, they are paired
        - If a thumbnail link is followed by an image link, they are paired

        When a pair is found, the thumbnail is associated with the image, and
        the consolidated link is added to the result.
        """
        valid_links = links[:]
        result = []
        i = 0

        while i < len(valid_links):
            current_link = valid_links[i]

            # If not an image or thumbnail, add as-is and continue
            if current_link.rel not in (Hyperlink.THUMBNAIL_IMAGE, Hyperlink.IMAGE):
                result.append(current_link)
                i += 1
                continue

            # Check if we have a next link to potentially form a pair
            if i + 1 < len(valid_links):
                next_link = valid_links[i + 1]

                # Case 1: Current is thumbnail, next is image
                if (
                    current_link.rel == Hyperlink.THUMBNAIL_IMAGE
                    and next_link.rel == Hyperlink.IMAGE
                ):
                    result.append(next_link.set_thumbnail(current_link))
                    i += 2  # Skip both links as they're now handled
                    continue

                # Case 2: Current is image, next is thumbnail
                if (
                    current_link.rel == Hyperlink.IMAGE
                    and next_link.rel == Hyperlink.THUMBNAIL_IMAGE
                ):
                    result.append(current_link.set_thumbnail(next_link))
                    i += 2  # Skip both links as they're now handled
                    continue

            # If we're here, this link doesn't form a pair with the next link
            result.append(current_link)
            i += 1

        return result

    @classmethod
    def _extract_links_xml(
        cls,
        parser: OPDSXMLParser,
        entry_xml: Element,
        base_url: str,
        rights_uri: str | None,
    ) -> list[LinkData]:
        return cls._consolidate_xml_links(
            [
                link
                for link_tag in parser._xpath(entry_xml, "atom:link")
                if (link := cls._extract_link(link_tag, base_url, rights_uri))
                is not None
            ]
        )

    @classmethod
    def _extract_links(
        cls,
        parser: OPDSXMLParser,
        entry_fp: dict[str, Any],
        entry_xml: Element,
        base_url: str,
        rights_uri: str | None,
    ) -> list[LinkData]:
        links = cls._extract_links_fp(entry_fp)
        links.extend(cls._extract_links_xml(parser, entry_xml, base_url, rights_uri))
        return links

    @classmethod
    def _extract_rights_uri(cls, entry: dict[str, str]) -> str | None:
        """Extract a rights URI from a parsed feedparser entry.

        :return: A rights URI.
        """
        rights_uri = entry.get("rights_uri", None)
        if rights_uri is None:
            return None

        return cls._lookup_rights_uri(rights_uri)

    @classmethod
    def _extract_alternate_identifier(
        cls, identifier_tag: Element
    ) -> IdentifierData | None:
        """Turn a <dcterms:identifier> tag into an IdentifierData object."""
        try:
            if identifier_tag.text is None:
                return None
            return IdentifierData.parse_urn(identifier_tag.text.lower())
        except ValueError:
            return None

    @classmethod
    def _extract_alternate_identifiers(
        cls, parser: OPDSXMLParser, entry_xml: Element
    ) -> list[IdentifierData]:
        alternate_identifiers = []
        for id_tag in parser._xpath(entry_xml, "dcterms:identifier"):
            v = cls._extract_alternate_identifier(id_tag)
            if v:
                alternate_identifiers.append(v)
        return alternate_identifiers

    @classmethod
    def _extract_contributor(
        cls, parser: OPDSXMLParser, author_tag: Element
    ) -> ContributorData | None:
        """Turn an <atom:author> tag into a ContributorData object."""
        subtag = parser.text_of_optional_subtag
        sort_name = subtag(author_tag, "simplified:sort_name")
        display_name = subtag(author_tag, "atom:name")
        family_name = subtag(author_tag, "simplified:family_name")
        wikipedia_name = subtag(author_tag, "simplified:wikipedia_name")
        # TODO: we need a way of conveying roles. I believe Bibframe
        #   has the answer.

        # TODO: Also collect VIAF and LC numbers if present.  This
        #   requires parsing the URIs. Only the metadata wrangler will
        #   provide this information.

        if sort_name or display_name:
            return ContributorData(
                sort_name=sort_name,
                display_name=display_name,
                family_name=family_name,
                wikipedia_name=wikipedia_name,
            )

        cls.logger().info(
            "Refusing to create ContributorData for contributor with no sort name or display name."
        )
        return None

    @classmethod
    def _extract_contributors(
        cls, parser: OPDSXMLParser, entry_tag: Element
    ) -> list[ContributorData]:
        return [
            contributor
            for author_tag in parser._xpath(entry_tag, "atom:author")
            if (contributor := cls._extract_contributor(parser, author_tag)) is not None
        ]

    @classmethod
    def _extract_subject(cls, category_tag: Element) -> SubjectData:
        """Turn an <atom:category> tag into a SubjectData object."""
        attr = category_tag.attrib

        # Retrieve the type of this subject - FAST, Dewey Decimal,
        # etc.
        scheme = attr.get("scheme")
        subject_type = Subject.by_uri.get(scheme) if scheme else None
        if not subject_type:
            # We can't represent this subject because we don't
            # know its scheme. Just treat it as a tag.
            subject_type = Subject.TAG

        # Retrieve the term (e.g. "827") and human-readable name
        # (e.g. "English Satire & Humor") for this subject.
        term = attr.get("term")
        name = attr.get("label")
        default_weight = 1

        weight = attr.get("{http://schema.org/}ratingValue", default_weight)
        try:
            weight = int(weight)
        except ValueError:
            weight = default_weight

        return SubjectData(type=subject_type, identifier=term, name=name, weight=weight)

    @classmethod
    def _extract_subjects(
        cls, parser: OPDSXMLParser, entry_tag: Element
    ) -> list[SubjectData]:
        return [
            cls._extract_subject(category_tag)
            for category_tag in parser._xpath(entry_tag, "atom:category")
        ]

    @classmethod
    def _extract_measurement(cls, rating_tag: Element) -> MeasurementData | None:
        type = rating_tag.get("{http://schema.org/}additionalType")
        value = rating_tag.get("{http://schema.org/}ratingValue")
        if not value:
            value = rating_tag.attrib.get("{http://schema.org}ratingValue")
        if not type:
            type = Measurement.RATING

        if value is None:
            return None

        try:
            float_value = float(value)
            return MeasurementData(
                quantity_measured=type,
                value=float_value,
            )
        except ValueError:
            return None

    @classmethod
    def _extract_measurements(
        cls, parser: OPDSXMLParser, entry_tag: Element
    ) -> list[MeasurementData]:
        return [
            measurement
            for measurement_tag in parser._xpath(entry_tag, "schema:Rating")
            if (measurement := cls._extract_measurement(measurement_tag)) is not None
        ]

    @classmethod
    def _derive_medium_from_links(cls, links: list[LinkData]) -> str | None:
        """Get medium if derivable from information in an acquisition link."""
        derived = None
        for link in links:
            if (
                not link.rel
                or not link.media_type
                or not link.rel.startswith("http://opds-spec.org/acquisition/")
            ):
                continue
            derived = Edition.medium_from_media_type(link.media_type)
            if derived:
                break
        return derived

    @classmethod
    def _extract_medium(cls, entry_tag: Element, default: str | None) -> str | None:
        """Derive a value for Edition.medium from schema:additionalType or
        from a <dcterms:format> subtag.

        :param entry_tag: A <atom:entry> tag.
        :param default: The value to use if nothing is found.
        """
        medium = None
        additional_type = entry_tag.get("{http://schema.org/}additionalType")
        if additional_type:
            medium = Edition.additional_type_to_medium.get(additional_type, None)
        if not medium:
            format_tag = entry_tag.find("{http://purl.org/dc/terms/}format")
            if format_tag is not None:
                media_type = format_tag.text
                medium = Edition.medium_from_media_type(media_type)
        return medium or default

    @classmethod
    def _extract_series(
        cls, parser: OPDSXMLParser, entry_tag: Element
    ) -> tuple[str | None, str | None]:
        series_tag = parser._xpath1(entry_tag, "schema:Series")
        if series_tag is None:
            return None, None
        attr = series_tag.attrib
        series_name = attr.get("{http://schema.org/}name", None)
        series_position = attr.get("{http://schema.org/}position", None)
        return series_name, series_position

    @staticmethod
    def _extract_published(parser: OPDSXMLParser, entry_tag: Element) -> date | None:
        issued_tag = parser._xpath1(entry_tag, "dcterms:issued")
        if issued_tag is None:
            return None
        date_string = issued_tag.text
        # By default, the date for strings that only have a year will
        # be set to January 1 rather than the current date.
        default = datetime_utc(utc_now().year, 1, 1)
        try:
            return dateutil.parser.parse(date_string, default=default).date()
        except:
            return None

    @classmethod
    def _extract_time_tracking(cls, parser: OPDSXMLParser, entry_tag: Element) -> bool:
        should_track_playtime = False
        time_tracking_tag = parser._xpath1(entry_tag, "palace:timeTracking")
        if time_tracking_tag is not None and time_tracking_tag.text:
            should_track_playtime = time_tracking_tag.text.lower() == "true"
        return should_track_playtime

    @classmethod
    def _extract_fp_identifier(cls, publication: OPDS1Publication) -> str | None:
        fp_id = publication.entry_fp.get("id")
        if not fp_id:
            return None
        return fp_id  # type: ignore[no-any-return]

    @classmethod
    def _extract_xml_identifier(cls, publication: OPDS1Publication) -> str | None:
        identifier_xml_tag = publication.parser._xpath1(
            publication.entry_xml, "atom:id"
        )
        if identifier_xml_tag is None or not identifier_xml_tag.text:
            return None

        return identifier_xml_tag.text  # type: ignore[no-any-return]

    @classmethod
    def _publication_xml_string(cls, publication: OPDS1Publication) -> str:
        return etree.tostring(publication.entry_xml, encoding=str, pretty_print=True)  # type: ignore[no-any-return]

    @classmethod
    def _extract_identifier(cls, publication: OPDS1Publication) -> IdentifierData:
        identifier_fp = cls._extract_fp_identifier(publication)
        identifier_xml = cls._extract_xml_identifier(publication)
        if identifier_xml is None or identifier_fp is None:
            raise PalaceValueError(
                f"Tried to extract bibliographic data from an entry without an ID. "
                f"'{cls._publication_xml_string(publication)}'"
            )

        if identifier_fp != identifier_xml:
            raise PalaceValueError(
                f"Mismatch between Feedparser '{identifier_fp}' and ElementTree '{identifier_xml}' ID."
            )
        return IdentifierData.parse_urn(identifier_fp)

    def feed_parse(self, feed: bytes) -> OPDS1Feed:
        parser = self._xml_parser
        feed_parser = feedparser.parse(feed)
        if feed_parser.bozo:
            raise PalaceValueError(
                f"Failed to parse OPDS 1.x feed: {feed_parser.bozo_exception}"
            ) from feed_parser.bozo_exception
        try:
            etree_root = etree.fromstring(feed)
        except etree.Error as e:
            raise PalaceValueError(f"Failed to parse OPDS 1.x feed XML: {e}") from e
        return OPDS1Feed(
            parser=parser,
            feed_parser=feed_parser,
            etree=etree_root,
        )

    @classmethod
    def feed_next_url(cls, feed: OPDS1Feed) -> str | None:
        parsed_feed = feed.feed_parser["feed"]
        if not parsed_feed or "links" not in parsed_feed:
            return None
        return first_or_default(
            [link["href"] for link in parsed_feed["links"] if link["rel"] == "next"]
        )

    def feed_publications(self, feed: OPDS1Feed) -> Generator[OPDS1Publication]:
        for entry_fp, entry_xml in zip(
            self._extract_feedparser_entries(feed),
            self._extract_elementtree_entries(feed),
        ):
            yield OPDS1Publication(
                parser=feed.parser,
                entry_fp=entry_fp,
                entry_xml=entry_xml,
            )

    @classmethod
    def publication_licenses(cls, publication: OPDS1Publication) -> list[License]:
        """
        Right now we don't support OPDS1 + ODL, so this will always return an empty list.
        """
        return []

    @classmethod
    def publication_available(cls, publication: OPDS1Publication) -> bool:
        """
        The availability draft only specifies this for OPDS 2, so for now we default
        availability to True for OPDS 1.
        TODO: We might want to implement this for OPDS 1.
        """
        return True

    def publication_identifier(self, publication: OPDS1Publication) -> IdentifierData:
        identifier = self._extract_identifier(publication)
        if self._primary_identifier_source == IdentifierSource.DCTERMS_IDENTIFIER:
            identifiers = self._extract_alternate_identifiers(
                publication.parser, publication.entry_xml
            )
            if identifiers:
                identifier = identifiers[0]

        return identifier

    @classmethod
    def failure_from_publication(
        cls, publication: OPDS1Publication, error: Exception, error_message: str
    ) -> FailedPublication:
        """Create a FailedPublication object from the given publication."""
        return FailedPublication(
            error=error,
            error_message=error_message,
            identifier=publication.entry_fp.get("id"),
            title=cls._extract_title(publication.entry_fp),
            publication_data=cls._publication_xml_string(publication),
        )

    def publication_bibliographic(
        self,
        identifier: IdentifierData,
        publication: OPDS1Publication,
        license_info_documents: dict[str, LicenseInfo] | None = None,
    ) -> BibliographicData:
        title = self._extract_title(publication.entry_fp)
        subtitle = self._extract_subtitle(publication.entry_fp)
        circulation_data_source = self._extract_circulation_data_source(
            publication.entry_fp, self._data_source
        )
        last_opds_update = self._extract_last_update_date(publication.entry_fp)
        publisher = self._extract_publisher(publication.entry_fp)
        language = self._extract_language(publication.entry_fp)
        rights_uri = self._extract_rights_uri(publication.entry_fp)
        links = self._extract_links(
            publication.parser,
            publication.entry_fp,
            publication.entry_xml,
            self._base_url,
            rights_uri,
        )
        identifiers = self._extract_alternate_identifiers(
            publication.parser, publication.entry_xml
        )
        contributors = self._extract_contributors(
            publication.parser, publication.entry_xml
        )
        subjects = self._extract_subjects(publication.parser, publication.entry_xml)
        measurments = self._extract_measurements(
            publication.parser, publication.entry_xml
        )
        medium = self._extract_medium(
            publication.entry_xml, self._derive_medium_from_links(links)
        )
        series, series_position = self._extract_series(
            publication.parser, publication.entry_xml
        )
        published = self._extract_published(publication.parser, publication.entry_xml)
        time_tracking = self._extract_time_tracking(
            publication.parser, publication.entry_xml
        )

        if self._primary_identifier_source == IdentifierSource.DCTERMS_IDENTIFIER:
            identifiers.append(self._extract_identifier(publication))

        if time_tracking and medium != Edition.AUDIO_MEDIUM:
            time_tracking = False
            self.log.warning(f"Ignoring the time tracking flag for entry {identifier}")

        if self._opds_for_distributors:
            # If we are parsing an OPDS for Distributors feed, we need to add some extra
            # format data to handle its Bearer Token DRM type.
            formats = [
                format_data
                for link in links
                if (format_data := self._bearer_token_format_data(link))
            ]
        else:
            formats = []

        circulation = CirculationData(
            data_source_name=circulation_data_source,
            links=links,
            default_rights_uri=rights_uri,
            primary_identifier_data=identifier,
            should_track_playtime=time_tracking,
            formats=formats,
            type=LicensePoolType.UNLIMITED,
            licenses_owned=LicensePool.UNLIMITED_ACCESS,
            licenses_available=LicensePool.UNLIMITED_ACCESS,
        )

        bibliographic = BibliographicData(
            title=title,
            subtitle=subtitle,
            language=language,
            publisher=publisher,
            links=links,
            # refers to when was updated in opds feed, not our db
            data_source_last_updated=last_opds_update,
            data_source_name=self._data_source,
            primary_identifier_data=identifier,
            identifiers=identifiers,
            contributors=contributors,
            subjects=subjects,
            measurements=measurments,
            medium=medium,
            series=series,
            series_position=series_position,
            published=published,
        )

        if circulation.formats:
            bibliographic.circulation = circulation
        else:
            # If the CirculationData has no formats, it
            # doesn't really offer any way to actually get the
            # book, and we don't want to create a
            # LicensePool. All the circulation data is
            # useless.
            #
            # TODO: This will need to be revisited when we add
            # ODL support.
            pass

        return bibliographic
