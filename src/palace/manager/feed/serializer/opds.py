from __future__ import annotations

import abc
import datetime
from functools import partial
from typing import Any, cast

from lxml import etree

from palace.manager.feed.facets.constants import FacetConstants
from palace.manager.feed.serializer.base import SerializerInterface
from palace.manager.feed.types import (
    Acquisition,
    Author,
    DataEntry,
    FeedData,
    FeedEntryType,
    FeedMetadata,
    IndirectAcquisition,
    Link,
    WorkEntryData,
)
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.opds_writer import AtomFeed, OPDSFeed, OPDSMessage

TAG_MAPPING = {
    "indirectAcquisition": f"{{{OPDSFeed.OPDS_NS}}}indirectAcquisition",
    "holds": f"{{{OPDSFeed.OPDS_NS}}}holds",
    "copies": f"{{{OPDSFeed.OPDS_NS}}}copies",
    "availability": f"{{{OPDSFeed.OPDS_NS}}}availability",
    "licensor": f"{{{OPDSFeed.DRM_NS}}}licensor",
    "patron": f"{{{OPDSFeed.SIMPLIFIED_NS}}}patron",
    "series": f"{{{OPDSFeed.SCHEMA_NS}}}series",
    "hashed_passphrase": f"{{{OPDSFeed.LCP_NS}}}hashed_passphrase",
}

V1_ATTRIBUTE_MAPPING = {
    "vendor": f"{{{OPDSFeed.DRM_NS}}}vendor",
    "scheme": f"{{{OPDSFeed.DRM_NS}}}scheme",
    "username": f"{{{OPDSFeed.SIMPLIFIED_NS}}}username",
    "authorizationIdentifier": f"{{{OPDSFeed.SIMPLIFIED_NS}}}authorizationIdentifier",
    "rights": f"{{{OPDSFeed.DCTERMS_NS}}}rights",
    "ProviderName": f"{{{OPDSFeed.BIBFRAME_NS}}}ProviderName",
    "facetGroup": f"{{{OPDSFeed.OPDS_NS}}}facetGroup",
    "facetGroupType": f"{{{OPDSFeed.SIMPLIFIED_NS}}}facetGroupType",
    "activeFacet": f"{{{OPDSFeed.OPDS_NS}}}activeFacet",
    "ratingValue": f"{{{OPDSFeed.SCHEMA_NS}}}ratingValue",
}

V2_ATTRIBUTE_MAPPING = {
    **V1_ATTRIBUTE_MAPPING,
    "defaultFacet": f"{{{OPDSFeed.PALACE_PROPS_NS}}}default",
    "activeSort": f"{{{OPDSFeed.PALACE_PROPS_NS}}}active-sort",
}

AUTHOR_MAPPING = {
    "name": f"{{{OPDSFeed.ATOM_NS}}}name",
    "role": f"{{{OPDSFeed.OPF_NS}}}role",
    "sort_name": f"{{{OPDSFeed.SIMPLIFIED_NS}}}sort_name",
    "wikipedia_name": f"{{{OPDSFeed.SIMPLIFIED_NS}}}wikipedia_name",
}


def is_sort_facet(link: Link) -> bool:
    """A until method that determines if the specified link is part of a sort facet"""
    return (
        hasattr(link, "facetGroup")
        and link.facetGroup
        == FacetConstants.GROUP_DISPLAY_TITLES[FacetConstants.ORDER_FACET_GROUP_NAME]
    )


class BaseOPDS1Serializer(SerializerInterface[etree._Element], OPDSFeed, abc.ABC):
    def __init__(self) -> None:
        pass

    def _tag(
        self, tag_name: str, *args: Any, mapping: dict[str, str] | None = None
    ) -> etree._Element:
        if not mapping:
            mapping = TAG_MAPPING
        return self.E(mapping.get(tag_name, tag_name), *args)

    def _attr_name(self, attr_name: str, mapping: dict[str, str] | None = None) -> str:
        if not mapping:
            mapping = self._get_attribute_mapping()
        return mapping.get(attr_name, attr_name)

    def serialize_feed(
        self, feed: FeedData, precomposed_entries: list[OPDSMessage] | None = None
    ) -> str:
        # First we do metadata
        serialized = self.E.feed()

        if feed.entrypoint:
            serialized.set(f"{{{OPDSFeed.SIMPLIFIED_NS}}}entrypoint", feed.entrypoint)

        serialized.extend(self._serialize_feed_metadata(feed.metadata))

        for entry in feed.entries:
            if entry.computed:
                element = self.serialize_work_entry(entry.computed)
                serialized.append(element)

        for data_entry in feed.data_entries:
            element = self._serialize_data_entry(data_entry)
            serialized.append(element)

        if precomposed_entries:
            for precomposed in precomposed_entries:
                if isinstance(precomposed, OPDSMessage):
                    serialized.append(self.serialize_opds_message(precomposed))

        for link in feed.links:
            serialized.append(self._serialize_feed_entry("link", link))

        if feed.breadcrumbs:
            breadcrumbs = OPDSFeed.E._makeelement(
                f"{{{OPDSFeed.SIMPLIFIED_NS}}}breadcrumbs"
            )
            for link in feed.breadcrumbs:
                breadcrumbs.append(self._serialize_feed_entry("link", link))
            serialized.append(breadcrumbs)

        for link in self._serialize_facet_links(feed):
            serialized.append(link)

        for link in self._serialize_sort_links(feed):
            serialized.append(link)

        etree.indent(serialized)
        return self.to_string(serialized)

    def _serialize_feed_metadata(self, metadata: FeedMetadata) -> list[etree._Element]:
        tags = []
        # Compulsory title
        tags.append(self._tag("title", metadata.title or ""))

        if metadata.id:
            tags.append(self._tag("id", metadata.id))
        if metadata.updated:
            tags.append(self._tag("updated", metadata.updated))
        if metadata.patron:
            tags.append(self._serialize_feed_entry("patron", metadata.patron))
        if metadata.drm_licensor:
            tags.append(self._serialize_feed_entry("licensor", metadata.drm_licensor))
        if metadata.lcp_hashed_passphrase:
            tags.append(
                self._serialize_feed_entry(
                    "hashed_passphrase", metadata.lcp_hashed_passphrase
                )
            )

        return tags

    def serialize_work_entry(self, feed_entry: WorkEntryData) -> etree._Element:
        entry: etree._Element = OPDSFeed.entry()

        if feed_entry.additionalType:
            entry.set(
                f"{{{OPDSFeed.SCHEMA_NS}}}additionalType", feed_entry.additionalType
            )

        if feed_entry.title:
            entry.append(OPDSFeed.E("title", feed_entry.title.text))

        if feed_entry.subtitle and feed_entry.subtitle.text:
            entry.append(
                OPDSFeed.E(
                    f"{{{OPDSFeed.SCHEMA_NS}}}alternativeHeadline",
                    feed_entry.subtitle.text,
                )
            )
        if feed_entry.duration is not None:
            entry.append(
                OPDSFeed.E(
                    f"{{{OPDSFeed.DCTERMS_NS}}}duration", str(feed_entry.duration)
                )
            )
        if feed_entry.summary:
            entry.append(OPDSFeed.E("summary", feed_entry.summary.text))
        if feed_entry.pwid:
            entry.append(
                OPDSFeed.E(f"{{{OPDSFeed.SIMPLIFIED_NS}}}pwid", feed_entry.pwid)
            )

        if feed_entry.language:
            entry.append(
                OPDSFeed.E(
                    f"{{{OPDSFeed.DCTERMS_NS}}}language", feed_entry.language.text
                )
            )
        if feed_entry.publisher:
            entry.append(
                OPDSFeed.E(
                    f"{{{OPDSFeed.DCTERMS_NS}}}publisher", feed_entry.publisher.text
                )
            )
        if feed_entry.imprint:
            entry.append(
                OPDSFeed.E(
                    f"{{{OPDSFeed.BIB_SCHEMA_NS}}}publisherImprint",
                    feed_entry.imprint.text,
                )
            )
        if feed_entry.issued:
            # Entry.issued is the date the ebook came out, as distinct
            # from Entry.published (which may refer to the print edition
            # or some original edition way back when).
            #
            # For Dublin Core 'issued' we use Entry.issued if we have it
            # and Entry.published if not. In general this means we use
            # issued date for Gutenberg and published date for other
            # sources.
            #
            # For the date the book was added to our collection we use
            # atom:published.
            #
            # Note: feedparser conflates dc:issued and atom:published, so
            # it can't be used to extract this information. However, these
            # tags are consistent with the OPDS spec.
            issued = feed_entry.issued
            if isinstance(issued, datetime.datetime) or isinstance(
                issued, datetime.date
            ):
                now = utc_now()
                today = datetime.date.today()
                issued_already = False
                if isinstance(issued, datetime.datetime):
                    issued_already = issued <= now
                elif isinstance(issued, datetime.date):
                    issued_already = issued <= today
                if issued_already:
                    entry.append(
                        OPDSFeed.E(
                            f"{{{OPDSFeed.DCTERMS_NS}}}issued",
                            issued.isoformat().split("T")[0],
                        )
                    )

        if feed_entry.identifier:
            entry.append(OPDSFeed.E("id", feed_entry.identifier))
        if feed_entry.distribution and (
            provider := getattr(feed_entry.distribution, "provider_name", None)
        ):
            entry.append(
                OPDSFeed.E(
                    f"{{{OPDSFeed.BIBFRAME_NS}}}distribution",
                    **{f"{{{OPDSFeed.BIBFRAME_NS}}}ProviderName": provider},
                )
            )
        if feed_entry.published:
            entry.append(OPDSFeed.E("published", feed_entry.published.text))
        if feed_entry.updated:
            entry.append(OPDSFeed.E("updated", feed_entry.updated.text))

        if feed_entry.series:
            entry.append(self._serialize_series_entry(feed_entry.series))

        for category in feed_entry.categories:
            element = OPDSFeed.category(
                scheme=category.scheme, term=category.term, label=category.label  # type: ignore[attr-defined]
            )
            entry.append(element)

        for rating in feed_entry.ratings:
            rating_tag = self._serialize_feed_entry("Rating", rating)
            entry.append(rating_tag)

        for author in feed_entry.authors:
            # Author must at a minimum have a name
            if author.name:
                entry.append(self._serialize_author_tag("author", author))
        for contributor in feed_entry.contributors:
            entry.append(self._serialize_author_tag("contributor", contributor))

        for link in feed_entry.image_links:
            entry.append(OPDSFeed.link(**link.asdict()))

        for link in feed_entry.acquisition_links:
            element = self._serialize_acquisition_link(link)
            entry.append(element)

        for link in feed_entry.other_links:
            entry.append(OPDSFeed.link(**link.asdict()))

        return entry

    def serialize_opds_message(self, entry: OPDSMessage) -> etree._Element:
        return entry.tag

    def _serialize_series_entry(self, series: FeedEntryType) -> etree._Element:
        entry = self._tag("series")
        if name := getattr(series, "name", None):
            entry.set("name", name)
        if position := getattr(series, "position", None):
            entry.append(self._tag("position", position))
        if link := getattr(series, "link", None):
            entry.append(self._serialize_feed_entry("link", link))

        return entry

    def _serialize_feed_entry(
        self, tag: str, feed_entry: FeedEntryType
    ) -> etree._Element:
        """Serialize a feed entry type in a recursive and blind manner"""
        entry: etree._Element = self._tag(tag)
        for attrib, value in feed_entry:
            if value is None:
                continue
            if isinstance(value, list):
                for item in value:
                    entry.append(self._serialize_feed_entry(attrib, item))
            elif isinstance(value, FeedEntryType):
                entry.append(self._serialize_feed_entry(attrib, value))
            else:
                if attrib == "text":
                    entry.text = value
                else:
                    attribute_mapping = self._get_attribute_mapping()
                    entry.set(
                        attribute_mapping.get(attrib, attrib),
                        value if value is not None else "",
                    )
        return entry

    @abc.abstractmethod
    def _get_attribute_mapping(self) -> dict[str, str]:
        """This method should return a mapping of object attributes found on links and objects in the FeedData
        to the related attribute names defined in the OPDS specification.
        """

    def _serialize_author_tag(self, tag: str, author: Author) -> etree._Element:
        entry: etree._Element = self._tag(tag)
        attr = partial(self._attr_name, mapping=AUTHOR_MAPPING)
        _tag = partial(self._tag, mapping=AUTHOR_MAPPING)
        if author.name:
            element = _tag("name")
            element.text = author.name
            entry.append(element)
        if author.role:
            entry.set(attr("role"), author.role)
        if author.link:
            entry.append(self._serialize_feed_entry("link", author.link))

        # Verbose
        if author.sort_name:
            entry.append(_tag("sort_name", author.sort_name))
        if author.wikipedia_name:
            entry.append(_tag("wikipedia_name", author.wikipedia_name))
        if author.viaf:
            entry.append(_tag("sameas", author.viaf))
        if author.lc:
            entry.append(_tag("sameas", author.lc))
        return entry

    def _serialize_acquisition_link(self, link: Acquisition) -> etree._Element:

        link_func = OPDSFeed.tlink if link.templated else OPDSFeed.link
        element = link_func(**link.link_attribs())

        def _indirect(item: IndirectAcquisition) -> etree._Element:
            tag = self._tag("indirectAcquisition")
            tag.set("type", item.type)
            for child in item.children:
                tag.append(_indirect(child))
            return tag

        for indirect in link.indirect_acquisitions:
            element.append(_indirect(indirect))

        if link.availability_status:
            avail_tag = self._tag("availability")
            avail_tag.set("status", link.availability_status)
            if link.availability_since:
                avail_tag.set(self._attr_name("since"), link.availability_since)
            if link.availability_until:
                avail_tag.set(self._attr_name("until"), link.availability_until)
            element.append(avail_tag)

        if link.holds_total is not None:
            holds_tag = self._tag("holds")
            holds_tag.set(self._attr_name("total"), link.holds_total)
            if link.holds_position:
                holds_tag.set(self._attr_name("position"), link.holds_position)
            element.append(holds_tag)

        if link.copies_total is not None:
            copies_tag = self._tag("copies")
            copies_tag.set(self._attr_name("total"), link.copies_total)
            if link.copies_available:
                copies_tag.set(self._attr_name("available"), link.copies_available)
            element.append(copies_tag)

        if link.lcp_hashed_passphrase:
            element.append(
                self._tag("hashed_passphrase", link.lcp_hashed_passphrase.text)
            )

        if link.drm_licensor:
            element.append(self._serialize_feed_entry("licensor", link.drm_licensor))

        return element

    def _serialize_data_entry(self, entry: DataEntry) -> etree._Element:
        element = self._tag("entry")
        if entry.title:
            element.append(self._tag("title", entry.title))
        if entry.id:
            element.append(self._tag("id", entry.id))
        for link in entry.links:
            link_ele = self._serialize_feed_entry("link", link)
            element.append(link_ele)
        return element

    @classmethod
    def to_string(cls, element: etree._Element) -> str:
        return cast(str, etree.tostring(element, encoding="unicode"))

    @abc.abstractmethod
    def content_type(self) -> str:
        """return the content type associated with the serialization. This value should include the api-version."""

    @abc.abstractmethod
    def _serialize_facet_links(self, feed: FeedData) -> list[Link]:
        """This method implements serialization of the facet_links from the feed data."""

    @abc.abstractmethod
    def _serialize_sort_links(self, feed: FeedData) -> list[Link]:
        """This method implements serialization of the sort links from the feed data."""


class OPDS1Version1Serializer(BaseOPDS1Serializer):
    """An OPDS 1.2 Atom feed serializer.  This version of the feed implements sort links as
    facets rather than using the http://palaceproject.io/terms/rel/sort rel and does not  support
    the http://palaceproject.io/terms/properties/default property indicating default facets
    """

    def _serialize_facet_links(self, feed: FeedData) -> list[Link]:
        links = []
        if feed.facet_links:
            for link in feed.facet_links:
                links.append(self._serialize_feed_entry("link", link))
        return links

    def _serialize_sort_links(self, feed: FeedData) -> list[Link]:
        # Since this version of the serializer implements sort links as facets,
        # we return an empty list of sort links.
        return []

    def _get_attribute_mapping(self) -> dict[str, str]:
        return V1_ATTRIBUTE_MAPPING

    def content_type(self) -> str:
        return OPDSFeed.ACQUISITION_FEED_TYPE + "; api-version=1"


class OPDS1Version2Serializer(BaseOPDS1Serializer):
    """An OPDS 1.2 Atom feed serializer with Palace specific modifications (version 2) to support
    new IOS and Android client features. Namely, this version of the feed implements sort links as
    links using the http://palaceproject.io/terms/rel/sort rel.  The active or selected sort link is indicated
    by the http://palaceproject.io/terms/properties/active-sort property.  Default facets and sort links are
    inidcated by the http://palaceproject.io/terms/properties/default property.
    """

    def _serialize_facet_links(self, feed: FeedData) -> list[Link]:
        # serializes the non-sort related facets.
        links: list[Link] = []
        facet_links = feed.facet_links
        if facet_links:
            for link in facet_links:
                # serialize all but the sort facets.
                if not is_sort_facet(link):
                    links.append(self._serialize_feed_entry("link", link))
        return links

    def _serialize_sort_links(self, feed: FeedData) -> list[Link]:
        # this version of the feed filters out the sort facets and
        # serializes them in a way that makes use of palace extensions.
        links: list[Link] = []
        facet_links = feed.facet_links
        if facet_links:
            for link in feed.facet_links:
                # select only the sort facets for serialization

                if is_sort_facet(link):
                    links.append(self._serialize_sort_link(link))
        return links

    def _serialize_sort_link(self, link: Link) -> etree._Element:
        sort_link = Link(
            href=link.href, title=link.title, rel=AtomFeed.PALACE_REL_NS + "sort"
        )
        attributes: dict[str, Any] = dict()
        if link.get("activeFacet", False):
            attributes.update(dict(activeSort="true"))
        if link.get("defaultFacet", False):
            attributes.update(dict(defaultFacet="true"))
        sort_link.add_attributes(attributes)

        return self._serialize_feed_entry("link", sort_link)

    def _get_attribute_mapping(self) -> dict[str, str]:
        return V2_ATTRIBUTE_MAPPING

    def content_type(self) -> str:
        return OPDSFeed.ACQUISITION_FEED_TYPE + "; api-version=2"
