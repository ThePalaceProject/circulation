from __future__ import annotations

from functools import partial
from typing import Any, Dict, List, Optional, cast

from lxml import etree

from core.feed_protocol.types import (
    Acquisition,
    Author,
    DataEntry,
    FeedData,
    FeedEntryType,
    IndirectAcquisition,
    WorkEntryData,
)
from core.util.opds_writer import OPDSFeed, OPDSMessage

TAG_MAPPING = {
    "indirectAcquisition": f"{{{OPDSFeed.OPDS_NS}}}indirectAcquisition",
    "holds": f"{{{OPDSFeed.OPDS_NS}}}holds",
    "copies": f"{{{OPDSFeed.OPDS_NS}}}copies",
    "availability": f"{{{OPDSFeed.OPDS_NS}}}availability",
    "licensor": f"{{{OPDSFeed.DRM_NS}}}licensor",
    "patron": f"{{{OPDSFeed.SIMPLIFIED_NS}}}patron",
    "series": f"{{{OPDSFeed.SCHEMA_NS}}}series",
}

ATTRIBUTE_MAPPING = {
    "vendor": f"{{{OPDSFeed.DRM_NS}}}vendor",
    "scheme": f"{{{OPDSFeed.DRM_NS}}}scheme",
    "username": f"{{{OPDSFeed.SIMPLIFIED_NS}}}username",
    "authorizationIdentifier": f"{{{OPDSFeed.SIMPLIFIED_NS}}}authorizationIdentifier",
    "rights": f"{{{OPDSFeed.DCTERMS_NS}}}rights",
    "ProviderName": f"{{{OPDSFeed.BIBFRAME_NS}}}ProviderName",
    "facetGroup": f"{{{OPDSFeed.OPDS_NS}}}facetGroup",
    "activeFacet": f"{{{OPDSFeed.OPDS_NS}}}activeFacet",
    "ratingValue": f"{{{OPDSFeed.SCHEMA_NS}}}ratingValue",
}

AUTHOR_MAPPING = {
    "name": f"{{{OPDSFeed.ATOM_NS}}}name",
    "role": f"{{{OPDSFeed.OPF_NS}}}role",
    "sort_name": f"{{{OPDSFeed.SIMPLIFIED_NS}}}sort_name",
    "wikipedia_name": f"{{{OPDSFeed.SIMPLIFIED_NS}}}wikipedia_name",
}


class OPDS1Serializer(OPDSFeed):
    """An OPDS 1.2 Atom feed serializer"""

    def __init__(self) -> None:
        pass

    def _tag(
        self, tag_name: str, *args: Any, mapping: Optional[Dict[str, str]] = None
    ) -> etree._Element:
        if not mapping:
            mapping = TAG_MAPPING
        return self.E(mapping.get(tag_name, tag_name), *args)

    def _attr_name(
        self, attr_name: str, mapping: Optional[Dict[str, str]] = None
    ) -> str:
        if not mapping:
            mapping = ATTRIBUTE_MAPPING
        return mapping.get(attr_name, attr_name)

    def serialize_feed(
        self, feed: FeedData, precomposed_entries: Optional[List[OPDSMessage]] = None
    ) -> bytes:
        # First we do metadata
        serialized = self.E.feed()

        if feed.entrypoint:
            serialized.set(f"{{{OPDSFeed.SIMPLIFIED_NS}}}entrypoint", feed.entrypoint)

        for name, metadata in feed.metadata.items():
            element = self._serialize_feed_entry(name, metadata)
            serialized.append(element)

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
                    serialized.append(precomposed.tag)

        for link in feed.links:
            serialized.append(self._serialize_feed_entry("link", link))

        if feed.breadcrumbs:
            breadcrumbs = OPDSFeed.E._makeelement(
                f"{{{OPDSFeed.SIMPLIFIED_NS}}}breadcrumbs"
            )
            for link in feed.breadcrumbs:
                breadcrumbs.append(self._serialize_feed_entry("link", link))
            serialized.append(breadcrumbs)

        for link in feed.facet_links:
            serialized.append(self._serialize_feed_entry("link", link))

        # TODO: REMOVE DEBUG INDENT
        etree.indent(serialized)
        return self.to_string(serialized)

    def serialize_work_entry(self, feed_entry: WorkEntryData) -> etree._Element:
        entry: etree._Element = OPDSFeed.entry()

        if feed_entry.additionalType:
            entry.set(
                f"{{{OPDSFeed.SCHEMA_NS}}}additionalType", feed_entry.additionalType
            )

        if feed_entry.title:
            entry.append(OPDSFeed.E("title", feed_entry.title.text))
        if feed_entry.subtitle:
            entry.append(
                OPDSFeed.E(
                    f"{{{OPDSFeed.SCHEMA_NS}}}alternativeHeadline",
                    feed_entry.subtitle.text,
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
            entry.append(
                OPDSFeed.E(f"{{{OPDSFeed.DCTERMS_NS}}}issued", feed_entry.issued.text)
            )
        if feed_entry.identifier:
            entry.append(OPDSFeed.E("id", feed_entry.identifier))
        if feed_entry.distribution and (
            provider := getattr(feed_entry.distribution, "ProviderName", None)
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
            entry.append(self._serialize_author_tag("author", author))
        for contributor in feed_entry.contributors:
            entry.append(self._serialize_author_tag("contributor", contributor))

        for link in feed_entry.image_links:
            entry.append(OPDSFeed.link(**link.dict()))

        for link in feed_entry.acquisition_links:
            element = self._serialize_acquistion_link(link)
            entry.append(element)

        for link in feed_entry.other_links:
            entry.append(OPDSFeed.link(**link.dict()))

        return entry

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
                    entry.set(
                        ATTRIBUTE_MAPPING.get(attrib, attrib),
                        value if value is not None else "",
                    )
        return entry

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

    def _serialize_acquistion_link(self, link: Acquisition) -> etree._Element:
        element = OPDSFeed.link(**link.link_attribs())

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
    def to_string(cls, element: etree._Element) -> bytes:
        return cast(bytes, etree.tostring(element))
