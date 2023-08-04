from __future__ import annotations

from lxml import etree

from core.feed_protocol.types import FeedData, FeedEntryType, Link, WorkEntryData
from core.util.opds_writer import OPDSFeed

TAG_MAPPING = {
    "indirectAcquisition": f"{{{OPDSFeed.OPDS_NS}}}indirectAcquisition",
    "hold": f"{{{OPDSFeed.OPDS_NS}}}hold",
    "copies": f"{{{OPDSFeed.OPDS_NS}}}copies",
    "availability": f"{{{OPDSFeed.OPDS_NS}}}availability",
}


class OPDS1Serializer(OPDSFeed):
    """An OPDS 1.2 Atom feed serializer"""

    def __init__(self):
        pass

    def serialize_feed(self, feed: FeedData):
        # First we do metadata
        serialized = self.E.feed()
        for name, metadata in feed.metadata.items():
            element = self._serialize_feed_entry(name, metadata)
            serialized.append(element)

        for entry in feed.entries:
            element = self._serialize_work_entry(entry.computed)
            serialized.append(element)

        for link in feed.links:
            serialized.append(self._serialize_feed_entry("link", link))

        # TODO: REMOVE DEBUG INDENT
        etree.indent(serialized)
        return etree.tostring(serialized)

    def _serialize_work_entry(self, feed_entry: WorkEntryData) -> etree.Element:
        entry: etree._Element = OPDSFeed.entry()

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
                    f"{{{OPDSFeed.BIBFRAME_NS}}}publisherImprint",
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
                    f"{{{OPDSFeed.BIBFRAME_NS}}}distribution", ProviderName=provider
                )
            )
        if feed_entry.published:
            entry.append(OPDSFeed.E("published", feed_entry.published.text))
        if feed_entry.updated:
            entry.append(OPDSFeed.E("updated", feed_entry.updated.text))

        for category in feed_entry.categories:
            element = OPDSFeed.category(
                scheme=category.scheme, term=category.term, label=category.label
            )
            entry.append(element)

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

    def _serialize_feed_entry(self, tag: str, feed_entry: FeedEntryType):
        """Serialize a feed entry type in a recursive and blind manner"""
        entry: etree._Element = OPDSFeed.E(TAG_MAPPING.get(tag, tag))
        for attrib, value in feed_entry:
            if isinstance(value, list):
                for item in value:
                    entry.append(self._serialize_feed_entry(attrib, item))
            elif isinstance(value, FeedEntryType):
                entry.append(self._serialize_feed_entry(attrib, value))
            else:
                if attrib == "text":
                    entry.text = value
                else:
                    entry.set(attrib, value if value is not None else "")
        return entry

    def _serialize_author_tag(self, tag: str, feed_entry: FeedEntryType):
        entry: etree._Element = OPDSFeed.E(TAG_MAPPING.get(tag, tag))
        name = getattr(feed_entry, "name", None)
        if name:
            element = OPDSFeed.E(f"{{{OPDSFeed.ATOM_NS}}}name")
            element.text = name
            entry.append(element)
        if role := getattr(feed_entry, "role", None):
            entry.set(f"{{{OPDSFeed.OPF_NS}}}role", role)
        return entry

    def _serialize_acquistion_link(self, link: Link) -> etree._Element:
        element = OPDSFeed.link(**link.dict())
        if indirects := getattr(link, "indirectAcquisition", None):
            for indirect in indirects:
                child = self._serialize_feed_entry("indirectAcquisition", indirect)
                element.append(child)

        if holds := getattr(link, "holds", None):
            element.append(self._serialize_feed_entry("holds", holds))
        if copies := getattr(link, "copies", None):
            element.append(self._serialize_feed_entry("copies", copies))
        if availability := getattr(link, "availability", None):
            element.append(self._serialize_feed_entry("availability", availability))
        return element
