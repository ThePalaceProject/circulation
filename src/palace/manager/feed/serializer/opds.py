from __future__ import annotations

import abc
import datetime
from functools import partial
from typing import Any, cast

from frozendict import frozendict
from lxml import etree

from palace.manager.core.user_profile import ProfileController
from palace.manager.feed.facets.constants import FacetConstants
from palace.manager.feed.serializer.base import SerializerInterface
from palace.manager.feed.types import (
    Acquisition,
    Author,
    DataEntry,
    DRMLicensor,
    FeedData,
    FeedMetadata,
    IndirectAcquisition,
    Link,
    LinkContentType,
    LinkType,
    PatronData,
    Rating,
    Series,
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
    "authorization_identifier": f"{{{OPDSFeed.SIMPLIFIED_NS}}}authorizationIdentifier",
    "rights": f"{{{OPDSFeed.DCTERMS_NS}}}rights",
    "ProviderName": f"{{{OPDSFeed.BIBFRAME_NS}}}ProviderName",
    "facet_group": f"{{{OPDSFeed.OPDS_NS}}}facetGroup",
    "facet_group_type": f"{{{OPDSFeed.SIMPLIFIED_NS}}}facetGroupType",
    "active_facet": f"{{{OPDSFeed.OPDS_NS}}}activeFacet",
    "rating_value": f"{{{OPDSFeed.SCHEMA_NS}}}ratingValue",
}

V2_ATTRIBUTE_MAPPING = {
    **V1_ATTRIBUTE_MAPPING,
    "default_facet": f"{{{OPDSFeed.PALACE_PROPS_NS}}}default",
    "active_sort": f"{{{OPDSFeed.PALACE_PROPS_NS}}}active-sort",
}

AUTHOR_MAPPING = {
    "name": f"{{{OPDSFeed.ATOM_NS}}}name",
    "role": f"{{{OPDSFeed.OPF_NS}}}role",
    "sort_name": f"{{{OPDSFeed.SIMPLIFIED_NS}}}sort_name",
    "wikipedia_name": f"{{{OPDSFeed.SIMPLIFIED_NS}}}wikipedia_name",
}


def is_sort_facet(link: Link) -> bool:
    """A utility method that determines if the specified link is part of a sort facet."""
    group_name = cast(
        str, FacetConstants.GROUP_DISPLAY_TITLES[FacetConstants.ORDER_FACET_GROUP_NAME]
    )
    return link.facet_group == group_name


class BaseOPDS1Serializer(SerializerInterface[etree._Element], OPDSFeed, abc.ABC):
    _CONTENT_TYPE_MAP: frozendict[LinkContentType, str] = frozendict(
        {
            LinkContentType.OPDS_FEED: OPDSFeed.ACQUISITION_FEED_TYPE,
            LinkContentType.OPDS_ENTRY: OPDSFeed.ENTRY_TYPE,
        }
    )

    # OPDS1 uses Palace-specific relation URIs for some standard rels.
    # OPDS2 uses standard IANA rels directly and needs no mapping.
    _REL_MAP: frozendict[str, str] = frozendict(
        {
            "profile": ProfileController.LINK_RELATION,
        }
    )

    def __init__(self) -> None:
        pass

    def _resolve_type(self, type_value: LinkType | None) -> str | None:
        """Map semantic LinkContentType values to OPDS1-specific types."""
        if isinstance(type_value, LinkContentType):
            return self._CONTENT_TYPE_MAP[type_value]
        return type_value

    def _resolve_rel(self, rel_value: str | None) -> str | None:
        """Map standard rels to OPDS1/Palace-specific rels."""
        if rel_value is not None and rel_value in self._REL_MAP:
            return self._REL_MAP[rel_value]
        return rel_value

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
            serialized.append(self._serialize_link(link))

        if feed.breadcrumbs:
            breadcrumbs = OPDSFeed.E._makeelement(
                f"{{{OPDSFeed.SIMPLIFIED_NS}}}breadcrumbs"
            )
            for link in feed.breadcrumbs:
                breadcrumbs.append(self._serialize_link(link))
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
            tags.append(self._serialize_patron(metadata.patron))
        if metadata.drm_licensor:
            tags.append(self._serialize_drm_licensor(metadata.drm_licensor))
        if metadata.lcp_hashed_passphrase:
            tags.append(
                self._serialize_hashed_passphrase(metadata.lcp_hashed_passphrase)
            )

        return tags

    def serialize_work_entry(self, feed_entry: WorkEntryData) -> etree._Element:
        entry: etree._Element = OPDSFeed.entry()

        if feed_entry.additional_type:
            entry.set(
                f"{{{OPDSFeed.SCHEMA_NS}}}additionalType", feed_entry.additional_type
            )

        if feed_entry.title:
            entry.append(OPDSFeed.E("title", feed_entry.title))

        if feed_entry.subtitle:
            entry.append(
                OPDSFeed.E(
                    f"{{{OPDSFeed.SCHEMA_NS}}}alternativeHeadline",
                    feed_entry.subtitle,
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
                OPDSFeed.E(f"{{{OPDSFeed.DCTERMS_NS}}}language", feed_entry.language)
            )
        if feed_entry.publisher:
            entry.append(
                OPDSFeed.E(f"{{{OPDSFeed.DCTERMS_NS}}}publisher", feed_entry.publisher)
            )
        if feed_entry.imprint:
            entry.append(
                OPDSFeed.E(
                    f"{{{OPDSFeed.BIB_SCHEMA_NS}}}publisherImprint",
                    feed_entry.imprint,
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
        if feed_entry.distribution:
            entry.append(
                OPDSFeed.E(
                    f"{{{OPDSFeed.BIBFRAME_NS}}}distribution",
                    **{
                        f"{{{OPDSFeed.BIBFRAME_NS}}}ProviderName": feed_entry.distribution.provider_name
                    },
                )
            )
        if feed_entry.published:
            entry.append(OPDSFeed.E("published", feed_entry.published))
        if feed_entry.updated:
            entry.append(OPDSFeed.E("updated", feed_entry.updated))

        if feed_entry.series:
            entry.append(self._serialize_series_entry(feed_entry.series))

        for category in feed_entry.categories:
            element = OPDSFeed.category(
                scheme=category.scheme, term=category.term, label=category.label
            )
            entry.append(element)

        for rating in feed_entry.ratings:
            entry.append(self._serialize_rating(rating))

        for author in feed_entry.authors:
            # Author must at a minimum have a name
            if author.name:
                entry.append(self._serialize_author_tag("author", author))
        for contributor in feed_entry.contributors:
            entry.append(self._serialize_author_tag("contributor", contributor))

        for link in feed_entry.image_links:
            entry.append(self._serialize_link(link))

        for link in feed_entry.acquisition_links:
            element = self._serialize_acquisition_link(link)
            entry.append(element)

        for link in feed_entry.other_links:
            entry.append(self._serialize_link(link))

        return entry

    def serialize_opds_message(self, entry: OPDSMessage) -> etree._Element:
        return entry.tag

    def _serialize_series_entry(self, series: Series) -> etree._Element:
        entry = self._tag("series")
        entry.set("name", series.name)
        if series.position:
            entry.append(self._tag("position", str(series.position)))
        if series.link:
            entry.append(self._serialize_link(series.link))

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
            entry.append(self._serialize_link(author.link))

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

    def _serialize_link(self, link: Link) -> etree._Element:
        resolved_type = self._resolve_type(link.type)
        resolved_rel = self._resolve_rel(link.rel)
        attrs: dict[str, str] = {}
        if link.href is not None:
            attrs["href"] = link.href
        if resolved_rel is not None:
            attrs["rel"] = resolved_rel
        if resolved_type is not None:
            attrs["type"] = resolved_type
        if link.title is not None:
            attrs["title"] = link.title
        if link.role is not None:
            attrs["role"] = link.role

        element = OPDSFeed.link(**attrs)
        attr_mapping = self._get_attribute_mapping()

        if link.facet_group:
            element.set(
                self._attr_name("facet_group", mapping=attr_mapping),
                link.facet_group,
            )
        if link.facet_group_type:
            element.set(
                self._attr_name("facet_group_type", mapping=attr_mapping),
                link.facet_group_type,
            )
        if link.active_facet and "active_facet" in attr_mapping:
            element.set(
                self._attr_name("active_facet", mapping=attr_mapping),
                "true",
            )
        if link.default_facet and "default_facet" in attr_mapping:
            element.set(
                self._attr_name("default_facet", mapping=attr_mapping),
                "true",
            )
        if link.active_sort and "active_sort" in attr_mapping:
            element.set(
                self._attr_name("active_sort", mapping=attr_mapping),
                "true",
            )

        return element

    def _serialize_patron(self, patron: PatronData) -> etree._Element:
        entry = self._tag("patron")
        attr_mapping = self._get_attribute_mapping()
        if patron.username:
            entry.set(
                self._attr_name("username", mapping=attr_mapping), patron.username
            )
        if patron.authorization_identifier:
            entry.set(
                self._attr_name("authorization_identifier", mapping=attr_mapping),
                patron.authorization_identifier,
            )
        return entry

    def _serialize_drm_licensor(self, licensor: DRMLicensor) -> etree._Element:
        entry = self._tag("licensor")
        attr_mapping = self._get_attribute_mapping()
        if licensor.vendor:
            entry.set(self._attr_name("vendor", mapping=attr_mapping), licensor.vendor)
        if licensor.scheme:
            entry.set(self._attr_name("scheme", mapping=attr_mapping), licensor.scheme)
        if licensor.client_token:
            entry.append(self._tag("clientToken", licensor.client_token))
        return entry

    def _serialize_hashed_passphrase(self, passphrase: str) -> etree._Element:
        return self._tag("hashed_passphrase", passphrase)

    def _serialize_rating(self, rating: Rating) -> etree._Element:
        entry = self._tag("Rating")
        attr_mapping = self._get_attribute_mapping()
        entry.set(
            self._attr_name("rating_value", mapping=attr_mapping),
            rating.rating_value,
        )
        if rating.additional_type:
            entry.set(f"{{{OPDSFeed.SCHEMA_NS}}}additionalType", rating.additional_type)
        return entry

    def _serialize_acquisition_link(self, link: Acquisition) -> etree._Element:
        resolved_type = self._resolve_type(link.type)
        resolved_rel = self._resolve_rel(link.rel)

        attrs: dict[str, str] = {"href": link.href}
        if resolved_rel is not None:
            attrs["rel"] = resolved_rel
        if resolved_type is not None:
            attrs["type"] = resolved_type

        link_func = OPDSFeed.tlink if link.templated else OPDSFeed.link
        element = link_func(**attrs)

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
            element.append(self._tag("hashed_passphrase", link.lcp_hashed_passphrase))

        if link.drm_licensor:
            element.append(self._serialize_drm_licensor(link.drm_licensor))

        return element

    def _serialize_data_entry(self, entry: DataEntry) -> etree._Element:
        element = self._tag("entry")
        if entry.title:
            element.append(self._tag("title", entry.title))
        if entry.id:
            element.append(self._tag("id", entry.id))
        for link in entry.links:
            element.append(self._serialize_link(link))
        return element

    @classmethod
    def to_string(cls, element: etree._Element) -> str:
        return cast(str, etree.tostring(element, encoding="unicode"))

    @abc.abstractmethod
    def content_type(self) -> str:
        """return the content type associated with the serialization. This value should include the api-version."""

    @abc.abstractmethod
    def _serialize_facet_links(self, feed: FeedData) -> list[etree._Element]:
        """This method implements serialization of the facet_links from the feed data."""

    @abc.abstractmethod
    def _serialize_sort_links(self, feed: FeedData) -> list[etree._Element]:
        """This method implements serialization of the sort links from the feed data."""


class OPDS1Version1Serializer(BaseOPDS1Serializer):
    """An OPDS 1.2 Atom feed serializer.  This version of the feed implements sort links as
    facets rather than using the http://palaceproject.io/terms/rel/sort rel and does not  support
    the http://palaceproject.io/terms/properties/default property indicating default facets
    """

    def _serialize_facet_links(self, feed: FeedData) -> list[etree._Element]:
        links: list[etree._Element] = []
        if feed.facet_links:
            for link in feed.facet_links:
                links.append(self._serialize_link(link))
        return links

    def _serialize_sort_links(self, feed: FeedData) -> list[etree._Element]:
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

    def _serialize_facet_links(self, feed: FeedData) -> list[etree._Element]:
        # serializes the non-sort related facets.
        links: list[etree._Element] = []
        facet_links = feed.facet_links
        if facet_links:
            for link in facet_links:
                # serialize all but the sort facets.
                if not is_sort_facet(link):
                    links.append(self._serialize_link(link))
        return links

    def _serialize_sort_links(self, feed: FeedData) -> list[etree._Element]:
        # this version of the feed filters out the sort facets and
        # serializes them in a way that makes use of palace extensions.
        links: list[etree._Element] = []
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
        sort_link.active_sort = link.active_facet
        sort_link.default_facet = link.default_facet

        return self._serialize_link(sort_link)

    def _get_attribute_mapping(self) -> dict[str, str]:
        return V2_ATTRIBUTE_MAPPING

    def content_type(self) -> str:
        return OPDSFeed.ACQUISITION_FEED_TYPE + "; api-version=2"
