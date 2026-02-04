import json
import logging
from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from pydantic import ValidationError

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.feed.serializer.base import SerializerInterface
from palace.manager.feed.serializer.opds import is_sort_facet
from palace.manager.feed.types import (
    Acquisition,
    Author,
    DataEntryTypes,
    FeedData,
    IndirectAcquisition,
    Link,
    WorkEntryData,
)
from palace.manager.opds import opds2, rwpm, schema_org
from palace.manager.opds.base import BaseOpdsModel
from palace.manager.opds.types.language import LanguageMap
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.util.opds_writer import AtomFeed, OPDSFeed, OPDSMessage

logger = logging.getLogger(__name__)

ALLOWED_ROLES = [
    "translator",
    "editor",
    "artist",
    "illustrator",
    "letterer",
    "penciler",
    "colorist",
    "inker",
    "narrator",
]
MARC_CODE_TO_ROLES = {
    code: name.lower()
    for name, code in Contributor.MARC_ROLE_CODES.items()
    if name.lower() in ALLOWED_ROLES
}

PALACE_REL_SORT = AtomFeed.PALACE_REL_SORT
PALACE_PROPERTIES_ACTIVE_SORT = AtomFeed.PALACE_PROPS_NS + "active-sort"
PALACE_PROPERTIES_DEFAULT = AtomFeed.PALACE_PROPERTIES_DEFAULT

DEFAULT_LINK_TYPE = "application/octet-stream"


class OPDS2Serializer(SerializerInterface[dict[str, Any]]):
    CONTENT_TYPE = opds2.Feed.content_type()

    def serialize_feed(
        self, feed: FeedData, precomposed_entries: list[Any] | None = None
    ) -> str:
        publications: list[opds2.Publication] = []
        for entry in feed.entries:
            if entry.computed is None:
                continue
            try:
                publications.append(self._publication(entry.computed))
            except (PalaceValueError, ValidationError, ValueError) as exc:
                logger.exception("Skipping invalid OPDS2 publication: %s", exc)

        feed_links = self._serialize_feed_links(feed)
        feed_links.extend(self._serialize_sort_links(feed))

        metadata = self._serialize_metadata(feed)
        navigation = self._serialize_navigation(feed)
        facets = self._serialize_facet_links(feed)

        feed_kwargs: dict[str, Any] = {
            "metadata": metadata,
            "links": feed_links,
            "publications": publications,
        }
        if navigation:
            feed_kwargs["navigation"] = navigation
        if facets:
            feed_kwargs["facets"] = facets

        feed_model = opds2.Feed(**feed_kwargs)

        return self.to_string(self._dump_model(feed_model))

    def _serialize_metadata(self, feed: FeedData) -> opds2.FeedMetadata:
        fmeta = feed.metadata
        title = fmeta.title or ""
        metadata_kwargs: dict[str, Any] = {"title": LanguageMap(title)}
        if fmeta.items_per_page is not None:
            metadata_kwargs["items_per_page"] = fmeta.items_per_page
        if fmeta.updated:
            metadata_kwargs["modified"] = fmeta.updated
        return opds2.FeedMetadata(**metadata_kwargs)

    def serialize_opds_message(self, entry: OPDSMessage) -> dict[str, Any]:
        return dict(urn=entry.urn, description=entry.message)

    def serialize_work_entry(self, data: WorkEntryData) -> dict[str, Any]:
        publication = self._publication(data)
        return self._dump_model(publication)

    def _publication(self, data: WorkEntryData) -> opds2.Publication:
        metadata = self._serialize_publication_metadata(data)
        images = self._serialize_image_links(data.image_links)
        links = self._serialize_publication_links(data)
        return opds2.Publication(metadata=metadata, images=images, links=links)

    def _serialize_publication_metadata(
        self, data: WorkEntryData
    ) -> opds2.PublicationMetadata:
        identifier = data.identifier or data.pwid
        if not identifier:
            raise PalaceValueError("OPDS2 publications require an identifier")

        additional_type = data.additional_type or schema_org.PublicationTypes.book
        title = data.title or OPDSFeed.NO_TITLE

        metadata_kwargs: dict[str, Any] = {
            "identifier": identifier,
            "type": additional_type,
            "title": LanguageMap(title),
        }

        if data.sort_title:
            metadata_kwargs["sort_as"] = data.sort_title
        if data.subtitle:
            metadata_kwargs["subtitle"] = LanguageMap(data.subtitle)
        if data.duration is not None:
            metadata_kwargs["duration"] = data.duration
        if data.language:
            metadata_kwargs["language"] = data.language
        if data.updated:
            metadata_kwargs["modified"] = data.updated
        if data.published:
            metadata_kwargs["published"] = data.published
        if data.summary:
            metadata_kwargs["description"] = data.summary.text

        if data.publisher:
            metadata_kwargs["publisher"] = rwpm.Contributor(
                name=LanguageMap(data.publisher)
            )
        if data.imprint:
            metadata_kwargs["imprint"] = rwpm.Contributor(
                name=LanguageMap(data.imprint)
            )

        if data.categories:
            subjects = [
                rwpm.Subject(
                    name=LanguageMap(category.label),
                    sort_as=category.label,
                    code=category.term,
                    scheme=category.scheme,
                )
                for category in data.categories
            ]
            metadata_kwargs["subject"] = tuple(subjects)

        if data.series:
            series_contributor = rwpm.Contributor(
                name=LanguageMap(data.series.name),
                position=data.series.position,
            )
            metadata_kwargs["belongs_to"] = rwpm.BelongsTo(
                series_data=series_contributor
            )

        if data.authors:
            metadata_kwargs["author"] = self._serialize_contributor(data.authors[0])
        for contributor in data.contributors:
            if role := MARC_CODE_TO_ROLES.get(contributor.role or "", None):
                metadata_kwargs[role] = self._serialize_contributor(contributor)

        return opds2.PublicationMetadata(**metadata_kwargs)

    def _serialize_image_links(self, links: Iterable[Link]) -> list[opds2.Link]:
        return [self._serialize_link(link) for link in links]

    def _serialize_publication_links(
        self, data: WorkEntryData
    ) -> list[opds2.StrictLink]:
        links: list[opds2.StrictLink] = []
        for link in data.other_links:
            if link.rel is None:
                logger.warning("Skipping OPDS2 link without rel: %s", link.href)
                continue
            link_type = link.type or DEFAULT_LINK_TYPE
            links.append(
                opds2.StrictLink(
                    href=link.href,
                    rel=link.rel,
                    type=link_type,
                    title=link.title,
                )
            )

        for acquisition in data.acquisition_links:
            links.append(self._serialize_acquisition_link(acquisition))
        return links

    def _serialize_link(self, link: Link) -> opds2.Link:
        return opds2.Link(
            href=link.href,
            rel=link.rel,
            type=link.type,
            title=link.title,
        )

    def _serialize_acquisition_link(self, link: Acquisition) -> opds2.StrictLink:
        link_type = self._acquisition_link_type(link)
        properties = self._serialize_acquisition_properties(link)
        link_kwargs: dict[str, Any] = {
            "href": link.href,
            "rel": link.rel or opds2.AcquisitionLinkRelations.acquisition,
            "type": link_type,
        }
        if link.title:
            link_kwargs["title"] = link.title
        if link.templated:
            link_kwargs["templated"] = True
        if properties:
            link_kwargs["properties"] = properties
        return opds2.StrictLink(**link_kwargs)

    def _serialize_acquisition_properties(
        self, link: Acquisition
    ) -> opds2.LinkProperties | None:
        props: dict[str, Any] = {}

        state = self._availability_state(link)
        if state is not None:
            availability_kwargs: dict[str, Any] = {"state": state}
            if link.availability_since:
                availability_kwargs["since"] = link.availability_since
            if link.availability_until:
                availability_kwargs["until"] = link.availability_until
            props["availability"] = opds2.Availability(**availability_kwargs)

        holds_total = self._parse_int(link.holds_total)
        holds_position = self._parse_int(link.holds_position)
        if holds_total is not None or holds_position is not None:
            props["holds"] = opds2.Holds(total=holds_total, position=holds_position)

        copies_total = self._parse_int(link.copies_total)
        copies_available = self._parse_int(link.copies_available)
        if copies_total is not None or copies_available is not None:
            props["copies"] = opds2.Copies(
                total=copies_total, available=copies_available
            )

        if link.indirect_acquisitions:
            props["indirect_acquisition"] = [
                self._serialize_indirect_acquisition(indirect)
                for indirect in link.indirect_acquisitions
            ]

        if link.is_hold:
            props["actions"] = opds2.LinkActions(cancellable=True)

        if link.lcp_hashed_passphrase:
            props["lcp_hashed_passphrase"] = link.lcp_hashed_passphrase

        if link.drm_licensor:
            props["licensor"] = opds2.PalaceLicensor(
                client_token=link.drm_licensor.client_token,
                vendor=link.drm_licensor.vendor,
            )

        if not props:
            return None
        return opds2.LinkProperties(**props)

    def _serialize_indirect_acquisition(
        self, indirect: IndirectAcquisition
    ) -> opds2.AcquisitionObject:
        children = [
            self._serialize_indirect_acquisition(child) for child in indirect.children
        ]
        return opds2.AcquisitionObject(
            type=indirect.type or DEFAULT_LINK_TYPE,
            child=children or None,
        )

    def _serialize_contributor(self, author: Author) -> rwpm.Contributor:
        if not author.name:
            raise PalaceValueError("Contributor name is required for OPDS2 output")
        contributor_kwargs: dict[str, Any] = {"name": LanguageMap(author.name)}
        if author.sort_name:
            contributor_kwargs["sort_as"] = author.sort_name
        if author.link:
            contributor_kwargs["links"] = [self._serialize_contributor_link(author)]
        return rwpm.Contributor(**contributor_kwargs)

    def _serialize_contributor_link(self, author: Author) -> rwpm.Link:
        if author.link is None:
            raise PalaceValueError("Contributor link is required for OPDS2 output")
        link_kwargs: dict[str, Any] = {
            "href": author.link.href,
        }
        if author.link.rel:
            link_kwargs["rel"] = author.link.rel
        if author.link.type:
            link_kwargs["type"] = author.link.type
        return rwpm.Link(**link_kwargs)

    def content_type(self) -> str:
        return self.CONTENT_TYPE

    @classmethod
    def to_string(cls, data: dict[str, Any]) -> str:
        return json.dumps(data, indent=2)

    def _serialize_feed_links(self, feed: FeedData) -> list[opds2.StrictLink]:
        links: list[opds2.StrictLink] = []
        for link in feed.links:
            strict = self._serialize_feed_link(link)
            if strict is not None:
                links.append(strict)

        if not any(self._is_self_link(link) for link in links):
            if feed.metadata.id:
                links.append(
                    opds2.StrictLink(
                        href=feed.metadata.id,
                        rel=rwpm.LinkRelations.self,
                        type=self.CONTENT_TYPE,
                    )
                )
            else:
                raise PalaceValueError("OPDS2 feeds require a self link")

        return links

    def _serialize_feed_link(self, link: Link) -> opds2.StrictLink | None:
        if link.rel is None:
            logger.warning("Skipping OPDS2 feed link without rel: %s", link.href)
            return None
        link_type = link.type or self.CONTENT_TYPE
        return opds2.StrictLink(
            href=link.href,
            rel=link.rel,
            type=link_type,
            title=link.title,
        )

    def _serialize_facet_links(self, feed: FeedData) -> list[opds2.Facet]:
        results: list[opds2.Facet] = []
        facet_links: dict[str, list[Link]] = defaultdict(list)
        for link in feed.facet_links:
            if not is_sort_facet(link):
                if link.facet_group:
                    facet_links[link.facet_group].append(link)

        for group, links in facet_links.items():
            if len(links) < 2:
                logger.warning("Skipping facet group '%s' with < 2 links", group)
                continue
            facet_link_models: list[opds2.TitleLink] = []
            for link in links:
                title = link.title or link.rel or link.href
                rel = "self" if link.active_facet else link.rel
                props = self._facet_properties(link)
                link_kwargs: dict[str, Any] = {
                    "href": link.href,
                    "title": title,
                }
                if rel:
                    link_kwargs["rel"] = rel
                if link.type:
                    link_kwargs["type"] = link.type
                if props:
                    link_kwargs["properties"] = props
                facet_link_models.append(opds2.TitleLink(**link_kwargs))

            results.append(
                opds2.Facet(
                    metadata=opds2.FeedMetadata(title=LanguageMap(group)),
                    links=facet_link_models,
                )
            )

        return results

    def _facet_properties(self, link: Link) -> opds2.LinkProperties | None:
        if not link.default_facet:
            return None
        return opds2.LinkProperties(palace_default="true")

    def _serialize_sort_links(self, feed: FeedData) -> list[opds2.StrictLink]:
        sort_links: list[opds2.StrictLink] = []
        for link in feed.facet_links:
            if is_sort_facet(link):
                sort_links.append(self._serialize_sort_link(link))
        return sort_links

    def _serialize_sort_link(self, link: Link) -> opds2.StrictLink:
        properties_kwargs: dict[str, Any] = {}
        if link.active_facet:
            properties_kwargs["palace_active_sort"] = "true"
        if link.default_facet:
            properties_kwargs["palace_default"] = "true"

        properties = (
            opds2.LinkProperties(**properties_kwargs) if properties_kwargs else None
        )

        link_kwargs: dict[str, Any] = {
            "href": link.href,
            "rel": PALACE_REL_SORT,
            "type": link.type or self.CONTENT_TYPE,
            "title": link.title,
        }
        if properties is not None:
            link_kwargs["properties"] = properties
        return opds2.StrictLink(**link_kwargs)

    def _serialize_navigation(self, feed: FeedData) -> list[opds2.TitleLink]:
        navigation: list[opds2.TitleLink] = []
        for entry in feed.data_entries:
            if entry.type != DataEntryTypes.NAVIGATION:
                continue
            for link in entry.links:
                title = entry.title or link.title or link.href
                link_kwargs: dict[str, Any] = {
                    "href": link.href,
                    "title": title,
                }
                if link.rel:
                    link_kwargs["rel"] = link.rel
                if link.type:
                    link_kwargs["type"] = link.type
                navigation.append(opds2.TitleLink(**link_kwargs))
        return navigation

    def _acquisition_link_type(self, link: Acquisition) -> str:
        if link.type:
            return link.type
        for indirect in link.indirect_acquisitions:
            if indirect.type:
                return indirect.type
        logger.warning("Defaulting OPDS2 acquisition link type: %s", link.href)
        return DEFAULT_LINK_TYPE

    def _availability_state(self, link: Acquisition) -> opds2.AvailabilityState | None:
        if link.is_loan:
            return opds2.AvailabilityState.ready
        if link.is_hold:
            return opds2.AvailabilityState.reserved
        if link.availability_status:
            try:
                return opds2.AvailabilityState(link.availability_status)
            except ValueError:
                logger.warning(
                    "Unknown availability status '%s' for %s",
                    link.availability_status,
                    link.href,
                )
        return None

    def _is_self_link(self, link: opds2.StrictLink) -> bool:
        return rwpm.LinkRelations.self in link.rels

    @staticmethod
    def _parse_int(value: str | None) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except ValueError:
            logger.warning("Expected numeric value, got '%s'", value)
            return None

    @staticmethod
    def _dump_model(model: BaseOpdsModel) -> dict[str, Any]:
        return model.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
            exclude_unset=True,
        )
