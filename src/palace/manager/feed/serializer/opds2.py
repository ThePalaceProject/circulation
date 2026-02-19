import json
from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from frozendict import frozendict
from pydantic import ValidationError

from palace.manager.feed.serializer.base import SerializerInterface
from palace.manager.feed.serializer.opds import is_sort_facet
from palace.manager.feed.types import (
    Acquisition,
    Author,
    DataEntryTypes,
    FeedData,
    IndirectAcquisition,
    Link,
    LinkContentType,
    LinkType,
    WorkEntryData,
)
from palace.manager.opds import opds2, rwpm, schema_org
from palace.manager.opds.base import BaseOpdsModel
from palace.manager.opds.palace import DrmMetadata, LinkActions
from palace.manager.opds.util import StrModelOrTuple
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.util.log import LoggerMixin
from palace.manager.util.opds_writer import AtomFeed, OPDSMessage

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


class OPDS2Serializer(SerializerInterface[dict[str, Any]], LoggerMixin):
    _CONTENT_TYPE_MAP: frozendict[LinkContentType, str] = frozendict(
        {
            LinkContentType.OPDS_FEED: opds2.Feed.content_type(),
            LinkContentType.OPDS_ENTRY: opds2.BasePublication.content_type(),
        }
    )

    def _resolve_type(self, type_value: LinkType | None) -> str | None:
        """Map semantic LinkContentType values to OPDS2-specific types."""
        if isinstance(type_value, LinkContentType):
            return self._CONTENT_TYPE_MAP[type_value]
        return type_value

    def serialize_feed(
        self, feed: FeedData, precomposed_entries: list[Any] | None = None
    ) -> str:
        publications: list[opds2.Publication] = []
        for entry in feed.entries:
            if entry.computed is None:
                self.log.warning(
                    f"Skipping entry for work '{entry.work.title}' (identifier={entry.identifier!r}): no computed data available"
                )
                continue
            try:
                publications.append(self._publication(entry.computed))
            except ValidationError as exc:
                self.log.exception(
                    f"Skipping invalid OPDS2 publication (identifier={entry.identifier!r}): {exc}"
                )

        feed_links = self._serialize_feed_links(feed)
        feed_links.extend(self._serialize_sort_links(feed))

        metadata = self._serialize_metadata(feed)
        navigation = self._serialize_navigation(feed)
        facets = self._serialize_facet_links(feed)

        feed_model = opds2.Feed(
            metadata=metadata,
            links=feed_links,
            publications=publications,
            navigation=navigation,
            facets=facets,
        )

        return self.to_string(self._dump_model(feed_model))

    def _serialize_metadata(self, feed: FeedData) -> opds2.FeedMetadata:
        feed_metadata = feed.metadata
        title = feed_metadata.title
        if not title:
            self.log.warning("Feed metadata has no title, defaulting to 'Feed'")
            title = "Feed"
        return opds2.FeedMetadata(
            title=title,
            items_per_page=feed_metadata.items_per_page,
            modified=feed_metadata.updated,
        )

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
        identifier = data.identifier
        additional_type = data.additional_type or schema_org.PublicationTypes.book
        title = data.title
        subtitle = data.subtitle
        description = data.summary.text if data.summary else None
        publisher = rwpm.Contributor(name=data.publisher) if data.publisher else None
        imprint = rwpm.Contributor(name=data.imprint) if data.imprint else None

        subjects = [
            rwpm.Subject(
                name=category.label,
                sort_as=category.label,
                code=category.term,
                scheme=category.scheme,
            )
            for category in data.categories
        ]

        belongs_to = (
            rwpm.BelongsTo(
                series_data=rwpm.Contributor(
                    name=data.series.name,
                    position=data.series.position,
                )
            )
            if data.series
            else rwpm.BelongsTo()
        )

        author: StrModelOrTuple[rwpm.Contributor] | None
        if len(data.authors) > 1:
            author = tuple(self._serialize_contributor(a) for a in data.authors)
        elif data.authors:
            author = self._serialize_contributor(data.authors[0])
        else:
            author = None

        role_map: dict[str, rwpm.Contributor] = {}
        generic_contributors: list[rwpm.ContributorWithRole] = []

        for contrib in data.contributors:
            role = MARC_CODE_TO_ROLES.get(contrib.role or "")
            if role:
                role_map[role] = self._serialize_contributor(contrib)
            else:
                generic_contributors.append(
                    rwpm.ContributorWithRole(
                        name=contrib.name,
                        sort_as=contrib.sort_name,
                        role=contrib.role,
                    )
                )

        return opds2.PublicationMetadata(
            identifier=identifier,
            type=additional_type,
            title=title,
            sort_as=data.sort_title,
            subtitle=subtitle,
            duration=data.duration,
            language=data.language,
            modified=data.updated,
            published=data.published,
            description=description,
            publisher=publisher,
            imprint=imprint,
            subject=subjects,
            author=author,
            translator=role_map.get("translator"),
            editor=role_map.get("editor"),
            artist=role_map.get("artist"),
            illustrator=role_map.get("illustrator"),
            letterer=role_map.get("letterer"),
            penciler=role_map.get("penciler"),
            colorist=role_map.get("colorist"),
            inker=role_map.get("inker"),
            narrator=role_map.get("narrator"),
            contributor=generic_contributors,
            belongs_to=belongs_to,
        )

    def _serialize_image_links(self, links: Iterable[Link]) -> list[opds2.Link]:
        return [self._serialize_link(link) for link in links]

    def _serialize_publication_links(
        self, data: WorkEntryData
    ) -> list[opds2.StrictLink]:
        links: list[opds2.StrictLink] = []
        for link in data.other_links:
            if link.rel is None:
                self.log.warning(f"Skipping OPDS2 link without rel: {link.href}")
                continue
            resolved_type = self._resolve_type(link.type)
            if resolved_type is None:
                self.log.error(f"Skipping OPDS2 link without type: {link.href}")
                continue
            links.append(
                self._strict_link(
                    href=link.href,
                    rel=link.rel,
                    type=resolved_type,
                    title=link.title,
                    properties=self._link_properties(),
                )
            )

        for acquisition in data.acquisition_links:
            acq_link = self._serialize_acquisition_link(acquisition)
            if acq_link is not None:
                links.append(acq_link)
        return links

    def _serialize_link(self, link: Link) -> opds2.Link:
        return opds2.Link(
            href=link.href,
            rel=link.rel,
            type=self._resolve_type(link.type),
            title=link.title,
        )

    def _serialize_acquisition_link(self, link: Acquisition) -> opds2.StrictLink | None:
        link_type = self._acquisition_link_type(link)
        if link_type is None:
            return None
        return self._strict_link(
            href=link.href,
            rel=link.rel or opds2.AcquisitionLinkRelations.acquisition,
            type=link_type,
            title=link.title,
            properties=self._serialize_acquisition_properties(link),
            templated=link.templated,
        )

    def _serialize_acquisition_properties(
        self, link: Acquisition
    ) -> opds2.LinkProperties:
        state = self._availability_state(link)
        availability_data: dict[str, Any] = {}
        if state is not None:
            availability_data.update(
                {
                    "state": state,
                    "since": link.availability_since,
                    "until": link.availability_until,
                }
            )
        availability = opds2.Availability(**availability_data)

        holds_total = self._parse_int(link.holds_total)
        holds_position = self._parse_int(link.holds_position)
        holds = opds2.Holds(total=holds_total, position=holds_position)

        copies_total = self._parse_int(link.copies_total)
        copies_available = self._parse_int(link.copies_available)
        copies = opds2.Copies(total=copies_total, available=copies_available)

        indirect_acquisition = [
            acq
            for indirect in link.indirect_acquisitions
            if (acq := self._serialize_indirect_acquisition(indirect)) is not None
        ]
        actions = LinkActions(cancellable=link.is_hold or None)
        licensor = DrmMetadata(
            client_token=(
                link.drm_licensor.client_token if link.drm_licensor else None
            ),
            vendor=link.drm_licensor.vendor if link.drm_licensor else None,
        )

        return self._link_properties(
            availability=availability,
            holds=holds,
            copies=copies,
            indirect_acquisition=indirect_acquisition,
            actions=actions,
            licensor=licensor,
            lcp_hashed_passphrase=link.lcp_hashed_passphrase,
        )

    def _serialize_indirect_acquisition(
        self, indirect: IndirectAcquisition
    ) -> opds2.AcquisitionObject | None:
        if indirect.type is None:
            self.log.error(f"Skipping indirect acquisition without type")
            return None
        children = [
            acq
            for child in indirect.children
            if (acq := self._serialize_indirect_acquisition(child)) is not None
        ]
        return opds2.AcquisitionObject(
            type=indirect.type,
            child=children,
        )

    def _serialize_contributor(self, author: Author) -> rwpm.Contributor:
        links = (
            [
                rwpm.Link(
                    href=link.href,
                    rel=link.rel,
                    type=self._resolve_type(link.type),
                )
            ]
            if (link := author.link) and link.href
            else []
        )
        return rwpm.Contributor(
            name=author.name,
            sort_as=author.sort_name,
            links=links,
        )

    @classmethod
    def content_type(cls) -> str:
        return opds2.Feed.content_type()

    @classmethod
    def to_string(cls, data: dict[str, Any]) -> str:
        return json.dumps(data, indent=2)

    def _serialize_feed_links(self, feed: FeedData) -> list[opds2.StrictLink]:
        links: list[opds2.StrictLink] = []
        for link in feed.links:
            strict = self._serialize_feed_link(link)
            if strict is not None:
                links.append(strict)

        return links

    def _serialize_feed_link(self, link: Link) -> opds2.StrictLink | None:
        if link.rel is None:
            self.log.warning(f"Skipping OPDS2 feed link without rel: {link.href}")
            return None
        resolved_type = self._resolve_type(link.type)
        return self._strict_link(
            href=link.href,
            rel=link.rel,
            type=resolved_type or self.content_type(),
            title=link.title,
            properties=self._link_properties(),
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
                self.log.warning(f"Skipping facet group '{group}' with < 2 links")
                continue
            facet_link_models: list[opds2.TitleLink] = []
            for link in links:
                title = link.title or link.rel or link.href
                rel = "self" if link.active_facet else link.rel
                props = self._facet_properties(link)
                facet_link_models.append(
                    self._title_link(
                        href=link.href,
                        title=title,
                        rel=rel,
                        type=self._resolve_type(link.type),
                        properties=props,
                    )
                )

            results.append(
                opds2.Facet(
                    metadata=opds2.FeedMetadata(title=group),
                    links=facet_link_models,
                )
            )

        return results

    def _facet_properties(self, link: Link) -> opds2.LinkProperties:
        return self._link_properties(palace_default=link.default_facet or None)

    def _serialize_sort_links(self, feed: FeedData) -> list[opds2.StrictLink]:
        sort_links: list[opds2.StrictLink] = []
        for link in feed.facet_links:
            if is_sort_facet(link):
                sort_links.append(self._serialize_sort_link(link))
        return sort_links

    def _serialize_sort_link(self, link: Link) -> opds2.StrictLink:
        return self._strict_link(
            href=link.href,
            rel=PALACE_REL_SORT,
            type=self._resolve_type(link.type) or self.content_type(),
            title=link.title,
            properties=self._link_properties(
                palace_active_sort=link.active_facet or None,
                palace_default=link.default_facet or None,
            ),
        )

    def _serialize_navigation(self, feed: FeedData) -> list[opds2.TitleLink]:
        navigation: list[opds2.TitleLink] = []
        for entry in feed.data_entries:
            if entry.type != DataEntryTypes.NAVIGATION:
                continue
            for link in entry.links:
                title = entry.title or link.title or link.href
                navigation.append(
                    self._title_link(
                        href=link.href,
                        title=title,
                        rel=link.rel,
                        type=self._resolve_type(link.type),
                        properties=self._link_properties(),
                    )
                )
        return navigation

    def _acquisition_link_type(self, link: Acquisition) -> str | None:
        if link.type:
            return self._resolve_type(link.type)
        for indirect in link.indirect_acquisitions:
            if indirect.type:
                return indirect.type
        self.log.error(f"Skipping acquisition link without type: {link.href}")
        return None

    def _availability_state(self, link: Acquisition) -> opds2.AvailabilityState | None:
        if link.is_loan:
            return opds2.AvailabilityState.ready
        if link.is_hold:
            return opds2.AvailabilityState.reserved
        if link.availability_status:
            try:
                return opds2.AvailabilityState(link.availability_status)
            except ValueError:
                self.log.warning(
                    "Unknown availability status '%s' for %s",
                    link.availability_status,
                    link.href,
                )
        return None

    def _parse_int(self, value: str | None) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except ValueError:
            self.log.warning(f"Expected numeric value, got '{value}'")
            return None

    @staticmethod
    def _dump_model(model: BaseOpdsModel) -> dict[str, Any]:
        return model.model_dump(
            mode="json",
            by_alias=True,
            exclude_unset=True,
            exclude_none=True,
        )

    @staticmethod
    def _link_properties(
        *,
        availability: opds2.Availability | None = None,
        holds: opds2.Holds | None = None,
        copies: opds2.Copies | None = None,
        indirect_acquisition: list[opds2.AcquisitionObject] | None = None,
        actions: LinkActions | None = None,
        licensor: DrmMetadata | None = None,
        lcp_hashed_passphrase: str | None = None,
        palace_default: bool | None = None,
        palace_active_sort: bool | None = None,
    ) -> opds2.LinkProperties:
        return opds2.LinkProperties(
            availability=availability or opds2.Availability(),
            holds=holds or opds2.Holds(),
            copies=copies or opds2.Copies(),
            indirect_acquisition=indirect_acquisition or [],
            actions=actions,
            licensor=licensor,
            lcp_hashed_passphrase=lcp_hashed_passphrase,
            palace_default=palace_default,
            palace_active_sort=palace_active_sort,
        )

    def _strict_link(
        self,
        *,
        href: str,
        rel: str,
        type: str,
        title: str | None = None,
        properties: opds2.LinkProperties,
        templated: bool = False,
    ) -> opds2.StrictLink:
        return opds2.StrictLink(
            href=href,
            rel=rel,
            type=type,
            title=title,
            properties=properties,
            templated=templated,
        )

    @staticmethod
    def _title_link(
        *,
        href: str,
        title: str,
        rel: str | None = None,
        type: str | None = None,
        properties: opds2.LinkProperties,
    ) -> opds2.TitleLink:
        return opds2.TitleLink(
            href=href,
            title=title,
            rel=rel,
            type=type,
            properties=properties,
        )
