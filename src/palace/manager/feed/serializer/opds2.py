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
from palace.manager.opds.palace import DrmMetadata, LinkActions
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

        feed_model = opds2.Feed(
            metadata=metadata,
            links=feed_links,
            publications=publications,
            navigation=navigation,
            facets=facets,
        )

        return self.to_string(self._dump_model(feed_model))

    def _serialize_metadata(self, feed: FeedData) -> opds2.FeedMetadata:
        fmeta = feed.metadata
        title = fmeta.title or ""
        return opds2.FeedMetadata(
            title=LanguageMap(title),
            items_per_page=fmeta.items_per_page,
            modified=fmeta.updated,
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
        identifier = data.identifier or data.pwid
        if not identifier:
            raise PalaceValueError("OPDS2 publications require an identifier")

        additional_type = data.additional_type or schema_org.PublicationTypes.book
        title = data.title or OPDSFeed.NO_TITLE

        subtitle = LanguageMap(data.subtitle) if data.subtitle else None
        description = data.summary.text if data.summary else None
        publisher = (
            rwpm.Contributor(name=LanguageMap(data.publisher))
            if data.publisher
            else None
        )
        imprint = (
            rwpm.Contributor(name=LanguageMap(data.imprint)) if data.imprint else None
        )

        subjects: tuple[rwpm.Subject, ...] | None = None
        if data.categories:
            subjects = tuple(
                rwpm.Subject(
                    name=LanguageMap(category.label),
                    sort_as=category.label,
                    code=category.term,
                    scheme=category.scheme,
                )
                for category in data.categories
            )

        belongs_to: rwpm.BelongsTo | None = None
        if data.series:
            series_contributor = rwpm.Contributor(
                name=LanguageMap(data.series.name),
                position=data.series.position,
            )
            belongs_to = rwpm.BelongsTo(series_data=series_contributor)

        author = self._serialize_contributor(data.authors[0]) if data.authors else None

        translator = editor = artist = illustrator = None
        letterer = penciler = colorist = inker = narrator = None

        for contributor in data.contributors:
            role = MARC_CODE_TO_ROLES.get(contributor.role or "")
            if role == "translator":
                translator = self._serialize_contributor(contributor)
            elif role == "editor":
                editor = self._serialize_contributor(contributor)
            elif role == "artist":
                artist = self._serialize_contributor(contributor)
            elif role == "illustrator":
                illustrator = self._serialize_contributor(contributor)
            elif role == "letterer":
                letterer = self._serialize_contributor(contributor)
            elif role == "penciler":
                penciler = self._serialize_contributor(contributor)
            elif role == "colorist":
                colorist = self._serialize_contributor(contributor)
            elif role == "inker":
                inker = self._serialize_contributor(contributor)
            elif role == "narrator":
                narrator = self._serialize_contributor(contributor)

        metadata = opds2.PublicationMetadata(
            identifier=identifier,
            type=additional_type,
            title=LanguageMap(title),
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
            translator=translator,
            editor=editor,
            artist=artist,
            illustrator=illustrator,
            letterer=letterer,
            penciler=penciler,
            colorist=colorist,
            inker=inker,
            narrator=narrator,
        )

        if belongs_to is None:
            return metadata
        return metadata.model_copy(update={"belongs_to": belongs_to})

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
            links.append(
                self._strict_link(
                    href=link.href,
                    rel=link.rel,
                    type=link.type or DEFAULT_LINK_TYPE,
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
        return self._strict_link(
            href=link.href,
            rel=link.rel or opds2.AcquisitionLinkRelations.acquisition,
            type=link_type,
            title=link.title,
            properties=properties,
            templated=link.templated,
        )

    def _serialize_acquisition_properties(
        self, link: Acquisition
    ) -> opds2.LinkProperties | None:
        state = self._availability_state(link)
        availability: opds2.Availability | None = None
        if state is not None:
            availability = opds2.Availability(
                state=state,
                since=link.availability_since,
                until=link.availability_until,
            )

        holds_total = self._parse_int(link.holds_total)
        holds_position = self._parse_int(link.holds_position)
        holds = (
            opds2.Holds(total=holds_total, position=holds_position)
            if holds_total is not None or holds_position is not None
            else None
        )

        copies_total = self._parse_int(link.copies_total)
        copies_available = self._parse_int(link.copies_available)
        copies = (
            opds2.Copies(total=copies_total, available=copies_available)
            if copies_total is not None or copies_available is not None
            else None
        )

        indirect_acquisition = (
            [
                self._serialize_indirect_acquisition(indirect)
                for indirect in link.indirect_acquisitions
            ]
            if link.indirect_acquisitions
            else None
        )
        actions = LinkActions(cancellable=True) if link.is_hold else None
        licensor = (
            DrmMetadata(
                client_token=link.drm_licensor.client_token,
                vendor=link.drm_licensor.vendor,
            )
            if link.drm_licensor
            else None
        )

        if (
            availability is None
            and holds is None
            and copies is None
            and indirect_acquisition is None
            and actions is None
            and licensor is None
            and link.lcp_hashed_passphrase is None
        ):
            return None

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
        if author.link:
            return rwpm.Contributor(
                name=LanguageMap(author.name),
                sort_as=author.sort_name,
                links=[self._serialize_contributor_link(author)],
            )
        return rwpm.Contributor(
            name=LanguageMap(author.name),
            sort_as=author.sort_name,
        )

    def _serialize_contributor_link(self, author: Author) -> rwpm.Link:
        if author.link is None:
            raise PalaceValueError("Contributor link is required for OPDS2 output")
        return rwpm.Link(
            href=author.link.href,
            rel=author.link.rel,
            type=author.link.type,
        )

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
        return self._strict_link(
            href=link.href,
            rel=link.rel,
            type=link.type or self.CONTENT_TYPE,
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
                facet_link_models.append(
                    self._title_link(
                        href=link.href,
                        title=title,
                        rel=rel,
                        type=link.type,
                        properties=props,
                    )
                )

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
        return self._link_properties(palace_default="true")

    def _serialize_sort_links(self, feed: FeedData) -> list[opds2.StrictLink]:
        sort_links: list[opds2.StrictLink] = []
        for link in feed.facet_links:
            if is_sort_facet(link):
                sort_links.append(self._serialize_sort_link(link))
        return sort_links

    def _serialize_sort_link(self, link: Link) -> opds2.StrictLink:
        properties = None
        if link.active_facet or link.default_facet:
            properties = self._link_properties(
                palace_active_sort="true" if link.active_facet else None,
                palace_default="true" if link.default_facet else None,
            )

        return self._strict_link(
            href=link.href,
            rel=PALACE_REL_SORT,
            type=link.type or self.CONTENT_TYPE,
            title=link.title,
            properties=properties,
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
                        type=link.type,
                    )
                )
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
        actions: palace.manager.opds.palace.LinkActions | None = None,
        licensor: palace.manager.opds.palace.DrmMetadata | None = None,
        lcp_hashed_passphrase: str | None = None,
        palace_default: str | None = None,
        palace_active_sort: str | None = None,
    ) -> opds2.LinkProperties:
        values: dict[str, Any] = {}
        fields_set: set[str] = set()

        if availability is not None:
            values["availability"] = availability
            fields_set.add("availability")
        if holds is not None:
            values["holds"] = holds
            fields_set.add("holds")
        if copies is not None:
            values["copies"] = copies
            fields_set.add("copies")
        if indirect_acquisition is not None:
            values["indirect_acquisition"] = indirect_acquisition
            fields_set.add("indirect_acquisition")
        if actions is not None:
            values["actions"] = actions
            fields_set.add("actions")
        if licensor is not None:
            values["licensor"] = licensor
            fields_set.add("licensor")
        if lcp_hashed_passphrase is not None:
            values["lcp_hashed_passphrase"] = lcp_hashed_passphrase
            fields_set.add("lcp_hashed_passphrase")
        if palace_default is not None:
            values["palace_default"] = palace_default
            fields_set.add("palace_default")
        if palace_active_sort is not None:
            values["palace_active_sort"] = palace_active_sort
            fields_set.add("palace_active_sort")

        return opds2.LinkProperties.model_construct(
            _fields_set=fields_set,
            **values,
        )

    def _strict_link(
        self,
        *,
        href: str,
        rel: str,
        type: str,
        title: str | None = None,
        properties: opds2.LinkProperties | None = None,
        templated: bool = False,
    ) -> opds2.StrictLink:
        if properties is None and not templated:
            return opds2.StrictLink(href=href, rel=rel, type=type, title=title)
        if properties is None:
            return opds2.StrictLink(
                href=href,
                rel=rel,
                type=type,
                title=title,
                templated=True,
            )
        if templated:
            return opds2.StrictLink(
                href=href,
                rel=rel,
                type=type,
                title=title,
                properties=properties,
                templated=True,
            )
        return opds2.StrictLink(
            href=href,
            rel=rel,
            type=type,
            title=title,
            properties=properties,
        )

    @staticmethod
    def _title_link(
        *,
        href: str,
        title: str,
        rel: str | None = None,
        type: str | None = None,
        properties: opds2.LinkProperties | None = None,
    ) -> opds2.TitleLink:
        if properties is None:
            return opds2.TitleLink(href=href, title=title, rel=rel, type=type)
        return opds2.TitleLink(
            href=href,
            title=title,
            rel=rel,
            type=type,
            properties=properties,
        )
