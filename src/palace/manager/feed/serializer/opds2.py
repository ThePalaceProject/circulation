import json
from collections import defaultdict
from typing import Any

from palace.manager.feed.serializer.base import SerializerInterface
from palace.manager.feed.serializer.opds import is_sort_facet
from palace.manager.feed.types import (
    Acquisition,
    Author,
    FeedData,
    IndirectAcquisition,
    Link,
    WorkEntryData,
)
from palace.manager.sqlalchemy.model.contributor import Contributor
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


class OPDS2Serializer(SerializerInterface[dict[str, Any]]):
    CONTENT_TYPE = "application/opds+json"

    def serialize_feed(
        self, feed: FeedData, precomposed_entries: list[Any] | None = None
    ) -> str:

        serialized: dict[str, Any] = {
            "publications": [],
            "metadata": self._serialize_metadata(feed),
        }

        for entry in feed.entries:
            if entry.computed:
                publication = self.serialize_work_entry(entry.computed)
                serialized["publications"].append(publication)

        link_data: dict[str, list[dict[str, Any]]] = {"links": [], "facets": []}

        for link in self._serialize_feed_links(feed):
            link_data["links"].append(link)

        for facet in self._serialize_facet_links(feed):
            link_data["facets"].append(facet)

        for sort_link in self._serialize_sort_links(feed):
            link_data["links"].append(sort_link)

        serialized.update(link_data)

        return self.to_string(serialized)

    def _serialize_metadata(self, feed: FeedData) -> dict[str, Any]:
        fmeta = feed.metadata
        metadata: dict[str, Any] = {}
        if fmeta.title:
            metadata["title"] = fmeta.title
        if fmeta.items_per_page is not None:
            metadata["itemsPerPage"] = fmeta.items_per_page
        return metadata

    def serialize_opds_message(self, entry: OPDSMessage) -> dict[str, Any]:
        return dict(urn=entry.urn, description=entry.message)

    def serialize_work_entry(self, data: WorkEntryData) -> dict[str, Any]:
        metadata: dict[str, Any] = {}
        if data.additional_type:
            metadata["@type"] = data.additional_type

        if data.title:
            metadata["title"] = data.title
        if data.sort_title:
            metadata["sortAs"] = data.sort_title
        if data.duration is not None:
            metadata["duration"] = data.duration

        if data.subtitle:
            metadata["subtitle"] = data.subtitle
        if data.identifier:
            metadata["identifier"] = data.identifier
        if data.language:
            metadata["language"] = data.language
        if data.updated:
            metadata["modified"] = data.updated
        if data.published:
            metadata["published"] = data.published
        if data.summary:
            metadata["description"] = data.summary.text

        if data.publisher:
            metadata["publisher"] = dict(name=data.publisher)
        if data.imprint:
            metadata["imprint"] = dict(name=data.imprint)

        subjects = []
        if data.categories:
            for subject in data.categories:
                subjects.append(
                    {
                        "scheme": subject.scheme,
                        "name": subject.label,
                        "sortAs": subject.label,  # Same as above, don't think we have an alternate
                    }
                )
            metadata["subject"] = subjects

        if data.series:
            position = int(data.series.position) if data.series.position else 1
            metadata["belongsTo"] = dict(name=data.series.name, position=position)

        if len(data.authors):
            metadata["author"] = self._serialize_contributor(data.authors[0])
        for contributor in data.contributors:
            if role := MARC_CODE_TO_ROLES.get(contributor.role or "", None):
                metadata[role] = self._serialize_contributor(contributor)

        images = [self._serialize_link(link) for link in data.image_links]
        links = [self._serialize_link(link) for link in data.other_links]

        for acquisition in data.acquisition_links:
            links.append(self._serialize_acquisition_link(acquisition))

        publication = {"metadata": metadata, "links": links, "images": images}
        return publication

    def _serialize_link(self, link: Link) -> dict[str, Any]:
        serialized: dict[str, Any] = {"href": link.href, "rel": link.rel}
        if link.type:
            serialized["type"] = link.type
        if link.title:
            serialized["title"] = link.title

        if link.active_facet:
            serialized["rel"] = "self"

        if link.default_facet:
            properties: dict[str, Any] = dict()
            properties.update({PALACE_PROPERTIES_DEFAULT: "true"})
            serialized["properties"] = properties
        return serialized

    def _serialize_acquisition_link(self, link: Acquisition) -> dict[str, Any]:
        item = self._serialize_link(link)

        if link.templated:
            item["templated"] = True

        def _indirect(indirect: IndirectAcquisition) -> dict[str, Any]:
            result: dict[str, Any] = dict(type=indirect.type)
            if indirect.children:
                result["child"] = []
            for child in indirect.children:
                result["child"].append(_indirect(child))
            return result

        props: dict[str, Any] = {}
        if link.availability_status:
            state = link.availability_status
            if link.is_loan:
                state = "ready"
            elif link.is_hold:
                state = "reserved"
                # This only exists in the serializer because there is no case where cancellable is false,
                # that logic should be in the annotator if it ever occurs
                props["actions"] = dict(cancellable=True)
            props["availability"] = dict(state=state)
            if link.availability_since:
                props["availability"]["since"] = link.availability_since
            if link.availability_until:
                props["availability"]["until"] = link.availability_until

        if link.indirect_acquisitions:
            props["indirectAcquisition"] = []
        for indirect in link.indirect_acquisitions:
            props["indirectAcquisition"].append(_indirect(indirect))

        if link.lcp_hashed_passphrase:
            props["lcp_hashed_passphrase"] = link.lcp_hashed_passphrase

        if link.drm_licensor:
            props["licensor"] = {
                "clientToken": link.drm_licensor.client_token,
                "vendor": link.drm_licensor.vendor,
            }

        if props:
            item["properties"] = props

        return item

    def _serialize_contributor(self, author: Author) -> dict[str, Any]:
        result: dict[str, Any] = {"name": author.name}
        if author.sort_name:
            result["sortAs"] = author.sort_name
        if author.link:
            link = self._serialize_link(author.link)
            # OPDS2 does not need "title" in the link
            link.pop("title", None)
            result["links"] = [link]
        return result

    def content_type(self) -> str:
        return self.CONTENT_TYPE

    @classmethod
    def to_string(cls, data: dict[str, Any]) -> str:
        return json.dumps(data, indent=2)

    def _serialize_feed_links(self, feed: FeedData) -> list[dict[str, Any]]:
        links = []
        if feed.links:
            for link in feed.links:
                links.append(self._serialize_link(link))
        return links

    def _serialize_facet_links(self, feed: FeedData) -> list[dict[str, Any]]:
        results = []
        facet_links: dict[str, Any] = defaultdict(lambda: {"metadata": {}, "links": []})
        for link in feed.facet_links:
            # TODO: When we remove the facet-based sort links [PP-1814],
            # this check can be removed.
            if not is_sort_facet(link):
                group = link.facet_group
                if group:
                    facet_links[group]["links"].append(self._serialize_link(link))
                    facet_links[group]["metadata"]["title"] = group
        for _, facets in facet_links.items():
            results.append(facets)

        return results

    def _serialize_sort_links(self, feed: FeedData) -> list[dict[str, Any]]:
        sort_links = []
        # TODO: When we remove the facet-based sort links [PP-1814],
        # we'll want to pull the sort link data from the feed.sort_links once that is in place.
        if feed.facet_links:
            for link in feed.facet_links:
                if is_sort_facet(link):
                    sort_links.append(self._serialize_sort_link(link))
        return sort_links

    @classmethod
    def _serialize_sort_link(cls, link: Link) -> dict[str, Any]:
        sort_link: dict[str, Any] = {
            "href": link.href,
            "title": link.title,
            "rel": PALACE_REL_SORT,
        }

        properties: dict[str, str] = {}

        sort_link["properties"] = properties

        if link.active_facet:
            properties.update({PALACE_PROPERTIES_ACTIVE_SORT: "true"})

        if link.default_facet:
            properties.update({PALACE_PROPERTIES_DEFAULT: "true"})
        return sort_link
