import json
from collections import defaultdict
from typing import Any

from core.feed.serializer.base import SerializerInterface
from core.feed.types import (
    Acquisition,
    Author,
    FeedData,
    IndirectAcquisition,
    Link,
    WorkEntryData,
)
from core.model import Contributor
from core.util.opds_writer import OPDSMessage

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


class OPDS2Serializer(SerializerInterface[dict[str, Any]]):
    def __init__(self) -> None:
        pass

    def serialize_feed(
        self, feed: FeedData, precomposed_entries: list[Any] | None = None
    ) -> str:
        serialized: dict[str, Any] = {"publications": []}
        serialized["metadata"] = self._serialize_metadata(feed)

        for entry in feed.entries:
            if entry.computed:
                publication = self.serialize_work_entry(entry.computed)
                serialized["publications"].append(publication)

        serialized.update(self._serialize_feed_links(feed))

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
        if data.additionalType:
            metadata["@type"] = data.additionalType

        if data.title:
            metadata["title"] = data.title.text
        if data.sort_title:
            metadata["sortAs"] = data.sort_title.text
        if data.duration is not None:
            metadata["duration"] = data.duration

        if data.subtitle:
            metadata["subtitle"] = data.subtitle.text
        if data.identifier:
            metadata["identifier"] = data.identifier
        if data.language:
            metadata["language"] = data.language.text
        if data.updated:
            metadata["modified"] = data.updated.text
        if data.published:
            metadata["published"] = data.published.text
        if data.summary:
            metadata["description"] = data.summary.text

        if data.publisher:
            metadata["publisher"] = dict(name=data.publisher.text)
        if data.imprint:
            metadata["imprint"] = dict(name=data.imprint.text)

        subjects = []
        if data.categories:
            for subject in data.categories:
                subjects.append(
                    {
                        "scheme": subject.scheme,  # type: ignore[attr-defined]
                        "name": subject.label,  # type: ignore[attr-defined]
                        "sortAs": subject.label,  # type: ignore[attr-defined] # Same as above, don't think we have an alternate
                    }
                )
            metadata["subject"] = subjects

        if data.series:
            name = getattr(data.series, "name", None)
            position = int(getattr(data.series, "position", 1))
            if name:
                metadata["belongsTo"] = dict(name=name, position=position)

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
        serialized = {"href": link.href, "rel": link.rel}
        if link.type:
            serialized["type"] = link.type
        if link.title:
            serialized["title"] = link.title
        return serialized

    def _serialize_acquisition_link(self, link: Acquisition) -> dict[str, Any]:
        item = self._serialize_link(link)

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
            props["lcp_hashed_passphrase"] = link.lcp_hashed_passphrase.text

        if link.drm_licensor:
            props["licensor"] = {
                "clientToken": getattr(
                    getattr(link.drm_licensor, "clientToken"), "text"
                ),
                "vendor": getattr(link.drm_licensor, "vendor"),
            }

        if props:
            item["properties"] = props

        return item

    def _serialize_feed_links(self, feed: FeedData) -> dict[str, Any]:
        link_data: dict[str, list[dict[str, Any]]] = {"links": [], "facets": []}
        for link in feed.links:
            link_data["links"].append(self._serialize_link(link))

        facet_links: dict[str, Any] = defaultdict(lambda: {"metadata": {}, "links": []})
        for link in feed.facet_links:
            group = getattr(link, "facetGroup", None)
            if group:
                facet_links[group]["links"].append(self._serialize_link(link))
                facet_links[group]["metadata"]["title"] = group
        for _, facets in facet_links.items():
            link_data["facets"].append(facets)

        return link_data

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
        return "application/opds+json"

    @classmethod
    def to_string(cls, data: dict[str, Any]) -> str:
        return json.dumps(data, indent=2)
