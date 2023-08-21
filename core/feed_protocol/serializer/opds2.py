import json
from typing import Any, Dict

from core.feed_protocol.types import (
    Acquisition,
    FeedData,
    IndirectAcquisition,
    Link,
    WorkEntryData,
)

AVAILABILITY_STATES = {
    "available": "ready",
    "unavailable": "unavailable",
}


class OPDS2Serializer:
    def __init__(self) -> None:
        pass

    def serialize_feed(self, feed: FeedData) -> bytes:
        serialized: Dict[str, Any] = {"publications": []}
        for entry in feed.entries:
            if entry.computed:
                publication = self._serialize_work_entry(entry.computed)
                serialized["publications"].append(publication)

        return json.dumps(serialized, indent=2).encode()

    def _serialize_work_entry(self, data: WorkEntryData) -> Dict[str, Any]:
        metadata: Dict[str, Any] = {}
        if data.title:
            metadata["title"] = data.title.text
            metadata["sortAs"] = data.title.text  # TODO: Change this!

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
                        "sortAs": subject.label,  # type: ignore[attr-defined] # TODO: Change this!
                    }
                )
            metadata["subject"] = subjects

        if data.series:
            name = getattr(data.series, "name", None)
            position = getattr(data.series, "position", 1)
            if name:
                metadata["belongsTo"] = dict(name=name, position=position)

        images = [self._serialize_link(link) for link in data.image_links]
        links = [self._serialize_link(link) for link in data.other_links]

        for acquisition in data.acquisition_links:
            links.append(self._serialize_acquisition_links(acquisition))

        publication = {"metadata": metadata, "links": links, "images": images}
        return publication

    def _serialize_link(self, link: Link) -> Dict[str, Any]:
        serialized = {"href": link.href, "rel": link.rel}
        if link.type:
            serialized["type"] = link.type
        return serialized

    def _serialize_acquisition_link(self, link: Acquisition):
        item = self._serialize_link(link)

        def _indirect(indirect: IndirectAcquisition):
            indirect = dict(type=indirect.type)
            if indirect.children:
                indirect["child"] = []
            for child in indirect.children:
                indirect["child"].append(_indirect(child))
            return indirect

        props = {}
        if link.availability_status:
            props["availability"] = dict(
                state=AVAILABILITY_STATES[link.availability_status]
            )
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
                "clientToken": getattr(link.drm_licensor, "clientToken"),
                "vendor": getattr(link.drm_licensor, "vendor"),
            }

        if props:
            item["properties"] = props

        return item
