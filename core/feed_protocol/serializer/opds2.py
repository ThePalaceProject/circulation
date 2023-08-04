import json

from core.feed_protocol.types import FeedData, Link, WorkEntryData


class OPDS2Serializer:
    def __init__(self) -> None:
        pass

    def serialize_feed(self, feed: FeedData):
        serialized = {"publications": []}
        for entry in feed.entries:
            publication = self._serialize_work_entry(entry.computed)
            serialized["publications"].append(publication)

        return json.dumps(serialized, indent=2).encode()

    def _serialize_work_entry(self, data: WorkEntryData):
        metadata = {}
        metadata["title"] = data.title.text
        metadata["sortAs"] = data.title.text  # TODO: Change this!
        metadata["subtitle"] = data.subtitle.text
        metadata["identifier"] = data.identifier
        metadata["language"] = data.language.text
        metadata["modified"] = data.updated.text
        metadata["published"] = data.published.text
        metadata["description"] = data.summary.text

        if data.publisher:
            metadata["publisher"] = dict(name=data.publisher.text)
        if data.imprint:
            metadata["imprint"] = dict(name=data.imprint.text)

        subjects = []
        for subject in data.categories:
            subjects.append(
                {
                    "scheme": subject.scheme,
                    "name": subject.label,
                    "sortAs": subject.label,  # TODO: Change this!
                }
            )
        metadata["subject"] = subjects

        images = [self._serialize_link(link) for link in data.image_links]
        links = [
            self._serialize_link(link)
            for link in data.acquisition_links + data.other_links
        ]

        publication = {"metadata": metadata, "links": links, "images": images}
        return publication

    def _serialize_link(self, link: Link):
        serialized = {"href": link.href, "rel": link.rel}
        if link.type:
            serialized["type"] = link.type
        return serialized

    # def _serialize_feed_entry(self, feed_entry: FeedEntryType) -> dict:
    #     metadata = {}
    #     links = []
    #     images = []

    #     for attrib, value in feed_entry:
    #         if isinstance(value, list):
    #             for item in value:
    #                 metadata[attrib] = self._serialize_feed_entry(item)
    #         elif isinstance(value, FeedEntryType):
    #             metadata[attrib] = self._serialize_feed_entry(value)
    #         else:
    #             metadata[attrib] = value
    #             print(attrib, value, metadata)

    #     return metadata
