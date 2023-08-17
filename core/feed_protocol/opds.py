from __future__ import annotations

from typing import TYPE_CHECKING

from flask import Response

from core.feed_protocol.base import FeedProtocol
from core.feed_protocol.serializer.opds import OPDS1Serializer
from core.feed_protocol.types import FeedData, WorkEntry
from core.util.flask_util import OPDSEntryResponse, OPDSFeedResponse

if TYPE_CHECKING:
    pass


class OPDSFeedProtocol(FeedProtocol):
    def __init__(self, title, url, precomposed_entries=None) -> None:
        self.url = url
        self.title = title
        self._precomposed_entries = precomposed_entries
        self._feed = FeedData()
        self._serializer = OPDS1Serializer()

    def generate_feed(self, work_entries):
        pass

    def serialize(self):
        return self._serializer.serialize_feed(self._feed)

    def add_link(self, href, rel=None, **kwargs):
        self._feed.add_link(href, rel=rel, **kwargs)

    def as_response(self, **kwargs) -> Response:
        """Serialize the feed using the serializer protocol"""
        return OPDSFeedResponse(
            self._serializer.serialize_feed(
                self._feed, precomposed_entries=self._precomposed_entries
            ),
            **kwargs,
        )

    @classmethod
    def entry_as_response(cls, entry: WorkEntry, **response_kwargs):
        serializer = OPDS1Serializer()
        return OPDSEntryResponse(
            response=serializer.serialize_work_entry(entry.computed), **response_kwargs
        )
