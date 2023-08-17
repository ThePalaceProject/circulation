from __future__ import annotations

from typing import TYPE_CHECKING

from flask import Response

from core.feed_protocol.base import FeedProtocol
from core.feed_protocol.serializer.opds import OPDS1Serializer
from core.feed_protocol.types import FeedData
from core.util.flask_util import OPDSFeedResponse

if TYPE_CHECKING:
    pass


class OPDSFeedProtocol(FeedProtocol):
    def __init__(
        self,
        title,
        url,
    ) -> None:
        self.url = url
        self.title = title
        self._feed = FeedData()
        self._serializer = OPDS1Serializer()

    def serialize(self):
        return self._serializer.serialize_feed(self._feed)

    def add_link(self, href, rel=None, **kwargs):
        self._feed.add_link(href, rel=rel, **kwargs)

    def as_response(self, **kwargs) -> Response:
        """Serialize the feed using the serializer protocol"""
        return OPDSFeedResponse(self._serializer.serialize_feed(self._feed), **kwargs)
