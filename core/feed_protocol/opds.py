from __future__ import annotations

import logging
from typing import Any, List, Optional

from core.feed_protocol.base import FeedInterface
from core.feed_protocol.serializer.opds import OPDS1Serializer
from core.feed_protocol.serializer.opds2 import OPDS2Serializer
from core.feed_protocol.types import FeedData, WorkEntry
from core.util.flask_util import OPDSEntryResponse, OPDSFeedResponse

def get_serializer(content_type):
    if content_type and "json" in content_type:
        return OPDS2Serializer()
    else:
        return OPDS1Serializer()

class BaseOPDSFeed(FeedInterface):
    def __init__(
        self, title: str, url: str, precomposed_entries: Optional[List[Any]] = None
    ) -> None:
        self.url = url
        self.title = title
        self._precomposed_entries = precomposed_entries or []
        self._feed = FeedData()
        self.log = logging.getLogger(self.__class__.__name__)

    def serialize(self) -> bytes:
        return self._serializer.serialize_feed(self._feed)

    def add_link(self, href: str, rel: Optional[str] = None, **kwargs: Any) -> None:
        self._feed.add_link(href, rel=rel, **kwargs)

    def as_response(
        self, requested_content_type: Optional[str] = None, **kwargs: Any
    ) -> OPDSFeedResponse:
        """Serialize the feed using the serializer protocol"""
        serializer = get_serializer(requested_content_type)
        return OPDSFeedResponse(
            serializer.serialize_feed(
                self._feed, precomposed_entries=self._precomposed_entries
            ),
            content_type=serializer.content_type(),
            **kwargs,
        )

    @classmethod
    def entry_as_response(
        cls,
        entry: WorkEntry,
        requested_content_type: Optional[str] = None,
        **response_kwargs: Any,
    ) -> OPDSEntryResponse:
        if not entry.computed:
            logging.getLogger().error(f"Entry data has not been generated for {entry}")
            raise ValueError(f"Entry data has not been generated")
        serializer = get_serializer(requested_content_type)
        response = OPDSEntryResponse(
            response=serializer.serialize_work_entry(entry.computed),
            **response_kwargs,
        )
        if requested_content_type and "json" in requested_content_type:
            # Only OPDS2 has the same content type for feed and entry
            response.content_type = serializer.content_type()
        return response
