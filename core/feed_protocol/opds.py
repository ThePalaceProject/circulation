from __future__ import annotations

import logging
from typing import Any, List, Optional

from core.feed_protocol.base import FeedInterface
from core.feed_protocol.serializer.opds import OPDS1Serializer
from core.feed_protocol.types import FeedData, WorkEntry
from core.util.flask_util import OPDSEntryResponse, OPDSFeedResponse


class BaseOPDSFeed(FeedInterface):
    def __init__(
        self, title: str, url: str, precomposed_entries: Optional[List[Any]] = None
    ) -> None:
        self.url = url
        self.title = title
        self._precomposed_entries = precomposed_entries or []
        self._feed = FeedData()
        self._serializer = OPDS1Serializer()
        self.log = logging.getLogger(self.__class__.__name__)

    def generate_feed(self) -> None:
        raise NotImplementedError()

    def serialize(self) -> bytes:
        return self._serializer.serialize_feed(self._feed)

    def add_link(self, href: str, rel: Optional[str] = None, **kwargs: Any) -> None:
        self._feed.add_link(href, rel=rel, **kwargs)

    def as_response(self, **kwargs: Any) -> OPDSFeedResponse:
        """Serialize the feed using the serializer protocol"""
        return OPDSFeedResponse(
            self._serializer.serialize_feed(
                self._feed, precomposed_entries=self._precomposed_entries
            ),
            **kwargs,
        )

    @classmethod
    def entry_as_response(
        cls, entry: WorkEntry, **response_kwargs: Any
    ) -> OPDSEntryResponse:
        if not entry.computed:
            logging.getLogger().error(f"Entry data has not been generated for {entry}")
            raise ValueError(f"Entry data has not been generated")
        serializer = OPDS1Serializer()
        return OPDSEntryResponse(
            response=serializer.serialize_work_entry(entry.computed), **response_kwargs
        )
