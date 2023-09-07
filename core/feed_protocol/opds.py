from __future__ import annotations

import logging
from typing import Any, List, Optional, Tuple

from werkzeug.datastructures import MIMEAccept

from core.feed_protocol.base import FeedInterface
from core.feed_protocol.serializer.opds import OPDS1Serializer
from core.feed_protocol.serializer.opds2 import OPDS2Serializer
from core.feed_protocol.types import FeedData, WorkEntry
from core.util.flask_util import OPDSEntryResponse, OPDSFeedResponse
from core.util.opds_writer import OPDSMessage


def get_serializer(
    mime_types: Optional[MIMEAccept | List[Tuple[str, float]]],
) -> OPDS1Serializer | OPDS2Serializer:
    # Loop through and return whichever mimetype is encountered first
    # Sort values by q-value first
    if mime_types is not None:
        mime_types = sorted(mime_types, key=lambda mime: mime[1], reverse=True)  # type: ignore[arg-type]
    for mime in mime_types or []:
        if "application/opds+json" in mime[0]:
            return OPDS2Serializer()
        elif "application/atom+xml" in mime[0]:
            return OPDS1Serializer()
    # Default
    return OPDS1Serializer()


class BaseOPDSFeed(FeedInterface):
    def __init__(
        self,
        title: str,
        url: str,
        precomposed_entries: Optional[List[OPDSMessage]] = None,
    ) -> None:
        self.url = url
        self.title = title
        self._precomposed_entries = precomposed_entries or []
        self._feed = FeedData()
        self.log = logging.getLogger(self.__class__.__name__)

    def serialize(
        self, mime_types: Optional[MIMEAccept | List[Tuple[str, float]]] = None
    ) -> bytes:
        serializer = get_serializer(mime_types)
        return serializer.serialize_feed(self._feed)

    def add_link(self, href: str, rel: Optional[str] = None, **kwargs: Any) -> None:
        self._feed.add_link(href, rel=rel, **kwargs)

    def as_response(
        self,
        mime_types: Optional[MIMEAccept | List[Tuple[str, float]]] = None,
        **kwargs: Any,
    ) -> OPDSFeedResponse:
        """Serialize the feed using the serializer protocol"""
        serializer = get_serializer(mime_types)
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
        mime_types: Optional[MIMEAccept | List[Tuple[str, float]]] = None,
        **response_kwargs: Any,
    ) -> OPDSEntryResponse:
        if not entry.computed:
            logging.getLogger().error(f"Entry data has not been generated for {entry}")
            raise ValueError(f"Entry data has not been generated")
        serializer = get_serializer(mime_types)
        response = OPDSEntryResponse(
            response=serializer.serialize_work_entry(entry.computed),
            **response_kwargs,
        )
        if isinstance(serializer, OPDS2Serializer):
            # Only OPDS2 has the same content type for feed and entry
            response.content_type = serializer.content_type()
        return response
