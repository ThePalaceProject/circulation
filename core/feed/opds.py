from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Type

from werkzeug.datastructures import MIMEAccept

from core.feed.base import FeedInterface
from core.feed.serializer.base import SerializerInterface
from core.feed.serializer.opds import OPDS1Serializer
from core.feed.serializer.opds2 import OPDS2Serializer
from core.feed.types import FeedData, WorkEntry
from core.util.flask_util import OPDSEntryResponse, OPDSFeedResponse
from core.util.opds_writer import OPDSMessage


def get_serializer(
    mime_types: Optional[MIMEAccept],
) -> SerializerInterface[Any]:
    # Loop through and return whichever mimetype is encountered first
    # Sort values by q-value first
    serializers: Dict[str, Type[SerializerInterface[Any]]] = {
        "application/opds+json": OPDS2Serializer,
        "application/atom+xml": OPDS1Serializer,
    }
    if mime_types:
        match = mime_types.best_match(
            serializers.keys(), default="application/atom+xml"
        )
        return serializers[match]()
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

    def serialize(self, mime_types: Optional[MIMEAccept] = None) -> bytes:
        serializer = get_serializer(mime_types)
        return serializer.serialize_feed(self._feed)

    def add_link(self, href: str, rel: Optional[str] = None, **kwargs: Any) -> None:
        self._feed.add_link(href, rel=rel, **kwargs)

    def as_response(
        self,
        mime_types: Optional[MIMEAccept] = None,
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
        entry: WorkEntry | OPDSMessage,
        mime_types: Optional[MIMEAccept] = None,
        **response_kwargs: Any,
    ) -> OPDSEntryResponse:
        serializer = get_serializer(mime_types)
        if isinstance(entry, OPDSMessage):
            return OPDSEntryResponse(
                response=serializer.to_string(serializer.serialize_opds_message(entry)),
                status=entry.status_code,
                content_type=serializer.content_type(),
                **response_kwargs,
            )

        # A WorkEntry
        if not entry.computed:
            logging.getLogger().error(f"Entry data has not been generated for {entry}")
            raise ValueError(f"Entry data has not been generated")
        response = OPDSEntryResponse(
            response=serializer.serialize_work_entry(entry.computed),
            **response_kwargs,
        )
        if isinstance(serializer, OPDS2Serializer):
            # Only OPDS2 has the same content type for feed and entry
            response.content_type = serializer.content_type()
        return response
