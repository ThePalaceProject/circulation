from __future__ import annotations

import logging
from typing import Any

from werkzeug.datastructures import MIMEAccept

from palace.manager.core.exceptions import BasePalaceException
from palace.manager.feed.base import FeedInterface
from palace.manager.feed.facets.feed import FeaturedFacets
from palace.manager.feed.serializer.base import SerializerInterface
from palace.manager.feed.serializer.opds import (
    OPDS1Version1Serializer,
    OPDS1Version2Serializer,
)
from palace.manager.feed.serializer.opds2 import OPDS2Serializer
from palace.manager.feed.types import FeedData, WorkEntry
from palace.manager.util.flask_util import OPDSEntryResponse, OPDSFeedResponse
from palace.manager.util.log import LoggerMixin
from palace.manager.util.opds_writer import OPDSMessage


def get_serializer(
    mime_types: MIMEAccept | None,
) -> SerializerInterface[Any]:
    # Ordering matters for poor matches (eg. */*), so we will keep OPDS1 first
    serializers: dict[str, type[SerializerInterface[Any]]] = {
        "application/atom+xml": OPDS1Version1Serializer,
        "application/atom+xml; api-version=2": OPDS1Version2Serializer,
        "application/opds+json": OPDS2Serializer,
    }
    if mime_types:
        match = mime_types.best_match(
            serializers.keys(), default="application/atom+xml"
        )
        return serializers[match]()
    # Default
    return OPDS1Version1Serializer()


class BaseOPDSFeed(FeedInterface, LoggerMixin):
    def __init__(
        self,
        title: str,
        url: str,
        precomposed_entries: list[OPDSMessage] | None = None,
    ) -> None:
        self.url = url
        self.title = title
        self._precomposed_entries = precomposed_entries or []
        self._feed = FeedData()

    def add_link(self, href: str, rel: str | None = None, **kwargs: Any) -> None:
        self._feed.add_link(href, rel=rel, **kwargs)

    def as_response(
        self,
        mime_types: MIMEAccept | None = None,
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
        mime_types: MIMEAccept | None = None,
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
            response=serializer.to_string(
                serializer.serialize_work_entry(entry.computed)
            ),
            **response_kwargs,
        )
        if isinstance(serializer, OPDS2Serializer):
            # Only OPDS2 has the same content type for feed and entry
            response.content_type = serializer.content_type()
        return response


class UnfulfillableWork(BasePalaceException):
    """Raise this exception when it turns out a Work currently cannot be
    fulfilled through any means, *and* this is a problem sufficient to
    cancel the creation of an <entry> for the Work.

    For commercial works, this might be because the collection
    contains no licenses. For open-access works, it might be because
    none of the delivery mechanisms could be mirrored.
    """


class NavigationFacets(FeaturedFacets):
    pass
