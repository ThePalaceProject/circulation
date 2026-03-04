from __future__ import annotations

import feedparser

from palace.manager.feed.serializer.opds2 import OPDS2Serializer
from palace.manager.util.opds_writer import AtomFeed, OPDSFeed


class OPDSSerializationTestHelper:
    OPDS2_CONTENT_TYPE = OPDS2Serializer.content_type()
    OPDS2_PUBLICATION_CONTENT_TYPE = OPDS2Serializer.entry_content_type()
    PARAMETRIZED_SINGLE_ENTRY_ACCEPT_HEADERS = (
        "accept_header,expected_content_type",
        [
            (None, OPDSFeed.ENTRY_TYPE),
            ("default-foo-bar", OPDSFeed.ENTRY_TYPE),
            (AtomFeed.ATOM_TYPE, OPDSFeed.ENTRY_TYPE),
            (OPDS2_CONTENT_TYPE, OPDS2_PUBLICATION_CONTENT_TYPE),
            (OPDS2_PUBLICATION_CONTENT_TYPE, OPDS2_PUBLICATION_CONTENT_TYPE),
        ],
    )
    PARAMETRIZED_NAVIGATION_ACCEPT_HEADERS = (
        "accept_header,expected_content_type",
        [
            (None, OPDSFeed.NAVIGATION_FEED_TYPE),
            ("default-foo-bar", OPDSFeed.NAVIGATION_FEED_TYPE),
            (AtomFeed.ATOM_TYPE, OPDSFeed.NAVIGATION_FEED_TYPE),
            (OPDS2_CONTENT_TYPE, OPDS2_CONTENT_TYPE),
            (OPDS2_PUBLICATION_CONTENT_TYPE, OPDS2_CONTENT_TYPE),
        ],
    )

    def __init__(
        self,
        accept_header: str | None = None,
        expected_content_type: str | None = None,
    ):
        self.accept_header = accept_header
        self.expected_content_type = expected_content_type

    def merge_accept_header(self, headers):
        return headers | ({"Accept": self.accept_header} if self.accept_header else {})

    def verify_and_get_single_entry_feed_links(self, response):
        assert response.content_type == self.expected_content_type
        if self.expected_content_type == OPDSFeed.ENTRY_TYPE:
            feed = feedparser.parse(response.get_data())
            [entry] = feed["entries"]
        elif self.expected_content_type == self.OPDS2_PUBLICATION_CONTENT_TYPE:
            entry = response.get_json()
        else:
            assert (
                False
            ), f"Unexpected content type prefix: {self.expected_content_type}"

        # Ensure that the response content parsed correctly.
        assert "links" in entry
        return entry["links"]
