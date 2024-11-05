import pytest
from flask import Request

from palace.manager.feed.opds import get_serializer
from palace.manager.feed.serializer.opds import (
    OPDS1Version1Serializer,
    OPDS1Version2Serializer,
)
from palace.manager.feed.serializer.opds2 import OPDS2Serializer


class TestBaseOPDSFeed:

    @pytest.mark.parametrize(
        "accept_header, serializer",
        [
            # test api-version parameter when specified return the appropriate version
            ["application/atom+xml;", OPDS1Version1Serializer],
            ["application/atom+xml;api-version=1", OPDS1Version1Serializer],
            ["application/atom+xml;api-version=2", OPDS1Version2Serializer],
            # test exact matches
            ["application/atom+xml", OPDS1Version1Serializer],
            ["application/opds+json", OPDS2Serializer],
            # The q - value should take priority
            ["application/atom+xml;q=0.8,application/opds+json;q=0.9", OPDS2Serializer],
            # Multiple additional key-value pairs don't matter
            [
                "application/atom+xml;profile=opds-catalog;kind=acquisition;q=0.08, application/opds+json;q=0.9",
                OPDS2Serializer,
            ],
            [
                "application/atom+xml;profile=opds-catalog;kind=acquisition",
                OPDS1Version1Serializer,
            ],
            # The default q-value should be 1, but opds2 specificity is higher
            [
                "application/atom+xml;profile=feed,application/opds+json;q=0.9",
                OPDS2Serializer,
            ],
            # The default q-value should sort above 0.9
            [
                "application/opds+json;q=0.9,application/atom+xml",
                OPDS1Version1Serializer,
            ],
            # Same q-values respect order of definition in the code
            [
                "application/opds+json;q=0.9,application/atom+xml;q=0.9",
                OPDS1Version1Serializer,
            ],
            # test api-version parameter when specified return the appropriate version
            ["application/atom+xml;api-version=1", OPDS1Version1Serializer],
            # complex multi value mimetype
            [
                "application/atom+xml;profile=opds-catalog;kind=acquisition;q=0.08, application/opds+json;api-version=2;q=0.84, application/atom+xml;profile=opds-catalog;kind=acquisition;api-version=2;q=0.9",
                OPDS1Version1Serializer,
            ],
            # No valid accept mimetype should default to OPDS1.x
            ["text/html", OPDS1Version1Serializer],
        ],
    )
    def test_get_serializer1(self, accept_header: str, serializer):
        request = Request.from_values(headers=dict(Accept=accept_header))
        assert isinstance(get_serializer(request.accept_mimetypes), serializer)
