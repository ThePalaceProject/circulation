from flask import Request

from core.feed_protocol.opds import get_serializer
from core.feed_protocol.serializer.opds import OPDS1Serializer
from core.feed_protocol.serializer.opds2 import OPDS2Serializer


class TestBaseOPDSFeed:
    def test_get_serializer(self):
        # The q-value should take priority
        request = Request.from_values(
            headers=dict(
                Accept="application/atom+xml;q=0.8,application/opds+json;q=0.9"
            )
        )
        assert isinstance(get_serializer(request.accept_mimetypes), OPDS2Serializer)

        # The default q-value should be 1
        request = Request.from_values(
            headers=dict(
                Accept="application/atom+xml;profile=feed,application/opds+json;q=0.9"
            )
        )
        assert isinstance(get_serializer(request.accept_mimetypes), OPDS1Serializer)

        # The default q-value should sort above 0.9
        request = Request.from_values(
            headers=dict(Accept="application/opds+json;q=0.9,application/atom+xml")
        )
        assert isinstance(get_serializer(request.accept_mimetypes), OPDS1Serializer)

        # Same q-values respect order fo arrival
        request = Request.from_values(
            headers=dict(
                Accept="application/opds+json;q=0.9,application/atom+xml;q=0.9"
            )
        )
        assert isinstance(get_serializer(request.accept_mimetypes), OPDS2Serializer)

        # No valid accept mimetype should default to OPDS1.x
        request = Request.from_values(headers=dict(Accept="text/html"))
        assert isinstance(get_serializer(request.accept_mimetypes), OPDS1Serializer)

        # Sorting occurs on list type values
        assert isinstance(
            get_serializer(
                [("application/opds+json", 0.9), ("application/atom+xml", 1)]
            ),
            OPDS1Serializer,
        )
