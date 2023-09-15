from flask import Request

from core.feed.opds import get_serializer
from core.feed.serializer.opds import OPDS1Serializer
from core.feed.serializer.opds2 import OPDS2Serializer


class TestBaseOPDSFeed:
    def test_get_serializer(self):
        # The q-value should take priority
        request = Request.from_values(
            headers=dict(
                Accept="application/atom+xml;q=0.8,application/opds+json;q=0.9"
            )
        )
        assert isinstance(get_serializer(request.accept_mimetypes), OPDS2Serializer)

        # Multiple additional key-value pairs don't matter
        request = Request.from_values(
            headers=dict(
                Accept="application/atom+xml;profile=opds-catalog;kind=acquisition;q=0.08, application/opds+json;q=0.9"
            )
        )
        assert isinstance(get_serializer(request.accept_mimetypes), OPDS2Serializer)

        request = Request.from_values(
            headers=dict(
                Accept="application/atom+xml;profile=opds-catalog;kind=acquisition"
            )
        )
        assert isinstance(get_serializer(request.accept_mimetypes), OPDS1Serializer)

        # The default q-value should be 1, but opds2 specificity is higher
        request = Request.from_values(
            headers=dict(
                Accept="application/atom+xml;profile=feed,application/opds+json;q=0.9"
            )
        )
        assert isinstance(get_serializer(request.accept_mimetypes), OPDS2Serializer)

        # The default q-value should sort above 0.9
        request = Request.from_values(
            headers=dict(Accept="application/opds+json;q=0.9,application/atom+xml")
        )
        assert isinstance(get_serializer(request.accept_mimetypes), OPDS1Serializer)

        # Same q-values respect order of definition in the code
        request = Request.from_values(
            headers=dict(
                Accept="application/opds+json;q=0.9,application/atom+xml;q=0.9"
            )
        )
        assert isinstance(get_serializer(request.accept_mimetypes), OPDS1Serializer)

        # No valid accept mimetype should default to OPDS1.x
        request = Request.from_values(headers=dict(Accept="text/html"))
        assert isinstance(get_serializer(request.accept_mimetypes), OPDS1Serializer)
