"""Tests for the standard Authentication for OPDS 1.0 models."""

import pytest

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.opds.authentication import (
    Authentication,
    AuthenticationDocument,
    AuthenticationLabels,
)
from palace.manager.opds.rwpm import Link


class TestAuthenticationLabels:
    def test_basic(self):
        labels = AuthenticationLabels(login="Barcode", password="PIN")
        assert labels.login == "Barcode"
        assert labels.password == "PIN"

    def test_serialization(self):
        labels = AuthenticationLabels(login="Username", password="Password")
        data = labels.model_dump()
        assert data == {"login": "Username", "password": "Password"}


class TestAuthentication:
    def test_basic(self):
        auth = Authentication(
            type="http://opds-spec.org/auth/basic",
            labels=AuthenticationLabels(login="Barcode", password="PIN"),
            links=[Link(href="http://example.com", rel="authenticate")],
        )
        assert auth.type == "http://opds-spec.org/auth/basic"
        assert auth.labels is not None
        assert auth.labels.login == "Barcode"

    def test_no_labels(self):
        auth = Authentication(
            type="http://example.com/auth",
            links=[],
        )
        assert auth.labels is None


class TestAuthenticationDocument:
    def test_constants(self):
        assert (
            AuthenticationDocument.MEDIA_TYPE
            == "application/vnd.opds.authentication.v1.0+json"
        )
        assert (
            AuthenticationDocument.LINK_RELATION == "http://opds-spec.org/auth/document"
        )

    def test_content_types(self):
        types = AuthenticationDocument.content_types()
        assert len(types) == 2
        assert "application/opds-authentication+json" in types

    def test_basic_document(self):
        doc = AuthenticationDocument(
            id="http://library.example.com",
            title="Test Library",
            authentication=[
                Authentication(
                    type="http://opds-spec.org/auth/basic",
                    links=[],
                )
            ],
        )
        assert doc.id == "http://library.example.com"
        assert doc.title == "Test Library"
        assert doc.description is None
        assert len(doc.authentication) == 1
        assert len(doc.links) == 0

    def test_validation_requires_authentication(self):
        with pytest.raises(ValueError, match="at least one"):
            AuthenticationDocument(
                id="http://example.com",
                title="Test",
                authentication=[],
            )

    def test_validation_rejects_duplicate_types(self):
        with pytest.raises(ValueError, match="Duplicate"):
            AuthenticationDocument(
                id="http://example.com",
                title="Test",
                authentication=[
                    Authentication(type="http://type1/", links=[]),
                    Authentication(type="http://type1/", links=[]),
                ],
            )

    def test_by_type(self):
        auth1 = Authentication(type="http://type1/", links=[])
        auth2 = Authentication(type="http://type2/", links=[])
        doc = AuthenticationDocument(
            id="http://example.com",
            title="Test",
            authentication=[auth1, auth2],
        )
        assert doc.by_type("http://type1/") is auth1
        assert doc.by_type("http://type2/") is auth2

    def test_by_type_not_found(self):
        doc = AuthenticationDocument(
            id="http://example.com",
            title="Test",
            authentication=[Authentication(type="http://type1/", links=[])],
        )
        with pytest.raises(PalaceValueError):
            doc.by_type("http://nonexistent/")

    def test_serialization_round_trip(self):
        doc = AuthenticationDocument(
            id="http://example.com",
            title="Test Library",
            description="A test library",
            authentication=[
                Authentication(
                    type="http://opds-spec.org/auth/basic",
                    labels=AuthenticationLabels(login="Barcode", password="PIN"),
                    links=[Link(href="http://example.com/auth", rel="authenticate")],
                )
            ],
            links=[Link(href="http://example.com/start", rel="start")],
        )
        data = doc.model_dump(mode="json", exclude_none=True)
        parsed = AuthenticationDocument.model_validate(data)
        assert parsed.id == doc.id
        assert parsed.title == doc.title
        assert parsed.description == doc.description
        assert len(parsed.authentication) == 1
        assert parsed.authentication[0].type == "http://opds-spec.org/auth/basic"
