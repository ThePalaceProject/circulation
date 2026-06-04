import json

import pytest
from pydantic import ValidationError

from palace.opds.authentication.document import (
    AUTH_DOCUMENT_MEDIA_TYPE,
    AUTH_DOCUMENT_REL,
    AuthenticateLink,
    Authentication,
    AuthenticationDocument,
    AuthenticationLabels,
    PalaceAuthentication,
    PalaceAuthenticationDocument,
)
from palace.opds.authentication.palace import (
    AnnouncementDocument,
    AuthenticationInput,
    AuthenticationInputs,
    Features,
    LocalizedValue,
    PublicKey,
    WebColorScheme,
)
from palace.opds.rwpm import Link
from palace.util.exceptions import PalaceValueError


class TestAuthenticationDocument:
    """The spec model is used to parse authentication documents from remote
    servers, so it should be lenient about extra fields and missing optionals.
    """

    def test_parse_remote_document(self) -> None:
        # A document that includes Palace-specific extension fields that the
        # spec model does not know about should still parse.
        raw = json.dumps(
            {
                "id": "http://example.com/auth",
                "title": "Remote Library",
                "service_description": "not a spec field",
                "authentication": [
                    {
                        "type": "http://opds-spec.org/auth/oauth/client_credentials",
                        "links": [
                            {"href": "http://example.com/token", "rel": "authenticate"}
                        ],
                        "description": "also not a spec field",
                    }
                ],
            }
        )
        document = AuthenticationDocument.model_validate_json(raw)
        auth = document.by_type("http://opds-spec.org/auth/oauth/client_credentials")
        assert (
            auth.links.get(rel="authenticate", raising=True).href
            == "http://example.com/token"
        )

    def test_by_type_missing(self) -> None:
        document = AuthenticationDocument(
            id="http://example.com/auth",
            title="Library",
            authentication=[Authentication(type="some-type")],
        )
        with pytest.raises(PalaceValueError, match="some-other-type"):
            document.by_type("some-other-type")

    def test_validate_authentication_empty(self) -> None:
        # The spec requires the ``authentication`` key but imposes no minItems,
        # so an empty list is valid (e.g. a library with no/misconfigured auth
        # providers). It must not raise.
        document = AuthenticationDocument(
            id="http://example.com/auth", title="Library", authentication=[]
        )
        assert document.authentication == []

    def test_validate_authentication_duplicate_types(self) -> None:
        with pytest.raises(ValidationError, match="Duplicate authentication type"):
            AuthenticationDocument(
                id="http://example.com/auth",
                title="Library",
                authentication=[
                    Authentication(type="same"),
                    Authentication(type="same"),
                ],
            )

    def test_content_type(self) -> None:
        assert (
            AuthenticationDocument.content_type()
            == "application/opds-authentication+json"
        )
        assert AUTH_DOCUMENT_MEDIA_TYPE in AuthenticationDocument.content_types()
        assert AUTH_DOCUMENT_REL == "http://opds-spec.org/auth/document"


class TestAuthenticateLink:
    def test_localized_arrays_always_present(self) -> None:
        # Even with no localized values set, all five arrays are serialized.
        link = AuthenticateLink(rel="authenticate", href="http://example.com/auth")
        data = link.serialize()
        for field in (
            "display_names",
            "descriptions",
            "information_urls",
            "privacy_statement_urls",
            "logo_urls",
        ):
            assert data[field] == []
        # The link should not leak an empty properties object.
        assert "properties" not in data

    def test_populated(self) -> None:
        link = AuthenticateLink(
            rel="authenticate",
            href="http://example.com/auth",
            display_names=[LocalizedValue(value="Name", language="en")],
            logo_urls=[LocalizedValue(value="http://example.com/logo.png")],
        )
        data = link.serialize()
        assert data["display_names"] == [{"value": "Name", "language": "en"}]
        # A logo without dimensions omits height/width.
        assert data["logo_urls"] == [{"value": "http://example.com/logo.png"}]

    def test_plain_link_has_no_localized_keys(self) -> None:
        # A plain link (e.g. logout) does not emit the localized arrays.
        link = Link(rel="logout", href="http://example.com/logout", templated=True)
        data = link.serialize()
        assert "display_names" not in data
        assert "properties" not in data
        assert data["templated"] is True


class TestPalaceAuthentication:
    def test_basic_flow_drops_empty_links(self) -> None:
        auth = PalaceAuthentication(
            type="http://opds-spec.org/auth/basic",
            description="Library Barcode",
            labels=AuthenticationLabels(login="Barcode", password="PIN"),
            inputs=AuthenticationInputs(
                login=AuthenticationInput(keyboard="Default"),
                password=AuthenticationInput(keyboard="NumberPad", maximum_length=4),
            ),
        )
        data = auth.serialize()
        assert data["labels"] == {"login": "Barcode", "password": "PIN"}
        assert data["inputs"]["password"]["maximum_length"] == 4
        # No links were supplied, so the key is dropped entirely.
        assert "links" not in data

    def test_flow_with_links_drops_empty_labels_and_inputs(self) -> None:
        auth = PalaceAuthentication(
            type="http://opds-spec.org/auth/oidc",
            description="OIDC",
            links=[AuthenticateLink(rel="authenticate", href="http://example.com/a")],
        )
        data = auth.serialize()
        assert "labels" not in data
        assert "inputs" not in data
        assert len(data["links"]) == 1


class TestFeatures:
    def test_both_arrays_always_present(self) -> None:
        assert Features().serialize() == {"enabled": [], "disabled": []}
        assert Features(enabled=("a",)).serialize() == {
            "enabled": ["a"],
            "disabled": [],
        }


class TestPublicKey:
    def test_type_always_present(self) -> None:
        # ``type`` defaults to "RSA" and is always serialized.
        assert PublicKey(value="abc").serialize() == {"type": "RSA", "value": "abc"}


class TestPalaceAuthenticationDocument:
    def _minimal(self, **kwargs: object) -> PalaceAuthenticationDocument:
        params: dict[str, object] = dict(
            id="http://example.com/auth",
            title="Library",
            authentication=[
                PalaceAuthentication(
                    type="http://opds-spec.org/auth/basic", description="Basic"
                )
            ],
        )
        params.update(kwargs)
        return PalaceAuthenticationDocument(**params)

    def test_features_and_announcements_always_present(self) -> None:
        data = self._minimal().serialize()
        assert data["features"] == {"enabled": [], "disabled": []}
        assert data["announcements"] == []

    def test_optional_fields_dropped_when_unset(self) -> None:
        data = self._minimal().serialize()
        for field in (
            "color_scheme",
            "web_color_scheme",
            "service_description",
            "public_key",
            "description",
        ):
            assert field not in data

    def test_full_document(self) -> None:
        data = self._minimal(
            color_scheme="blue",
            web_color_scheme=WebColorScheme(
                primary="#012345",
                secondary="#abcdef",
                background="#012345",
                foreground="#abcdef",
            ),
            service_description="A test library",
            public_key=PublicKey(value="abc"),
            features=Features(enabled=("reservations",)),
            announcements=(AnnouncementDocument(id="1", content="Hello"),),
        ).serialize()

        assert data["color_scheme"] == "blue"
        assert data["web_color_scheme"]["primary"] == "#012345"
        assert data["service_description"] == "A test library"
        assert data["public_key"] == {"type": "RSA", "value": "abc"}
        assert data["features"] == {"enabled": ["reservations"], "disabled": []}
        assert data["announcements"] == [{"id": "1", "content": "Hello"}]
        # The spec ``description`` field is never emitted; we use
        # ``service_description`` instead.
        assert "description" not in data
