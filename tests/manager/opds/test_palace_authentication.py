"""Tests for the Palace-specific authentication document extensions."""

from palace.manager.opds.authentication import AuthenticationLabels
from palace.manager.opds.palace_authentication import (
    AuthenticationAnnouncement,
    AuthenticationInputs,
    Features,
    InputDescriptor,
    LocalizedLogoUrl,
    LocalizedString,
    PalaceAuthentication,
    PalaceAuthenticationDocument,
    PalaceAuthenticationLink,
    PublicKey,
    WebColorScheme,
)


class TestLocalizedString:
    def test_basic(self):
        ls = LocalizedString(language="en", value="Hello")
        assert ls.language == "en"
        assert ls.value == "Hello"

    def test_serialization(self):
        ls = LocalizedString(language="es", value="Hola")
        data = ls.model_dump()
        assert data == {"language": "es", "value": "Hola"}


class TestLocalizedLogoUrl:
    def test_with_dimensions(self):
        logo = LocalizedLogoUrl(
            language="en", value="http://logo.png", height=16, width=16
        )
        assert logo.height == 16
        assert logo.width == 16

    def test_without_dimensions(self):
        logo = LocalizedLogoUrl(language="en", value="http://logo.png")
        assert logo.height is None
        assert logo.width is None


class TestPalaceAuthenticationLink:
    def test_basic_link(self):
        link = PalaceAuthenticationLink(
            href="http://example.com/auth",
            rel="authenticate",
        )
        assert link.href == "http://example.com/auth"
        assert link.rel == "authenticate"
        assert link.display_names is None

    def test_link_with_metadata(self):
        link = PalaceAuthenticationLink(
            href="http://example.com/auth",
            rel="authenticate",
            display_names=[LocalizedString(language="en", value="My IdP")],
            descriptions=[LocalizedString(language="en", value="Description")],
        )
        assert link.display_names is not None
        assert len(link.display_names) == 1
        assert link.display_names[0].value == "My IdP"


class TestInputDescriptor:
    def test_basic(self):
        desc = InputDescriptor(keyboard="Default")
        assert desc.keyboard == "Default"
        assert desc.maximum_length is None
        assert desc.barcode_format is None

    def test_with_all_fields(self):
        desc = InputDescriptor(
            keyboard="Number pad", maximum_length=14, barcode_format="Codabar"
        )
        assert desc.maximum_length == 14
        assert desc.barcode_format == "Codabar"


class TestPalaceAuthentication:
    def test_basic(self):
        auth = PalaceAuthentication(
            type="http://opds-spec.org/auth/basic",
            description="Library Card",
        )
        assert auth.type == "http://opds-spec.org/auth/basic"
        assert auth.description == "Library Card"
        assert auth.inputs is None
        assert len(auth.links) == 0

    def test_with_inputs(self):
        auth = PalaceAuthentication(
            type="http://opds-spec.org/auth/basic",
            description="Library Card",
            labels=AuthenticationLabels(login="Barcode", password="PIN"),
            inputs=AuthenticationInputs(
                login=InputDescriptor(keyboard="Default"),
                password=InputDescriptor(keyboard="Number pad"),
            ),
        )
        assert auth.labels is not None
        assert auth.labels.login == "Barcode"
        assert auth.inputs is not None
        assert auth.inputs.login.keyboard == "Default"

    def test_serialization(self):
        auth = PalaceAuthentication(
            type="http://opds-spec.org/auth/basic",
            description="Library Card",
            labels=AuthenticationLabels(login="Barcode", password="PIN"),
            inputs=AuthenticationInputs(
                login=InputDescriptor(keyboard="Default", maximum_length=14),
                password=InputDescriptor(keyboard="Number pad"),
            ),
            links=[
                PalaceAuthenticationLink(href="http://example.com/logo", rel="logo")
            ],
        )
        data = auth.model_dump(mode="json", exclude_none=True)
        assert data["type"] == "http://opds-spec.org/auth/basic"
        assert data["description"] == "Library Card"
        assert data["labels"] == {"login": "Barcode", "password": "PIN"}
        assert data["inputs"]["login"]["keyboard"] == "Default"
        assert data["inputs"]["login"]["maximum_length"] == 14
        assert "barcode_format" not in data["inputs"]["login"]
        assert len(data["links"]) == 1
        assert data["links"][0]["rel"] == "logo"
        assert data["links"][0]["href"] == "http://example.com/logo"


class TestPalaceAuthenticationDocument:
    def test_allows_empty_authentication(self):
        doc = PalaceAuthenticationDocument(
            id="http://example.com/auth",
            title="Open Access Library",
            authentication=[],
        )
        assert doc.authentication == []

    def test_basic_document(self):
        doc = PalaceAuthenticationDocument(
            id="http://example.com/auth",
            title="Test Library",
            authentication=[
                PalaceAuthentication(
                    type="http://opds-spec.org/auth/basic",
                    description="Library Card",
                )
            ],
        )
        assert doc.id == "http://example.com/auth"
        assert doc.title == "Test Library"
        assert doc.service_description is None
        assert doc.color_scheme is None

    def test_full_document(self):
        doc = PalaceAuthenticationDocument(
            id="http://example.com/auth",
            title="Test Library",
            authentication=[
                PalaceAuthentication(
                    type="http://opds-spec.org/auth/basic",
                    description="Library Card",
                )
            ],
            service_description="A great library",
            color_scheme="blue",
            web_color_scheme=WebColorScheme(
                primary="#000", secondary="#fff", background="#000", foreground="#fff"
            ),
            public_key=PublicKey(type="RSA", value="key-data"),
            features=Features(enabled=["reservations"], disabled=[]),
            announcements=[AuthenticationAnnouncement(id="1", content="Welcome!")],
        )
        assert doc.service_description == "A great library"
        assert doc.color_scheme == "blue"
        assert doc.web_color_scheme is not None
        assert doc.public_key is not None
        assert doc.features is not None
        assert len(doc.announcements) == 1

    def test_serialization_drops_falsy(self):
        """Falsy optional fields should be omitted from serialization."""
        doc = PalaceAuthenticationDocument(
            id="http://example.com/auth",
            title="Test Library",
            authentication=[
                PalaceAuthentication(
                    type="http://opds-spec.org/auth/basic",
                )
            ],
            features=Features(enabled=[], disabled=[]),
            announcements=[],
        )
        data = doc.model_dump(mode="json", exclude_none=True, exclude_unset=True)
        assert "service_description" not in data
        assert "color_scheme" not in data
        assert "web_color_scheme" not in data
        assert "public_key" not in data
        # features and announcements are always present when explicitly set
        assert data["features"] == {"enabled": [], "disabled": []}
        assert data["announcements"] == []

    def test_serialization_round_trip(self):
        doc = PalaceAuthenticationDocument(
            id="http://example.com/auth",
            title="Test Library",
            authentication=[
                PalaceAuthentication(
                    type="http://opds-spec.org/auth/basic",
                    description="Library Card",
                    labels=AuthenticationLabels(login="Barcode", password="PIN"),
                    inputs=AuthenticationInputs(
                        login=InputDescriptor(keyboard="Default"),
                        password=InputDescriptor(keyboard="Number pad"),
                    ),
                )
            ],
            links=[
                PalaceAuthenticationLink(
                    href="http://example.com/start", rel="start", type="text/html"
                )
            ],
            service_description="A library",
            features=Features(enabled=["reservations"], disabled=[]),
        )
        data = doc.model_dump(mode="json", exclude_none=True)
        assert data["id"] == "http://example.com/auth"
        assert data["title"] == "Test Library"
        assert data["service_description"] == "A library"
        assert len(data["authentication"]) == 1
        auth = data["authentication"][0]
        assert auth["type"] == "http://opds-spec.org/auth/basic"
        assert auth["labels"]["login"] == "Barcode"
        assert len(data["links"]) == 1
        assert data["links"][0]["href"] == "http://example.com/start"
