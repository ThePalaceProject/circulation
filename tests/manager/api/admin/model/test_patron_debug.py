"""Tests for admin API patron debug response models."""

from palace.manager.api.admin.model.patron_debug import (
    AuthMethodInfo,
    AuthMethodsResponse,
    PatronDebugResponse,
)
from palace.manager.api.authentication.base import PatronAuthResult


class TestAuthMethodInfo:
    def test_serialization(self):
        info = AuthMethodInfo(
            id=42,
            name="SIP2 Provider",
            protocol="api.sip",
            supports_debug=True,
            supports_password=True,
            identifier_label="Barcode",
            password_label="PIN",
        )
        data = info.api_dict()
        assert data["id"] == 42
        assert data["name"] == "SIP2 Provider"
        assert data["protocol"] == "api.sip"
        assert data["supportsDebug"] is True
        assert data["supportsPassword"] is True
        assert data["identifierLabel"] == "Barcode"
        assert data["passwordLabel"] == "PIN"


class TestAuthMethodsResponse:
    def test_serialization(self):
        response = AuthMethodsResponse(
            auth_methods=[
                AuthMethodInfo(
                    id=1,
                    name="SIP2",
                    protocol="api.sip",
                    supports_debug=True,
                    supports_password=True,
                    identifier_label="Barcode",
                    password_label="PIN",
                ),
                AuthMethodInfo(
                    id=2,
                    name="SAML",
                    protocol="api.saml.provider",
                    supports_debug=False,
                    supports_password=False,
                    identifier_label="Username",
                    password_label="Password",
                ),
            ]
        )
        data = response.api_dict()
        assert len(data["authMethods"]) == 2
        assert data["authMethods"][0]["name"] == "SIP2"
        assert data["authMethods"][1]["supportsDebug"] is False


class TestPatronDebugResponse:
    def test_serialization(self):
        response = PatronDebugResponse(
            results=[
                PatronAuthResult(label="Step 1", success=True, details="ok"),
                PatronAuthResult(
                    label="Step 2",
                    success=False,
                    details={"reason": "bad password"},
                ),
            ]
        )
        data = response.api_dict()
        assert len(data["results"]) == 2
        assert data["results"][0]["label"] == "Step 1"
        assert data["results"][0]["success"] is True
        assert data["results"][1]["success"] is False
        assert data["results"][1]["details"] == {"reason": "bad password"}

    def test_serialization_mixed_value_types(self):
        """Details dict supports non-string values like int, float, bool, and None."""
        response = PatronDebugResponse(
            results=[
                PatronAuthResult(
                    label="Parsed Patron Data",
                    success=True,
                    details={
                        "username": "jdoe",
                        "fines": 1.50,
                        "active": True,
                        "block_reason": None,
                    },
                ),
            ]
        )
        data = response.api_dict()
        details = data["results"][0]["details"]
        assert details["username"] == "jdoe"
        assert details["fines"] == 1.50
        assert details["active"] is True
        assert details["block_reason"] is None
