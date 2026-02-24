"""Admin API models for the patron debug authentication feature."""

from __future__ import annotations

from palace.manager.api.authentication.base import PatronAuthResult
from palace.manager.util.flask_util import CustomBaseModel


class AuthMethodInfo(CustomBaseModel):
    """Information about a single authentication method available for a library."""

    id: int
    name: str
    protocol: str
    supports_debug: bool
    supports_password: bool
    identifier_label: str
    password_label: str


class AuthMethodsResponse(CustomBaseModel):
    """Response listing all authentication methods for a library."""

    auth_methods: list[AuthMethodInfo]


class PatronDebugResponse(CustomBaseModel):
    """Response containing the results of a patron debug authentication run."""

    results: list[PatronAuthResult]
