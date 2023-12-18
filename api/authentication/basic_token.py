from __future__ import annotations

from collections.abc import Generator
from typing import TYPE_CHECKING, cast

from flask import url_for
from sqlalchemy.orm import Session
from werkzeug.datastructures import Authorization

from api.authentication.access_token import AccessTokenProvider
from api.authentication.base import (
    AuthenticationProvider,
    AuthProviderLibrarySettings,
    AuthProviderSettings,
)
from api.authentication.basic import BasicAuthenticationProvider
from api.problem_details import PATRON_AUTH_ACCESS_TOKEN_INVALID
from core.integration.base import LibrarySettingsType, SettingsType
from core.model import Patron, Session, get_one
from core.selftest import SelfTestResult
from core.util.problem_detail import ProblemDetail, ProblemError

if TYPE_CHECKING:
    from core.model import Library


class BasicTokenAuthenticationProvider(
    AuthenticationProvider[AuthProviderSettings, AuthProviderLibrarySettings]
):
    """Patron Authentication based on a CM generated Access Token
    It is a companion to the basic authentication, and has no meaning without it.
    """

    @classmethod
    def library_settings_class(cls) -> type[LibrarySettingsType]:
        raise NotImplementedError()

    @classmethod
    def settings_class(cls) -> type[SettingsType]:
        raise NotImplementedError()

    FLOW_TYPE = "http://thepalaceproject.org/authtype/basic-token"

    def __init__(
        self,
        _db: Session,
        library: Library,
        basic_provider: BasicAuthenticationProvider,
    ):
        self._db = _db
        self.library_id = cast(int, library.id)
        # An access token provider is a companion authentication to the basic providers
        self.basic_provider = basic_provider

    @property
    def patron_lookup_provider(self):
        return self.basic_provider

    def authenticated_patron(
        self, _db: Session, token: dict | str
    ) -> Patron | ProblemDetail | None:
        """Authenticate the patron by decoding the JWE token and fetching the patron from the DB based on the patron ID"""

        if type(token) is not str:
            return None

        try:
            data = AccessTokenProvider.decode_token(_db, token)
        except ProblemError as ex:
            data = ex.problem_detail

        if type(data) == ProblemDetail:
            return data

        # This exists because of mypy
        assert type(data) is dict

        try:
            patron_id = data["id"]
            # Ensure the password exists
            if "pwd" not in data:
                return PATRON_AUTH_ACCESS_TOKEN_INVALID
        except KeyError:
            return PATRON_AUTH_ACCESS_TOKEN_INVALID

        patron: Patron | None = get_one(_db, Patron, id=patron_id)
        if patron is None:
            return None

        return patron

    def get_credential_from_header(self, auth: Authorization) -> str | None:
        """If we are the right type of token, then decode the password from the token"""
        if (
            auth
            and auth.type.lower() == "bearer"
            and auth.token
            and AccessTokenProvider.is_access_token(auth.token)
        ):
            token = AccessTokenProvider.decode_token(self._db, auth.token)
            if type(token) == dict:
                return token.get("pwd")

        return None

    def _authentication_flow_document(self, _db):
        """This auth type should follow the entry of it's basic auth provider"""
        token_url = url_for(
            "patron_auth_token",
            library_short_name=self.library(_db).short_name,
            _external=True,
        )
        links = [
            {
                "rel": "authenticate",
                "href": token_url,
            }
        ]
        flow_doc = self.basic_provider._authentication_flow_document(_db)
        flow_doc["description"] = str(self.label())
        flow_doc["links"] = links
        return flow_doc

    def remote_patron_lookup(self, _db):
        """There is no remote lookup"""
        raise NotImplementedError()

    @property
    def flow_type(self) -> str:
        return self.FLOW_TYPE

    @classmethod
    def description(cls) -> str:
        return "An internal authentication mechanism, DO NOT CREATE MANUALLY!!"

    @classmethod
    def identifies_individuals(cls):
        return True

    @classmethod
    def label(cls) -> str:
        return "Library Barcode + Token"

    def _run_self_tests(self, _db: Session) -> Generator[SelfTestResult, None, None]:
        raise NotImplementedError()
