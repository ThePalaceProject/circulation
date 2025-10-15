from __future__ import annotations

from collections.abc import Generator
from typing import TYPE_CHECKING

from flask import url_for
from sqlalchemy.orm import Session
from werkzeug.datastructures import Authorization

from palace.manager.api.authentication.access_token import PatronJWEAccessTokenProvider
from palace.manager.api.authentication.base import (
    AuthenticationProvider,
    AuthProviderLibrarySettings,
    AuthProviderSettings,
)
from palace.manager.api.authentication.basic import BasicAuthenticationProvider
from palace.manager.core.selftest import SelfTestResult
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.library import Library


class BasicTokenAuthenticationProvider(
    AuthenticationProvider[AuthProviderSettings, AuthProviderLibrarySettings]
):
    """Patron Authentication based on a CM generated Access Token
    It is a companion to the basic authentication, and has no meaning without it.
    """

    @classmethod
    def library_settings_class(cls) -> type[AuthProviderLibrarySettings]:
        raise NotImplementedError()

    @classmethod
    def settings_class(cls) -> type[AuthProviderSettings]:
        raise NotImplementedError()

    FLOW_TYPE = "http://thepalaceproject.org/authtype/basic-token"

    def __init__(
        self,
        _db: Session,
        library: Library,
        basic_provider: BasicAuthenticationProvider,
    ):
        self._db = _db
        self.library_id = library.id
        # An access token provider is a companion authentication to the basic providers
        self.basic_provider = basic_provider

    @property
    def patron_lookup_provider(self):
        return self.basic_provider

    def authenticated_patron(
        self, _db: Session, token: dict | str
    ) -> Patron | ProblemDetail | None:
        """Authenticate the patron by decoding the JWE token and fetching the patron from the DB based on the patron ID"""

        if not isinstance(token, str):
            return None

        try:
            data = PatronJWEAccessTokenProvider.decrypt_token(_db, token)
        except ProblemDetailException as ex:
            return ex.problem_detail

        patron: Patron | None = get_one(_db, Patron, id=data.id)
        if patron is None:
            return None

        return patron

    def get_credential_from_header(self, auth: Authorization) -> str | None:
        """If we are the right type of token, then decode the password from the token"""
        if auth and auth.type.lower() == "bearer" and auth.token:
            try:
                token = PatronJWEAccessTokenProvider.decrypt_token(self._db, auth.token)
                return token.pwd
            except ProblemDetailException:
                ...

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

    def _run_self_tests(self, _db: Session) -> Generator[SelfTestResult]:
        raise NotImplementedError()
