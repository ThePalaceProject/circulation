from __future__ import annotations

from sqlalchemy.orm import Session
from werkzeug.datastructures import Authorization

from api.authentication.access_token import AccessTokenProvider
from api.authentication.base import AuthenticationProvider
from api.problem_details import PATRON_AUTH_ACCESS_TOKEN_INVALID
from core.model import Patron, Session, get_one
from core.util.problem_detail import ProblemDetail


class PatronAccessTokenAuthenticationProvider(AuthenticationProvider):
    FLOW_TYPE = "http://librarysimplified.org/authtype/bearer"

    def __init__(self, _db: Session):
        self._db = _db
        self.external_integration_id = AccessTokenProvider.get_integration(_db).id

    def authenticated_patron(
        self, _db: Session, token: str
    ) -> Patron | ProblemDetail | None:
        data = AccessTokenProvider.decode_token(_db, token)
        try:
            patron_id = data["id"]
            # Ensure the password exists
            data["pwd"]
        except KeyError:
            return PATRON_AUTH_ACCESS_TOKEN_INVALID

        patron: Patron = get_one(_db, Patron, id=patron_id)
        if patron is None:
            return None

        return patron

    def get_credential_from_header(self, auth: Authorization) -> str | None:
        if (
            auth
            and auth.type.lower() == "bearer"
            and AccessTokenProvider.is_access_token(auth.token)
        ):
            return AccessTokenProvider.decode_token(self._db, auth.token).get("pwd")

    def _authentication_flow_document(self, _db):
        return None

    def remote_patron_lookup(self, _db):
        raise NotImplementedError()
