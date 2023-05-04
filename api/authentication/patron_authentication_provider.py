from __future__ import annotations

from sqlalchemy.orm import Session
from werkzeug.datastructures import Authorization

from api.authentication.access_token import AccessTokenProvider
from api.authentication.base import AuthenticationProvider
from api.problem_details import PATRON_AUTH_ACCESS_TOKEN_INVALID
from api.sirsidynix_authentication_provider import SirsiDynixPatronData
from core.model import Patron, Session, get_one
from core.util.problem_detail import ProblemDetail


class PatronAccessTokenAuthenticationProvider(AuthenticationProvider):
    def __init__(self, _db: Session):
        self._db = _db

    def authenticated_patron(
        self, _db: Session, token: str
    ) -> Patron | ProblemDetail | None:
        data = AccessTokenProvider.decode_token(_db, token)
        try:
            patron_id = data["id"]
            patron_type = data["typ"]
            password = data["pwd"]
        except KeyError:
            return PATRON_AUTH_ACCESS_TOKEN_INVALID

        patron: Patron = get_one(_db, Patron, id=patron_id)
        if patron is None:
            return None

        patron.plaintext_password = password

        # Only a sirsi type patron has additional data
        if patron_type == "sirsi":
            patron.patrondata = SirsiDynixPatronData(
                session_token=data.get("session_token")
            )

        print("GOT PATRON FROM AT", patron)

        return patron

    def get_credential_from_header(self, auth: Authorization) -> str | None:
        return ""
