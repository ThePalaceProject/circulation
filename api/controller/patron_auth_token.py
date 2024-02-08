from __future__ import annotations

import flask

from api.authentication.access_token import PatronJWEAccessTokenProvider
from api.controller.circulation_manager import CirculationManagerController
from api.model.patron_auth import PatronAuthAccessToken
from api.problem_details import PATRON_AUTH_ACCESS_TOKEN_NOT_POSSIBLE
from core.util.log import LoggerMixin
from core.util.problem_detail import ProblemError


class PatronAuthTokenController(CirculationManagerController, LoggerMixin):
    def get_token(self):
        """Create a Patron Auth access token for an authenticated patron"""
        patron = flask.request.patron
        auth = flask.request.authorization
        token_expiry = 3600

        if not patron or auth.type.lower() != "basic":
            return PATRON_AUTH_ACCESS_TOKEN_NOT_POSSIBLE

        try:
            token = PatronJWEAccessTokenProvider.generate_token(
                self._db,
                patron,
                auth["password"],
                expires_in=token_expiry,
            )
        except ProblemError as ex:
            self.log.error(f"Could not generate Patron Auth Access Token: {ex}")
            return ex.problem_detail

        return PatronAuthAccessToken(
            access_token=token, expires_in=token_expiry, token_type="Bearer"
        ).api_dict()
