from __future__ import annotations

import flask

from palace.manager.api.authentication.access_token import PatronJWEAccessTokenProvider
from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)
from palace.manager.api.model.patron_auth import PatronAuthAccessToken
from palace.manager.api.problem_details import PATRON_AUTH_ACCESS_TOKEN_NOT_POSSIBLE
from palace.manager.api.util.flask import get_request_patron
from palace.manager.util.log import LoggerMixin
from palace.manager.util.problem_detail import ProblemDetailException


class PatronAuthTokenController(CirculationManagerController, LoggerMixin):
    def get_token(self):
        """Create a Patron Auth access token for an authenticated patron"""
        patron = get_request_patron(default=None)
        auth = flask.request.authorization
        token_expiry = 3600

        if patron is None or auth is None or auth.type.lower() != "basic":
            return PATRON_AUTH_ACCESS_TOKEN_NOT_POSSIBLE

        try:
            token = PatronJWEAccessTokenProvider.generate_token(
                self._db,
                patron,
                auth["password"],
                expires_in=token_expiry,
            )
        except ProblemDetailException as ex:
            self.log.error(f"Could not generate Patron Auth Access Token: {ex}")
            return ex.problem_detail

        return PatronAuthAccessToken(
            access_token=token, expires_in=token_expiry, token_type="Bearer"
        ).api_dict()
