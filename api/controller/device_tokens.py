from __future__ import annotations

import flask
from flask import Response
from sqlalchemy.exc import NoResultFound

from api.controller.circulation_manager import CirculationManagerController
from api.problem_details import DEVICE_TOKEN_NOT_FOUND, DEVICE_TOKEN_TYPE_INVALID
from core.model import DeviceToken
from core.model.devicetokens import DuplicateDeviceTokenError, InvalidTokenTypeError


class DeviceTokensController(CirculationManagerController):
    def get_patron_device(self):
        patron = flask.request.patron
        device_token = flask.request.args["device_token"]
        token: DeviceToken = (
            self._db.query(DeviceToken)
            .filter(
                DeviceToken.patron_id == patron.id,
                DeviceToken.device_token == device_token,
            )
            .first()
        )
        if not token:
            return DEVICE_TOKEN_NOT_FOUND
        return dict(token_type=token.token_type, device_token=token.device_token), 200

    def create_patron_device(self):
        patron = flask.request.patron
        device_token = flask.request.json["device_token"]
        token_type = flask.request.json["token_type"]

        try:
            device = DeviceToken.create(self._db, token_type, device_token, patron)
        except InvalidTokenTypeError:
            return DEVICE_TOKEN_TYPE_INVALID
        except DuplicateDeviceTokenError:
            return dict(exists=True), 200

        return "", 201

    def delete_patron_device(self):
        patron = flask.request.patron
        device_token = flask.request.json["device_token"]
        token_type = flask.request.json["token_type"]

        try:
            device: DeviceToken = (
                self._db.query(DeviceToken)
                .filter(
                    DeviceToken.patron == patron,
                    DeviceToken.device_token == device_token,
                    DeviceToken.token_type == token_type,
                )
                .one()
            )
            self._db.delete(device)
        except NoResultFound:
            return DEVICE_TOKEN_NOT_FOUND

        return Response("", 204)
