import json
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Protocol

import firebase_admin
from firebase_admin import messaging
from firebase_admin.exceptions import FirebaseError
from firebase_admin.messaging import UnregisteredError
from sqlalchemy.orm import Session

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.service.fcm.configuration import FcmConfiguration
from palace.manager.sqlalchemy.model.devicetokens import DeviceToken


def credentials(config_file: Path | None, config_json: str | None) -> dict[str, str]:
    """Returns a dictionary containing Firebase Cloud Messaging credentials.

    Credentials are provided as a JSON string, either (1) directly in an environment
    variable or (2) in a file that is specified in another environment variable.
    """
    if config_json and config_file:
        raise CannotLoadConfiguration(
            f"Both JSON ('{FcmConfiguration.credentials_json_env_var()}') "
            f"and file-based ('{FcmConfiguration.credentials_file_env_var()}') "
            "FCM Credential environment variables are defined, but only one is allowed."
        )

    if config_json:
        try:
            return json.loads(config_json, strict=False)  # type: ignore[no-any-return]
        except:
            raise CannotLoadConfiguration(
                "Cannot parse value of FCM credential environment variable "
                f"'{FcmConfiguration.credentials_json_env_var()}' as JSON."
            )

    if config_file:
        if not config_file.exists():
            raise CannotLoadConfiguration(
                f"The FCM credentials file ('{config_file}') does not exist."
            )
        with config_file.open("r") as f:
            try:
                return json.load(f)  # type: ignore[no-any-return]
            except:
                raise CannotLoadConfiguration(
                    f"Cannot parse contents of FCM credentials file ('{config_file}') as JSON."
                )

    # If we get here, neither the JSON nor the file-based configuration was provided.
    raise CannotLoadConfiguration(
        "FCM Credentials configuration environment variable not defined. "
        f"Use either '{FcmConfiguration.credentials_json_env_var()}' "
        f"or '{FcmConfiguration.credentials_file_env_var()}'."
    )


log = logging.getLogger(__name__)


class SendNotificationsCallable(Protocol):
    def __call__(
        self,
        tokens: list[DeviceToken],
        title: str,
        body: str,
        data: Mapping[str, str | None],
        *,
        dry_run: bool = False,
    ) -> list[str]: ...


def send_notifications(
    tokens: list[DeviceToken],
    title: str,
    body: str,
    data: Mapping[str, str | None],
    *,
    app: firebase_admin.App,
    dry_run: bool = False,
) -> list[str]:
    responses = []

    data_typed = {}

    # Make sure our data is all typed as strings for Firebase
    for key, value in data.items():
        if value is None:
            # Firebase doesn't like null values
            log.warning(f"Removing {key} from notification data because it is None")
            continue
        elif not isinstance(value, str):
            log.warning(f"Converting {key} from {type(value)} to str")  # type: ignore[unreachable]
            data_typed[key] = str(value)
        else:
            data_typed[key] = value

    # Make sure title and body are included in the notification data
    if "title" not in data_typed:
        data_typed["title"] = title
    if "body" not in data_typed:
        data_typed["body"] = body

    for token in tokens:
        try:
            msg = messaging.Message(
                token=token.device_token,
                notification=messaging.Notification(title=title, body=body),
                data=data_typed,
            )
            resp = messaging.send(msg, dry_run=dry_run, app=app)
            log.info(
                f"Sent notification for patron {token.patron.authorization_identifier} "
                f"notification ID: {resp}"
            )
            responses.append(resp)
        except UnregisteredError:
            log.info(
                f"Device token {token.device_token} for patron {token.patron.authorization_identifier} "
                f"is no longer registered, deleting"
            )
            db = Session.object_session(token)
            db.delete(token)
        except FirebaseError:
            log.exception(
                f"Failed to send notification for patron {token.patron.authorization_identifier}"
            )
    return responses
