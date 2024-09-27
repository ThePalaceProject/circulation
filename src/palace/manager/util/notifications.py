from __future__ import annotations

import sys
from collections.abc import Mapping

import firebase_admin
from firebase_admin import messaging
from firebase_admin.exceptions import FirebaseError
from firebase_admin.messaging import UnregisteredError
from sqlalchemy.orm import Session

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.sqlalchemy.model.devicetokens import DeviceToken, DeviceTokenTypes
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.patron import Hold, Loan, Patron
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.log import LoggerMixin

# TODO: Remove this when we drop support for Python 3.10
if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum


class NotificationType(StrEnum):
    LOAN_EXPIRY = "LoanExpiry"
    HOLD_AVAILABLE = "HoldAvailable"


class PushNotifications(LoggerMixin):
    VALID_TOKEN_TYPES = [DeviceTokenTypes.FCM_ANDROID, DeviceTokenTypes.FCM_IOS]

    def __init__(
        self,
        base_url: str | None,
        fcm_app: firebase_admin.App,
        testing_mode: bool = False,
    ) -> None:
        self.base_url = base_url
        if self.base_url is None:
            raise CannotLoadConfiguration(
                f"Missing required environment variable: PALACE_BASE_URL."
            )
        self.fcm_app = fcm_app
        self.testing_mode = testing_mode

    @classmethod
    def notifiable_tokens(cls, patron: Patron) -> list[DeviceToken]:
        return [
            token
            for token in patron.device_tokens
            if token.token_type in cls.VALID_TOKEN_TYPES
        ]

    def send_messages(
        self,
        tokens: list[DeviceToken],
        notification: messaging.Notification | None,
        data: Mapping[str, str | None],
    ) -> list[str]:
        responses = []

        data_typed = {}

        # Make sure our data is all typed as strings for Firebase
        for key, value in data.items():
            if value is None:
                # Firebase doesn't like null values
                self.log.warning(
                    f"Removing {key} from notification data because it is None"
                )
                continue
            elif not isinstance(value, str):
                self.log.warning(f"Converting {key} from {type(value)} to str")  # type: ignore[unreachable]
                data_typed[key] = str(value)
            else:
                data_typed[key] = value

        for token in tokens:
            try:
                msg = messaging.Message(
                    token=token.device_token,
                    notification=notification,
                    data=data_typed,
                )
                resp = messaging.send(msg, dry_run=self.testing_mode, app=self.fcm_app)
                self.log.info(
                    f"Sent notification for patron {token.patron.authorization_identifier} "
                    f"notification ID: {resp}"
                )
                responses.append(resp)
            except UnregisteredError:
                self.log.info(
                    f"Device token {token.device_token} for patron {token.patron.authorization_identifier} "
                    f"is no longer registered, deleting"
                )
                db = Session.object_session(token)
                db.delete(token)
            except FirebaseError:
                self.log.exception(
                    f"Failed to send notification for patron {token.patron.authorization_identifier}"
                )
        return responses

    def send_loan_expiry_message(
        self, loan: Loan, days_to_expiry: int, tokens: list[DeviceToken]
    ) -> list[str]:
        """Send a loan expiry reminder to the mobile Apps, with enough information
        to identify two things
        - Which loan is being mentioned, in order to correctly deep link
        - Which patron and make the loans api request with the right authentication"""
        url = self.base_url
        edition = loan.license_pool.presentation_edition
        identifier = loan.license_pool.identifier
        library = loan.library
        # It shouldn't be possible to get here for a loan without a library, but for mypy
        # and safety we will assert it anyway
        assert library is not None
        library_short_name = library.short_name
        library_name = library.name
        title = f"Only {days_to_expiry} {'days' if days_to_expiry != 1 else 'day'} left on your loan!"
        body = f'Your loan for "{edition.title}" at {library_name} is expiring soon'
        data = dict(
            title=title,
            body=body,
            event_type=NotificationType.LOAN_EXPIRY,
            loans_endpoint=f"{url}/{library.short_name}/loans",
            type=identifier.type,
            identifier=identifier.identifier,
            library=library_short_name,
            days_to_expiry=str(days_to_expiry),
        )
        if loan.patron.external_identifier:
            data["external_identifier"] = loan.patron.external_identifier
        if loan.patron.authorization_identifier:
            data["authorization_identifier"] = loan.patron.authorization_identifier

        self.log.info(
            f"Patron {loan.patron.authorization_identifier} has {len(tokens)} device tokens. "
            f"Sending loan expiry notification(s)."
        )
        responses = self.send_messages(
            tokens, messaging.Notification(title=title, body=body), data
        )
        if len(responses) > 0:
            # At least one notification succeeded
            loan.patron_last_notified = utc_now().date()
        return responses

    def send_holds_notifications(self, holds: list[Hold]) -> list[str]:
        """Send out notifications to all patron devices that their hold is ready for checkout."""
        if not holds:
            return []

        responses = []
        _db = Session.object_session(holds[0])
        url = self.base_url
        for hold in holds:
            try:
                tokens = self.notifiable_tokens(hold.patron)
                work_title = hold.work.title  # type: ignore[union-attr]
                self.log.info(
                    f"Notifying patron {hold.patron.authorization_identifier or hold.patron.username} for "
                    f"hold: {work_title}. Patron has {len(tokens)} device tokens."
                )
                loans_api = f"{url}/{hold.patron.library.short_name}/loans"
                identifier: Identifier = hold.license_pool.identifier
                library_name = hold.patron.library.name
                title = "Your hold is available!"
                body = f'Your hold on "{work_title}" is available at {library_name}!'
                data = dict(
                    title=title,
                    body=body,
                    event_type=NotificationType.HOLD_AVAILABLE,
                    loans_endpoint=loans_api,
                    identifier=identifier.identifier,
                    type=identifier.type,
                    library=hold.patron.library.short_name,
                )
                if hold.patron.external_identifier:
                    data["external_identifier"] = hold.patron.external_identifier
                if hold.patron.authorization_identifier:
                    data["authorization_identifier"] = (
                        hold.patron.authorization_identifier
                    )

                resp = self.send_messages(
                    tokens, messaging.Notification(title=title, body=body), data
                )
                if len(resp) > 0:
                    # At least one notification succeeded
                    hold.patron_last_notified = utc_now().date()

                responses.extend(resp)
            except AttributeError:
                error = f"Failed to send notification for hold {hold.id}"
                if hold.patron is not None:
                    error += f" to patron {hold.patron.authorization_identifier or hold.patron.username}"
                self.log.exception(error)

        return responses
