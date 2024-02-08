from __future__ import annotations

from collections.abc import Mapping

import firebase_admin
from firebase_admin import credentials, messaging
from firebase_admin.exceptions import FirebaseError
from firebase_admin.messaging import UnregisteredError
from sqlalchemy.orm import Session

from core.config import CannotLoadConfiguration, Configuration
from core.model.constants import NotificationConstants
from core.model.devicetokens import DeviceToken, DeviceTokenTypes
from core.model.identifier import Identifier
from core.model.patron import Hold, Loan, Patron
from core.model.work import Work
from core.util.datetime_helpers import utc_now
from core.util.log import LoggerMixin


class PushNotifications(LoggerMixin):
    VALID_TOKEN_TYPES = [DeviceTokenTypes.FCM_ANDROID, DeviceTokenTypes.FCM_IOS]

    def __init__(
        self,
        base_url: str | None,
        fcm_app: firebase_admin.App | None = None,
        testing_mode: bool = False,
    ) -> None:
        self.base_url = base_url
        if self.base_url is None:
            raise CannotLoadConfiguration(
                f"Missing required environment variable: PALACE_BASE_URL."
            )
        self.fcm_app = fcm_app or firebase_admin.initialize_app(
            credentials.Certificate(Configuration.fcm_credentials())
        )
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
        library_short_name = loan.library.short_name
        title = f"Only {days_to_expiry} {'days' if days_to_expiry != 1 else 'day'} left on your loan!"
        body = f"Your loan on {edition.title} is expiring soon"
        data = dict(
            title=title,
            body=body,
            event_type=NotificationConstants.LOAN_EXPIRY_TYPE,
            loans_endpoint=f"{url}/{loan.library.short_name}/loans",
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

    def send_activity_sync_message(self, patrons: list[Patron]) -> list[str]:
        """Send notifications to the given patrons to sync their bookshelves.
        Enough information needs to be sent to identify a patron on the mobile Apps,
        and make the loans api request with the right authentication"""
        if not patrons:
            return []

        responses = []
        url = self.base_url
        for patron in patrons:
            tokens = self.notifiable_tokens(patron)
            loans_api = f"{url}/{patron.library.short_name}/loans"
            data = dict(
                event_type=NotificationConstants.ACTIVITY_SYNC_TYPE,
                loans_endpoint=loans_api,
            )
            if patron.external_identifier:
                data["external_identifier"] = patron.external_identifier
            if patron.authorization_identifier:
                data["authorization_identifier"] = patron.authorization_identifier

            self.log.info(
                f"Must sync patron activity for {patron.authorization_identifier}, has {len(tokens)} device tokens. "
                f"Sending activity sync notification(s)."
            )

            resp = self.send_messages(tokens, None, data)
            responses.extend(resp)

        return responses

    def send_holds_notifications(self, holds: list[Hold]) -> list[str]:
        """Send out notifications to all patron devices that their hold is ready for checkout."""
        if not holds:
            return []

        responses = []
        _db = Session.object_session(holds[0])
        url = self.base_url
        for hold in holds:
            tokens = self.notifiable_tokens(hold.patron)
            self.log.info(
                f"Notifying patron {hold.patron.authorization_identifier or hold.patron.username} for "
                f"hold: {hold.work.title}. Patron has {len(tokens)} device tokens."
            )
            loans_api = f"{url}/{hold.patron.library.short_name}/loans"
            work: Work = hold.work
            identifier: Identifier = hold.license_pool.identifier
            title = "Your hold is available!"
            body = f'Your hold on "{work.title}" is available!'
            data = dict(
                title=title,
                body=body,
                event_type=NotificationConstants.HOLD_AVAILABLE_TYPE,
                loans_endpoint=loans_api,
                identifier=identifier.identifier,
                type=identifier.type,
                library=hold.patron.library.short_name,
            )
            if hold.patron.external_identifier:
                data["external_identifier"] = hold.patron.external_identifier
            if hold.patron.authorization_identifier:
                data["authorization_identifier"] = hold.patron.authorization_identifier

            resp = self.send_messages(
                tokens, messaging.Notification(title=title, body=body), data
            )
            if len(resp) > 0:
                # At least one notification succeeded
                hold.patron_last_notified = utc_now().date()

            responses.extend(resp)

        return responses
