from __future__ import annotations

from typing import TYPE_CHECKING

import firebase_admin
from firebase_admin import credentials, messaging
from sqlalchemy.orm import Session

from core.config import Configuration
from core.model.configuration import ConfigurationSetting
from core.model.constants import NotificationConstants
from core.model.devicetokens import DeviceToken, DeviceTokenTypes
from core.model.edition import Edition
from core.model.identifier import Identifier
from core.model.patron import Hold, Loan, Patron
from core.model.work import Work
from core.util.log import LoggerMixin

if TYPE_CHECKING:
    from firebase_admin.messaging import SendResponse


class PushNotifications(LoggerMixin):
    # Should be set to true while unit testing
    TESTING_MODE = False
    _fcm_app = None
    _base_url = None

    VALID_TOKEN_TYPES = [DeviceTokenTypes.FCM_ANDROID, DeviceTokenTypes.FCM_IOS]

    @classmethod
    def notifiable_tokens(cls, patron: Patron) -> list[DeviceToken]:
        return [
            token
            for token in patron.device_tokens
            if token.token_type in cls.VALID_TOKEN_TYPES
        ]

    @classmethod
    def fcm_app(cls):
        if not cls._fcm_app:
            cls._fcm_app = firebase_admin.initialize_app(
                credentials.Certificate(Configuration.fcm_credentials())
            )
        return cls._fcm_app

    @classmethod
    def base_url(cls, _db: Session) -> str:
        if not cls._base_url:
            cls._base_url = ConfigurationSetting.sitewide(
                _db, Configuration.BASE_URL_KEY
            ).value
        return cls._base_url

    @classmethod
    def send_loan_expiry_message(
        cls, loan: Loan, days_to_expiry, tokens: list[DeviceToken]
    ) -> list[SendResponse]:
        """Send a loan expiry reminder to the mobile Apps, with enough information
        to identify two things
        - Which loan is being mentioned, in order to correctly deep link
        - Which patron and make the loans api request with the right authentication"""
        responses = []
        _db = Session.object_session(loan)
        url = cls.base_url(_db)
        edition: Edition = loan.license_pool.presentation_edition
        identifier: Identifier = loan.license_pool.identifier
        library_short_name = loan.library and loan.library.short_name
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

        cls.logger().info(
            f"Patron {loan.patron.authorization_identifier} has {len(tokens)} device tokens."
        )
        for token in tokens:
            msg = messaging.Message(
                token=token.device_token,
                notification=messaging.Notification(title=title, body=body),
                data=data,
            )
            resp = messaging.send(msg, dry_run=cls.TESTING_MODE, app=cls.fcm_app())
            cls.logger().info(
                f"Sent loan expiry notification for {loan.patron.authorization_identifier} ID: {resp}"
            )
            responses.append(resp)
        return responses

    @classmethod
    def send_activity_sync_message(cls, patrons: list[Patron]) -> list[str]:
        """Send notifications to the given patrons to sync their bookshelves.
        Enough information needs to be sent to identify a patron on the mobile Apps,
        and make the loans api request with the right authentication"""
        if not patrons:
            return []

        msgs = []
        _db = Session.object_session(patrons[0])
        url = cls.base_url(_db)
        for patron in patrons:
            tokens = cls.notifiable_tokens(patron)
            loans_api = f"{url}/{patron.library.short_name}/loans"
            data = dict(
                event_type=NotificationConstants.ACTIVITY_SYNC_TYPE,
                loans_endpoint=loans_api,
            )
            if patron.external_identifier:
                data["external_identifier"] = patron.external_identifier
            if patron.authorization_identifier:
                data["authorization_identifier"] = patron.authorization_identifier

            cls.logger().info(
                f"Must sync patron activity for {patron.authorization_identifier}, has {len(tokens)} device tokens."
            )

            for token in tokens:
                msg = messaging.Message(
                    token=token.device_token,
                    data=data,
                )
                msgs.append(msg)
        batch: messaging.BatchResponse = messaging.send_all(
            msgs, dry_run=cls.TESTING_MODE, app=cls.fcm_app()
        )
        cls.logger().info(
            f"Activity Sync Notifications: Successes {batch.success_count}, failures {batch.failure_count}."
        )
        return [resp.message_id for resp in batch.responses]

    @classmethod
    def send_holds_notifications(cls, holds: list[Hold]) -> list[str]:
        """Send out notifications to all patron devices that their hold is ready for checkout."""
        if not holds:
            return []

        msgs = []
        _db = Session.object_session(holds[0])
        url = cls.base_url(_db)
        for hold in holds:
            tokens = cls.notifiable_tokens(hold.patron)
            cls.logger().info(
                f"Notifying patron {hold.patron.authorization_identifier or hold.patron.username} for hold: {hold.work.title}. "
                f"Patron has {len(tokens)} device tokens."
            )
            loans_api = f"{url}/{hold.patron.library.short_name}/loans"
            work: Work = hold.work
            identifier: Identifier = hold.license_pool.identifier
            title = f'Your hold on "{work.title}" is available!'
            data = dict(
                title=title,
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

            for token in tokens:
                msg = messaging.Message(
                    token=token.device_token,
                    notification=messaging.Notification(title=title),
                    data=data,
                )
                msgs.append(msg)
        batch: messaging.BatchResponse = messaging.send_all(
            msgs, dry_run=cls.TESTING_MODE, app=cls.fcm_app()
        )
        cls.logger().info(
            f"Hold Notifications: Successes {batch.success_count}, failures {batch.failure_count}."
        )
        return [resp.message_id for resp in batch.responses]
