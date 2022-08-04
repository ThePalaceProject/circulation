from typing import List

import firebase_admin
from firebase_admin import credentials, messaging
from sqlalchemy.orm import Session

from core.config import Configuration
from core.model.configuration import ConfigurationSetting
from core.model.constants import NotificationConstants
from core.model.devicetokens import DeviceToken, DeviceTokenTypes
from core.model.edition import Edition
from core.model.identifier import Identifier
from core.model.patron import Loan, Patron


class PushNotifications:
    # Should be set to true while unit testing
    TESTING_MODE = False
    _fcm_app = None
    _base_url = None

    VALID_TOKEN_TYPES = [DeviceTokenTypes.FCM_ANDROID, DeviceTokenTypes.FCM_IOS]

    @classmethod
    @property
    def fcm_app(cls):
        if not cls._fcm_app:
            cls._fcm_app = firebase_admin.initialize_app(
                credentials.Certificate(Configuration.fcm_credentials_file())
            )
        return cls._fcm_app

    @classmethod
    def base_url(cls, _db):
        if not cls._base_url:
            cls._base_url = ConfigurationSetting.sitewide(
                _db, Configuration.BASE_URL_KEY
            ).value
        return cls._base_url

    @classmethod
    def send_loan_expiry_message(
        cls, loan: Loan, days_to_expiry, tokens: List[DeviceToken]
    ):
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
        for token in tokens:
            msg = messaging.Message(
                token=token.device_token,
                data=dict(
                    title=f"Only {days_to_expiry} {'days' if days_to_expiry != 1 else 'day'} left on your loan!",
                    body=f"Your loan on {edition.title} is expiring soon",
                    event_type=NotificationConstants.LOAN_EXPIRY_TYPE,
                    loans_endpoint=f"{url}/{loan.library.short_name}/loans",
                    external_identifier=loan.patron.external_identifier,
                    authorization_identifier=loan.patron.authorization_identifier,
                    identifier=identifier.identifier,
                    type=identifier.type,
                    library=library_short_name,
                    days_to_expiry=days_to_expiry,
                ),
            )
            resp = messaging.send(msg, dry_run=cls.TESTING_MODE, app=cls.fcm_app)
            responses.append(resp)
        return responses

    @classmethod
    def send_activity_sync_message(cls, patrons: List[Patron]):
        """Send notifications to the given patrons to sync their bookshelves
        Enough information needs to be sent to identify a patron on the mobile Apps
        and make the loans api request with the right authentication"""
        if not patrons:
            return []

        msgs = []
        _db = Session.object_session(patrons[0])
        url = cls.base_url(_db)
        for patron in patrons:
            tokens = [
                token
                for token in patron.device_tokens
                if token.token_type in cls.VALID_TOKEN_TYPES
            ]
            loans_api = f"{url}/{patron.library.short_name}/loans"
            for token in tokens:
                msg = messaging.Message(
                    token=token.device_token,
                    data=dict(
                        event_type=NotificationConstants.ACTIVITY_SYNC_TYPE,
                        loans_endpoint=loans_api,
                        external_identifier=patron.external_identifier,
                        authorization_identifier=patron.authorization_identifier,
                    ),
                )
                msgs.append(msg)
        batch: messaging.BatchResponse = messaging.send_all(
            msgs, dry_run=cls.TESTING_MODE, app=cls.fcm_app
        )
        return [resp.message_id for resp in batch.responses]
