from typing import List

import firebase_admin
from firebase_admin import credentials, messaging

from core.config import Configuration
from core.model.devicetokens import DeviceToken
from core.model.edition import Edition
from core.model.identifier import Identifier
from core.model.patron import Loan


class PushNotifications:
    # Should be set to true while unit testing
    TESTING_MODE = False
    _fcm_app = None

    @classmethod
    @property
    def fcm_app(cls):
        if not cls._fcm_app:
            cls._fcm_app = firebase_admin.initialize_app(
                credentials.Certificate(Configuration.fcm_credentials_file())
            )
        return cls._fcm_app

    @classmethod
    def send_loan_expiry_message(
        cls, loan: Loan, days_to_expiry, tokens: List[DeviceToken]
    ):
        responses = []
        edition: Edition = loan.license_pool.presentation_edition
        identifier: Identifier = loan.license_pool.identifier
        library_short_name = loan.library and loan.library.short_name
        for token in tokens:
            msg = messaging.Message(
                token=token.device_token,
                data=dict(
                    title=f"Only {days_to_expiry} {'days' if days_to_expiry != 1 else 'day'} left on your loan!",
                    body=f"Your loan on {edition.title} is expiring soon",
                    identifier=identifier.identifier,
                    type=identifier.type,
                    library=library_short_name,
                    days_to_expiry=days_to_expiry,
                ),
            )
            resp = messaging.send(msg, dry_run=cls.TESTING_MODE, app=cls.fcm_app)
            responses.append(resp)
        return responses
