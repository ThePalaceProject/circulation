import datetime

from sqlalchemy import or_
from sqlalchemy.orm import Session

from palace.manager.scripts.base import Script
from palace.manager.service.container import Services
from palace.manager.sqlalchemy.model.devicetokens import DeviceToken, DeviceTokenTypes
from palace.manager.sqlalchemy.model.patron import Loan, Patron
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.notifications import PushNotifications


class LoanNotificationsScript(Script):
    """Notifications must be sent to Patrons based on when their current loans
    are expiring"""

    # List of days before expiration on which we will send out notifications.
    DEFAULT_LOAN_EXPIRATION_DAYS = [3]
    BATCH_SIZE = 100

    def __init__(
        self,
        _db: Session | None = None,
        services: Services | None = None,
        notifications: PushNotifications | None = None,
        *args,
        loan_expiration_days: list[int] | None = None,
        **kwargs,
    ):
        super().__init__(_db, services, *args, **kwargs)
        self.loan_expiration_days = (
            loan_expiration_days or self.DEFAULT_LOAN_EXPIRATION_DAYS
        )
        self.notifications = notifications or PushNotifications(
            self.services.config.sitewide.base_url(),
            self.services.fcm.app(),
        )

    def do_run(self):
        self.log.info("Loan Notifications Job started")

        _query = (
            self._db.query(Loan)
            .filter(
                or_(
                    Loan.patron_last_notified != utc_now().date(),
                    Loan.patron_last_notified == None,
                )
            )
            .order_by(Loan.id)
        )
        last_loan_id = None
        processed_loans = 0

        while True:
            query = _query.limit(self.BATCH_SIZE)
            if last_loan_id:
                query = _query.filter(Loan.id > last_loan_id)

            loans = query.all()
            if len(loans) == 0:
                break

            for loan in loans:
                processed_loans += 1
                self.process_loan(loan)
            last_loan_id = loan.id
            # Commit every batch
            self._db.commit()

        self.log.info(
            f"Loan Notifications Job ended: {processed_loans} loans processed"
        )

    def process_loan(self, loan: Loan):
        tokens = []
        patron: Patron = loan.patron
        t: DeviceToken
        for t in patron.device_tokens:
            if t.token_type in [DeviceTokenTypes.FCM_ANDROID, DeviceTokenTypes.FCM_IOS]:
                tokens.append(t)

        # No tokens means no notifications
        if not tokens:
            return

        now = utc_now()
        if loan.end is None:
            self.log.warning(f"Loan: {loan.id} has no end date, skipping")
            return
        delta: datetime.timedelta = loan.end - now
        if delta.days in self.loan_expiration_days:
            self.log.info(
                f"Patron {patron.authorization_identifier} has an expiring loan on ({loan.license_pool.identifier.urn})"
            )
            self.notifications.send_loan_expiry_message(loan, delta.days, tokens)
