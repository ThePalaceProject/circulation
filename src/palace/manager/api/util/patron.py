import datetime

import dateutil

from palace.manager.api.circulation.exceptions import (
    AuthorizationBlocked,
    AuthorizationExpired,
    CannotLoan,
    OutstandingFines,
)
from palace.manager.api.config import Configuration
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util import MoneyUtility
from palace.manager.util.datetime_helpers import utc_now


class PatronUtility:
    """Apply circulation-specific logic to Patron model objects."""

    @classmethod
    def needs_external_sync(cls, patron):
        """Could this patron stand to have their metadata synced with the
        remote?

        By default, all patrons get synced once every twelve
        hours. Patrons who lack borrowing privileges can always stand
        to be synced, since their privileges may have just been
        restored.
        """
        if not patron.last_external_sync:
            # This patron has never been synced.
            return True

        now = utc_now()
        if cls.has_borrowing_privileges(patron):
            # A patron who has borrowing privileges gets synced every twelve
            # hours. Their account is unlikely to change rapidly.
            check_every = Patron.MAX_SYNC_TIME
        else:
            # A patron without borrowing privileges might get synced
            # every time they make a request. It's likely they are
            # taking action to get their account reinstated and we
            # don't want to make them wait twelve hours to get access.
            check_every = datetime.timedelta(seconds=5)
        expired_at = patron.last_external_sync + check_every
        if now > expired_at:
            return True
        return False

    @classmethod
    def has_borrowing_privileges(cls, patron):
        """Is the given patron allowed to check out books?

        :return: A boolean
        """
        try:
            cls.assert_borrowing_privileges(patron)
            return True
        except CannotLoan as e:
            return False

    @classmethod
    def assert_borrowing_privileges(cls, patron):
        """Raise an exception unless the patron currently has borrowing
        privileges.

        :raises AuthorizationExpired: If the patron's authorization has expired.
        :raises OutstandingFines: If the patron has too many outstanding fines.

        """
        if not cls.authorization_is_active(patron):
            # The patron's card has expired.
            raise AuthorizationExpired()

        if cls.has_excess_fines(patron):
            raise OutstandingFines(fines=patron.fines)

        from palace.manager.api.authentication.base import PatronData

        if patron.block_reason is None:
            return

        if patron.block_reason == PatronData.NO_VALUE:
            return

        if patron.block_reason is PatronData.EXCESSIVE_FINES:
            # The authentication mechanism itself may know that
            # the patron has outstanding fines, even if the circulation
            # manager is not configured to make that deduction.
            raise OutstandingFines(fines=patron.fines)

        raise AuthorizationBlocked()

    @classmethod
    def has_excess_fines(cls, patron):
        """Does this patron have fines in excess of the maximum fine amount set for their library?

        :param a Patron:
        :return: A boolean
        """
        if not patron.fines:
            return False

        actual_fines = MoneyUtility.parse(patron.fines)
        max_fines = Configuration.max_outstanding_fines(patron.library)
        if max_fines is not None and actual_fines > max_fines:
            return True
        return False

    @classmethod
    def authorization_is_active(cls, patron):
        """Return True unless the patron's authorization has expired."""
        # Unlike pretty much every other place in this app, we use
        # (server) local time here instead of UTC. This is to make it
        # less likely that a patron's authorization will expire before
        # they think it should.
        now_local = datetime.datetime.now(tz=dateutil.tz.tzlocal())
        if patron.authorization_expires and cls._to_date(
            patron.authorization_expires
        ) < cls._to_date(now_local):
            return False
        return True

    @classmethod
    def _to_date(cls, x):
        """Convert a datetime into a date. Leave a date alone."""
        if isinstance(x, datetime.datetime):
            return x.date()
        return x
