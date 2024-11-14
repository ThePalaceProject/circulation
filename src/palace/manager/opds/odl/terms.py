from datetime import date, datetime, timezone
from functools import cached_property

from pydantic import AwareDatetime, NonNegativeInt

from palace.manager.opds.base import BaseOpdsModel


class Terms(BaseOpdsModel):
    """
    https://drafts.opds.io/odl-1.0.html#33-terms
    """

    checkouts: NonNegativeInt | None = None
    expires: AwareDatetime | date | None = None
    concurrency: NonNegativeInt | None = None
    length: NonNegativeInt | None = None

    @cached_property
    def expires_datetime(self) -> datetime | None:
        if self.expires is None or isinstance(self.expires, datetime):
            return self.expires

        # We were given expires as a date, which means we need to convert it
        # to as datetime. This is a bit fraught, since we don't have any
        # timezone information, but for now we will assume UTC.
        return datetime(
            self.expires.year, self.expires.month, self.expires.day, tzinfo=timezone.utc
        )
