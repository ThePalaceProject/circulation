from pydantic import NonNegativeInt

from palace.manager.opds.base import BaseOpdsModel
from palace.manager.opds.types.date import Iso8601DateOrAwareDatetime


class Terms(BaseOpdsModel):
    """
    https://drafts.opds.io/odl-1.0.html#33-terms
    """

    checkouts: NonNegativeInt | None = None
    expires: Iso8601DateOrAwareDatetime | None = None
    concurrency: NonNegativeInt | None = None
    length: NonNegativeInt | None = None
