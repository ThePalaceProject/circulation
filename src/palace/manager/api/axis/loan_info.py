from __future__ import annotations

from dataclasses import dataclass

from palace.manager.api.circulation import Fulfillment, LoanInfo


@dataclass(kw_only=True)
class AxisLoanInfo(LoanInfo):
    """
    An extension of the normal LoanInfo dataclass that includes some Axis 360-specific
    information, since the Axis 360 API uses this object to get information about
    loan fulfillment in addition to checkout.
    """

    fulfillment: Fulfillment | None
