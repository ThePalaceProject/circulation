from palace.manager.opds.base import BaseOpdsModel


class Price(BaseOpdsModel):
    """
    https://drafts.opds.io/opds-2.0#53-acquisition-links
    """

    currency: str
    value: float
