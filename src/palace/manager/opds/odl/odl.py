from functools import cached_property

from pydantic import AwareDatetime, Field, NonNegativeInt

from palace.manager.opds.base import BaseOpdsModel, obj_or_set_to_set


class Terms(BaseOpdsModel):
    """
    https://drafts.opds.io/odl-1.0.html#33-terms
    """

    checkouts: NonNegativeInt | None = None
    expires: AwareDatetime | None = None
    concurrency: NonNegativeInt | None = None
    length: NonNegativeInt | None = None


class Protection(BaseOpdsModel):
    """
    https://drafts.opds.io/odl-1.0.html#34-protection
    """

    format: frozenset[str] | str = Field(default_factory=frozenset)
    devices: int | None = None
    # This is aliased because 'copy' is a method on BaseModel, the
    # other fields are aliased for consistency.
    allow_copy: bool = Field(True, alias="copy")
    allow_print: bool = Field(True, alias="print")
    allow_tts: bool = Field(True, alias="tts")

    @cached_property
    def formats(self) -> frozenset[str]:
        return obj_or_set_to_set(self.format)
