from collections.abc import Sequence
from functools import cached_property

from pydantic import Field

from palace.manager.opds.base import BaseOpdsModel
from palace.manager.opds.util import StrOrTuple, obj_or_tuple_to_tuple


class Protection(BaseOpdsModel):
    """
    https://drafts.opds.io/odl-1.0.html#34-protection
    """

    format: StrOrTuple[str] = tuple()

    @cached_property
    def formats(self) -> Sequence[str]:
        return obj_or_tuple_to_tuple(self.format)

    devices: int | None = None
    # This is aliased because 'copy' is a method on BaseModel, the
    # other fields are aliased for consistency.
    allow_copy: bool = Field(True, alias="copy")
    allow_print: bool = Field(True, alias="print")
    allow_tts: bool = Field(True, alias="tts")
