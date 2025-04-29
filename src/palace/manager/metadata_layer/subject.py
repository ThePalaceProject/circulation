from __future__ import annotations

from typing import Annotated

from pydantic import constr

from palace.manager.metadata_layer.frozen_data import BaseFrozenData


class SubjectData(BaseFrozenData):
    type: str
    identifier: Annotated[str, constr(strip_whitespace=True)] | None
    name: Annotated[str, constr(strip_whitespace=True)] | None = None
    weight: int = 1
