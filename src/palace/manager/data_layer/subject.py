from __future__ import annotations

from typing import Annotated

from pydantic import StringConstraints

from palace.manager.data_layer.base.frozen import BaseFrozenData


class SubjectData(BaseFrozenData):
    type: str
    identifier: Annotated[str, StringConstraints(strip_whitespace=True)] | None
    name: Annotated[str, StringConstraints(strip_whitespace=True)] | None = None
    weight: int = 1
