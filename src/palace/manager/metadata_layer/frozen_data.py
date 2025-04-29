from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class BaseFrozenData(BaseModel):
    model_config = ConfigDict(
        frozen=True,
    )
