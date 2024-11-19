from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class BaseOpdsModel(BaseModel):
    """Base class for OPDS models."""

    model_config = ConfigDict(
        populate_by_name=True,
        frozen=True,
    )
