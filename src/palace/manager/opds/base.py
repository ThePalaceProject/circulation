from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class BaseOpdsModel(BaseModel):
    """Base class for OPDS models."""

    model_config = ConfigDict(
        populate_by_name=True,
        frozen=True,
    )

    def serialize(self) -> dict[str, Any]:
        """Serialize the model to a JSON-compatible dict using OPDS conventions.

        Uses aliases for field names and excludes unset/None values.
        """
        return self.model_dump(
            mode="json",
            by_alias=True,
            exclude_unset=True,
            exclude_none=True,
        )
