from pydantic import Field

from palace.manager.util.flask_util import CustomBaseModel


class InventoryReportCollectionInfo(CustomBaseModel):
    """Collection information."""

    id: int = Field(..., description="Collection identifier.")
    name: str = Field(..., description="Collection name.")


class InventoryReportInfo(CustomBaseModel):
    """Inventory report information."""

    collections: list[InventoryReportCollectionInfo] = Field(
        ..., description="List of collections."
    )
