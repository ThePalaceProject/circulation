from pydantic import NonNegativeInt

from palace.manager.util.flask_util import CustomBaseModel


class CustomListSharePostResponse(CustomBaseModel):
    successes: int = 0
    failures: int = 0


class CustomListPostRequest(CustomBaseModel):
    name: str
    id: NonNegativeInt | None = None
    entries: list[dict] = []
    collections: list[int] = []
    deletedEntries: list[dict] = []
    # For auto updating lists
    auto_update: bool = False
    auto_update_query: dict | None = None
    auto_update_facets: dict | None = None
