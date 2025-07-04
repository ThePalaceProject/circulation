from typing import Any

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from palace.manager.opds.types.link import BaseLink


class KdrmFulfillmentResponse(BaseModel):
    """
    Response returned from the CM when doing a Baker & Taylor KDRM fulfillment.

    This response encapsulates the license document in the `license_document` field,
    which we get directly from the Boundless API.

    It also includes a links field, formatted like an OPDS link list, which contains
    the links to the actual content files that can be downloaded.
    """

    model_config = ConfigDict(
        alias_generator=to_camel,
        validate_by_name=True,
    )

    license_document: dict[str, Any]
    links: list[BaseLink]
