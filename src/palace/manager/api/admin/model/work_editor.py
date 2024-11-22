from pydantic import ConfigDict, NonNegativeInt

from palace.manager.util.flask_util import CustomBaseModel


class CustomListResponse(CustomBaseModel):
    name: str
    id: NonNegativeInt | None = None

    model_config = ConfigDict(
        # TODO: circulation-admin includes extra fields in its response that we don't
        #   need / use. It should be updated to just send the data we need, then we
        #   can forbid extras like we do in our other models.
        extra="ignore",
    )
