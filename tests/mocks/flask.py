from pydantic import BaseModel
from werkzeug.datastructures import MultiDict

from palace.manager.util.flask_util import parse_multi_dict


def add_request_context(
    request, model: type[BaseModel], form: MultiDict | None = None
) -> None:
    """Add form data into the request context.

    Before doing so, we verify that it can be parsed into the Pydantic model.

    :param model: A pydantic model
    :param form: A form multidict
    """

    if form is not None:
        model.parse_obj(parse_multi_dict(form))
        request.form = form
