from flask_pydantic_spec.flask_backend import Context
from flask_pydantic_spec.utils import parse_multi_dict


def add_request_context(request, model, form=None) -> None:
    """Add a flask pydantic model into the request context
    :param model: The pydantic model
    :param form: A form multidict
    TODO:
    - query params
    - json post requests
    """
    body = None
    query = None
    if form is not None:
        request.form = form
        body = model.parse_obj(parse_multi_dict(form))

    request.context = Context(query, body, None, None)
