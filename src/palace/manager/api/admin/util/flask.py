from typing import Literal, overload

from palace.manager.api.util.flask import get_request_var
from palace.manager.sqlalchemy.model.admin import Admin
from palace.manager.util.sentinel import SentinelType


@overload
def get_request_admin() -> Admin: ...


@overload
def get_request_admin[TDefault](*, default: TDefault) -> Admin | TDefault: ...


def get_request_admin[TDefault](
    *, default: TDefault | Literal[SentinelType.NotGiven] = SentinelType.NotGiven
) -> Admin | TDefault:
    """
    Retrieve the 'admin' attribute from the current Flask request object.

    This attribute should be set by using the @requires_admin decorator on the route
    or by calling the AdminController.authenticated_admin_from_request
    method.

    :param default: The default value to return if the 'admin' attribute is not set.
        If not provided, a `PalaceValueError` will be raised if the attribute is missing
        or has an incorrect type.

    :return: The `Admin` object from the request, or the default value if provided.
    """
    return get_request_var("admin", Admin, default=default)
