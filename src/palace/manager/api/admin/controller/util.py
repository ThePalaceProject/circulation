from flask import Request

from palace.manager.api.admin.problem_details import ADMIN_NOT_AUTHORIZED
from palace.manager.api.problem_details import LIBRARY_NOT_FOUND
from palace.manager.sqlalchemy.model.admin import Admin
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.util.problem_detail import ProblemDetailException


def optional_admin_from_request(request: Request) -> Admin | None:
    return getattr(request, "admin", None)


def required_admin_from_request(request: Request) -> Admin:
    if (admin := optional_admin_from_request(request)) is None:
        raise ProblemDetailException(ADMIN_NOT_AUTHORIZED)
    return admin


def optional_library_from_request(request: Request) -> Library | None:
    return getattr(request, "library", None)


def required_library_from_request(request: Request) -> Library:
    if (library := optional_library_from_request(request)) is None:
        raise ProblemDetailException(LIBRARY_NOT_FOUND)
    return library
