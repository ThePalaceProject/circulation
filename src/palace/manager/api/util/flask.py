from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, overload

import flask
from sqlalchemy.orm import Session

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.sentinel import SentinelType

if TYPE_CHECKING:
    from palace.manager.api.circulation_manager import CirculationManager


@overload
def get_request_var[TVar](
    name: str, var_cls: type[TVar], *, default: Literal[SentinelType.NotGiven] = ...
) -> TVar: ...


@overload
def get_request_var[TVar, TDefault](
    name: str, var_cls: type[TVar], *, default: TDefault
) -> TVar | TDefault: ...


def get_request_var[TVar, TDefault](
    name: str,
    var_cls: type[TVar],
    *,
    default: TDefault | Literal[SentinelType.NotGiven] = SentinelType.NotGiven,
) -> TVar | TDefault:
    """
    Retrieve an attribute from the current Flask request object.

    This helper function handles edge cases such as missing request context or unset attributes.
    It ensures type checking and provides type hints for the expected attribute type.

    :param name: The name of the attribute to retrieve.
    :param var_cls: The expected type of the attribute.
    :param default: The default value to return if the attribute is not set or if there is no request context.

    :return: The attribute from the request object, or the default value if provided.

    :raises PalaceValueError: If the attribute is not set or if the attribute type is incorrect,
        and no default is provided.
    :raises RuntimeError: If there is no request context and no default is provided.
    """

    if default is not SentinelType.NotGiven and not flask.request:
        # We are not in a request context, so we can't get the variable
        # if we access it, it will raise an error, so we return the default
        return default

    try:
        var = getattr(flask.request, name)
    except AttributeError:
        if default is SentinelType.NotGiven:
            raise PalaceValueError(f"No '{name}' set on 'flask.request'")
        return default

    if not isinstance(var, var_cls):
        if default is SentinelType.NotGiven:
            raise PalaceValueError(
                f"'{name}' on 'flask.request' has incorrect type "
                f"'{var.__class__.__name__}' expected '{var_cls.__name__}'",
            )
        return default
    return var


@overload
def get_request_library() -> Library: ...


@overload
def get_request_library[TDefault](*, default: TDefault) -> Library | TDefault: ...


def get_request_library[TDefault](
    *, default: TDefault | Literal[SentinelType.NotGiven] = SentinelType.NotGiven
) -> Library | TDefault:
    """
    Retrieve the 'library' attribute from the current Flask request object.

    This attribute should be set by using the @has_library or @allows_library decorator
    on the route or by calling the BaseCirculationManagerController.library_for_request
    method.

    Note: You need to specify a default of None if you want to allow the library to be
      None (for example if you are using the @allows_library decorator).

    :param default: The default value to return if the 'library' attribute is not set.
        If not provided, a `PalaceValueError` will be raised if the attribute is missing
        or has an incorrect type.

    :return: The `Library` object from the request, or the default value if provided.
    """
    return get_request_var("library", Library, default=default)


@overload
def get_request_patron() -> Patron: ...


@overload
def get_request_patron[TDefault](*, default: TDefault) -> Patron | TDefault: ...


def get_request_patron[TDefault](
    *, default: TDefault | Literal[SentinelType.NotGiven] = SentinelType.NotGiven
) -> Patron | TDefault:
    """
    Retrieve the 'patron' attribute from the current Flask request object.

    This attribute should be set by using the @requires_auth or @allows_auth decorator
    on the route or by calling the BaseCirculationManagerController.authenticated_patron_from_request
    method.

    :param default: The default value to return if the 'patron' attribute is not set
      or if there is no request context. If not provided, a `PalaceValueError` will be
      raised if the attribute is missing or has an incorrect type.

    :return: The `Patron` object from the request, or the default value if provided.
    """
    return get_request_var("patron", Patron, default=default)


class PalaceFlask(flask.Flask):
    """
    A subclass of Flask sets properties used by Palace.

                       ╒▓▓@=:===:╗φ
                        ,██Ü_╠░_█▌_
             ___     ─-'██▌  ▌H ▐█▓
      ______░╓___________▐█▌▄╬▄▄█____________________
     ▐██▒         `````░░░░░Ü░░░░╠╠╠╠╬╬╬╬╬╬╬╬╠▒▒Ü` ▐▓L
     ▐██▌              »»»░░░░░░░▒▒╠╠╠╬╬╬╬╬╬╠▒▒ÜÜ  ╚▓▒
     ▐██▌              `»│»»»░░░░▒▒▒╠╠╬╬╬╬╬╬╠▒▒░░  ╚▓▌
     ▐██▌              »»»»»░░░░░▒▒▒╠╠╬╬╬╬╬╬╠▒ÜÜ░  ▐▓▌
     ▐██▌             `»»»»░░░░░░▒▒▒╠╠╬╬╬╬╬╬▒▒Ü░░  ▐▓▌
     ▐█▓▌              `»»»»░░░░░▒▒▒╠╠╠╬╬╬╬╬▒▒Ü░░  ╚▓▌
     ▐█▓▌              »»»»░░░░░░▒▒▒╠╠╠╠╬╬╬╠Ü▒░░░  ╠▓▌
     ▐█▓▌              »»»»░░░░░░▒▒▒▒╠╠╠╬╬╬╠▒Ü░░░  ╠╣▌
     ▐██▌                               ╬╬╬╠▒▒░░░  ╠╣▌
     ▐██▌           The Palace Project       ▒░░░  ╠╣▌
     ▐██▌                               ╠╬╠╠▒▒░░░  ║╣▌
     ▐██▌              »»»»░░░░░░▒▒▒▒▒╠╠╠╠╠╠▒▒░░░  ║▓▌
     ▐██▌             `»»»░░░░░░░░▒▒▒▒╠╠╠╠╠▒▒▒░░░  ╠╣▌
     ▐██▌             »»»»░░░░░░░░▒Ü▒▒╠╠╠╠╠▒▒Ü░░░  ▐╫▌
     ▐██▌             `»»»░░░░░░░░▒▒▒▒╠╠╠╠╠▒▒Ü░░░  ▐╣▌
      ██╬░     `»»»``»»»»░░░░░░░░░░▒▒▒▒▒▒▒▒▒▒Ü░░░░ [╣▌
      █▓╬░     `»` »`»»»»»░░░░░░░░░░▒▒▒▒▒▒▒▒▒Ü░░░░ [╫▌
      ▓▓╬░      `   `»»»»»»░░░░░░░Ü▒▒▒▒▒▒▒▒▒▒Ü░░░░ [╫▌
      ▓▓╬░»``  » `  » »»»»░░░░░░░░▒▒▒▒▒▒▒▒▒▒▒Ü░░░░ [╫▌
      ▓▓╬░»_` ``»`»»»»»»»»»░░░░░░Ü▒▒▒▒▒▒▒▒▒▒▒▒Ü░░░ |╫▌
      ▓▓╬░»»»»»»»»»»»»»»»»░░░░░░░▒░▒▒▒▒▒▒▒▒▒▒ÜÜ░Ü░_|╟▌
      ╣▓╬░»»»»»»»»»»»»»»»»»░░░░░░░▒▒▒▒▒▒▒▒▒▒▒▒▒ÜÜ░⌐|╟▌
      ╝▓╬░__»»»»»»»»»»░»░░░░░░░░░▒▒▒▒▒▒▒▒▒▒▒▒▒▒░░░_|╠╛
      ╚╚╚╙╙╙╙╙╙╙╙╙╙╙╙╙╙╙╙╙╙╙╙╙╚╚╚╚╚╚╚ÜÜÜÜÜÜÜÜÜÜÜÜÜ╙╚ÜH

    Palace: You're going to need a stiff drink after this.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._db: Session
        self.manager: CirculationManager
