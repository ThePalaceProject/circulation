from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, TypeVar, overload

import flask
from sqlalchemy.orm import Session

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.util.sentinel import SentinelType

if TYPE_CHECKING:
    from palace.manager.api.circulation_manager import CirculationManager


TVar = TypeVar("TVar")
TDefault = TypeVar("TDefault")


@overload
def get_request_var(
    name: str, var_cls: type[TVar], *, default: Literal[SentinelType.NotGiven] = ...
) -> TVar: ...


@overload
def get_request_var(
    name: str, var_cls: type[TVar], *, default: TDefault
) -> TVar | TDefault: ...


def get_request_var(
    name: str,
    var_cls: type[TVar],
    *,
    default: TDefault | Literal[SentinelType.NotGiven] = SentinelType.NotGiven,
) -> TVar | TDefault:
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
def get_request_library(*, default: TDefault) -> Library | TDefault: ...


def get_request_library(
    *, default: TDefault | Literal[SentinelType.NotGiven] = SentinelType.NotGiven
) -> Library | TDefault:
    return get_request_var("library", Library, default=default)


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
