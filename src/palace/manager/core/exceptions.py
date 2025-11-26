from typing import Any


class BasePalaceException(Exception):
    """Base class for all Exceptions in the Palace manager."""

    def __init__(self, message: str | None = None):
        """Initializes a new instance of BasePalaceException class

        :param message: String containing description of the exception that occurred
        """
        super().__init__(message)
        self.message = message

    def __getstate__(self) -> dict[str, Any]:
        return {"dict": self.__dict__, "args": self.args}

    def __setstate__(self, state: dict[str, Any] | None) -> None:
        # state is always a dict from __getstate__, but the signature must
        # accept None to match BaseException.__setstate__
        assert state is not None
        self.__dict__.update(state["dict"])
        self.args = state["args"]

    def __reduce__(self) -> tuple[Any, ...]:
        state = self.__getstate__()
        return self.__class__.__new__, (self.__class__,), state


class PalaceValueError(BasePalaceException, ValueError): ...


class PalaceTypeError(BasePalaceException, TypeError): ...


class IntegrationException(BasePalaceException):
    """An exception that happens when the site's connection to a
    third-party service is broken.

    This may be because communication failed
    (RemoteIntegrationException), or because local configuration is
    missing or obviously wrong (CannotLoadConfiguration).
    """

    def __init__(self, message: str | None, debug_message: str | None = None) -> None:
        """Constructor.

        :param message: The normal message passed to any Exception
        constructor.

        :param debug_message: An extra human-readable explanation of the
        problem, shown to admins but not to patrons. This may include
        instructions on what bits of the integration configuration might need
        to be changed.

        For example, an API key might be wrong, or the API key might
        be correct but the API provider might not have granted that
        key enough permissions.
        """
        super().__init__(message)
        self.debug_message = debug_message
