from typing import Any


class BasePalaceException(Exception):
    """Base class for all Exceptions in Palace packages."""

    def __init__(self, message: str | None = None):
        """Initializes a new instance of BasePalaceException class

        :param message: String containing description of the exception that occurred
        """
        super().__init__(message)
        self.message = message

    def __getstate__(self) -> dict[str, Any]:
        return {"dict": self.__dict__, "args": self.args}

    def __setstate__(self, state: dict[str, Any] | None) -> None:
        # The signature must accept None to match BaseException.__setstate__ for mypy.
        # In practice, state is always a dict from our __getstate__ implementation.
        assert (
            state is not None
        ), "__setstate__ received None; expected dict from __getstate__"
        self.__dict__.update(state["dict"])
        self.args = state["args"]

    def __reduce__(self) -> tuple[Any, ...]:
        state = self.__getstate__()
        return self.__class__.__new__, (self.__class__,), state


class PalaceValueError(BasePalaceException, ValueError): ...


class PalaceTypeError(BasePalaceException, TypeError): ...
