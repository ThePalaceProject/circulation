from typing import Optional


class BaseError(Exception):
    """Base class for all errors"""

    def __init__(
        self, message: Optional[str] = None, inner_exception: Optional[Exception] = None
    ):
        """Initializes a new instance of BaseError class

        :param message: String containing description of the error occurred
        :param inner_exception: (Optional) Inner exception
        """
        if inner_exception and not message:
            message = str(inner_exception)

        super().__init__(message)

        self._inner_exception = str(inner_exception) if inner_exception else None

    def __hash__(self):
        return hash(str(self))

    @property
    def inner_exception(self) -> Optional[str]:
        """Returns an inner exception

        :return: Inner exception
        """
        return self._inner_exception

    def __eq__(self, other: object) -> bool:
        """Compares two BaseError objects

        :param other: BaseError object
        :return: Boolean value indicating whether two items are equal
        """
        if not isinstance(other, BaseError):
            return False

        return str(self) == str(other)

    def __repr__(self):
        return "<BaseError(message={}, inner_exception={})>".format(
            (self), self.inner_exception
        )
