class BaseError(Exception):
    """Base class for all errors"""

    def __init__(
        self, message: str | None = None, inner_exception: Exception | None = None
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
    def inner_exception(self) -> str | None:
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


class IntegrationException(Exception):
    """An exception that happens when the site's connection to a
    third-party service is broken.

    This may be because communication failed
    (RemoteIntegrationException), or because local configuration is
    missing or obviously wrong (CannotLoadConfiguration).
    """

    def __init__(self, message, debug_message=None):
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
