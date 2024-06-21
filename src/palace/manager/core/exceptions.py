class BasePalaceException(Exception):
    """Base class for all Exceptions in the Palace manager."""

    def __init__(self, message: str | None = None):
        """Initializes a new instance of BasePalaceException class

        :param message: String containing description of the exception that occurred
        """
        super().__init__(message)
        self.message = message


class PalaceValueError(BasePalaceException, ValueError):
    ...


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
