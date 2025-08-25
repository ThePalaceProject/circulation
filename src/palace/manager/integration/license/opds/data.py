from dataclasses import dataclass


@dataclass(frozen=True)
class FailedPublication:
    """
    Represents a publication that failed to extract or import.

    Provides details about the error encountered during extraction, so that
    the caller can handle it appropriately.
    """

    error: Exception
    error_message: str
    identifier: str | None
    title: str | None
    publication_data: str
