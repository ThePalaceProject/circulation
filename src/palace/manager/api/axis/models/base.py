from abc import ABC, abstractmethod


class BaseAxisResponse(ABC):
    """
    Abstract base class for Axis API responses.

    All the API responses are expected to have a `status` field, so this base class
    provides a common interface for raising exceptions based on the status code.
    """

    @abstractmethod
    def raise_on_error(self) -> None: ...
