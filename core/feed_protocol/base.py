from abc import ABC, abstractmethod

from flask import Response


class FeedProtocol(ABC):
    @abstractmethod
    def generate_feed(
        self,
    ) -> None:
        ...

    @abstractmethod
    def as_response(
        self,
    ) -> Response:
        ...
