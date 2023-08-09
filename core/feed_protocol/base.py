from abc import ABC, abstractmethod

from flask import Response


class FeedProtocol(ABC):
    @abstractmethod
    def generate_feed(
        self,
        work_entries,
    ):
        ...

    @abstractmethod
    def as_response(
        self,
    ) -> Response:
        ...
