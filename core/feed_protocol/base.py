from abc import ABC
from typing import Any, Dict

from flask import Response


class FeedProtocol(ABC):
    def generate_feed(
        self,
    ):
        ...

    def response_headers(
        self,
    ) -> Dict[str, Any]:
        ...

    def as_response(
        self,
    ) -> Response:
        ...
