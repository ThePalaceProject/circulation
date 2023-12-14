from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from core.feed.types import FeedData, WorkEntryData
from core.util.opds_writer import OPDSMessage

T = TypeVar("T")


class SerializerInterface(ABC, Generic[T]):
    @classmethod
    @abstractmethod
    def to_string(cls, data: T) -> str:
        ...

    @abstractmethod
    def serialize_feed(
        self, feed: FeedData, precomposed_entries: list[OPDSMessage] | None = None
    ) -> str:
        ...

    @abstractmethod
    def serialize_work_entry(self, entry: WorkEntryData) -> T:
        ...

    @abstractmethod
    def serialize_opds_message(self, message: OPDSMessage) -> T:
        ...

    @abstractmethod
    def content_type(self) -> str:
        ...
