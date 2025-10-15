from abc import ABC, abstractmethod

from palace.manager.feed.types import FeedData, WorkEntryData
from palace.manager.util.opds_writer import OPDSMessage


class SerializerInterface[T](
    ABC,
):
    @classmethod
    @abstractmethod
    def to_string(cls, data: T) -> str: ...

    @abstractmethod
    def serialize_feed(
        self, feed: FeedData, precomposed_entries: list[OPDSMessage] | None = None
    ) -> str: ...

    @abstractmethod
    def serialize_work_entry(self, entry: WorkEntryData) -> T: ...

    @abstractmethod
    def serialize_opds_message(self, message: OPDSMessage) -> T: ...

    @abstractmethod
    def content_type(self) -> str: ...
