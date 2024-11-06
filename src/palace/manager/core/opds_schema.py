from collections.abc import Callable
from typing import Any

from pydantic import ValidationError
from requests import Response

from palace.manager.api.odl.importer import OPDS2WithODLImportMonitor
from palace.manager.core.coverage import CoverageFailure
from palace.manager.core.opds2_import import OPDS2ImportMonitor
from palace.manager.opds.odl.odl import Feed
from palace.manager.opds.opds2 import BasePublicationFeed, PublicationFeed
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.util.log import LoggerMixin


class OPDS2SchemaValidationMixin(LoggerMixin):
    @classmethod
    def validate_schema(
        cls, feed_cls: type[BasePublicationFeed[Any]], feed: bytes | str
    ) -> None:
        try:
            feed_cls.model_validate_json(feed)
        except ValidationError as e:
            print(str(e))
            raise


class OPDS2SchemaValidation(OPDS2ImportMonitor, OPDS2SchemaValidationMixin):
    def import_one_feed(
        self, feed: bytes | str
    ) -> tuple[list[Edition], dict[str, list[CoverageFailure]]]:
        self.validate_schema(PublicationFeed, feed)
        return [], {}

    def follow_one_link(
        self, url: str, do_get: Callable[..., Response] | None = None
    ) -> tuple[list[str], bytes | None]:
        """We don't need all pages, the first page should be fine for validation"""
        next_links, feed = super().follow_one_link(url, do_get)
        return [], feed

    def feed_contains_new_data(self, feed: bytes | str) -> bool:
        return True


class OPDS2WithODLSchemaValidation(
    OPDS2WithODLImportMonitor, OPDS2SchemaValidationMixin
):
    def import_one_feed(
        self, feed: bytes | str
    ) -> tuple[list[Edition], dict[str, list[CoverageFailure]]]:
        self.validate_schema(Feed, feed)
        return [], {}

    def follow_one_link(
        self, url: str, do_get: Callable[..., Response] | None = None
    ) -> tuple[list[str], bytes | None]:
        """We don't need all pages, the first page should be fine for validation"""
        next_links, feed = super().follow_one_link(url, do_get)
        return [], feed

    def feed_contains_new_data(self, feed: bytes | str) -> bool:
        return True
