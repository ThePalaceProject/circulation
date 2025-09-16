from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property
from typing import Generic, Literal, TypeVar
from urllib.parse import urljoin

from pydantic import ValidationError
from sqlalchemy.orm import Session

from palace.manager.celery.tasks.apply import (
    ApplyBibliographicCallable,
    ApplyCirculationCallable,
)
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.license.opds.data import FailedPublication
from palace.manager.integration.license.opds.extractor import (
    OpdsExtractor,
)
from palace.manager.integration.license.opds.requests import BaseOpdsHttpRequest
from palace.manager.opds import rwpm
from palace.manager.opds.odl.info import LicenseInfo
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.util.http.exception import BadResponseException
from palace.manager.util.log import LoggerMixin

FeedType = TypeVar("FeedType")
PublicationType = TypeVar("PublicationType")


class OpdsImporter(Generic[FeedType, PublicationType], LoggerMixin):
    """
    An importer for OPDS or OPDS+ODL feeds.
    """

    def __init__(
        self,
        request: BaseOpdsHttpRequest,
        extractor: OpdsExtractor[FeedType, PublicationType],
        feed_base_url: str,
        accept_header: str,
        ignored_identifier_types: Iterable[str],
    ) -> None:
        """
        Constructor.

        :param request: The HTTP request handler to use for fetching the feed.
        :param extractor: The extractor to use for extracting bibliographic data.
        :param settings: The settings for the importer.
        """
        self._request = request
        self._extractor = extractor
        self._feed_base_url = feed_base_url
        self._accept_header = accept_header
        self._ignored_identifier_types = set(ignored_identifier_types)

    def _absolute_url(self, url: str | None) -> str:
        """
        Get the absolute URL for the given URL relative to the feed base URL.

        :param url: The URL to convert to an absolute URL. If None, the base URL is used.

        :return: The absolute URL.
        """
        return urljoin(self._feed_base_url, url)

    def _fetch_feed(self, url: str) -> FeedType:
        """
        Fetch the feed from the given URL and parse it with the extractor.

        :param url: The URL of the feed to fetch. If None, the base URL is used.

        :return: A FeedType object containing the feed data.
        """
        joined_url = urljoin(self._feed_base_url, url)
        self.log.info(f"Fetching feed page: {joined_url}")
        return self._request(
            "GET",
            joined_url,
            parser=self._extractor.feed_parse,
            allowed_response_codes=["2xx"],
            headers={"Accept": self._accept_header},
        )

    def _is_identifier_ignored(self, identifier: IdentifierData) -> bool:
        """
        Test if the identifier should be ignored by the importer.
        """
        return identifier.type in self._ignored_identifier_types

    def _fetch_license_document(self, document_link: str) -> LicenseInfo | None:
        """
        Fetch a license document from the given link and return it as a LicenseInfo object.
        """
        try:
            return self._request(
                "GET",
                document_link,
                parser=LicenseInfo.model_validate_json,
                allowed_response_codes=["2xx"],
            )
        except BadResponseException as e:
            resp = e.response
            self.log.warning(
                f"License Info Document is not available. "
                f"Status link {document_link} failed with {resp.status_code} code."
            )
            return None
        except ValidationError as e:
            self.log.error(
                f"License Info Document at {document_link} is not valid. {e}"
            )
            return None

    def _fetch_license_documents(
        self, publication: PublicationType
    ) -> dict[str, LicenseInfo]:
        """
        Fetch the license documents for a publication.

        :param publication: The publication from which to fetch license documents.
        :return: A dictionary mapping license identifiers to LicenseInfo objects.
        """
        publication_available = self._extractor.publication_available(publication)
        return {
            license_document.identifier: license_document
            for license_ in self._extractor.publication_licenses(publication)
            if license_.metadata.availability.available
            and publication_available
            and (
                license_document := self._fetch_license_document(
                    license_.links.get(
                        rel=rwpm.LinkRelations.self,
                        type=LicenseInfo.content_type(),
                        raising=True,
                    ).href
                )
            )
            is not None
        }

    def _extract_publications_from_feed(
        self, feed: FeedType
    ) -> tuple[dict[IdentifierData, BibliographicData], list[FailedPublication]]:
        """
        Extract each publication's bibliographic data from the feed.
        """
        bibliographic_data = {}
        failures = []

        for publication in self._extractor.feed_publications(feed):
            if isinstance(publication, FailedPublication):
                failures.append(publication)
                continue

            try:
                identifier = self._extractor.publication_identifier(publication)
            except ValueError as e:
                failures.append(
                    self._extractor.failure_from_publication(
                        publication,
                        e,
                        "Could not extract an identifier from the publication",
                    )
                )
                continue

            if self._is_identifier_ignored(identifier):
                self.log.warning(
                    f"Publication {identifier} not imported because its "
                    f"identifier type is not allowed: {identifier.type}"
                )
                continue

            license_info_documents = self._fetch_license_documents(publication)

            try:
                bibliographic_data[identifier] = (
                    self._extractor.publication_bibliographic(
                        identifier, publication, license_info_documents
                    )
                )
            except ValueError as e:
                failures.append(
                    self._extractor.failure_from_publication(
                        publication,
                        e,
                        "Could not extract bibliographic data from the publication",
                    )
                )
                continue

        return bibliographic_data, failures

    def import_feed(
        self,
        collection: Collection,
        url: str | None = None,
        *,
        apply_bibliographic: ApplyBibliographicCallable,
        apply_circulation: ApplyCirculationCallable | None = None,
        identifier_set: IdentifierSet | None = None,
        import_even_if_unchanged: bool = False,
    ) -> FeedImportResult[FeedType] | Literal[False]:
        """
        Import the feed.

        This method will extract bibliographic data from the feed, check if the
        bibliographic data has changed, and if so, apply the bibliographic data
        using the provided `apply_bibliographic` callable. If the bibliographic data
        has not changed and `apply_circulation` is provided, it will also apply the
        circulation data.

        :param collection: The collection to which the data belongs.
        :param url: The url of the feed to import from, if not given or None the feeds base URL is used.
        :param apply_bibliographic: A callable that applies bibliographic data.
        :param apply_circulation: A callable that applies circulation data, or None if not applicable.
        :param identifier_set: An optional IdentifierSet to track imported identifiers.
        :param import_even_if_unchanged: If True the bibliographic data will be imported even if it has not changed.

        :return: A boolean indicating whether any publication was unchanged.
          If True, it means that at least one publication was not changed and thus not imported.
          If False, it means that all publications were either changed or imported.
        """
        session = Session.object_session(collection)
        feed_url = self._absolute_url(url)
        try:
            feed = self._fetch_feed(feed_url)
        except ValueError as e:
            self.log.error(
                f"Failed to fetch or parse the feed from '{feed_url}': {e}",
                exc_info=e,
            )
            return False

        feed_bibliographic, failures = self._extract_publications_from_feed(feed)
        next_url = self._extractor.feed_next_url(feed)
        results = {}

        for identifier, bibliographic in feed_bibliographic.items():
            has_changed = bibliographic.has_changed(session)
            called_bibliographic_apply = False
            called_circulation_apply = False
            if import_even_if_unchanged or has_changed:
                # Queue task to import publication
                apply_bibliographic(
                    bibliographic,
                    collection_id=collection.id,
                )
                called_bibliographic_apply = True
            elif (
                bibliographic.circulation is not None and apply_circulation is not None
            ):
                circulation_data = bibliographic.circulation
                # If the bibliographic data is unchanged, we still want to apply the circulation data
                apply_circulation(
                    circulation_data,
                    collection_id=collection.id,
                )
                called_circulation_apply = True
            results[identifier] = PublicationImportResult(
                bibliographic=bibliographic,
                changed=has_changed,
                called_bibliographic_apply=called_bibliographic_apply,
                called_circulation_apply=called_circulation_apply,
            )

        if identifier_set is not None:
            identifier_set.add(*feed_bibliographic.keys())

        if failures:
            self.log.error(
                f"Failed to import {len(failures)} publications from '{feed_url}'."
            )
            for failure in failures:
                self.log.error(
                    f"Failed to import publication: {failure.identifier} ({failure.title})"
                    f" - {failure.error_message}: {failure.error}",
                    exc_info=failure.error,
                    extra={"palace_publication_data": failure.publication_data},
                )

        return FeedImportResult(
            next_url=next_url,
            feed=feed,
            results=results,
            failures=failures,
            identifier_set=identifier_set,
        )


@dataclass(frozen=True)
class PublicationImportResult:
    bibliographic: BibliographicData
    changed: bool
    called_bibliographic_apply: bool
    called_circulation_apply: bool


@dataclass(frozen=True)
class FeedImportResult(Generic[FeedType], LoggerMixin):
    next_url: str | None
    feed: FeedType
    results: dict[IdentifierData, PublicationImportResult]
    failures: list[FailedPublication]
    identifier_set: IdentifierSet | None

    @cached_property
    def found_unchanged_publication(self) -> bool:
        """
        Check if any publication in the feed was unchanged.
        """
        return any(not result.changed for result in self.results.values())
