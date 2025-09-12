from __future__ import annotations

import asyncio
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
from palace.manager.opds.odl.odl import License
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.util.http.async_http import AsyncClient
from palace.manager.util.http.exception import BadResponseException
from palace.manager.util.log import LoggerMixin, elapsed_time_logging

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
        *,
        async_http_client: AsyncClient | None = None,
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
        self._async_http_client = async_http_client or AsyncClient.for_worker()

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

    async def _fetch_license_document(
        self, license_: License
    ) -> tuple[str, LicenseInfo] | None:
        """
        Fetch a license document from the given link and return it as a LicenseInfo object.
        """
        license_link = license_.links.get(
            rel=rwpm.LinkRelations.self,
            type=LicenseInfo.content_type(),
            raising=True,
        ).href

        try:
            response = await self._async_http_client.get(
                license_link, allowed_response_codes=["2xx"]
            )
            return license_.metadata.identifier, LicenseInfo.model_validate_json(
                response.content
            )
        except BadResponseException as e:
            self.log.warning(
                f"License Info Document is not available. "
                f"Status link {license_link} failed with {e.response.status_code} code. {e}"
            )
            return None
        except ValidationError as e:
            self.log.error(f"License Info Document at {license_link} is not valid. {e}")
            return None

    async def _fetch_license_documents(
        self, publication: PublicationType
    ) -> dict[str, LicenseInfo]:
        """
        Fetch the license documents for a publication.

        :param publication: The publication from which to fetch license documents.
        :return: A dictionary mapping license identifiers to LicenseInfo objects.
        """
        publication_available = self._extractor.publication_available(publication)
        requests = [
            self._fetch_license_document(license_)
            for license_ in self._extractor.publication_licenses(publication)
            if license_.metadata.availability.available and publication_available
        ]
        responses = [
            response
            for response in await asyncio.gather(*requests)
            if response is not None
        ]

        return dict(responses)

    def _license_document_urls(self, publication: PublicationType) -> dict[str, str]:
        """
        Get the license document URLs for a publication.

        :param publication: The publication from which to get license document URLs.
        :return: A list of license document URLs.
        """
        publication_available = self._extractor.publication_available(publication)
        if not publication_available:
            return {}

        return {
            license_.metadata.identifier: license_.links.get(
                rel=rwpm.LinkRelations.self,
                type=LicenseInfo.content_type(),
                raising=True,
            ).href
            for license_ in self._extractor.publication_licenses(publication)
            if license_.metadata.availability.available
        }

    async def _fetch_license_documents_concurrently(
        self,
        results: list[tuple[IdentifierData, PublicationType, dict[str, str]]],
    ) -> list[tuple[IdentifierData, PublicationType, dict[str, LicenseInfo]]]:
        """
        Fetch license documents for multiple publications concurrently.

        :param results: A list of tuples containing the identifier, publication, and license document URLs.
        :return: A list of tuples containing the identifier, publication, and fetched license documents.
        """
        tasks = [
            self._fetch_license_documents(publication) for _, publication, _ in results
        ]
        fetched_license_documents = await asyncio.gather(*tasks)

        return [
            (identifier, publication, license_info_documents)
            for (identifier, publication, _), license_info_documents in zip(
                results, fetched_license_documents
            )
        ]

    def _extract_publications_from_feed(
        self, feed: FeedType
    ) -> tuple[dict[IdentifierData, BibliographicData], list[FailedPublication]]:
        """
        Extract each publication's bibliographic data from the feed.

        This method processes publications in three phases:
        1. Initial validation and filtering
        2. Concurrent license document fetching
        3. Bibliographic data extraction
        """
        # Phase 1: Initial validation and filtering
        valid_results, failures = self._validate_and_filter_publications(feed)

        if not valid_results:
            return {}, failures

        # Phase 2: Fetch license documents concurrently
        publications_with_licenses = self._fetch_all_license_documents(valid_results)

        # Phase 3: Extract bibliographic data
        bibliographic_data, extraction_failures = self._extract_bibliographic_data(
            publications_with_licenses
        )
        failures.extend(extraction_failures)

        return bibliographic_data, failures

    def _validate_and_filter_publications(self, feed: FeedType) -> tuple[
        list[tuple[IdentifierData, PublicationType, dict[str, str]]],
        list[FailedPublication],
    ]:
        """
        Phase 1: Validate publications and filter out invalid ones.

        Returns a tuple of (valid_results, failures).
        """
        results = []
        failures = []

        for publication in self._extractor.feed_publications(feed):
            # Handle already failed publications
            if isinstance(publication, FailedPublication):
                failures.append(publication)
                continue

            # Extract and validate identifier
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

            # Check if identifier type should be ignored
            if self._is_identifier_ignored(identifier):
                self.log.warning(
                    f"Publication {identifier} not imported because its "
                    f"identifier type is not allowed: {identifier.type}"
                )
                continue

            # Get license document URLs for later fetching
            license_urls = self._license_document_urls(publication)
            results.append((identifier, publication, license_urls))

        return results, failures

    def _fetch_all_license_documents(
        self, results: list[tuple[IdentifierData, PublicationType, dict[str, str]]]
    ) -> list[tuple[IdentifierData, PublicationType, dict[str, LicenseInfo]]]:
        """
        Phase 2: Fetch license documents concurrently for all valid publications.
        """
        if not results:
            return []

        with elapsed_time_logging(
            log_method=self.log.info,
            message_prefix=f"Fetching {len(results)} license documents",
        ):
            results_with_license_info = asyncio.run(
                self._fetch_license_documents_concurrently(results)
            )

        return results_with_license_info

    def _extract_bibliographic_data(
        self,
        publications_with_licenses: list[
            tuple[IdentifierData, PublicationType, dict[str, LicenseInfo]]
        ],
    ) -> tuple[dict[IdentifierData, BibliographicData], list[FailedPublication]]:
        """
        Phase 3: Extract bibliographic data from publications with license information.
        """
        bibliographic_data = {}
        failures = []

        for (
            identifier,
            publication,
            license_info_documents,
        ) in publications_with_licenses:
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
