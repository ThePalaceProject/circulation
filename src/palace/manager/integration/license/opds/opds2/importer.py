from __future__ import annotations

from typing import Any, cast
from urllib.parse import urljoin

from pydantic import ValidationError
from sqlalchemy.orm import Session
from typing_extensions import Self

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.base import integration_settings_load
from palace.manager.integration.license.opds.opds2.api import OPDS2API
from palace.manager.integration.license.opds.opds2.extractor import Opds2Extractor
from palace.manager.integration.license.opds.requests import (
    OPDS2AuthType,
    get_opds_requests,
)
from palace.manager.opds import opds2, rwpm
from palace.manager.opds.opds2 import PublicationFeedNoValidation
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.resource import Hyperlink
from palace.manager.util.log import LoggerMixin


class OPDS2Importer(LoggerMixin):

    def __init__(
        self,
        *,
        username: str | None,
        password: str | None,
        feed_base_url: str,
        data_source: str,
        ignored_identifier_types: list[str],
        max_retry_count: int,
        accept_header: str,
    ) -> None:
        """
        Constructor.
        """
        self._request = get_opds_requests(
            (OPDS2AuthType.BASIC if username and password else OPDS2AuthType.NONE),
            username,
            password,
            feed_base_url,
        )

        self._data_source_name = data_source
        self._ignored_identifier_types = ignored_identifier_types
        self._feed_base_url = feed_base_url
        self._max_retry_count = max_retry_count
        self._accept_header = accept_header

    @classmethod
    def from_collection(
        cls, collection: Collection, registry: LicenseProvidersRegistry
    ) -> Self:
        """Create an instance from a Collection."""
        if not registry.equivalent(collection.protocol, OPDS2API):
            raise PalaceValueError(
                f"Collection {collection.name} [id={collection.id} protocol={collection.protocol}] is not a OPDS2 collection."
            )
        settings = integration_settings_load(
            OPDS2API.settings_class(), collection.integration_configuration
        )
        return cls(
            username=settings.username,
            password=settings.password,
            feed_base_url=settings.external_account_id,
            data_source=settings.data_source,
            ignored_identifier_types=settings.ignored_identifier_types,
            max_retry_count=settings.max_retry_count,
            accept_header=settings.custom_accept_header,
        )

    def get_feed(self, url: str | None) -> PublicationFeedNoValidation:
        joined_url = urljoin(self._feed_base_url, url)
        self.log.info(f"Fetching OPDS2 feed page: {joined_url}")
        return self._request(
            "GET",
            joined_url,
            parser=PublicationFeedNoValidation.model_validate_json,
            allowed_response_codes=["2xx"],
            headers={"Accept": self._accept_header},
            max_retry_count=self._max_retry_count,
        )

    @classmethod
    def next_page(cls, feed: PublicationFeedNoValidation) -> str | None:
        """Get the next page URL from the feed."""
        next_link = feed.links.get(
            rel="next", type=PublicationFeedNoValidation.content_type()
        )
        if not next_link:
            return None
        return next_link.href

    def _is_identifier_allowed(self, identifier: IdentifierData) -> bool:
        """Check the identifier and return a boolean value indicating whether CM can import it.

        :param identifier: Identifier object
        :return: Boolean value indicating whether CM can import the identifier
        """
        return identifier.type not in self._ignored_identifier_types

    @classmethod
    def _get_publication(
        cls,
        publication: dict[str, Any],
    ) -> opds2.BasePublication:
        try:
            return opds2.Publication.model_validate(publication)
        except ValidationError as e:
            raw_identifier = publication.get("metadata", {}).get("identifier")
            cls.logger().exception(
                f"Error validating publication (identifier: {raw_identifier}): {e}"
            )
            raise

    @classmethod
    def update_integration_context(
        cls, feed: PublicationFeedNoValidation, collection: Collection
    ) -> bool:
        """Parse the global feed links. Currently only parses the token endpoint link"""
        links = feed.links
        token_auth_link = links.get(rel=Hyperlink.TOKEN_AUTH)
        if token_auth_link is None:
            return False

        integration = collection.integration_configuration
        if (
            integration.context.get(OPDS2API.TOKEN_AUTH_CONFIG_KEY)
            == token_auth_link.href
        ):
            # No change, so we don't need to update the context.
            return False

        integration.context_update(
            {OPDS2API.TOKEN_AUTH_CONFIG_KEY: token_auth_link.href}
        )
        return True

    def extract_feed_data(
        self, feed: PublicationFeedNoValidation
    ) -> list[BibliographicData]:
        """
        Turn an OPDS 2.0 feed into lists of BibliographicData and CirculationData objects.
        """
        results = []
        for publication_dict in feed.publications:
            try:
                publication = self._get_publication(publication_dict)
            except ValidationError as e:
                raw_identifier = publication_dict.get("metadata", {}).get("identifier")
                raw_title = publication_dict.get("metadata", {}).get("title")
                self.log.error(
                    f"Error validating publication (identifier: {raw_identifier}, title: {raw_title}): {e}"
                )
                continue

            feed_self_url = feed.links.get(
                rel=rwpm.LinkRelations.self, raising=True
            ).href
            try:
                publication_bibliographic = Opds2Extractor.extract_publication_data(
                    publication, self._data_source_name, feed_self_url
                )
            except PalaceValueError:
                self.log.exception(
                    "Error extracting publication data. Most likely the publications identifier could not be parsed. Skipping publication."
                )
                continue

            # We cast here because we know that Opds2Extractor.extract_publication_data always sets
            # the primary_identifier_data field to an IdentifierData object.
            # TODO: Maybe we can tighten up the type hint for BibliographicData to reflect this?
            identifier = cast(
                IdentifierData, publication_bibliographic.primary_identifier_data
            )
            if identifier is None or not self._is_identifier_allowed(identifier):
                self.log.warning(
                    f"Publication {identifier} not imported because its identifier type is not allowed: {identifier.type}"
                )
                continue

            results.append(publication_bibliographic)

        return results

    @classmethod
    def is_changed(cls, session: Session, bibliographic: BibliographicData) -> bool:
        edition = bibliographic.load_edition(session)
        if not edition:
            return True

        # If we don't have any information about the last update time, assume we need to update.
        if edition.updated_at is None or bibliographic.data_source_last_updated is None:
            return True

        if bibliographic.data_source_last_updated > edition.updated_at:
            return True

        cls.logger().info(
            f"Publication {bibliographic.primary_identifier_data} is unchanged. Last updated at "
            f"{edition.updated_at}, data source last updated at {bibliographic.data_source_last_updated}"
        )
        return False
