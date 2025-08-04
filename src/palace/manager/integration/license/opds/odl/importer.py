from __future__ import annotations

from collections.abc import Callable, Generator
from typing import Any, Generic, Protocol, TypeVar
from urllib.parse import urljoin

from pydantic import TypeAdapter, ValidationError
from sqlalchemy.orm import Session

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.base import integration_settings_load
from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
from palace.manager.integration.license.opds.odl.extractor import OPDS2WithODLExtractor
from palace.manager.integration.license.opds.opds2.settings import OPDS2ImporterSettings
from palace.manager.integration.license.opds.requests import (
    BaseOpdsHttpRequest,
    OPDS2AuthType,
    get_opds_requests,
)
from palace.manager.opds import opds2, rwpm
from palace.manager.opds.odl import odl
from palace.manager.opds.odl.info import LicenseInfo
from palace.manager.opds.odl.odl import Opds2OrOpds2WithOdlPublication
from palace.manager.opds.opds2 import PublicationFeedNoValidation
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.util.http import BadResponseException
from palace.manager.util.log import LoggerMixin


class ApplyBibliographicCallable(Protocol):
    def __call__(
        self, bibliographic: BibliographicData, /, *, collection_id: int
    ) -> Any: ...


class ApplyCirculationCallable(Protocol):
    def __call__(
        self, circulation: CirculationData, /, *, collection_id: int
    ) -> Any: ...


class ImporterSettingsProtocol(Protocol):
    @property
    def external_account_id(self) -> str: ...
    @property
    def ignored_identifier_types(self) -> list[str]: ...
    @property
    def custom_accept_header(self) -> str: ...
    @property
    def max_retry_count(self) -> int: ...


PublicationType = TypeVar("PublicationType", bound=opds2.BasePublication)
SettingsType = TypeVar("SettingsType", bound=ImporterSettingsProtocol)


class OPDS2WithODLImporter(Generic[PublicationType, SettingsType], LoggerMixin):

    def __init__(
        self,
        request: BaseOpdsHttpRequest,
        extractor: OPDS2WithODLExtractor,
        parse_publication: Callable[[dict[str, Any]], PublicationType],
        settings: SettingsType,
    ) -> None:
        """
        Constructor.
        """
        self._request = request
        self._extractor = extractor
        self._settings = settings
        self._parse_publication = parse_publication
        self._feed_base_url = settings.external_account_id
        self._ignored_identifier_types = set(settings.ignored_identifier_types)

    def get_feed(self, url: str | None) -> PublicationFeedNoValidation:
        joined_url = urljoin(self._feed_base_url, url)
        self.log.info(f"Fetching feed page: {joined_url}")
        return self._request(
            "GET",
            joined_url,
            parser=PublicationFeedNoValidation.model_validate_json,
            allowed_response_codes=["2xx"],
            headers={"Accept": self._settings.custom_accept_header},
            max_retry_count=self._settings.max_retry_count,
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

    def _filtered_publications(
        self, publications: list[dict[str, Any]]
    ) -> Generator[tuple[IdentifierData, PublicationType]]:
        for publication_dict in publications:
            try:
                publication = self._parse_publication(publication_dict)
            except ValidationError as e:
                raw_identifier = publication_dict.get("metadata", {}).get("identifier")
                raw_title = publication_dict.get("metadata", {}).get("title")
                self.log.error(
                    f"Error validating publication (identifier: {raw_identifier}, title: {raw_title}): {e}"
                )
                continue

            try:
                identifier = self._extractor.extract_identifier(publication)
            except PalaceValueError:
                self.log.exception(
                    "The publications identifier could not be parsed. Skipping publication."
                )
                continue

            if not self._is_identifier_allowed(identifier):
                self.log.warning(
                    f"Publication {identifier} not imported because its identifier type is not allowed: {identifier.type}"
                )
                continue

            yield identifier, publication

    def _fetch_license_document(self, document_link: str) -> LicenseInfo | None:
        try:
            return self._request(
                "GET",
                document_link,
                parser=LicenseInfo.model_validate_json,
                allowed_response_codes=["2xx"],
                max_retry_count=self._settings.max_retry_count,
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
    ) -> dict[str, LicenseInfo] | None:
        publication_available = publication.metadata.availability.available
        return (
            {
                license_info.identifier: license_info
                for odl_license in publication.licenses
                if odl_license.metadata.availability.available
                and publication_available
                and (
                    license_info := self._fetch_license_document(
                        odl_license.links.get(
                            rel=rwpm.LinkRelations.self,
                            type=LicenseInfo.content_type(),
                            raising=True,
                        ).href
                    )
                )
                is not None
            }
            if isinstance(publication, odl.Publication)
            else None
        )

    def extract_feed_data(
        self, feed: PublicationFeedNoValidation
    ) -> dict[IdentifierData, BibliographicData]:
        results = {}

        for identifier, publication in self._filtered_publications(feed.publications):
            license_info_documents = self._fetch_license_documents(publication)
            results[identifier] = self._extractor.extract(
                identifier, publication, license_info_documents
            )

        return results

    def import_feed(
        self,
        session: Session,
        feed: PublicationFeedNoValidation,
        collection: Collection,
        *,
        apply_bibliographic: ApplyBibliographicCallable,
        apply_circulation: ApplyCirculationCallable | None = None,
        identifier_set: IdentifierSet | None = None,
        import_even_if_unchanged: bool = False,
    ) -> bool:
        feed_data = self.extract_feed_data(feed)

        unchanged_publication = False
        for bibliographic in feed_data.values():
            if import_even_if_unchanged or self.is_changed(session, bibliographic):
                # Queue task to import publication
                apply_bibliographic(
                    bibliographic,
                    collection_id=collection.id,
                )
            else:
                unchanged_publication = True
                if (
                    bibliographic.circulation is not None
                    and apply_circulation is not None
                ):
                    circulation_data = bibliographic.circulation
                    # If the bibliographic data is unchanged, we still want to apply the circulation data
                    apply_circulation(
                        circulation_data,
                        collection_id=collection.id,
                    )

        if identifier_set is not None:
            identifier_set.add(*feed_data.keys())

        return unchanged_publication


_ODL_PUBLICATION_ADAPTOR: TypeAdapter[Opds2OrOpds2WithOdlPublication] = TypeAdapter(
    Opds2OrOpds2WithOdlPublication
)


def importer_from_collection(
    collection: Collection, registry: LicenseProvidersRegistry
) -> OPDS2WithODLImporter[Opds2OrOpds2WithOdlPublication, OPDS2ImporterSettings]:
    if not registry.equivalent(collection.protocol, OPDS2WithODLApi):
        raise PalaceValueError(
            f"Collection {collection.name} [id={collection.id} protocol={collection.protocol}] is not a OPDS2+ODL collection."
        )
    settings = integration_settings_load(
        OPDS2WithODLApi.settings_class(), collection.integration_configuration
    )
    request = get_opds_requests(
        settings.auth_type,
        settings.username,
        settings.password,
        settings.external_account_id,
    )
    extractor = OPDS2WithODLExtractor(
        settings.external_account_id,
        settings.data_source,
        settings.skipped_license_formats,
        settings.auth_type == OPDS2AuthType.OAUTH,
    )
    return OPDS2WithODLImporter(
        request, extractor, _ODL_PUBLICATION_ADAPTOR.validate_python, settings
    )
