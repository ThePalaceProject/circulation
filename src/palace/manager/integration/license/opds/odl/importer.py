from __future__ import annotations

import datetime
from collections.abc import Callable, Mapping, Sequence
from functools import cached_property
from typing import Any, cast
from urllib.parse import urljoin

from frozendict import frozendict
from pydantic import TypeAdapter, ValidationError
from requests import Response
from sqlalchemy.orm import Session

from palace.manager.core.coverage import CoverageFailure
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.license import LicenseData
from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
from palace.manager.integration.license.opds.odl.constants import FEEDBOOKS_AUDIO
from palace.manager.integration.license.opds.odl.settings import OPDS2WithODLSettings
from palace.manager.integration.license.opds.opds1 import (
    BaseOPDSImporter,
    OPDSImportMonitor,
)
from palace.manager.integration.license.opds.opds2 import (
    Opds2Extractor,
)
from palace.manager.integration.license.opds.requests import (
    OPDS2AuthType,
    get_opds_requests,
)
from palace.manager.opds import opds2, rwpm
from palace.manager.opds.lcp.status import LoanStatus
from palace.manager.opds.odl import odl
from palace.manager.opds.odl.info import LicenseInfo, LicenseStatus
from palace.manager.opds.odl.odl import Opds2OrOpds2WithOdlPublication
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    DeliveryMechanismTuple,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.resource import Hyperlink
from palace.manager.util.http import HTTP, BadResponseException, GetRequestCallable
from palace.manager.util.log import LoggerMixin


class OPDS2WithODLImporter(BaseOPDSImporter[OPDS2WithODLSettings]):
    """
    Import information and formats from an ODL feed.
    """

    NAME = OPDS2WithODLApi.label()

    @classmethod
    def settings_class(cls) -> type[OPDS2WithODLSettings]:
        return OPDS2WithODLSettings

    def __init__(
        self,
        db: Session,
        collection: Collection,
        data_source_name: str | None = None,
        http_get: GetRequestCallable | None = None,
    ):
        """Initialize a new instance of OPDS2WithODLImporter class.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param collection: Circulation Manager's collection.
            LicensePools created by this OPDS2Import class will be associated with the given Collection.
            If this is None, no LicensePools will be created -- only Editions.
        :type collection: Collection

        :param data_source_name: Name of the source of this OPDS feed.
            All Editions created by this import will be associated with this DataSource.
            If there is no DataSource with this name, one will be created.
            NOTE: If `collection` is provided, its .data_source will take precedence over any value provided here.
            This is only for use when you are importing OPDS metadata without any particular Collection in mind.
        :type data_source_name: str
        """
        super().__init__(
            db,
            collection,
            data_source_name,
        )

        self.http_get = http_get or HTTP.get_with_timeout
        self.ignored_identifier_types = self.settings.ignored_identifier_types

    @classmethod
    def fetch_license_info(
        cls, document_link: str, do_get: Callable[..., Response]
    ) -> LicenseInfo | None:
        resp = do_get(document_link, headers={})
        if resp.status_code in (200, 201):
            try:
                return LicenseInfo.model_validate_json(resp.content)
            except ValidationError as e:
                cls.logger().error(
                    f"License Info Document at {document_link} is not valid. {e}"
                )
                return None
        else:
            cls.logger().warning(
                f"License Info Document is not available. "
                f"Status link {document_link} failed with {resp.status_code} code."
            )
            return None

    @cached_property
    def _publication_type_adapter(self) -> TypeAdapter[Opds2OrOpds2WithOdlPublication]:
        return TypeAdapter(Opds2OrOpds2WithOdlPublication)

    def _get_publication(
        self,
        publication: dict[str, Any],
    ) -> opds2.Publication | odl.Publication:
        return self._publication_type_adapter.validate_python(publication)

    def _parse_feed(self, feed: str | bytes) -> opds2.PublicationFeedNoValidation:
        try:
            return opds2.PublicationFeedNoValidation.model_validate_json(feed)
        except ValidationError as e:
            self.log.exception(f"Error parsing feed: {e}")
            raise

    def extract_next_links(self, feed: str | bytes) -> list[str]:
        """Extracts "next" links from the feed.

        :param feed: OPDS 2.0 feed
        :return: List of "next" links
        """
        try:
            parsed_feed = self._parse_feed(feed)
        except ValidationError:
            return []

        next_links = [
            next_link.href for next_link in parsed_feed.links.get_collection(rel="next")
        ]

        return next_links

    def extract_last_update_dates(
        self, feed: str | bytes
    ) -> list[tuple[str | None, datetime.datetime | None]]:
        """Extract last update date of the feed.

        :param feed: OPDS 2.0 feed
        :return: A list of 2-tuples containing publication's identifiers and their last modified dates
        """
        last_update_dates: list[tuple[str | None, datetime.datetime | None]] = []
        try:
            parsed_feed = self._parse_feed(feed)
        except ValidationError:
            return last_update_dates

        for publication_dict in parsed_feed.publications:
            try:
                publication = self._get_publication(publication_dict)
            except ValidationError:
                continue
            last_update_dates.append(
                (publication.metadata.identifier, publication.metadata.modified)
            )
        return last_update_dates

    def _record_coverage_failure(
        self,
        failures: dict[str, list[CoverageFailure]],
        identifier: Identifier,
        error_message: str,
        transient: bool = True,
    ) -> CoverageFailure:
        """Record a new coverage failure.

        :param failures: Dictionary mapping publication identifiers to corresponding CoverageFailure objects
        :param identifier: Publication's identifier
        :param error_message: Message describing the failure
        :param transient: Boolean value indicating whether the failure is final or it can go away in the future
        :return: CoverageFailure object describing the error
        """
        if identifier.identifier is None:
            raise ValueError

        if identifier not in failures:
            failures[identifier.identifier] = []

        failure = CoverageFailure(
            identifier,
            error_message,
            data_source=self.data_source,
            transient=transient,
            collection=self.collection,
        )
        failures[identifier.identifier].append(failure)

        return failure

    def _record_publication_unrecognizable_identifier(
        self, identifier: str | None, title: str | None
    ) -> None:
        """Record a publication's unrecognizable identifier, i.e. identifier that has an unknown format
            and could not be parsed by CM.

        :param publication: OPDS 2.x publication object
        """
        if identifier is None:
            self.log.warning(f"Publication '{title}' does not have an identifier.")
        else:
            self.log.warning(
                f"Publication # {identifier} ('{title}') has an unrecognizable identifier."
            )

    def _is_identifier_allowed(self, identifier: Identifier) -> bool:
        """Check the identifier and return a boolean value indicating whether CM can import it.

        :param identifier: Identifier object
        :return: Boolean value indicating whether CM can import the identifier
        """
        return identifier.type not in self.ignored_identifier_types

    def _get_allowed_identifier(
        self, identifier: str | None, title: str | None
    ) -> Identifier | None:
        recognized_identifier = self.parse_identifier(identifier)
        if not recognized_identifier or not self._is_identifier_allowed(
            recognized_identifier
        ):
            self._record_publication_unrecognizable_identifier(identifier, title)
            return None
        return recognized_identifier

    def extract_feed_data(
        self, feed: str | bytes, feed_url: str | None = None
    ) -> tuple[dict[str, BibliographicData], dict[str, list[CoverageFailure]]]:
        """Turn an OPDS 2.0 feed into lists of BibliographicData and CirculationData objects.
        :param feed: OPDS 2.0 feed
        :param feed_url: Feed URL used to resolve relative links
        """
        try:
            parsed_feed = self._parse_feed(feed)
        except ValidationError:
            return {}, {}

        publication_bibliographic_dictionary = {}
        failures: dict[str, list[CoverageFailure]] = {}

        feed_self_url = parsed_feed.links.get(
            rel=rwpm.LinkRelations.self, raising=True
        ).href

        for publication_dict in parsed_feed.publications:
            try:
                publication = self._get_publication(publication_dict)
            except ValidationError as e:
                raw_identifier = publication_dict.get("metadata", {}).get("identifier")
                raw_title = publication_dict.get("metadata", {}).get("title")
                recognized_identifier = self._get_allowed_identifier(
                    raw_identifier, raw_title
                )
                if recognized_identifier:
                    self._record_coverage_failure(
                        failures, recognized_identifier, str(e)
                    )

                continue
            recognized_identifier = self._get_allowed_identifier(
                publication.metadata.identifier, str(publication.metadata.title)
            )

            if not recognized_identifier:
                continue

            publication_available = publication.metadata.availability.available

            license_info_documents = (
                [
                    (
                        self.fetch_license_info(
                            odl_license.links.get(
                                rel=rwpm.LinkRelations.self,
                                type=LicenseInfo.content_type(),
                                raising=True,
                            ).href,
                            self.http_get,
                        )
                        if odl_license.metadata.availability.available
                        and publication_available
                        else None
                    )
                    for odl_license in publication.licenses
                ]
                if isinstance(publication, odl.Publication)
                else []
            )

            publication_bibliographic = OPDS2WithODLExtractor.extract_publication_data(
                publication,
                license_info_documents,
                self.data_source.name,
                feed_self_url,
                self.settings.auth_type,
                set(self.settings.skipped_license_formats),
            )

            # Make sure we have a primary identifier before trying to use it
            if publication_bibliographic.primary_identifier_data is not None:
                publication_bibliographic_dictionary[
                    publication_bibliographic.primary_identifier_data.identifier
                ] = publication_bibliographic

        return publication_bibliographic_dictionary, failures


class OPDS2WithODLExtractor(LoggerMixin):
    _LICENSE_FORMATS_MAPPING = frozendict(
        {
            FEEDBOOKS_AUDIO: DeliveryMechanismTuple(
                MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
                DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM,
            )
        }
    )

    _SUPPORTED_BEARER_TOKEN_MEDIA_TYPES = frozenset(
        (
            frmt
            for frmt, drm in DeliveryMechanism.default_client_can_fulfill_lookup
            if drm == DeliveryMechanism.BEARER_TOKEN and frmt is not None
        )
    )

    @classmethod
    def _process_unlimited_access_title(
        cls, circulation: CirculationData, auth_type: OPDS2AuthType
    ) -> None:
        if auth_type != OPDS2AuthType.OAUTH:
            return

        # Links to items with a non-open access acquisition type cannot be directly accessed
        # if the feed is protected by OAuth. So we need to add a BEARER_TOKEN delivery mechanism
        # to the formats, so we know we are able to fulfill these items indirectly via a bearer token.

        def create_format_data(frmt: FormatData) -> FormatData:
            return FormatData(
                content_type=frmt.content_type,
                drm_scheme=DeliveryMechanism.BEARER_TOKEN,
                link=frmt.link,
                rights_uri=RightsStatus.IN_COPYRIGHT,
            )

        new_formats = [
            (
                create_format_data(frmt)
                if frmt.content_type in cls._SUPPORTED_BEARER_TOKEN_MEDIA_TYPES
                and frmt.drm_scheme is None
                and (frmt.link and frmt.link.rel == Hyperlink.GENERIC_OPDS_ACQUISITION)
                else frmt
            )
            for frmt in circulation.formats
        ]

        circulation.formats = new_formats

    @classmethod
    def _extract_publication_odl_data(
        cls,
        publication: odl.Publication,
        license_info_documents: Sequence[LicenseInfo | None],
        skipped_license_formats: set[str],
    ) -> tuple[str | None, list[FormatData], list[LicenseData]]:
        formats = []
        licenses = []
        medium = None

        publication_available = publication.metadata.availability.available

        for odl_license, license_info_document in zip(
            publication.licenses, license_info_documents
        ):
            if (
                license_info_document is None
                and odl_license.metadata.availability.available
                and publication_available
            ):
                cls.logger().warning(
                    f"No License info document for license {odl_license.metadata.identifier}, skipping."
                )
                continue

            identifier = odl_license.metadata.identifier

            parsed_license = (
                LicenseData(
                    identifier=identifier,
                    checkout_url=None,
                    status_url=odl_license.links.get(
                        rel=rwpm.LinkRelations.self,
                        type=LicenseInfo.content_type(),
                        raising=True,
                    ).href,
                    status=LicenseStatus.unavailable,
                    checkouts_available=0,
                )
                if license_info_document is None
                else cls._extract_license_data(
                    license_info_document,
                    odl_license,
                )
            )

            if parsed_license is not None:
                licenses.append(parsed_license)

            license_formats = set(odl_license.metadata.formats)
            for license_format in license_formats:
                if license_format in skipped_license_formats:
                    continue

                if not medium:
                    medium = Edition.medium_from_media_type(license_format)

                drm_schemes: Sequence[str | None]
                if (
                    updated_format := cls._LICENSE_FORMATS_MAPPING.get(license_format)
                ) is not None and updated_format.content_type is not None:
                    # Special case to handle DeMarque audiobooks which include the protection
                    # in the content type. When we see a license format of
                    # application/audiobook+json; protection=http://www.feedbooks.com/audiobooks/access-restriction
                    # it means that this audiobook title is available through the DeMarque streaming manifest
                    # endpoint.
                    drm_schemes = [updated_format.drm_scheme]
                    license_format = updated_format.content_type
                else:
                    drm_schemes = (
                        odl_license.metadata.protection.formats
                        if odl_license.metadata.protection
                        else []
                    )

                for drm_scheme in drm_schemes or [None]:
                    formats.append(
                        FormatData(
                            content_type=license_format,
                            drm_scheme=drm_scheme,
                            rights_uri=RightsStatus.IN_COPYRIGHT,
                        )
                    )

        return medium, formats, licenses

    @classmethod
    def _extract_license_data(
        cls,
        license_info_document: LicenseInfo,
        odl_license: odl.License,
    ) -> LicenseData | None:

        checkout_link = odl_license.links.get(
            rel=opds2.AcquisitionLinkRelations.borrow,
            type=LoanStatus.content_type(),
            raising=True,
        ).href

        license_info_document_link = odl_license.links.get(
            rel=rwpm.LinkRelations.self,
            type=LicenseInfo.content_type(),
            raising=True,
        ).href

        status = None

        if license_info_document.identifier != odl_license.metadata.identifier:
            # There is a mismatch between the license info document and
            # the feed we are importing. Since we don't know which to believe
            # we log an error and continue.
            cls.logger().error(
                f"Mismatch between license identifier in the feed ({odl_license.metadata.identifier}) "
                f"and the identifier in the license info document "
                f"({license_info_document.identifier}) ignoring license completely."
            )
            return None

        if (
            license_info_document.terms.expires_datetime
            != odl_license.metadata.terms.expires_datetime
        ):
            cls.logger().error(
                f"License identifier {odl_license.metadata.identifier}. Mismatch between license "
                f"expiry in the feed ({odl_license.metadata.terms.expires_datetime}) and the expiry in the license "
                f"info document ({license_info_document.terms.expires_datetime}) setting license status "
                f"to unavailable."
            )
            status = LicenseStatus.unavailable

        if (
            license_info_document.terms.concurrency
            != odl_license.metadata.terms.concurrency
        ):
            cls.logger().error(
                f"License identifier {odl_license.metadata.identifier}. Mismatch between license "
                f"concurrency in the feed ({odl_license.metadata.terms.concurrency}) and the "
                f"concurrency in the license info document ("
                f"{license_info_document.terms.concurrency}) setting license status "
                f"to unavailable."
            )
            status = LicenseStatus.unavailable

        return LicenseData(
            identifier=license_info_document.identifier,
            checkout_url=checkout_link,
            status_url=license_info_document_link,
            expires=license_info_document.terms.expires_datetime,
            checkouts_left=license_info_document.checkouts.left,
            checkouts_available=license_info_document.checkouts.available,
            status=license_info_document.status if status is None else status,
            terms_concurrency=license_info_document.terms.concurrency,
            content_types=list(license_info_document.formats),
        )

    @classmethod
    def extract_publication_data(
        cls,
        publication: opds2.Publication | odl.Publication,
        license_info_documents: Sequence[LicenseInfo | None],
        data_source_name: str,
        feed_self_url: str,
        auth_type: OPDS2AuthType,
        skipped_license_formats: set[str],
    ) -> BibliographicData:
        """Extract a BibliographicData object from OPDS2 or OPDS2+ODL Publication.

        :param publication: Publication object
        :param data_source_name: Data source's name
        :param feed_self_url: Feed's self URL

        :return: Publication's BibliographicData
        """
        # Since OPDS2+ODL is basically an OPDS2 feed with some additional
        # information, we start by extracting the basic bibliographic data
        # using the base OPDS2 extractor.
        bibliographic = Opds2Extractor.extract_publication_data(
            publication, data_source_name, feed_self_url
        )

        # We know that bibliographic.circulation is set by Opds2Extractor.extract_publication_data
        # and should not be None, but mypy can't know that, so we assert it here
        # TODO: See if we can tighten up the type hint for BibliographicData
        assert bibliographic.circulation is not None
        circulation = bibliographic.circulation

        if not isinstance(publication, odl.Publication):
            # This is a generic OPDS2 publication, not an ODL publication.
            cls._process_unlimited_access_title(circulation, auth_type)
            return bibliographic

        # If we have an ODL publication, we need to extract circulation data from
        # the ODL licenses. At this point license_info should not be None. If it is
        # raise an error.
        if len(license_info_documents) != len(publication.licenses):
            raise PalaceValueError(
                "Number of license info documents does not match number of licenses in publication."
            )

        medium, formats, licenses = cls._extract_publication_odl_data(
            publication,
            license_info_documents,
            skipped_license_formats,
        )

        circulation.licenses = licenses
        circulation.licenses_owned = None
        circulation.licenses_available = None
        circulation.licenses_reserved = None
        circulation.patrons_in_hold_queue = None
        circulation.formats.extend(formats)

        bibliographic.medium = medium

        return bibliographic


class OPDS2WithODLImportMonitor(OPDSImportMonitor):
    """Import information from an ODL feed."""

    PROTOCOL = OPDS2WithODLApi.label()
    SERVICE_NAME = "ODL 2.x Import Monitor"
    MEDIA_TYPE = opds2.PublicationFeed.content_type(), "application/json"

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        import_class: type[OPDS2WithODLImporter],
        **import_class_kwargs: Any,
    ) -> None:
        # Always force reimport ODL collections to get up to date license information
        super().__init__(
            _db, collection, import_class, force_reimport=True, **import_class_kwargs
        )
        self.settings = cast(OPDS2WithODLSettings, self.importer.settings)
        self._request = get_opds_requests(
            self.settings.auth_type,
            self.settings.username,
            self.settings.password,
            self.settings.external_account_id,
        )

    def _get(self, url: str, headers: Mapping[str, str] | None = None) -> Response:
        headers = self._update_headers(headers)
        if not url.startswith("http"):
            url = urljoin(self._feed_base_url, url)
        return self._request(
            "GET",
            url,
            headers=headers,
            timeout=120,
            max_retry_count=self._max_retry_count,
            allowed_response_codes=["2xx", "3xx"],
        )

    def _verify_media_type(self, url: str, resp: Response) -> None:
        # Make sure we got an OPDS feed, and not an error page that was
        # sent with a 200 status code.
        media_type = resp.headers.get("content-type")
        if not media_type or not any(x in media_type for x in self.MEDIA_TYPE):
            message = "Expected {} OPDS 2.0 feed, got {}".format(
                self.MEDIA_TYPE, media_type
            )

            raise BadResponseException(url, message=message, response=resp)

    def _get_accept_header(self) -> str:
        return "{}, {};q=0.9, */*;q=0.1".format(
            opds2.PublicationFeed.content_type(), "application/json"
        )
