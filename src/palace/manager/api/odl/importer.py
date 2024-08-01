from __future__ import annotations

import datetime
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urljoin

import dateutil
from requests import Response
from sqlalchemy.orm import Session
from webpub_manifest_parser.odl import ODLFeedParserFactory
from webpub_manifest_parser.opds2.registry import OPDS2LinkRelationsRegistry

from palace.manager.api.odl.api import OPDS2WithODLApi
from palace.manager.api.odl.auth import ODLAuthenticatedGet
from palace.manager.api.odl.constants import FEEDBOOKS_AUDIO
from palace.manager.api.odl.settings import OPDS2AuthType, OPDS2WithODLSettings
from palace.manager.core.metadata_layer import FormatData, LicenseData, Metadata
from palace.manager.core.opds2_import import (
    OPDS2Importer,
    OPDS2ImportMonitor,
    RWPMManifestParser,
)
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicenseStatus,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.resource import Hyperlink
from palace.manager.util import first_or_default
from palace.manager.util.datetime_helpers import to_utc
from palace.manager.util.http import HTTP

if TYPE_CHECKING:
    from webpub_manifest_parser.opds2.ast import OPDS2Feed, OPDS2Publication


class OPDS2WithODLImporter(OPDS2Importer):
    """Import information and formats from an ODL feed.

    The only change from OPDS2Importer is that this importer extracts
    FormatData and LicenseData from ODL "licenses" collection.
    """

    DRM_SCHEME = "drm-scheme"
    CONTENT_TYPE = "content-type"
    LICENSE_FORMATS = {
        FEEDBOOKS_AUDIO: {
            CONTENT_TYPE: MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
            DRM_SCHEME: DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM,
        }
    }
    NAME = OPDS2WithODLApi.label()

    @classmethod
    def settings_class(cls) -> type[OPDS2WithODLSettings]:
        return OPDS2WithODLSettings

    def __init__(
        self,
        db: Session,
        collection: Collection,
        parser: RWPMManifestParser | None = None,
        data_source_name: str | None = None,
        http_get: Callable[..., Response] | None = None,
    ):
        """Initialize a new instance of OPDS2WithODLImporter class.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param collection: Circulation Manager's collection.
            LicensePools created by this OPDS2Import class will be associated with the given Collection.
            If this is None, no LicensePools will be created -- only Editions.
        :type collection: Collection

        :param parser: Feed parser
        :type parser: RWPMManifestParser

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
            parser if parser else RWPMManifestParser(ODLFeedParserFactory()),
            data_source_name,
        )

        self.http_get = http_get or HTTP.get_with_timeout

    def _process_unlimited_access_title(self, metadata: Metadata) -> Metadata:
        if self.settings.auth_type != OPDS2AuthType.OAUTH:  # type: ignore[attr-defined]
            return metadata

        # Links to items with a non-open access acquisition type cannot be directly accessed
        # if the feed is protected by OAuth. So we need to add a BEARER_TOKEN delivery mechanism
        # to the formats, so we know we are able to fulfill these items indirectly via a bearer token.
        circulation = metadata.circulation
        supported_media_types = {
            format
            for format, drm in DeliveryMechanism.default_client_can_fulfill_lookup
            if drm == DeliveryMechanism.BEARER_TOKEN and format is not None
        }

        def create_format_data(format: FormatData) -> FormatData:
            return FormatData(
                content_type=format.content_type,
                drm_scheme=DeliveryMechanism.BEARER_TOKEN,
                link=format.link,
                rights_uri=RightsStatus.IN_COPYRIGHT,
            )

        new_formats = [
            create_format_data(format)
            if format.content_type in supported_media_types
            and format.drm_scheme is None
            and format.link.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
            else format
            for format in circulation.formats
        ]

        circulation.formats = new_formats
        return metadata

    def _extract_publication_metadata(
        self,
        feed: OPDS2Feed,
        publication: OPDS2Publication,
        data_source_name: str | None,
    ) -> Metadata:
        """Extract a Metadata object from webpub-manifest-parser's publication.

        :param publication: Feed object
        :param publication: Publication object
        :param data_source_name: Data source's name

        :return: Publication's metadata
        """
        metadata = super()._extract_publication_metadata(
            feed, publication, data_source_name
        )

        if not publication.licenses:
            # This is an unlimited-access title with no license information. Nothing to do.
            return self._process_unlimited_access_title(metadata)

        formats = []
        licenses = []
        medium = None

        skipped_license_formats = set(self.settings.skipped_license_formats)  # type: ignore[attr-defined]
        publication_availability = self._extract_availability(
            publication.metadata.availability
        )

        for odl_license in publication.licenses:
            identifier = odl_license.metadata.identifier

            checkout_link = first_or_default(
                odl_license.links.get_by_rel(OPDS2LinkRelationsRegistry.BORROW.key)
            )
            if checkout_link:
                checkout_link = checkout_link.href

            license_info_document_link = first_or_default(
                odl_license.links.get_by_rel(OPDS2LinkRelationsRegistry.SELF.key)
            )
            if license_info_document_link:
                license_info_document_link = license_info_document_link.href

            expires = (
                to_utc(odl_license.metadata.terms.expires)
                if odl_license.metadata.terms
                else None
            )
            concurrency = (
                int(odl_license.metadata.terms.concurrency)
                if odl_license.metadata.terms
                else None
            )

            if not license_info_document_link:
                parsed_license = None
            elif (
                not self._extract_availability(odl_license.metadata.availability)
                or not publication_availability
            ):
                # No need to fetch the license document, we already know that this title is not available.
                parsed_license = LicenseData(
                    identifier=identifier,
                    checkout_url=None,
                    status_url=license_info_document_link,
                    status=LicenseStatus.unavailable,
                    checkouts_available=0,
                )
            else:
                parsed_license = self.get_license_data(
                    license_info_document_link,
                    checkout_link,
                    identifier,
                    expires,
                    concurrency,
                    self.http_get,
                )

            if parsed_license is not None:
                licenses.append(parsed_license)

            license_formats = set(odl_license.metadata.formats)
            for license_format in license_formats:
                if (
                    skipped_license_formats
                    and license_format in skipped_license_formats
                ):
                    continue

                if not medium:
                    medium = Edition.medium_from_media_type(license_format)

                drm_schemes: list[str | None]
                if license_format in self.LICENSE_FORMATS:
                    # Special case to handle DeMarque audiobooks which include the protection
                    # in the content type. When we see a license format of
                    # application/audiobook+json; protection=http://www.feedbooks.com/audiobooks/access-restriction
                    # it means that this audiobook title is available through the DeMarque streaming manifest
                    # endpoint.
                    drm_schemes = [
                        self.LICENSE_FORMATS[license_format][self.DRM_SCHEME]
                    ]
                    license_format = self.LICENSE_FORMATS[license_format][
                        self.CONTENT_TYPE
                    ]
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

        metadata.circulation.licenses = licenses
        metadata.circulation.licenses_owned = None
        metadata.circulation.licenses_available = None
        metadata.circulation.licenses_reserved = None
        metadata.circulation.patrons_in_hold_queue = None
        metadata.circulation.formats.extend(formats)
        metadata.medium = medium

        return metadata

    @classmethod
    def fetch_license_info(
        cls, document_link: str, do_get: Callable[..., Response]
    ) -> dict[str, Any] | None:
        resp = do_get(document_link, headers={})
        if resp.status_code in (200, 201):
            license_info_document = resp.json()
            return license_info_document  # type: ignore[no-any-return]
        else:
            cls.logger().warning(
                f"License Info Document is not available. "
                f"Status link {document_link} failed with {resp.status_code} code."
            )
            return None

    @classmethod
    def parse_license_info(
        cls,
        license_info_document: dict[str, Any],
        license_info_link: str,
        checkout_link: str | None,
    ) -> LicenseData | None:
        """Check the license's attributes passed as parameters:
        - if they're correct, turn them into a LicenseData object
        - otherwise, return a None

        :param license_info_document: License Info Document
        :param license_info_link: Link to fetch License Info Document
        :param checkout_link: License's checkout link

        :return: LicenseData if all the license's attributes are correct, None, otherwise
        """

        identifier = license_info_document.get("identifier")
        document_status = license_info_document.get("status")
        document_checkouts = license_info_document.get("checkouts", {})
        document_left = document_checkouts.get("left")
        document_available = document_checkouts.get("available")
        document_terms = license_info_document.get("terms", {})
        document_expires = document_terms.get("expires")
        document_concurrency = document_terms.get("concurrency")
        document_format = license_info_document.get("format")

        if identifier is None:
            cls.logger().error("License info document has no identifier.")
            return None

        expires = None
        if document_expires is not None:
            expires = dateutil.parser.parse(document_expires)
            expires = to_utc(expires)

        if document_status is not None:
            status = LicenseStatus.get(document_status)
            if status.value != document_status:
                cls.logger().warning(
                    f"Identifier # {identifier} unknown status value "
                    f"{document_status} defaulting to {status.value}."
                )
        else:
            status = LicenseStatus.unavailable
            cls.logger().warning(
                f"Identifier # {identifier} license info document does not have "
                f"required key 'status'."
            )

        if document_available is not None:
            available = int(document_available)
        else:
            available = 0
            cls.logger().warning(
                f"Identifier # {identifier} license info document does not have "
                f"required key 'checkouts.available'."
            )

        left = None
        if document_left is not None:
            left = int(document_left)

        concurrency = None
        if document_concurrency is not None:
            concurrency = int(document_concurrency)

        content_types = None
        if document_format is not None:
            if isinstance(document_format, str):
                content_types = [document_format]
            elif isinstance(document_format, list):
                content_types = document_format

        return LicenseData(
            identifier=identifier,
            checkout_url=checkout_link,
            status_url=license_info_link,
            expires=expires,
            checkouts_left=left,
            checkouts_available=available,
            status=status,
            terms_concurrency=concurrency,
            content_types=content_types,
        )

    @classmethod
    def get_license_data(
        cls,
        license_info_link: str,
        checkout_link: str | None,
        feed_license_identifier: str | None,
        feed_license_expires: datetime.datetime | None,
        feed_concurrency: int | None,
        do_get: Callable[..., Response],
    ) -> LicenseData | None:
        license_info_document = cls.fetch_license_info(license_info_link, do_get)

        if not license_info_document:
            return None

        parsed_license = cls.parse_license_info(
            license_info_document, license_info_link, checkout_link
        )

        if not parsed_license:
            return None

        if parsed_license.identifier != feed_license_identifier:
            # There is a mismatch between the license info document and
            # the feed we are importing. Since we don't know which to believe
            # we log an error and continue.
            cls.logger().error(
                f"Mismatch between license identifier in the feed ({feed_license_identifier}) "
                f"and the identifier in the license info document "
                f"({parsed_license.identifier}) ignoring license completely."
            )
            return None

        if parsed_license.expires != feed_license_expires:
            cls.logger().error(
                f"License identifier {feed_license_identifier}. Mismatch between license "
                f"expiry in the feed ({feed_license_expires}) and the expiry in the license "
                f"info document ({parsed_license.expires}) setting license status "
                f"to unavailable."
            )
            parsed_license.status = LicenseStatus.unavailable

        if parsed_license.terms_concurrency != feed_concurrency:
            cls.logger().error(
                f"License identifier {feed_license_identifier}. Mismatch between license "
                f"concurrency in the feed ({feed_concurrency}) and the "
                f"concurrency in the license info document ("
                f"{parsed_license.terms_concurrency}) setting license status "
                f"to unavailable."
            )
            parsed_license.status = LicenseStatus.unavailable

        return parsed_license


class OPDS2WithODLImportMonitor(ODLAuthenticatedGet, OPDS2ImportMonitor):
    """Import information from an ODL feed."""

    PROTOCOL = OPDS2WithODLApi.label()
    SERVICE_NAME = "ODL 2.x Import Monitor"

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

    @property
    def _username(self) -> str:
        return self.settings.username

    @property
    def _password(self) -> str:
        return self.settings.password

    @property
    def _auth_type(self) -> OPDS2AuthType:
        return self.settings.auth_type

    @property
    def _feed_url(self) -> str:
        return self.settings.external_account_id

    def _get(
        self, url: str, headers: Mapping[str, str] | None = None, **kwargs: Any
    ) -> Response:
        headers = self._update_headers(headers)
        kwargs["timeout"] = 120
        kwargs["max_retry_count"] = self._max_retry_count
        kwargs["allowed_response_codes"] = ["2xx", "3xx"]
        if not url.startswith("http"):
            url = urljoin(self._feed_base_url, url)
        return super()._get(url, headers, **kwargs)
