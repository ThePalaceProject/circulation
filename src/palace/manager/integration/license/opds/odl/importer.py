from __future__ import annotations

import datetime
from collections.abc import Callable, Mapping, Sequence
from functools import cached_property
from typing import Any, cast
from urllib.parse import urljoin

from pydantic import TypeAdapter, ValidationError
from requests import Response
from sqlalchemy.orm import Session

from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.license import LicenseData
from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
from palace.manager.integration.license.opds.odl.constants import FEEDBOOKS_AUDIO
from palace.manager.integration.license.opds.odl.settings import OPDS2WithODLSettings
from palace.manager.integration.license.opds.opds2 import (
    OPDS2Importer,
    OPDS2ImportMonitor,
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
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism, RightsStatus
from palace.manager.sqlalchemy.model.resource import Hyperlink
from palace.manager.util.http import HTTP, GetRequestCallable


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

    def _process_unlimited_access_title(
        self, metadata: BibliographicData
    ) -> BibliographicData:
        if self.settings.auth_type != OPDS2AuthType.OAUTH:  # type: ignore[attr-defined]
            return metadata

        # Links to items with a non-open access acquisition type cannot be directly accessed
        # if the feed is protected by OAuth. So we need to add a BEARER_TOKEN delivery mechanism
        # to the formats, so we know we are able to fulfill these items indirectly via a bearer token.
        circulation = metadata.circulation
        # There should be no way that circulation data can be None here, but mypy can't
        # know that, so we assert it here to fail quickly if it is None.
        assert circulation is not None
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
            (
                create_format_data(format)
                if format.content_type in supported_media_types
                and format.drm_scheme is None
                and (
                    format.link
                    and format.link.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
                )
                else format
            )
            for format in circulation.formats
        ]

        circulation.formats = new_formats
        return metadata

    def _extract_publication_bibliographic_data(
        self,
        publication: opds2.BasePublication,
        data_source_name: str,
        feed_self_url: str,
    ) -> BibliographicData:
        """Extract a BibliographicData object from webpub-manifest-parser's publication.

        :param publication: Feed object
        :param publication: Publication object
        :param data_source_name: Data source's name

        :return: Publication's bibliographic data
        """
        metadata = super()._extract_publication_bibliographic_data(
            publication, data_source_name, feed_self_url
        )

        if not isinstance(publication, odl.Publication):
            # This is a generic OPDS2 publication, not an ODL publication.
            return self._process_unlimited_access_title(metadata)

        formats = []
        licenses = []
        medium = None

        skipped_license_formats = set(self.settings.skipped_license_formats)  # type: ignore[attr-defined]
        publication_availability = publication.metadata.availability.available

        for odl_license in publication.licenses:
            identifier = odl_license.metadata.identifier

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

            expires = odl_license.metadata.terms.expires_datetime
            concurrency = odl_license.metadata.terms.concurrency

            parsed_license = (
                LicenseData(
                    identifier=identifier,
                    checkout_url=None,
                    status_url=license_info_document_link,
                    status=LicenseStatus.unavailable,
                    checkouts_available=0,
                )
                if (
                    not odl_license.metadata.availability.available
                    or not publication_availability
                )
                else self.get_license_data(
                    license_info_document_link,
                    checkout_link,
                    identifier,
                    expires,
                    concurrency,
                    self.http_get,
                )
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

                drm_schemes: Sequence[str | None]
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

        # metadata.circulation was set in an earlier step and shouldn't be None.
        # We assert this explicitly to satisfy mypy and fail fast if something changes.
        assert metadata.circulation is not None
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
    ) -> bytes | None:
        resp = do_get(document_link, headers={})
        if resp.status_code in (200, 201):
            return resp.content
        else:
            cls.logger().warning(
                f"License Info Document is not available. "
                f"Status link {document_link} failed with {resp.status_code} code."
            )
            return None

    @classmethod
    def parse_license_info(
        cls,
        license_info_document: bytes | str | None,
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

        if license_info_document is None:
            return None

        try:
            document = LicenseInfo.model_validate_json(license_info_document)
        except ValidationError as e:
            cls.logger().error(
                f"License Info Document at {license_info_link} is not valid. {e}"
            )
            return None

        return LicenseData(
            identifier=document.identifier,
            checkout_url=checkout_link,
            status_url=license_info_link,
            expires=document.terms.expires_datetime,
            checkouts_left=document.checkouts.left,
            checkouts_available=document.checkouts.available,
            status=document.status,
            terms_concurrency=document.terms.concurrency,
            content_types=list(document.formats),
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
            parsed_license = parsed_license.model_copy(
                update={
                    "status": LicenseStatus.unavailable,
                }
            )

        if parsed_license.terms_concurrency != feed_concurrency:
            cls.logger().error(
                f"License identifier {feed_license_identifier}. Mismatch between license "
                f"concurrency in the feed ({feed_concurrency}) and the "
                f"concurrency in the license info document ("
                f"{parsed_license.terms_concurrency}) setting license status "
                f"to unavailable."
            )
            parsed_license = parsed_license.model_copy(
                update={
                    "status": LicenseStatus.unavailable,
                }
            )

        return parsed_license

    @cached_property
    def _publication_type_adapter(self) -> TypeAdapter[Opds2OrOpds2WithOdlPublication]:
        return TypeAdapter(Opds2OrOpds2WithOdlPublication)

    def _get_publication(
        self,
        publication: dict[str, Any],
    ) -> opds2.Publication | odl.Publication:
        return self._publication_type_adapter.validate_python(publication)


class OPDS2WithODLImportMonitor(OPDS2ImportMonitor):
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
