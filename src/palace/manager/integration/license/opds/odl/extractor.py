from __future__ import annotations

from collections.abc import Sequence

from frozendict import frozendict

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.license import LicenseData
from palace.manager.integration.license.opds.odl.constants import FEEDBOOKS_AUDIO
from palace.manager.integration.license.opds.opds2.extractor import Opds2Extractor
from palace.manager.integration.license.opds.requests import OPDS2AuthType
from palace.manager.opds import opds2, rwpm
from palace.manager.opds.lcp.status import LoanStatus
from palace.manager.opds.odl import odl
from palace.manager.opds.odl.info import LicenseInfo, LicenseStatus
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    DeliveryMechanismTuple,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.resource import Hyperlink
from palace.manager.util.log import LoggerMixin


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
