from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Optional

from flask_babel import lazy_gettext as _
from webpub_manifest_parser.odl import ODLFeedParserFactory
from webpub_manifest_parser.opds2.registry import OPDS2LinkRelationsRegistry

from api.circulation_exceptions import PatronHoldLimitReached, PatronLoanLimitReached
from api.odl import ODLAPI, ODLImporter, ODLSettings
from core.integration.settings import (
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)
from core.metadata_layer import FormatData
from core.model import Edition, RightsStatus
from core.model.configuration import ExternalIntegration, HasExternalIntegration
from core.opds2_import import OPDS2Importer, OPDS2ImportMonitor, RWPMManifestParser
from core.util import first_or_default
from core.util.datetime_helpers import to_utc

if TYPE_CHECKING:
    from core.model.patron import Patron


class ODL2Settings(ODLSettings):
    skipped_license_formats: Optional[List[str]] = FormField(
        default=["text/html"],
        alias="odl2_skipped_license_formats",
        form=ConfigurationFormItem(
            label=_("Skipped license formats"),
            description=_(
                "List of license formats that will NOT be imported into Circulation Manager."
            ),
            type=ConfigurationFormItemType.LIST,
            required=False,
        ),
    )

    loan_limit: Optional[int] = FormField(
        default=None,
        alias="odl2_loan_limit",
        form=ConfigurationFormItem(
            label=_("Loan limit per patron"),
            description=_(
                "The maximum number of books a patron can have loaned out at any given time."
            ),
            type=ConfigurationFormItemType.NUMBER,
            required=False,
        ),
    )

    hold_limit: Optional[int] = FormField(
        default=None,
        alias="odl2_hold_limit",
        form=ConfigurationFormItem(
            label=_("Hold limit per patron"),
            description=_(
                "The maximum number of books a patron can have on hold at any given time."
            ),
            type=ConfigurationFormItemType.NUMBER,
            required=False,
        ),
    )


class ODL2API(ODLAPI):
    NAME = ExternalIntegration.ODL2

    @classmethod
    def settings_class(cls):
        return ODL2Settings

    def __init__(self, _db, collection):
        super().__init__(_db, collection)
        config = self.configuration()
        self.loan_limit = config.loan_limit
        self.hold_limit = config.hold_limit

    def _checkout(self, patron_or_client: Patron, licensepool, hold=None):
        # If the loan limit is not None or 0
        if self.loan_limit:
            loans = list(
                filter(
                    lambda x: x.license_pool.collection.id == self.collection_id,
                    patron_or_client.loans,
                )
            )
            if len(loans) >= self.loan_limit:
                raise PatronLoanLimitReached(limit=self.loan_limit)
        return super()._checkout(patron_or_client, licensepool, hold)

    def _place_hold(self, patron_or_client: Patron, licensepool):
        # If the hold limit is not None or 0
        if self.hold_limit:
            holds = list(
                filter(
                    lambda x: x.license_pool.collection.id == self.collection_id,
                    patron_or_client.holds,
                )
            )
            if len(holds) >= self.hold_limit:
                raise PatronHoldLimitReached(limit=self.hold_limit)
        return super()._place_hold(patron_or_client, licensepool)


class ODL2Importer(OPDS2Importer, HasExternalIntegration):
    """Import information and formats from an ODL feed.

    The only change from OPDS2Importer is that this importer extracts
    FormatData and LicenseData from ODL 2.x's "licenses" arrays.
    """

    NAME = ODL2API.NAME

    @classmethod
    def settings_class(cls):
        return ODL2Settings

    def __init__(
        self,
        db,
        collection,
        parser=None,
        data_source_name=None,
        identifier_mapping=None,
        http_get=None,
        content_modifier=None,
        map_from_collection=None,
        mirrors=None,
    ):
        """Initialize a new instance of ODL2Importer class.

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

        :param identifier_mapping: Dictionary used for mapping external identifiers into a set of internal ones
        :type identifier_mapping: Dict

        :param content_modifier: A function that may modify-in-place representations (such as images and EPUB documents)
            as they come in from the network.
        :type content_modifier: Callable

        :param map_from_collection: Identifier mapping
        :type map_from_collection: Dict

        :param mirrors: A dictionary of different MirrorUploader objects for different purposes
        :type mirrors: Dict[MirrorUploader]
        """
        super().__init__(
            db,
            collection,
            parser if parser else RWPMManifestParser(ODLFeedParserFactory()),
            data_source_name,
            identifier_mapping,
            http_get,
            content_modifier,
            map_from_collection,
            mirrors,
        )
        self._logger = logging.getLogger(__name__)

    def _extract_publication_metadata(self, feed, publication, data_source_name):
        """Extract a Metadata object from webpub-manifest-parser's publication.

        :param publication: Feed object
        :type publication: opds2_ast.OPDS2Feed

        :param publication: Publication object
        :type publication: opds2_ast.OPDS2Publication

        :param data_source_name: Data source's name
        :type data_source_name: str

        :return: Publication's metadata
        :rtype: Metadata
        """
        metadata = super()._extract_publication_metadata(
            feed, publication, data_source_name
        )
        formats = []
        licenses = []
        medium = None

        skipped_license_formats = self.configuration().skipped_license_formats
        if skipped_license_formats:
            skipped_license_formats = set(skipped_license_formats)

        if publication.licenses:
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
                else:
                    parsed_license = ODLImporter.get_license_data(
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

                    if license_format in ODLImporter.LICENSE_FORMATS:
                        # Special case to handle DeMarque audiobooks which include the protection
                        # in the content type. When we see a license format of
                        # application/audiobook+json; protection=http://www.feedbooks.com/audiobooks/access-restriction
                        # it means that this audiobook title is available through the DeMarque streaming manifest
                        # endpoint.
                        drm_schemes = [
                            ODLImporter.LICENSE_FORMATS[license_format][
                                ODLImporter.DRM_SCHEME
                            ]
                        ]
                        license_format = ODLImporter.LICENSE_FORMATS[license_format][
                            ODLImporter.CONTENT_TYPE
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

    def external_integration(self, db):
        return self.collection.external_integration


class ODL2ImportMonitor(OPDS2ImportMonitor):
    """Import information from an ODL feed."""

    PROTOCOL = ODL2Importer.NAME
    SERVICE_NAME = "ODL 2.x Import Monitor"

    def __init__(self, _db, collection, import_class, **import_class_kwargs):
        # Always force reimport ODL collections to get up to date license information
        super().__init__(
            _db, collection, import_class, force_reimport=True, **import_class_kwargs
        )
