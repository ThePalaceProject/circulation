import logging

from api.odl import ODLAPI
from contextlib2 import contextmanager
from flask_babel import lazy_gettext as _
from webpub_manifest_parser.odl import ODLFeedParserFactory
from webpub_manifest_parser.opds2.registry import OPDS2LinkRelationsRegistry

from core import util
from core.metadata_layer import FormatData, LicenseData
from core.model import DeliveryMechanism, Edition, MediaTypes, RightsStatus
from api.odl import ODLAPI, ODLImporter
from core.model.configuration import (
    ConfigurationAttributeType,
    ConfigurationFactory,
    ConfigurationMetadata,
    ConfigurationStorage,
    ExternalIntegration,
    HasExternalIntegration,
)
from core.opds2_import import (
    OPDS2Importer,
    OPDS2ImportMonitor,
    OPDS2ImporterConfiguration,
    RWPMManifestParser,
)
from core.util import first_or_default
from core.util.datetime_helpers import to_utc


class ODL2APIConfiguration(OPDS2ImporterConfiguration):
    skipped_license_formats = ConfigurationMetadata(
        key="odl2_skipped_license_formats",
        label=_("Skipped license formats"),
        description=_(
            "List of license formats that will NOT be imported into Circulation Manager."
        ),
        type=ConfigurationAttributeType.LIST,
        required=False,
        default=["text/html"],
    )


class ODL2API(ODLAPI):
    NAME = ExternalIntegration.ODL2
    SETTINGS = ODLAPI.SETTINGS + ODL2APIConfiguration.to_settings()


class ODL2Importer(OPDS2Importer, HasExternalIntegration):
    """Import information and formats from an ODL feed.

    The only change from OPDS2Importer is that this importer extracts
    FormatData and LicenseData from ODL 2.x's "licenses" arrays.
    """

    NAME = ODL2API.NAME

    FEEDBOOKS_AUDIO = "{0}; protection={1}".format(
        MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
        DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM,
    )

    CONTENT_TYPE = "content-type"
    DRM_SCHEME = "drm-scheme"

    LICENSE_FORMATS = {
        FEEDBOOKS_AUDIO: {
            CONTENT_TYPE: MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
            DRM_SCHEME: DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM,
        }
    }

    def __init__(
        self,
        db,
        collection,
        parser=None,
        data_source_name=None,
        identifier_mapping=None,
        http_get=None,
        metadata_client=None,
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

        :param metadata_client: A SimplifiedOPDSLookup object that is used to fill in missing metadata
        :type metadata_client: SimplifiedOPDSLookup

        :param content_modifier: A function that may modify-in-place representations (such as images and EPUB documents)
            as they come in from the network.
        :type content_modifier: Callable

        :param map_from_collection: Identifier mapping
        :type map_from_collection: Dict

        :param mirrors: A dictionary of different MirrorUploader objects for different purposes
        :type mirrors: Dict[MirrorUploader]
        """
        super(ODL2Importer, self).__init__(
            db,
            collection,
            parser if parser else RWPMManifestParser(ODLFeedParserFactory()),
            data_source_name,
            identifier_mapping,
            http_get,
            metadata_client,
            content_modifier,
            map_from_collection,
            mirrors,
        )

        self._logger = logging.getLogger(__name__)

        self._configuration_storage = ConfigurationStorage(self)
        self._configuration_factory = ConfigurationFactory()

    @contextmanager
    def _get_configuration(self, db):
        """Return the configuration object.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: Configuration object
        :rtype: ODL2APIConfiguration
        """
        with self._configuration_factory.create(
            self._configuration_storage, db, ODL2APIConfiguration
        ) as configuration:
            yield configuration

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
        metadata = super(ODL2Importer, self)._extract_publication_metadata(
            feed, publication, data_source_name
        )
        formats = []
        licenses = []
        medium = None

        with self._get_configuration(self._db) as configuration:
            skipped_license_formats = configuration.skipped_license_formats

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

                # DPLA feed doesn't have information about a DRM protection used for audiobooks.
                # We want to try to extract that information from the License Info Document it's present there.
                license_formats = set(odl_license.metadata.formats)
                if parsed_license and parsed_license.content_types:
                    license_formats |= set(parsed_license.content_types)

                for license_format in license_formats:
                    if (
                        skipped_license_formats
                        and license_format in skipped_license_formats
                    ):
                        continue

                    if not medium:
                        medium = Edition.medium_from_media_type(license_format)

                    if license_format in ODLImporter.LICENSE_FORMATS:
                        # Special case to handle DeMarque audiobooks which
                        # include the protection in the content type
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
