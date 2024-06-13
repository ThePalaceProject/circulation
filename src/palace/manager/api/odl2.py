from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from flask_babel import lazy_gettext as _
from pydantic import NonNegativeInt, PositiveInt
from sqlalchemy.orm import Session
from webpub_manifest_parser.odl import ODLFeedParserFactory
from webpub_manifest_parser.opds2.registry import OPDS2LinkRelationsRegistry

from palace.manager.api.circulation_exceptions import (
    HoldsNotPermitted,
    PatronHoldLimitReached,
    PatronLoanLimitReached,
)
from palace.manager.api.odl import (
    BaseODLAPI,
    BaseODLImporter,
    ODLLibrarySettings,
    ODLSettings,
)
from palace.manager.core.metadata_layer import FormatData, LicenseData, TimestampData
from palace.manager.core.monitor import CollectionMonitor
from palace.manager.core.opds2_import import (
    OPDS2Importer,
    OPDS2ImporterSettings,
    OPDS2ImportMonitor,
    RWPMManifestParser,
)
from palace.manager.integration.settings import (
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicenseStatus,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.patron import Hold
from palace.manager.sqlalchemy.model.resource import HttpResponseTuple
from palace.manager.util import first_or_default
from palace.manager.util.datetime_helpers import to_utc, utc_now

if TYPE_CHECKING:
    from webpub_manifest_parser.core.ast import Metadata
    from webpub_manifest_parser.opds2.ast import OPDS2Feed, OPDS2Publication

    from palace.manager.api.circulation import HoldInfo
    from palace.manager.sqlalchemy.model.patron import Loan, Patron


class ODL2Settings(ODLSettings, OPDS2ImporterSettings):
    skipped_license_formats: list[str] = FormField(
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

    loan_limit: PositiveInt | None = FormField(
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

    hold_limit: NonNegativeInt | None = FormField(
        default=None,
        alias="odl2_hold_limit",
        form=ConfigurationFormItem(
            label=_("Hold limit per patron"),
            description=_(
                "The maximum number of books from this collection that a patron can "
                "have on hold at any given time. "
                "<br>A value of 0 means that holds are NOT permitted."
                "<br>No value means that no limit is imposed by this setting."
            ),
            type=ConfigurationFormItemType.NUMBER,
            required=False,
        ),
    )


class ODL2API(BaseODLAPI[ODL2Settings, ODLLibrarySettings]):
    @classmethod
    def settings_class(cls) -> type[ODL2Settings]:
        return ODL2Settings

    @classmethod
    def library_settings_class(cls) -> type[ODLLibrarySettings]:
        return ODLLibrarySettings

    @classmethod
    def label(cls) -> str:
        return "ODL 2.0"

    @classmethod
    def description(cls) -> str:
        return "Import books from a distributor that uses OPDS2 + ODL (Open Distribution to Libraries)."

    def __init__(self, _db: Session, collection: Collection) -> None:
        super().__init__(_db, collection)
        self.loan_limit = self.settings.loan_limit
        self.hold_limit = self.settings.hold_limit

    def _checkout(
        self, patron: Patron, licensepool: LicensePool, hold: Hold | None = None
    ) -> Loan:
        # If the loan limit is not None or 0
        if self.loan_limit:
            loans = list(
                filter(
                    lambda x: x.license_pool.collection.id == self.collection_id,
                    patron.loans,
                )
            )
            if len(loans) >= self.loan_limit:
                raise PatronLoanLimitReached(limit=self.loan_limit)
        return super()._checkout(patron, licensepool, hold)

    def _place_hold(self, patron: Patron, licensepool: LicensePool) -> HoldInfo:
        if self.hold_limit is not None:
            holds = list(
                filter(
                    lambda x: x.license_pool.collection.id == self.collection_id,
                    patron.holds,
                )
            )
            if self.hold_limit == 0:
                raise HoldsNotPermitted()
            if len(holds) >= self.hold_limit:
                raise PatronHoldLimitReached(limit=self.hold_limit)
        return super()._place_hold(patron, licensepool)


class ODL2Importer(BaseODLImporter[ODL2Settings], OPDS2Importer):
    """Import information and formats from an ODL feed.

    The only change from OPDS2Importer is that this importer extracts
    FormatData and LicenseData from ODL 2.x's "licenses" arrays.
    """

    NAME = ODL2API.label()

    @classmethod
    def settings_class(cls) -> type[ODL2Settings]:
        return ODL2Settings

    def __init__(
        self,
        db: Session,
        collection: Collection,
        parser: RWPMManifestParser | None = None,
        data_source_name: str | None = None,
        http_get: Callable[..., HttpResponseTuple] | None = None,
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
        """
        super().__init__(
            db,
            collection,
            parser if parser else RWPMManifestParser(ODLFeedParserFactory()),
            data_source_name,
            http_get,
        )
        self._logger = logging.getLogger(__name__)

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

        if (
            metadata.circulation.licenses_owned == 0
            and metadata.circulation.licenses_available == 0
        ):
            # This title is not available, so we don't need to process it.
            return metadata

        if not publication.licenses:
            # This title is an open-access title, no need to process licenses.
            return metadata

        formats = []
        licenses = []
        medium = None

        skipped_license_formats = set(self.settings.skipped_license_formats)

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
            elif not self._extract_availability(odl_license.metadata.availability):
                # No need to fetch the license document, we already know that this title is not available.
                parsed_license = LicenseData(
                    identifier=identifier,
                    checkout_url=None,
                    status_url=license_info_document_link,
                    status=LicenseStatus.get(odl_license.metadata.availability.state),
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


class ODL2ImportMonitor(OPDS2ImportMonitor):
    """Import information from an ODL feed."""

    PROTOCOL = ODL2API.label()
    SERVICE_NAME = "ODL 2.x Import Monitor"

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        import_class: type[ODL2Importer],
        **import_class_kwargs: Any,
    ) -> None:
        # Always force reimport ODL collections to get up to date license information
        super().__init__(
            _db, collection, import_class, force_reimport=True, **import_class_kwargs
        )


class ODL2HoldReaper(CollectionMonitor):
    """Check for holds that have expired and delete them, and update
    the holds queues for their pools."""

    SERVICE_NAME = "ODL2 Hold Reaper"
    PROTOCOL = ODL2API.label()

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        api: ODL2API | None = None,
        **kwargs: Any,
    ):
        super().__init__(_db, collection, **kwargs)
        self.api = api or ODL2API(_db, collection)

    def run_once(self, progress: TimestampData) -> TimestampData:
        # Find holds that have expired.
        expired_holds = (
            self._db.query(Hold)
            .join(Hold.license_pool)
            .filter(LicensePool.collection_id == self.api.collection_id)
            .filter(Hold.end < utc_now())
            .filter(Hold.position == 0)
        )

        changed_pools = set()
        total_deleted_holds = 0
        for hold in expired_holds:
            changed_pools.add(hold.license_pool)
            self._db.delete(hold)
            # log circulation event:  hold expired
            total_deleted_holds += 1

        for pool in changed_pools:
            self.api.update_licensepool(pool)

        message = "Holds deleted: %d. License pools updated: %d" % (
            total_deleted_holds,
            len(changed_pools),
        )
        progress = TimestampData(achievements=message)
        return progress
