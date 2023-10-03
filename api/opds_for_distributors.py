from __future__ import annotations

import datetime
import json
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
)

import feedparser
from flask_babel import lazy_gettext as _

from api.circulation import BaseCirculationAPI, FulfillmentInfo, LoanInfo
from api.circulation_exceptions import (
    CannotFulfill,
    LibraryAuthorizationFailedException,
)
from api.selftest import HasCollectionSelfTests
from core.integration.base import HasLibraryIntegrationConfiguration
from core.integration.settings import BaseSettings, ConfigurationFormItem, FormField
from core.metadata_layer import FormatData, TimestampData
from core.model import (
    Collection,
    Credential,
    DeliveryMechanism,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    LicensePool,
    Loan,
    RightsStatus,
    Session,
    get_one,
)
from core.opds_import import BaseOPDSImporterSettings, OPDSImporter, OPDSImportMonitor
from core.util.datetime_helpers import utc_now
from core.util.http import HTTP
from core.util.string_helpers import base64

if TYPE_CHECKING:
    from requests import Response

    from api.circulation import HoldInfo
    from core.coverage import CoverageFailure
    from core.metadata_layer import CirculationData
    from core.model import Edition, LicensePoolDeliveryMechanism, Patron, Work
    from core.selftest import SelfTestResult


class OPDSForDistributorsSettings(BaseOPDSImporterSettings):
    username: str = FormField(
        form=ConfigurationFormItem(
            label=_("Library's username or access key"),
            required=True,
        )
    )

    password: str = FormField(
        form=ConfigurationFormItem(
            label=_("Library's password or secret key"),
            required=True,
        )
    )


class OPDSForDistributorsLibrarySettings(BaseSettings):
    pass


class OPDSForDistributorsAPI(
    BaseCirculationAPI[OPDSForDistributorsSettings, OPDSForDistributorsLibrarySettings],
    HasCollectionSelfTests,
    HasLibraryIntegrationConfiguration,
):
    NAME = "OPDS for Distributors"
    DESCRIPTION = _(
        "Import books from a distributor that requires authentication to get the OPDS feed and download books."
    )
    BEARER_TOKEN_CREDENTIAL_TYPE = "OPDS For Distributors Bearer Token"

    # In OPDS For Distributors, all items are gated through the
    # BEARER_TOKEN access control scheme.
    #
    # If the default client supports a given media type when
    # combined with the BEARER_TOKEN scheme, then we should import
    # titles with that media type...
    SUPPORTED_MEDIA_TYPES = [
        format
        for (format, drm) in DeliveryMechanism.default_client_can_fulfill_lookup
        if drm == (DeliveryMechanism.BEARER_TOKEN) and format is not None
    ]

    # ...and we should map requests for delivery of that media type to
    # the (type, BEARER_TOKEN) DeliveryMechanism.
    delivery_mechanism_to_internal_format = {
        (type, DeliveryMechanism.BEARER_TOKEN): type for type in SUPPORTED_MEDIA_TYPES
    }

    @classmethod
    def settings_class(cls) -> Type[OPDSForDistributorsSettings]:
        return OPDSForDistributorsSettings

    @classmethod
    def library_settings_class(cls) -> Type[OPDSForDistributorsLibrarySettings]:
        return OPDSForDistributorsLibrarySettings

    @classmethod
    def description(cls) -> str:
        return cls.DESCRIPTION  # type: ignore[no-any-return]

    @classmethod
    def label(cls) -> str:
        return cls.NAME

    def __init__(self, _db: Session, collection: Collection):
        super().__init__(_db, collection)
        self.external_integration_id = collection.external_integration.id

        config = self.configuration()
        self.data_source_name = config.data_source
        self.username = config.username
        self.password = config.password
        self.feed_url = collection.external_account_id
        self.auth_url: Optional[str] = None

    def external_integration(self, _db: Session) -> Optional[ExternalIntegration]:
        return get_one(_db, ExternalIntegration, id=self.external_integration_id)

    def _run_self_tests(self, _db: Session) -> Generator[SelfTestResult, None, None]:
        """Try to get a token."""
        yield self.run_test("Negotiate a fulfillment token", self._get_token, _db)

    def _request_with_timeout(
        self, method: str, url: Optional[str], *args: Any, **kwargs: Any
    ) -> Response:
        """Wrapper around HTTP.request_with_timeout to be overridden for tests."""
        if url is None:
            name = self.collection.name if self.collection else "unknown"
            raise LibraryAuthorizationFailedException(
                f"No URL provided to request_with_timeout for collection: {name}/{self.collection_id}."
            )
        return HTTP.request_with_timeout(method, url, *args, **kwargs)

    def _get_token(self, _db: Session) -> Credential:
        # If this is the first time we're getting a token, we
        # need to find the authenticate url in the OPDS
        # authentication document.
        if not self.auth_url:
            # Keep track of the most recent URL we retrieved for error
            # reporting purposes.
            current_url = self.feed_url
            response = self._request_with_timeout("GET", current_url)

            if response.status_code != 401:
                # This feed doesn't require authentication, so
                # we need to find a link to the authentication document.
                feed = feedparser.parse(response.content)
                links = feed.get("feed", {}).get("links", [])
                auth_doc_links = [
                    l for l in links if l["rel"] == "http://opds-spec.org/auth/document"
                ]
                if not auth_doc_links:
                    raise LibraryAuthorizationFailedException(
                        "No authentication document link found in %s" % current_url
                    )
                current_url = auth_doc_links[0].get("href")

                response = self._request_with_timeout("GET", current_url)

            try:
                auth_doc = json.loads(response.content)
            except Exception as e:
                raise LibraryAuthorizationFailedException(
                    "Could not load authentication document from %s" % current_url
                )
            auth_types = auth_doc.get("authentication", [])
            credentials_types = [
                t
                for t in auth_types
                if t["type"] == "http://opds-spec.org/auth/oauth/client_credentials"
            ]
            if not credentials_types:
                raise LibraryAuthorizationFailedException(
                    "Could not find any credential-based authentication mechanisms in %s"
                    % current_url
                )

            links = credentials_types[0].get("links", [])
            auth_links = [l for l in links if l.get("rel") == "authenticate"]
            if not auth_links:
                raise LibraryAuthorizationFailedException(
                    "Could not find any authentication links in %s" % current_url
                )
            self.auth_url = auth_links[0].get("href")

        def refresh(credential: Credential) -> None:
            headers = dict()
            auth_header = "Basic %s" % base64.b64encode(
                f"{self.username}:{self.password}"
            )
            headers["Authorization"] = auth_header
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            body = dict(grant_type="client_credentials")
            token_response = self._request_with_timeout(
                "POST", self.auth_url, data=body, headers=headers
            )
            token = json.loads(token_response.content)
            access_token = token.get("access_token")
            expires_in = token.get("expires_in")
            if not access_token or not expires_in:
                raise LibraryAuthorizationFailedException(
                    "Document retrieved from %s is not a bearer token: %s"
                    % (
                        # Response comes in as a byte string.
                        self.auth_url,
                        token_response.content.decode("utf-8"),
                    )
                )
            credential.credential = access_token
            expires_in = expires_in
            # We'll avoid edge cases by assuming the token expires 75%
            # into its useful lifetime.
            credential.expires = utc_now() + datetime.timedelta(
                seconds=expires_in * 0.75
            )

        return Credential.lookup(
            _db,
            self.data_source_name,
            self.BEARER_TOKEN_CREDENTIAL_TYPE,
            collection=self.collection,
            patron=None,
            refresher_method=refresh,
        )

    def can_fulfill_without_loan(
        self,
        patron: Optional[Patron],
        pool: LicensePool,
        lpdm: LicensePoolDeliveryMechanism,
    ) -> bool:
        """Since OPDS For Distributors delivers books to the library rather
        than creating loans, any book can be fulfilled without
        identifying the patron, assuming the library's policies
        allow it.

        Just to be safe, though, we require that the
        DeliveryMechanism's drm_scheme be either 'no DRM' or 'bearer
        token', since other DRM schemes require identifying a patron.
        """
        if not lpdm or not lpdm.delivery_mechanism:
            return False
        drm_scheme = lpdm.delivery_mechanism.drm_scheme
        if drm_scheme in (DeliveryMechanism.NO_DRM, DeliveryMechanism.BEARER_TOKEN):
            return True
        return False

    def checkin(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        # Delete the patron's loan for this licensepool.
        _db = Session.object_session(patron)
        try:
            loan = get_one(
                _db,
                Loan,
                patron_id=patron.id,
                license_pool_id=licensepool.id,
            )
            _db.delete(loan)
        except Exception as e:
            # The patron didn't have this book checked out.
            pass

    def checkout(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        internal_format: Optional[str],
    ) -> LoanInfo:
        now = utc_now()
        return LoanInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            start_date=now,
            end_date=None,
        )

    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        internal_format: Optional[str] = None,
        part: Optional[str] = None,
        fulfill_part_url: Optional[Callable[[Optional[str]], str]] = None,
    ) -> FulfillmentInfo:
        """Retrieve a bearer token that can be used to download the book.

        :param kwargs: A container for arguments to fulfill()
           which are not relevant to this vendor.

        :return: a FulfillmentInfo object.
        """

        links = licensepool.identifier.links
        # Find the acquisition link with the right media type.
        for link in links:
            media_type = link.resource.representation.media_type
            if (
                link.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
                and media_type == internal_format
            ):
                url = link.resource.representation.url

                # Obtain a Credential with the information from our
                # bearer token.
                _db = Session.object_session(licensepool)
                credential = self._get_token(_db)

                # Build a application/vnd.librarysimplified.bearer-token
                # document using information from the credential.
                now = utc_now()
                expiration = int((credential.expires - now).total_seconds())  # type: ignore[operator]
                token_document = dict(
                    token_type="Bearer",
                    access_token=credential.credential,
                    expires_in=expiration,
                    location=url,
                )

                return FulfillmentInfo(
                    licensepool.collection,
                    licensepool.data_source.name,
                    licensepool.identifier.type,
                    licensepool.identifier.identifier,
                    content_link=None,
                    content_type=DeliveryMechanism.BEARER_TOKEN,
                    content=json.dumps(token_document),
                    content_expires=credential.expires,
                )

        # We couldn't find an acquisition link for this book.
        raise CannotFulfill()

    def patron_activity(self, patron: Patron, pin: str) -> List[LoanInfo | HoldInfo]:
        # Look up loans for this collection in the database.
        _db = Session.object_session(patron)
        loans = (
            _db.query(Loan)
            .join(Loan.license_pool)
            .filter(LicensePool.collection_id == self.collection_id)
            .filter(Loan.patron == patron)
        )
        return [
            LoanInfo(
                loan.license_pool.collection,
                loan.license_pool.data_source.name,
                loan.license_pool.identifier.type,
                loan.license_pool.identifier.identifier,
                loan.start,
                loan.end,
            )
            for loan in loans
        ]

    def release_hold(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        # All the books for this integration are available as simultaneous
        # use, so there's no need to release a hold.
        raise NotImplementedError()

    def place_hold(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        notification_email_address: Optional[str],
    ) -> HoldInfo:
        # All the books for this integration are available as simultaneous
        # use, so there's no need to place a hold.
        raise NotImplementedError()

    def update_availability(self, licensepool: LicensePool) -> None:
        pass


class OPDSForDistributorsImporter(OPDSImporter):
    NAME = OPDSForDistributorsAPI.NAME

    @classmethod
    def settings_class(cls) -> Type[OPDSForDistributorsSettings]:  # type: ignore[override]
        return OPDSForDistributorsSettings

    def update_work_for_edition(
        self,
        edition: Edition,
        is_open_access: bool = False,
    ) -> tuple[LicensePool | None, Work | None]:
        """After importing a LicensePool, set its availability appropriately.

        Books imported through OPDS For Distributors can be designated as
        either Open Access (handled elsewhere) or licensed (handled here). For
        licensed content, a library that can perform this import is deemed to
        have a license for the title and can distribute unlimited copies.
        """
        pool, work = super().update_work_for_edition(edition, is_open_access=False)
        if pool:
            pool.unlimited_access = True

        return pool, work

    @classmethod
    def _add_format_data(cls, circulation: CirculationData) -> None:
        for link in circulation.links:
            if (
                link.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
                and link.media_type in OPDSForDistributorsAPI.SUPPORTED_MEDIA_TYPES
            ):
                circulation.formats.append(
                    FormatData(
                        content_type=link.media_type,
                        drm_scheme=DeliveryMechanism.BEARER_TOKEN,
                        link=link,
                        rights_uri=RightsStatus.IN_COPYRIGHT,
                    )
                )


class OPDSForDistributorsImportMonitor(OPDSImportMonitor):
    """Monitor an OPDS feed that requires or allows authentication,
    such as Biblioboard or Plympton.
    """

    PROTOCOL = OPDSForDistributorsImporter.NAME
    SERVICE_NAME = "OPDS for Distributors Import Monitor"

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        import_class: Type[OPDSImporter],
        **kwargs: Any,
    ) -> None:
        super().__init__(_db, collection, import_class, **kwargs)

        self.api = OPDSForDistributorsAPI(_db, collection)

    def _get(
        self, url: str, headers: Dict[str, str]
    ) -> Tuple[int, Dict[str, str], bytes]:
        """Make a normal HTTP request for an OPDS feed, but add in an
        auth header with the credentials for the collection.
        """

        token = self.api._get_token(self._db).credential
        headers = dict(headers or {})
        auth_header = "Bearer %s" % token
        headers["Authorization"] = auth_header

        return super()._get(url, headers)


class OPDSForDistributorsReaperMonitor(OPDSForDistributorsImportMonitor):
    """This is an unusual import monitor that crawls the entire OPDS feed
    and keeps track of every identifier it sees, to find out if anything
    has been removed from the collection.
    """

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        import_class: Type[OPDSImporter],
        **kwargs: Any,
    ) -> None:
        super().__init__(_db, collection, import_class, **kwargs)
        self.seen_identifiers: Set[str] = set()

    def feed_contains_new_data(self, feed: bytes | str) -> bool:
        # Always return True so that the importer will crawl the
        # entire feed.
        return True

    def import_one_feed(
        self, feed: bytes | str
    ) -> Tuple[List[Edition], Dict[str, CoverageFailure | List[CoverageFailure]]]:
        # Collect all the identifiers in the feed.
        parsed_feed = feedparser.parse(feed)
        identifiers = [entry.get("id") for entry in parsed_feed.get("entries", [])]
        self.seen_identifiers.update(identifiers)
        return [], {}

    def run_once(self, progress: TimestampData) -> TimestampData:
        """Check to see if any identifiers we know about are no longer
        present on the remote. If there are any, remove them.

        :param progress: A TimestampData, ignored.
        """
        super().run_once(progress)

        # self.seen_identifiers is full of URNs. We need the values
        # that go in Identifier.identifier.
        identifiers, failures = Identifier.parse_urns(self._db, self.seen_identifiers)
        identifier_ids = [x.id for x in list(identifiers.values())]

        # At this point we've gone through the feed and collected all the identifiers.
        # If there's anything we didn't see, we know it's no longer available.
        qu = (
            self._db.query(LicensePool)
            .join(Identifier)
            .filter(LicensePool.collection_id == self.collection.id)
            .filter(~Identifier.id.in_(identifier_ids))
            .filter(LicensePool.licenses_available == LicensePool.UNLIMITED_ACCESS)
        )
        pools_reaped = qu.count()
        self.log.info(
            "Reaping %s license pools for collection %s."
            % (pools_reaped, self.collection.name)
        )

        for pool in qu:
            pool.unlimited_access = False

        self._db.commit()
        achievements = "License pools removed: %d." % pools_reaped
        return TimestampData(achievements=achievements)
