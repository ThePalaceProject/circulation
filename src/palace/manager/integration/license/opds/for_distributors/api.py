from __future__ import annotations

import datetime
import json
from collections.abc import Generator
from typing import Unpack
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from celery.canvas import Signature
from sqlalchemy.orm import Session

from palace.manager.api.circulation.base import BaseCirculationAPI
from palace.manager.api.circulation.data import HoldInfo, LoanInfo
from palace.manager.api.circulation.exceptions import (
    CannotFulfill,
    DeliveryMechanismError,
)
from palace.manager.api.circulation.fulfillment import (
    DirectFulfillment,
    Fulfillment,
    RedirectFulfillment,
    StreamingFulfillment,
)
from palace.manager.api.selftest import HasCollectionSelfTests
from palace.manager.core.selftest import SelfTestResult
from palace.manager.integration.license.opds.for_distributors.settings import (
    OPDSForDistributorsLibrarySettings,
    OPDSForDistributorsSettings,
)
from palace.manager.integration.license.opds.requests import OAuthOpdsRequest
from palace.manager.integration.license.opds.settings.format_priority import (
    FormatPriorities,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.patron import Loan, Patron
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.datetime_helpers import utc_now


class OPDSForDistributorsAPI(
    BaseCirculationAPI[OPDSForDistributorsSettings, OPDSForDistributorsLibrarySettings],
    HasCollectionSelfTests,
):
    @classmethod
    def settings_class(cls) -> type[OPDSForDistributorsSettings]:
        return OPDSForDistributorsSettings

    @classmethod
    def library_settings_class(cls) -> type[OPDSForDistributorsLibrarySettings]:
        return OPDSForDistributorsLibrarySettings

    @classmethod
    def description(cls) -> str:
        return "Import books from a distributor that requires authentication to get the OPDS feed and download books."

    @classmethod
    def label(cls) -> str:
        return "OPDS for Distributors"

    def __init__(self, _db: Session, collection: Collection):
        super().__init__(_db, collection)

        self.data_source_name = self.settings.data_source
        self._make_request = OAuthOpdsRequest(
            self.settings.external_account_id,
            self.settings.username,
            self.settings.password,
        )
        self._format_priorities = FormatPriorities(
            self.settings.prioritized_drm_schemes,
            self.settings.prioritized_content_types,
            self.settings.deprioritize_lcp_non_epubs,
        )

    @property
    def data_source(self) -> DataSource:
        return DataSource.lookup(self._db, self.settings.data_source, autocreate=True)

    def _run_self_tests(self, _db: Session) -> Generator[SelfTestResult]:
        """Try to get a token."""
        yield self.run_test(
            "Negotiate a fulfillment token", self._make_request.refresh_token
        )

    def can_fulfill_without_loan(
        self,
        patron: Patron | None,
        pool: LicensePool,
        lpdm: LicensePoolDeliveryMechanism,
    ) -> bool:
        """Since OPDS For Distributors delivers books to the library rather
        than creating loans, any book can be fulfilled without
        identifying the patron, assuming the library's policies
        allow it.

        Just to be safe, though, we require that the
        DeliveryMechanism's drm_scheme be either 'no DRM', 'bearer
        token', or 'streaming', since other DRM schemes require identifying a patron.
        """
        if not lpdm or not lpdm.delivery_mechanism:
            return False
        drm_scheme = lpdm.delivery_mechanism.drm_scheme
        if drm_scheme in (
            DeliveryMechanism.NO_DRM,
            DeliveryMechanism.BEARER_TOKEN,
            DeliveryMechanism.STREAMING_DRM,
        ):
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
        pin: str | None,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism | None,
    ) -> LoanInfo:
        now = utc_now()
        return LoanInfo.from_license_pool(
            licensepool,
            start_date=now,
            end_date=None,
        )

    @staticmethod
    def _append_token_to_url(url: str, access_token: str) -> str:
        """Append an access_token query parameter to a URL.

        :param url: The base URL.
        :param access_token: The OAuth access token to append.
        :return: The URL with the access_token query parameter.
        """
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        query_params["token"] = [access_token]
        new_query = urlencode(query_params, doseq=True)
        return urlunparse(parsed._replace(query=new_query))

    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
        **kwargs: Unpack[BaseCirculationAPI.FulfillKwargs],
    ) -> Fulfillment:
        """Retrieve a bearer token that can be used to download the book,
        or for streaming content, return a URL with the token appended.

        :return: A fulfillment object appropriate for the delivery mechanism.
        """
        media_type = delivery_mechanism.delivery_mechanism.content_type
        drm_scheme = delivery_mechanism.delivery_mechanism.drm_scheme

        if drm_scheme not in [
            DeliveryMechanism.NO_DRM,
            DeliveryMechanism.BEARER_TOKEN,
            DeliveryMechanism.STREAMING_DRM,
        ]:
            raise DeliveryMechanismError(
                "Cannot fulfill a loan through OPDS For Distributors using a delivery mechanism with DRM scheme %s"
                % drm_scheme
            )

        if (
            delivery_mechanism.resource is None
            or delivery_mechanism.resource.url is None
        ):
            # We couldn't find an acquisition link for this book.
            raise CannotFulfill()

        url = delivery_mechanism.resource.url

        if drm_scheme == DeliveryMechanism.NO_DRM:
            return RedirectFulfillment(content_link=url, content_type=media_type)

        # Make sure we have a session token to pass to the app. If the token expires in the
        # next 10 minutes, we'll refresh it to make sure the app has enough time to download the book.
        token = self._make_request.session_token
        if token is None or token.expires - datetime.timedelta(minutes=10) < utc_now():
            token = self._make_request.refresh_token()

        if drm_scheme == DeliveryMechanism.STREAMING_DRM:
            link_content_type = (
                DeliveryMechanism.MEDIA_TYPES_FOR_STREAMING.get(media_type)
                if media_type
                else None
            )
            streaming_url = self._append_token_to_url(url, token.access_token)
            return StreamingFulfillment(
                content_link=streaming_url,
                content_type=link_content_type,
            )

        # Build an application/vnd.librarysimplified.bearer-token
        # document using information from the credential.
        now = utc_now()
        expiration = int((token.expires - now).total_seconds())
        token_document = dict(
            token_type="Bearer",
            access_token=token.access_token,
            expires_in=expiration,
            location=url,
        )

        return DirectFulfillment(
            content=json.dumps(token_document),
            content_type=DeliveryMechanism.BEARER_TOKEN,
        )

    def release_hold(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        # All the books for this integration are available as simultaneous
        # use, so there's no need to release a hold.
        raise NotImplementedError()

    def place_hold(
        self,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
        notification_email_address: str | None,
    ) -> HoldInfo:
        # All the books for this integration are available as simultaneous
        # use, so there's no need to place a hold.
        raise NotImplementedError()

    def update_availability(self, licensepool: LicensePool) -> None:
        pass

    def sort_delivery_mechanisms(
        self, lpdms: list[LicensePoolDeliveryMechanism]
    ) -> list[LicensePoolDeliveryMechanism]:
        return self._format_priorities.prioritize_mechanisms(lpdms)

    @classmethod
    def import_task(cls, collection_id: int, force: bool = False) -> Signature:
        from palace.manager.celery.tasks.opds_for_distributors import import_collection

        return import_collection.s(collection_id, force=force)
