from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from palace.manager.api.circulation.data import LoanInfo
from palace.manager.api.circulation.exceptions import (
    CurrentlyAvailable,
    FormatNotAvailable,
    NotOnHold,
)
from palace.manager.api.circulation.fulfillment import RedirectFulfillment
from palace.manager.integration.license.opds.opds1.api import OPDSAPI
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.resource import Representation, Resource
from tests.fixtures.database import DatabaseTransactionFixture


class OPDSAPIFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.session = db.session
        self.collection = db.collection(
            protocol=OPDSAPI,
            settings=db.opds_settings(
                external_account_id="http://opds2.example.org/feed",
            ),
        )
        self.api = OPDSAPI(self.session, self.collection)

        self.mock_patron = MagicMock()
        self.mock_pin = MagicMock(spec=str)
        self.mock_licensepool = MagicMock(spec=LicensePool)
        self.mock_licensepool.collection = self.collection


@pytest.fixture
def opds_api_fixture(db: DatabaseTransactionFixture) -> OPDSAPIFixture:
    return OPDSAPIFixture(db)


class TestOPDSAPI:
    def test_checkin(self, opds_api_fixture: OPDSAPIFixture) -> None:
        # Make sure we can call checkin() without getting an exception.
        # The function is a no-op for this api, so we don't need to
        # test anything else.
        opds_api_fixture.api.checkin(
            opds_api_fixture.mock_patron,
            opds_api_fixture.mock_pin,
            opds_api_fixture.mock_licensepool,
        )

    def test_release_hold(self, opds_api_fixture: OPDSAPIFixture) -> None:
        # This api doesn't support holds. So we expect an exception.
        with pytest.raises(NotOnHold):
            opds_api_fixture.api.release_hold(
                opds_api_fixture.mock_patron,
                opds_api_fixture.mock_pin,
                opds_api_fixture.mock_licensepool,
            )

    def test_place_hold(self, opds_api_fixture: OPDSAPIFixture) -> None:
        # This api doesn't support holds. So we expect an exception.
        with pytest.raises(CurrentlyAvailable):
            opds_api_fixture.api.place_hold(
                opds_api_fixture.mock_patron,
                opds_api_fixture.mock_pin,
                opds_api_fixture.mock_licensepool,
                None,
            )

    def test_update_availability(self, opds_api_fixture: OPDSAPIFixture) -> None:
        # This function is a no-op since we already know the availability
        # of the license pool for any OPDS content. So we just make sure
        # we can call it without getting an exception.
        opds_api_fixture.api.update_availability(opds_api_fixture.mock_licensepool)

    def test_checkout(self, opds_api_fixture: OPDSAPIFixture) -> None:
        # Make sure checkout returns a LoanInfo object with the correct
        # collection id.
        delivery_mechanism = MagicMock(spec=LicensePoolDeliveryMechanism)
        loan = opds_api_fixture.api.checkout(
            opds_api_fixture.mock_patron,
            opds_api_fixture.mock_pin,
            opds_api_fixture.mock_licensepool,
            delivery_mechanism,
        )
        assert isinstance(loan, LoanInfo)
        assert loan.collection_id == opds_api_fixture.mock_licensepool.collection_id

    def test_can_fulfill_without_loan(self, opds_api_fixture: OPDSAPIFixture) -> None:
        # This should always return True.
        mock_lpdm = MagicMock(spec=LicensePoolDeliveryMechanism)
        assert (
            opds_api_fixture.api.can_fulfill_without_loan(
                opds_api_fixture.mock_patron,
                opds_api_fixture.mock_licensepool,
                mock_lpdm,
            )
            is True
        )

    def test_fulfill(self, opds_api_fixture: OPDSAPIFixture) -> None:
        # We only fulfill if the requested format matches an available format
        # for the license pool.
        mock_mechanism = MagicMock(spec=DeliveryMechanism)
        mock_lpdm = MagicMock(spec=LicensePoolDeliveryMechanism)
        mock_lpdm.delivery_mechanism = mock_mechanism

        # This license pool has no available formats.
        opds_api_fixture.mock_licensepool.available_delivery_mechanisms = []
        with pytest.raises(FormatNotAvailable):
            opds_api_fixture.api.fulfill(
                opds_api_fixture.mock_patron,
                opds_api_fixture.mock_pin,
                opds_api_fixture.mock_licensepool,
                mock_lpdm,
            )

        # This license pool has a delivery mechanism, but it's not the one
        # we're looking for.
        opds_api_fixture.mock_licensepool.available_delivery_mechanisms = [
            MagicMock(),
            MagicMock(),
        ]
        with pytest.raises(FormatNotAvailable):
            opds_api_fixture.api.fulfill(
                opds_api_fixture.mock_patron,
                opds_api_fixture.mock_pin,
                opds_api_fixture.mock_licensepool,
                mock_lpdm,
            )

        # This license pool has the delivery mechanism we're looking for, but
        # it does not have a resource.
        mock_lpdm.resource = None
        opds_api_fixture.mock_licensepool.available_delivery_mechanisms = [mock_lpdm]
        with pytest.raises(FormatNotAvailable):
            opds_api_fixture.api.fulfill(
                opds_api_fixture.mock_patron,
                opds_api_fixture.mock_pin,
                opds_api_fixture.mock_licensepool,
                mock_lpdm,
            )

        # This license pool has the delivery mechanism we're looking for, and
        # it has a resource, but the resource doesn't have a representation.
        mock_lpdm.resource = MagicMock(spec=Resource)
        mock_lpdm.resource.representation = None
        opds_api_fixture.mock_licensepool.available_delivery_mechanisms = [mock_lpdm]
        with pytest.raises(FormatNotAvailable):
            opds_api_fixture.api.fulfill(
                opds_api_fixture.mock_patron,
                opds_api_fixture.mock_pin,
                opds_api_fixture.mock_licensepool,
                mock_lpdm,
            )

        # This license pool has the delivery mechanism we're looking for, and
        # it has a resource, the resource has a representation, but the
        # representation doesn't have a URL.
        mock_lpdm.resource.representation = MagicMock(spec=Representation)
        mock_lpdm.resource.representation.public_url = None
        opds_api_fixture.mock_licensepool.available_delivery_mechanisms = [mock_lpdm]
        with pytest.raises(FormatNotAvailable):
            opds_api_fixture.api.fulfill(
                opds_api_fixture.mock_patron,
                opds_api_fixture.mock_pin,
                opds_api_fixture.mock_licensepool,
                mock_lpdm,
            )

        # This license pool has everything we need, so we can fulfill.
        mock_lpdm.resource.representation.public_url = "http://foo.com/bar.epub"
        opds_api_fixture.mock_licensepool.available_delivery_mechanisms = [
            MagicMock(),
            MagicMock(),
            mock_lpdm,
        ]
        fulfillment = opds_api_fixture.api.fulfill(
            opds_api_fixture.mock_patron,
            opds_api_fixture.mock_pin,
            opds_api_fixture.mock_licensepool,
            mock_lpdm,
        )
        assert isinstance(fulfillment, RedirectFulfillment)
        assert fulfillment.content_link == mock_lpdm.resource.representation.public_url
        assert fulfillment.content_type == mock_lpdm.resource.representation.media_type
