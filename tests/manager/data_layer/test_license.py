from palace.manager.data_layer.license import LicenseData
from palace.manager.opds.odl.info import LicenseStatus
from tests.fixtures.database import DatabaseTransactionFixture


class TestLicense:
    def test_hash(self) -> None:
        """That LicenseData is hashable."""
        license_data = LicenseData(
            identifier="12345",
            checkout_url="http://example.com/checkout",
            status_url="http://example.com/status",
            status=LicenseStatus.available,
            checkouts_available=5,
        )
        assert hash(license_data)

    def test_terms_checkouts_set_and_retrieved(self) -> None:
        """That terms_checkouts can be set and retrieved on LicenseData."""
        license_data = LicenseData(
            identifier="12345",
            checkout_url="http://example.com/checkout",
            status_url="http://example.com/status",
            status=LicenseStatus.available,
            checkouts_available=5,
            terms_checkouts=10,
        )
        assert license_data.terms_checkouts == 10

    def test_terms_checkouts_defaults_to_none(self) -> None:
        """That terms_checkouts defaults to None when not provided."""
        license_data = LicenseData(
            identifier="12345",
            checkout_url="http://example.com/checkout",
            status_url="http://example.com/status",
            status=LicenseStatus.available,
            checkouts_available=5,
        )
        assert license_data.terms_checkouts is None

    def test_terms_checkouts_can_be_none(self) -> None:
        """That terms_checkouts can be explicitly set to None."""
        license_data = LicenseData(
            identifier="12345",
            checkout_url="http://example.com/checkout",
            status_url="http://example.com/status",
            status=LicenseStatus.available,
            checkouts_available=5,
            terms_checkouts=None,
        )
        assert license_data.terms_checkouts is None

    def test_add_to_pool_transfers_terms_checkouts(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """That add_to_pool properly transfers terms_checkouts from LicenseData to License."""
        from palace.manager.data_layer.license import LicenseData
        from palace.manager.opds.odl.info import LicenseStatus

        pool = db.licensepool(None, collection=db.default_collection())
        license_data = LicenseData(
            identifier="test-license-123",
            checkout_url="http://example.com/checkout",
            status_url="http://example.com/status",
            status=LicenseStatus.available,
            checkouts_available=5,
            terms_checkouts=20,
        )

        license_obj = license_data.add_to_pool(db.session, pool)
        db.session.commit()

        assert license_obj.terms_checkouts == 20
        assert license_obj.identifier == "test-license-123"
        assert license_obj.checkouts_available == 5

    def test_add_to_pool_transfers_terms_checkouts_none(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """That add_to_pool properly transfers None terms_checkouts from LicenseData to License."""
        from palace.manager.data_layer.license import LicenseData
        from palace.manager.opds.odl.info import LicenseStatus

        pool = db.licensepool(None, collection=db.default_collection())
        license_data = LicenseData(
            identifier="test-license-456",
            checkout_url="http://example.com/checkout",
            status_url="http://example.com/status",
            status=LicenseStatus.available,
            checkouts_available=5,
            terms_checkouts=None,
        )

        license_obj = license_data.add_to_pool(db.session, pool)
        db.session.commit()

        assert license_obj.terms_checkouts is None
        assert license_obj.identifier == "test-license-456"
        assert license_obj.checkouts_available == 5

    def test_add_to_pool_updates_existing_license_terms_checkouts(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """That add_to_pool updates terms_checkouts on an existing License."""
        from palace.manager.data_layer.license import LicenseData
        from palace.manager.opds.odl.info import LicenseStatus

        pool = db.licensepool(None, collection=db.default_collection())
        # Create an existing license
        existing_license = db.license(
            pool,
            identifier="test-license-789",
            checkouts_available=3,
            terms_checkouts=10,
        )
        db.session.commit()

        # Create LicenseData with same identifier but different terms_checkouts
        license_data = LicenseData(
            identifier="test-license-789",
            checkout_url="http://example.com/checkout",
            status_url="http://example.com/status",
            status=LicenseStatus.available,
            checkouts_available=5,
            terms_checkouts=25,
        )

        license_obj = license_data.add_to_pool(db.session, pool)
        db.session.commit()

        # Should be the same license object
        assert license_obj.id == existing_license.id
        # terms_checkouts should be updated
        assert license_obj.terms_checkouts == 25
