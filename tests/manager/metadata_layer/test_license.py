from palace.manager.metadata_layer.license import LicenseData
from palace.manager.opds.odl.info import LicenseStatus


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
