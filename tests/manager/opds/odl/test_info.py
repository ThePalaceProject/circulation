import pytest

from palace.manager.opds.odl.info import LicenseInfo
from tests.fixtures.files import OPDSFilesFixture


class TestLicenseInfo:

    @pytest.mark.parametrize(
        "filename",
        [
            "feedbooks-ab-checked-out.json",
            "feedbooks-ab-loan-limited.json",
            "feedbooks-ab-not-checked-out.json",
            "feedbooks-book-adept.json",
            "feedbooks-book-unavailable.json",
            "ul-ab.json",
            "ul-book.json",
        ],
    )
    def test_license_info(
        self, filename: str, opds_files_fixture: OPDSFilesFixture
    ) -> None:
        info = LicenseInfo.model_validate_json(
            opds_files_fixture.sample_data("odl/info/" + filename)
        )
        assert info.identifier == "urn:uuid:123"
        assert isinstance(info.protection.formats, frozenset)
        assert len(info.protection.formats) == 1
