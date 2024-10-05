import pytest

from palace.manager.opds.lcp.license import LicenseDocument
from tests.fixtures.files import OPDSFilesFixture


class TestLicenseDocument:

    @pytest.mark.parametrize(
        "filename",
        [
            "fb.json",
            "ul.json",
        ],
    )
    def test_license_document(
        self, filename: str, opds_files_fixture: OPDSFilesFixture
    ) -> None:
        LicenseDocument.model_validate_json(
            opds_files_fixture.sample_data("lcp/license/" + filename)
        )
