import pytest

from palace.manager.opds.lcp.status import LoanStatus
from tests.fixtures.files import OPDSFilesFixture


class TestLcpStatus:

    @pytest.mark.parametrize(
        "filename",
        [
            "fb-active.json",
            "fb-book-adobe.json",
            "fb-early-return.json",
            "ul-active.json",
            "ul-returned.json",
        ],
    )
    def test_lcp_status(
        self, filename: str, opds_files_fixture: OPDSFilesFixture
    ) -> None:
        LoanStatus.model_validate_json(
            opds_files_fixture.sample_data("lcp/status/" + filename)
        )
