import pytest

from palace.manager.opds.lcp.status import LoanStatus
from tests.fixtures.files import OPDS2FilesFixture


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
        self, filename: str, opds2_files_fixture: OPDS2FilesFixture
    ) -> None:
        LoanStatus.model_validate_json(
            opds2_files_fixture.sample_data("lcp/status/" + filename)
        )
