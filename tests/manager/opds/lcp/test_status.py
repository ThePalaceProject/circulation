import pytest

from palace.manager.opds.lcp.status import LcpStatus
from tests.fixtures.files import OPDSFilesFixture


class TestLcpStatus:

    @pytest.mark.parametrize(
        "filename",
        [
            "fb-active.json",
            "ul-active.json",
        ],
    )
    def test_lcp_status(
        self, filename: str, opds_files_fixture: OPDSFilesFixture
    ) -> None:
        LcpStatus.model_validate_json(
            opds_files_fixture.sample_data("lcp/status/" + filename)
        )
