from palace.manager.integration.license.opds.requests import OpdsAuthType
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.problem_detail import raises_problem_detail


class TestOPDS2WithODLSettings:
    def test_auth_validation(self, db: DatabaseTransactionFixture) -> None:
        # No auth allows no username and no password
        settings = db.opds2_odl_settings(
            auth_type=OpdsAuthType.NONE,
            username=None,
            password=None,
        )

        # But basic auth requires both
        with raises_problem_detail() as pd_info:
            db.opds2_odl_settings(
                auth_type=OpdsAuthType.BASIC,
                username=None,
                password=None,
            )
        assert pd_info.value.detail == (
            "Missing required fields for Basic Auth authentication: "
            "'Library's API username', 'Library's API password'"
        )

        with raises_problem_detail() as pd_info:
            db.opds2_odl_settings(
                auth_type=OpdsAuthType.BASIC,
                username=None,
                password="xyz",
            )
        assert pd_info.value.detail == (
            "Missing required field for Basic Auth authentication: "
            "'Library's API username'"
        )

        # And so does OAuth
        with raises_problem_detail() as pd_info:
            db.opds2_odl_settings(
                auth_type=OpdsAuthType.OAUTH,
                username=None,
                password=None,
            )
        assert pd_info.value.detail == (
            "Missing required fields for OAuth (via OPDS authentication document) "
            "authentication: 'Library's API username', 'Library's API password'"
        )

        with raises_problem_detail() as pd_info:
            db.opds2_odl_settings(
                auth_type=OpdsAuthType.OAUTH,
                username="abc",
                password=None,
            )
        assert pd_info.value.detail == (
            "Missing required field for OAuth (via OPDS authentication document) "
            "authentication: 'Library's API password'"
        )
