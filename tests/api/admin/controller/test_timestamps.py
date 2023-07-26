import pytest

from api.admin.exceptions import AdminNotAuthorized
from core.model import AdminRole, Timestamp, create
from core.util.datetime_helpers import utc_now
from tests.fixtures.api_admin import AdminControllerFixture


class TimestampsFixture:
    def __init__(self, admin_ctrl_fixture: AdminControllerFixture):
        self.admin_ctrl_fixture = admin_ctrl_fixture

        db = self.admin_ctrl_fixture.ctrl.db.session

        for timestamp in db.query(Timestamp):
            db.delete(timestamp)

        self.collection = self.admin_ctrl_fixture.ctrl.db.default_collection()
        self.start = utc_now()
        self.finish = utc_now()

        cp, ignore = create(
            db,
            Timestamp,
            service_type="coverage_provider",
            service="test_cp",
            start=self.start,
            finish=self.finish,
            collection=self.collection,
        )

        monitor, ignore = create(
            db,
            Timestamp,
            service_type="monitor",
            service="test_monitor",
            start=self.start,
            finish=self.finish,
            collection=self.collection,
            exception="stack trace string",
        )

        script, ignore = create(
            db,
            Timestamp,
            achievements="ran a script",
            service_type="script",
            service="test_script",
            start=self.start,
            finish=self.finish,
        )

        other, ignore = create(
            db,
            Timestamp,
            service="test_other",
            start=self.start,
            finish=self.finish,
        )


@pytest.fixture(scope="function")
def timestamps_fixture(admin_ctrl_fixture: AdminControllerFixture) -> TimestampsFixture:
    return TimestampsFixture(admin_ctrl_fixture)


class TestTimestampsController:
    def test_diagnostics_admin_not_authorized(
        self, timestamps_fixture: TimestampsFixture
    ):
        with timestamps_fixture.admin_ctrl_fixture.request_context_with_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                timestamps_fixture.admin_ctrl_fixture.manager.timestamps_controller.diagnostics,
            )

    def test_diagnostics(self, timestamps_fixture: TimestampsFixture):
        duration = (
            timestamps_fixture.finish - timestamps_fixture.start
        ).total_seconds()

        with timestamps_fixture.admin_ctrl_fixture.request_context_with_admin("/"):
            timestamps_fixture.admin_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = (
                timestamps_fixture.admin_ctrl_fixture.manager.timestamps_controller.diagnostics()
            )

        assert set(response.keys()) == {
            "coverage_provider",
            "monitor",
            "script",
            "other",
        }

        cp_service = response["coverage_provider"]
        cp_name, cp_collection = list(cp_service.items())[0]
        assert cp_name == "test_cp"
        cp_collection_name, [cp_timestamp] = list(cp_collection.items())[0]
        assert cp_collection_name == timestamps_fixture.collection.name
        assert cp_timestamp.get("exception") == None
        assert cp_timestamp.get("start") == timestamps_fixture.start
        assert cp_timestamp.get("duration") == duration
        assert cp_timestamp.get("achievements") == None

        monitor_service = response["monitor"]
        monitor_name, monitor_collection = list(monitor_service.items())[0]
        assert monitor_name == "test_monitor"
        monitor_collection_name, [monitor_timestamp] = list(monitor_collection.items())[
            0
        ]
        assert monitor_collection_name == timestamps_fixture.collection.name
        assert monitor_timestamp.get("exception") == "stack trace string"
        assert monitor_timestamp.get("start") == timestamps_fixture.start
        assert monitor_timestamp.get("duration") == duration
        assert monitor_timestamp.get("achievements") == None

        script_service = response["script"]
        script_name, script_collection = list(script_service.items())[0]
        assert script_name == "test_script"
        script_collection_name, [script_timestamp] = list(script_collection.items())[0]
        assert script_collection_name == "No associated collection"
        assert script_timestamp.get("exception") == None
        assert script_timestamp.get("duration") == duration
        assert script_timestamp.get("start") == timestamps_fixture.start
        assert script_timestamp.get("achievements") == "ran a script"

        other_service = response["other"]
        other_name, other_collection = list(other_service.items())[0]
        assert other_name == "test_other"
        other_collection_name, [other_timestamp] = list(other_collection.items())[0]
        assert other_collection_name == "No associated collection"
        assert other_timestamp.get("exception") == None
        assert other_timestamp.get("duration") == duration
        assert other_timestamp.get("start") == timestamps_fixture.start
        assert other_timestamp.get("achievements") == None
