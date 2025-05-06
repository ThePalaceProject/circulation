from __future__ import annotations

from unittest.mock import MagicMock

from palace.manager.api.metadata.novelist import NoveListAPI
from palace.manager.scripts.novelist import NovelistSnapshotScript
from tests.fixtures.database import DatabaseTransactionFixture


class TestNovelistSnapshotScript:
    def mockNoveListAPI(self, *args, **kwargs):
        self.called_with = (args, kwargs)

    def test_do_run(self, db: DatabaseTransactionFixture):
        """Test that NovelistSnapshotScript.do_run() calls the NoveList api."""

        class MockNovelistSnapshotScript(NovelistSnapshotScript):
            pass

        oldNovelistConfig = NoveListAPI.from_config
        NoveListAPI.from_config = self.mockNoveListAPI
        NoveListAPI.is_configured_db_check = MagicMock()
        NoveListAPI.is_configured_db_check.return_value = True

        l1 = db.library()

        cmd_args = [l1.name]
        script = MockNovelistSnapshotScript(db.session)
        script.do_run(cmd_args=cmd_args)

        (params, args) = self.called_with

        assert params[0] == l1

        NoveListAPI.from_config = oldNovelistConfig
