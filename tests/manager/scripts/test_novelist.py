from __future__ import annotations

from unittest.mock import patch

import pytest

from palace.manager.scripts.novelist import NovelistSnapshotScript
from palace.manager.service.logging.configuration import LogLevel
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


class TestNovelistSnapshotScript:
    def mockNoveListAPI(self, *args, **kwargs):
        self.called_with = (args, kwargs)

    def test_do_run(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that NovelistSnapshotScript.do_run() queues the update_novelists_by_library task."""
        caplog.set_level(LogLevel.info)
        l1 = db.library()
        cmd_args = [l1.name]

        with patch(
            "palace.manager.scripts.novelist.update_novelists_by_library"
        ) as update:
            script = NovelistSnapshotScript(
                db.session,
            )
            script.do_run(cmd_args=cmd_args)
            update.delay.assert_called_once_with(library_id=l1.id)
            assert (
                f'Queued novelist_update task for library: name="{l1.name}", id={l1.id}'
                in caplog.text
            )
