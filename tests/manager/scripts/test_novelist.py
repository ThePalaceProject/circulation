from __future__ import annotations

from unittest.mock import patch

import pytest

from palace.manager.integration.goals import Goals
from palace.manager.integration.metadata.novelist import (
    NoveListAPI,
    NoveListApiSettings,
)
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
        l2 = db.library()
        db.integration_configuration(
            name="test novelist integration",
            protocol=NoveListAPI,
            goal=Goals.METADATA_GOAL,
            libraries=[l1],
            settings=NoveListApiSettings(username="test", password="test"),
        )

        with patch(
            "palace.manager.scripts.novelist.update_novelists_by_library"
        ) as update:
            script = NovelistSnapshotScript(
                db.session,
            )
            script.do_run()
            update.delay.assert_called_once_with(library_id=l1.id)
            assert (
                f'Queued novelist_update task for library: name="{l1.name}", id={l1.id}'
                in caplog.text
            )
            assert (
                f'The library name "{l2.name}" is not associated with Novelist API integration and '
                f"therefore will not be queued."
            ) in caplog.text
