from __future__ import annotations

from unittest.mock import MagicMock, patch

from palace.manager.scripts.customlist import CustomListEntriesSweepScript
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


class TestCustomListEntriesSweepScript:
    @patch("palace.manager.scripts.customlist.update_custom_list_entries_sweep")
    def test_do_run_queues_task(
        self,
        mock_task: MagicMock,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
    ):
        """do_run dispatches the sweep task asynchronously and nothing else."""
        CustomListEntriesSweepScript(db.session).do_run()
        mock_task.delay.assert_called_once_with()
