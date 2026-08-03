from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from palace.manager.scripts.search import RebuildSearchIndexScript
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


class TestRebuildSearchIndexScript:
    @patch("palace.manager.scripts.search.search_reindex")
    def test_do_run_no_args(
        self,
        mock_search_reindex: MagicMock,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
    ):
        # If we are called with no arguments, we default to asynchronously rebuilding the search index.
        RebuildSearchIndexScript(db.session, cmd_args=[]).do_run()
        mock_search_reindex.s.return_value.delay.assert_called_once_with()
        # But we don't delete the index before rebuilding.
        services_fixture.search_index.clear_search_documents.assert_not_called()

    @patch("palace.manager.scripts.search.search_reindex")
    def test_do_run_blocking(
        self, mock_search_reindex: MagicMock, db: DatabaseTransactionFixture
    ):
        # If we are called with the --blocking argument, we rebuild the search index synchronously.
        RebuildSearchIndexScript(db.session, cmd_args=["--blocking"]).do_run()
        mock_search_reindex.s.return_value.assert_called_once_with()

    @patch("palace.manager.scripts.search.search_reindex")
    def test_do_run_delete(
        self,
        mock_search_reindex: MagicMock,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
    ):
        # If we are called with the --delete argument, we clear the index before rebuilding.
        RebuildSearchIndexScript(db.session, cmd_args=["--delete"]).do_run()
        services_fixture.search_index.clear_search_documents.assert_called_once_with()
        mock_search_reindex.s.return_value.delay.assert_called_once_with()

    def test_do_run_migration_flag_removed(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ):
        # search_reindex advances the read pointer itself, so there is no longer a
        # separate migration mode to opt into.
        with pytest.raises(SystemExit):
            RebuildSearchIndexScript(db.session, cmd_args=["--migration"])
