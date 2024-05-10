from __future__ import annotations

from unittest.mock import MagicMock, patch

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
        RebuildSearchIndexScript(db.session).do_run()
        mock_search_reindex.s.return_value.delay.assert_called_once_with()
        # But we don't delete the index before rebuilding.
        services_fixture.search_fixture.index_mock.clear_search_documents.assert_not_called()

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
        services_fixture.search_fixture.index_mock.clear_search_documents.assert_called_once_with()
        mock_search_reindex.s.return_value.delay.assert_called_once_with()

    @patch("palace.manager.scripts.search.get_migrate_search_chain")
    def test_do_run_migration(
        self, mock_get_migrate_search_chain: MagicMock, db: DatabaseTransactionFixture
    ):
        # If we are called with the --migration argument, we treat the reindex as completing a migration.
        RebuildSearchIndexScript(db.session, cmd_args=["--migration"]).do_run()
        mock_get_migrate_search_chain.return_value.delay.assert_called_once_with()

        # We can also combine --blocking and --migration.
        RebuildSearchIndexScript(
            db.session, cmd_args=["--migration", "--blocking"]
        ).do_run()
        mock_get_migrate_search_chain.return_value.assert_called_once_with()
