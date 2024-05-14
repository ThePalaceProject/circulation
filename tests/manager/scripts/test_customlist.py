from __future__ import annotations

from palace.manager.scripts.customlist import UpdateCustomListSizeScript
from tests.fixtures.database import DatabaseTransactionFixture


class TestUpdateCustomListSizeScript:
    def test_do_run(self, db: DatabaseTransactionFixture):
        customlist, ignore = db.customlist(num_entries=1)
        customlist.library = db.default_library()
        customlist.size = 100
        UpdateCustomListSizeScript(db.session).do_run(cmd_args=[])
        assert 1 == customlist.size
