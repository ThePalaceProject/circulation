from __future__ import annotations

from palace.manager.scripts.contributor_names import CheckContributorNamesInDB
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks.stdin import MockStdin


class TestCheckContributorNamesInDB:
    def test_process_contribution_local(self, db: DatabaseTransactionFixture):
        stdin = MockStdin()
        cmd_args: list[str] = []

        edition_alice, pool_alice = db.edition(
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            identifier_id="1",
            with_open_access_download=True,
            title="Alice Writes Books",
        )

        alice, new = db.contributor(sort_name="Alice Alrighty")
        alice._sort_name = "Alice Alrighty"
        alice.display_name = "Alice Alrighty"

        edition_alice.add_contributor(alice, [Contributor.Role.PRIMARY_AUTHOR])
        edition_alice.sort_author = "Alice Rocks"

        # everything is set up as we expect
        assert "Alice Alrighty" == alice.sort_name
        assert "Alice Alrighty" == alice.display_name
        assert "Alice Rocks" == edition_alice.sort_author

        edition_bob, pool_bob = db.edition(
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            identifier_id="2",
            with_open_access_download=True,
            title="Bob Writes Books",
        )

        bob, new = db.contributor(sort_name="Bob")
        bob.display_name = "Bob Bitshifter"

        edition_bob.add_contributor(bob, [Contributor.Role.PRIMARY_AUTHOR])
        edition_bob.sort_author = "Bob Rocks"

        assert "Bob" == bob.sort_name
        assert "Bob Bitshifter" == bob.display_name
        assert "Bob Rocks" == edition_bob.sort_author

        contributor_fixer = CheckContributorNamesInDB(
            _db=db.session, cmd_args=cmd_args, stdin=stdin
        )
        contributor_fixer.do_run()

        # Alice got fixed up.
        assert "Alrighty, Alice" == alice.sort_name
        assert "Alice Alrighty" == alice.display_name
        assert "Alrighty, Alice" == edition_alice.sort_author

        # Bob's repairs were too extensive to make.
        assert "Bob" == bob.sort_name
        assert "Bob Bitshifter" == bob.display_name
        assert "Bob Rocks" == edition_bob.sort_author
