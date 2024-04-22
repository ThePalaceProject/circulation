from tests.fixtures.database import DatabaseTransactionFixture


class TestDatabaseInitialization:
    """Check that the test suite's database fixture initializes the database."""

    def test_database_initialization(self, db: DatabaseTransactionFixture):
        db.transaction.rollback()
