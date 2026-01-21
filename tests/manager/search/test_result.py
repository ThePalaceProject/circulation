from palace.manager.search.result import WorkSearchResult
from tests.fixtures.database import DatabaseTransactionFixture


class TestWorkSearchResult:
    # Test the WorkSearchResult class, which wraps together a data
    # model Work and an OpenSearch Hit into something that looks
    # like a Work.

    def test_constructor(self, db: DatabaseTransactionFixture):
        work = db.work()
        hit = object()
        result = WorkSearchResult(work, hit)

        # The original Work object is available as ._work
        assert work == result._work

        # The Opensearch Hit object is available as ._hit
        assert hit == result._hit

        # Any other attributes are delegated to the Work.
        assert work.sort_title == result.sort_title
