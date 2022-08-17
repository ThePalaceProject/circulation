import time
from unittest.mock import MagicMock

from core.model.datasource import DataSource
from core.testing import DatabaseTest
from core.util.cache import CachedData, _signature, memoize


class TestMemoize:
    def test_memoize(self):
        @memoize(ttls=10)
        def _func():
            return time.time()

        # 10 second ttl remains the same
        result = _func()
        time.sleep(0.1)
        assert _func() == result

        # 0 second ttl will change the result
        _func.ttls = 0
        result = _func()
        time.sleep(0.1)
        assert _func() != result

    def test_signature(self):
        def _func():
            pass

        o = object()
        assert (
            _signature(_func, 1, "x", o, one="one", obj=o)
            == f"{str(_func)}::1;x;{o}::one=one;obj={str(o)}"
        )


class TestCacheData(DatabaseTest):
    def test_data_sources(self):
        CachedData.initialize(self._db)
        all_sources = self._db.query(DataSource).all()
        assert CachedData.cache.data_sources() == all_sources

        # Mock the db object
        CachedData.initialize(MagicMock())
        # No changes to output
        assert CachedData.cache.data_sources() == all_sources
        assert CachedData.cache.data_sources() == all_sources
        # mock object was never called due to memoize
        assert CachedData.cache._db.query.call_count == 0
