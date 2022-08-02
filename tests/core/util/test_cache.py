import time

from core.util.cache import _signature, memoize


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
