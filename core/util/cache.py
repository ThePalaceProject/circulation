from __future__ import annotations

import sys
import time
from functools import wraps
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, TypeVar, cast

from sqlalchemy.orm import Session

from core.model.datasource import DataSource

# TODO: Remove this when we drop support for Python 3.9
if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec


P = ParamSpec("P")
T = TypeVar("T")


def _signature(func: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> str:
    """Create a hashable function signature
    by stringifying and joining all arguments"""
    strargs = ";".join([str(a) for a in args])
    strkwargs = ";".join([f"{k}={v}" for k, v in kwargs.items()])
    return str(func) + "::" + strargs + "::" + strkwargs


def memoize(ttls: int = 3600) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """An in-memory cache based off the function and arguments
    Usage:
    @memoize(ttls=<seconds>)
    def func(...):
        ...

    The `func` will be memoized and results cached based on the arguments passed in
    When `func` is a bound-method then each instance of the object will have its own cache
    because the first argument will always be the instance itself
    Hence the signatures will be different for each object
    """
    cache: Dict[str, Any] = {}

    def outer(func: Callable[P, T]) -> Callable[P, T]:
        @wraps(func)
        def inner(*args: P.args, **kwargs: P.kwargs) -> T:
            signature = _signature(func, *args, **kwargs)
            cached = cache.get(signature)

            # Has the cache expired?
            if cached and time.time() - cached["last_updated"] < inner.ttls:  # type: ignore[attr-defined]
                response = cast(T, cached["response"])
            else:
                response = func(*args, **kwargs)
                cache[signature] = dict(last_updated=time.time(), response=response)

            return response

        # The ttl is configurable from anywhere through the decorated function
        inner.ttls = ttls  # type: ignore[attr-defined]
        return inner

    return outer


class CachedData:
    """Threadsafe in-memory data caching using the memoize method
    Cache data using the CachedData.cache instance
    This must be initialized somewhere in the vicinity of its usage with CacheData.initialize(_db)
    While writing methods to cache, always lock the body to the _db is used and updated in a threadsafe manner
    Always expunge objects before returning the data, to avoid stale/cross-thread session usage
    """

    # Instance of itself
    cache: Optional[CachedData] = None

    @classmethod
    def initialize(cls, _db: Session) -> CachedData:
        """Initialize the cache data instance or update the _db instance for the global cache instance
        Use this method liberally in the vicinity of the usage of the cache functions so the _db instance
        is constantly being updated
        """
        if not cls.cache:
            cls.cache = cls(_db)
        else:
            # Simply update the DB session
            with cls.cache.lock:
                cls.cache._db = _db

        return cls.cache

    def __init__(self, _db: Session) -> None:
        self._db = _db
        self.lock = Lock()

    @memoize(ttls=3600)
    def data_sources(self) -> List[DataSource]:
        """List of all datasources within the system"""
        with self.lock:
            sources = self._db.query(DataSource).order_by(DataSource.id).all()
            for s in sources:
                self._db.expunge(s)
        return sources
