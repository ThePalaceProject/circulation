import time
from functools import wraps
from threading import Lock
from typing import Any, Callable, Dict, List

from ..model.datasource import DataSource


def _signature(func: Callable, *args, **kwargs) -> str:
    """Create a hashable function signature
    by stringifying and joining all arguments"""
    strargs = ";".join([str(a) for a in args])
    strkwargs = ";".join([f"{k}={v}" for k, v in kwargs.items()])
    return str(func) + "::" + strargs + "::" + strkwargs


def memoize(ttls: int = 3600):
    """An in-memory cache based off the funcion and arguments
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

    def outer(func):
        @wraps(func)
        def inner(*args, **kwargs):
            signature = _signature(func, *args, **kwargs)
            cached = cache.get(signature)

            # Has the cache expired?
            if cached and time.time() - cached["last_updated"] < inner.ttls:
                response = cached["response"]
            else:
                response = func(*args, **kwargs)
                cache[signature] = dict(last_updated=time.time(), response=response)

            return response

        # The ttl is configurable from anywhere through the decorated function
        inner.ttls = ttls
        return inner

    return outer


class CachedData:
    """Threadsafe in-memory data caching using the memoize method
    Cache data using the CachedData.cache instance
    This must be initialized somewhere in the vicinity of its usage with CacheData.initialize(_db)
    While writing methods to cache, always lock the body to the _db is used and updated in a threadsafe manner
    Always expunge objects before returning the data, to avoid stale/cross-thread session usage"""

    # Instance of itself
    cache = None

    @classmethod
    def initialize(cls, _db):
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

    def __init__(self, _db) -> None:
        self._db = _db
        self.lock = Lock()

    @memoize(ttls=3600)
    def data_sources(self) -> List[DataSource]:
        """List of all datasources within the system"""
        with self.lock:
            sources = self._db.query(DataSource).all()
            for s in sources:
                self._db.expunge(s)
        return sources
