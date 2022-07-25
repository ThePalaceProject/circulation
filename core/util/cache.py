import time
from functools import wraps


def _signature(func, *args, **kwargs):
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
    cache = {}

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
