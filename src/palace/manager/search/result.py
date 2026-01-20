from __future__ import annotations


class WorkSearchResult:
    """Wraps a Work object to give extra information obtained from
    Opensearch.

    This object acts just like a Work (though isinstance(x, Work) will
    fail), with one exception: you can access the raw Opensearch Hit
    result as ._hit.

    This is useful when a Work needs to be 'tagged' with information
    obtained through Opensearch, such as its 'last modified' date
    the context of a specific lane.
    """

    def __init__(self, work, hit):
        self._work = work
        self._hit = hit

    def __getattr__(self, k):
        return getattr(self._work, k)
