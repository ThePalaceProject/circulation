from collections.abc import Iterable
from typing import Self


class CountingIterator[I]():
    """An iterator that counts the number of rows yielded."""

    def __init__(self, iterable: Iterable[I]) -> None:
        self.iterator = iter(iterable)
        self.count = 0

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> I:
        row = next(self.iterator)
        self.count += 1
        return row

    def get_count(self) -> int:
        return self.count
