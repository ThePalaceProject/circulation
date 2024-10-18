import dataclasses
from collections.abc import Generator
from contextlib import contextmanager

import pytest

from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException


@dataclasses.dataclass
class ProblemDetailInfo:
    _value: ProblemDetail | None = None
    _exception: ProblemDetailException | None = None

    @property
    def value(self) -> ProblemDetail:
        assert self._value is not None
        return self._value

    @property
    def exception(self) -> ProblemDetailException:
        assert self._exception is not None
        return self._exception


@contextmanager
def raises_problem_detail(
    *,
    pd: ProblemDetail | None = None,
    detail: str | None = None,
) -> Generator[ProblemDetailInfo]:
    info = ProblemDetailInfo()
    with pytest.raises(ProblemDetailException) as e:
        yield info
    info._exception = e.value
    info._value = e.value.problem_detail

    if pd is not None:
        assert info.value == pd

    if detail is not None:
        assert info.value.detail == detail
