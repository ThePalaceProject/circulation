import os

import pytest
from pytest import MonkeyPatch


class MonkeyPatchEnvFixture:
    def __init__(self, monkeypatch: MonkeyPatch):
        self.monkeypatch = monkeypatch

    def __call__(self, key: str, value: str | None) -> None:
        if value:
            self.monkeypatch.setenv(key, value)
        elif key in os.environ:
            self.monkeypatch.delenv(key)


@pytest.fixture
def monkeypatch_env(monkeypatch: MonkeyPatch) -> MonkeyPatchEnvFixture:
    return MonkeyPatchEnvFixture(monkeypatch)
