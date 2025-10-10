from __future__ import annotations

import dataclasses
import os
from typing import Self

from pydantic_settings import SettingsConfigDict

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)


@dataclasses.dataclass
class ToxUrlTuple:
    scheme: str
    host: str
    port: str

    def as_string(self) -> str:
        return f"{self.scheme}://{self.host}:{self.port}"


@dataclasses.dataclass
class ToxPasswordUrlTuple(ToxUrlTuple):
    user: str
    password: str

    def as_string(self) -> str:
        return f"{self.scheme}://{self.user}:{self.password}@{self.host}:{self.port}"


class FixtureTestUrlConfiguration(ServiceConfiguration):
    url: str

    model_config = SettingsConfigDict(
        env_prefix="PALACE_TEST_",
        extra="ignore",
    )

    @classmethod
    def url_cls(cls) -> type[ToxUrlTuple]:
        return ToxUrlTuple

    @classmethod
    def from_env(cls) -> Self:
        # We do a bit of preprocessing of the environment because tox-docker forces us to
        # set some URLs in multiple pieces. We read the URL parts from the environment, and
        # if we are missing any part, we just load the configuration from the environment like
        # normal as a fallback. This fallback is the normal behavior that will be used when
        # we are running outside tox-docker.
        prefix = cls.model_config.get("env_prefix")
        assert prefix is not None, "env_prefix must be set on the class"

        prefix += "URL"

        fields = [f.name for f in dataclasses.fields(cls.url_cls())]
        try:
            url_parts = {f: os.environ[f"{prefix}_{f.upper()}"] for f in fields}
        except KeyError:
            return cls()

        return cls(url=cls.url_cls()(**url_parts).as_string())
