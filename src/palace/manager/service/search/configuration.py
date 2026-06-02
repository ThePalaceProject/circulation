from __future__ import annotations

import os

from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import SettingsConfigDict

from palace.util.log import LoggerMixin

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)
from palace.manager.util.pydantic import HttpUrl

# Deprecated environment variable name for ``write_timeout``, still honored for
# backwards compatibility. Remove once deployments have migrated.
_DEPRECATED_WRITE_TIMEOUT_ENV = "PALACE_SEARCH_TIMEOUT"
_WRITE_TIMEOUT_ENV = "PALACE_SEARCH_WRITE_TIMEOUT"


class SearchConfiguration(ServiceConfiguration, LoggerMixin):
    url: HttpUrl
    index_prefix: str = "circulation-works"
    # Timeout (seconds) for indexing and admin operations, which legitimately
    # run longer. ``PALACE_SEARCH_TIMEOUT`` is the deprecated name for this
    # setting and is still accepted; prefer ``PALACE_SEARCH_WRITE_TIMEOUT``.
    write_timeout: int = Field(
        default=20,
        validation_alias=AliasChoices(
            _WRITE_TIMEOUT_ENV, _DEPRECATED_WRITE_TIMEOUT_ENV
        ),
    )
    # Timeout (seconds) for the user-facing read path. Kept well below
    # ``write_timeout`` so a node that goes briefly unresponsive during
    # OpenSearch maintenance fails over quickly instead of stalling a web
    # worker for the full write timeout.
    read_timeout: int = 4
    # Retry timed-out reads against another node. With two nodes per domain
    # this lets a read survive a single node bouncing during maintenance.
    max_retries: int = 2
    retry_on_timeout: bool = True
    maxsize: int = 25
    model_config = SettingsConfigDict(
        env_prefix="PALACE_SEARCH_", populate_by_name=True
    )

    @model_validator(mode="after")
    def _warn_deprecated_write_timeout_env(self) -> SearchConfiguration:
        # AliasChoices accepts the deprecated env var, but we want operators to
        # migrate, so emit a warning when only the deprecated name is set.
        if (
            _DEPRECATED_WRITE_TIMEOUT_ENV in os.environ
            and _WRITE_TIMEOUT_ENV not in os.environ
        ):
            self.log.warning(
                "%s is deprecated; use %s instead.",
                _DEPRECATED_WRITE_TIMEOUT_ENV,
                _WRITE_TIMEOUT_ENV,
            )
        return self
