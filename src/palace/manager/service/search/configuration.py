from pydantic_settings import SettingsConfigDict

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)
from palace.manager.util.pydantic import HttpUrl


class SearchConfiguration(ServiceConfiguration):
    url: HttpUrl
    index_prefix: str = "circulation-works"
    # Timeout (seconds) for indexing and admin operations, which legitimately
    # run longer.
    write_timeout: int = 20
    # Timeout (seconds) for the user-facing read path.
    read_timeout: int = 10
    # Read-path request retries, applied only to the read client.
    read_max_retries: int = 0
    read_retry_on_timeout: bool = False
    maxsize: int = 25
    model_config = SettingsConfigDict(env_prefix="PALACE_SEARCH_")
