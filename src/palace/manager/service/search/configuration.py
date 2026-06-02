from pydantic_settings import SettingsConfigDict

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)
from palace.manager.util.pydantic import HttpUrl


class SearchConfiguration(ServiceConfiguration):
    url: HttpUrl
    index_prefix: str = "circulation-works"
    timeout: int = 20
    # Per-request timeout (seconds) for the user-facing read/search path. Kept
    # well below ``timeout`` (which still applies to indexing/admin calls) so a
    # node that goes briefly unresponsive during OpenSearch maintenance fails
    # over quickly instead of stalling a web worker for the full ``timeout``.
    search_timeout: int = 4
    # Retry timed-out requests against another node. With two nodes per domain
    # this lets a read survive a single node bouncing during maintenance.
    max_retries: int = 2
    retry_on_timeout: bool = True
    maxsize: int = 25
    model_config = SettingsConfigDict(env_prefix="PALACE_SEARCH_")
