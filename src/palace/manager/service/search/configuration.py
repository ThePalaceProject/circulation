from pydantic import AnyHttpUrl

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)


class SearchConfiguration(ServiceConfiguration):
    url: AnyHttpUrl
    index_prefix: str = "circulation-works"
    timeout: int = 20
    maxsize: int = 25

    class Config:
        env_prefix = "PALACE_SEARCH_"
