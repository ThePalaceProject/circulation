from dependency_injector import providers
from dependency_injector.containers import DeclarativeContainer
from dependency_injector.providers import Provider
from opensearchpy import OpenSearch

from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.search.revision_directory import SearchRevisionDirectory
from palace.manager.search.service import SearchServiceOpensearch1


class Search(DeclarativeContainer):
    config = providers.Configuration()

    # Used for indexing and admin operations, which legitimately run longer and
    # keep the full client-wide timeout. No timeout retries here, so indexing
    # and admin behavior is unchanged.
    write_client: Provider[OpenSearch] = providers.Singleton(
        OpenSearch,
        hosts=config.url,
        timeout=config.write_timeout,
        maxsize=config.maxsize,
    )

    # Used for the user-facing read path. A bounded ``read_timeout`` keeps a
    # stalled read from holding a web worker for the full ``write_timeout``.
    # Timeout retries default off (see config) since they cannot fail over when
    # the host is a single endpoint; operators can enable them via config.
    read_client: Provider[OpenSearch] = providers.Singleton(
        OpenSearch,
        hosts=config.url,
        timeout=config.read_timeout,
        maxsize=config.maxsize,
        max_retries=config.read_max_retries,
        retry_on_timeout=config.read_retry_on_timeout,
    )

    service: Provider[SearchServiceOpensearch1] = providers.Singleton(
        SearchServiceOpensearch1,
        write_client=write_client,
        read_client=read_client,
        base_revision_name=config.index_prefix,
    )

    revision_directory: Provider[SearchRevisionDirectory] = providers.Singleton(
        SearchRevisionDirectory.create,
    )

    index: Provider[ExternalSearchIndex] = providers.Singleton(
        ExternalSearchIndex,
        service=service,
    )
