from dependency_injector import providers
from dependency_injector.containers import DeclarativeContainer
from dependency_injector.providers import Provider
from opensearchpy import OpenSearch

from core.external_search import ExternalSearchIndex
from core.search.revision_directory import SearchRevisionDirectory
from core.search.service import SearchServiceOpensearch1


class Search(DeclarativeContainer):
    config = providers.Configuration()

    client: Provider[OpenSearch] = providers.Singleton(
        OpenSearch,
        hosts=config.url,
        timeout=config.timeout,
        maxsize=config.maxsize,
    )

    service: Provider[SearchServiceOpensearch1] = providers.Singleton(
        SearchServiceOpensearch1,
        client=client,
        base_revision_name=config.index_prefix,
    )

    revision_directory: Provider[SearchRevisionDirectory] = providers.Singleton(
        SearchRevisionDirectory.create,
    )

    index: Provider[ExternalSearchIndex] = providers.Singleton(
        ExternalSearchIndex,
        service=service,
        revision_directory=revision_directory,
    )
