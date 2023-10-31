from dependency_injector import providers
from dependency_injector.containers import DeclarativeContainer

from core.analytics import Analytics


class AnalyticsContainer(DeclarativeContainer):
    config = providers.Configuration()
    storage = providers.DependenciesContainer()

    analytics = providers.Singleton(
        Analytics,
        config=config,
        storage_client=storage.analytics,
    )
