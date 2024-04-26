from celery import Celery
from dependency_injector import providers
from dependency_injector.containers import DeclarativeContainer

from palace.manager.service.celery.celery import celery_factory


class CeleryContainer(DeclarativeContainer):
    config = providers.Configuration()

    app: providers.Provider[Celery] = providers.Resource(celery_factory, config=config)
