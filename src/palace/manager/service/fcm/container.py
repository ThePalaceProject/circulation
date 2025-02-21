from __future__ import annotations

from dependency_injector import providers
from dependency_injector.containers import DeclarativeContainer
from firebase_admin import App, initialize_app
from firebase_admin.credentials import Certificate

from palace.manager.service.fcm.fcm import (
    SendNotificationsCallable,
    credentials,
    send_notifications,
)


class FcmContainer(DeclarativeContainer):
    config = providers.Configuration()

    credentials: providers.Provider[dict[str, str]] = providers.Singleton(
        credentials,
        config_json=config.credentials_json,
        config_file=config.credentials_file,
    )

    certificate: providers.Provider[Certificate] = providers.Singleton(
        Certificate, credentials
    )

    app: providers.Provider[App] = providers.Singleton(
        initialize_app, credential=certificate
    )

    send_notifications: SendNotificationsCallable = providers.Callable(
        send_notifications,
        app=app,
    )
