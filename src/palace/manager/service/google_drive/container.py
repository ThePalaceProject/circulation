from __future__ import annotations

from dependency_injector import providers
from dependency_injector.containers import DeclarativeContainer
from dependency_injector.providers import Provider

from palace.manager.service.google_drive.google_drive import GoogleDriveService


class GoogleDrive(DeclarativeContainer):
    config = providers.Configuration()

    palace_internal: Provider[GoogleDriveService] = providers.Singleton(
        GoogleDriveService.factory,
        service_account_key_file_path=config.service_account_key_file_path,
    )
