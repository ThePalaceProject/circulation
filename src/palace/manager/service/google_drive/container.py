from __future__ import annotations

from dependency_injector import providers
from dependency_injector.containers import DeclarativeContainer
from dependency_injector.providers import Provider
from google.oauth2 import service_account
from googleapiclient.discovery import build

from palace.manager.service.google_drive.google_drive import GoogleDriveService


class GoogleDrive(DeclarativeContainer):
    config = providers.Configuration()

    credentials = providers.Singleton(
        service_account.Credentials.from_service_account_info,
        info=config.service_account_info_json,
        scopes=["https://www.googleapis.com/auth/drive"],
    )

    api_client = providers.Singleton(
        build,
        serviceName="drive",
        version="v3",
        credentials=credentials,
    )

    service: Provider[GoogleDriveService] = providers.Singleton(
        GoogleDriveService,
        api_client=api_client,
    )
