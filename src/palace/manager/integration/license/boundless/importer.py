import datetime
from collections.abc import Generator

from sqlalchemy.orm import Session

from palace.manager.celery.tasks.apply import (
    ApplyBibliographicCallable,
)
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.integration.base import integration_settings_load
from palace.manager.integration.license.boundless.api import BoundlessApi
from palace.manager.integration.license.boundless.parser import BibliographicParser
from palace.manager.integration.license.boundless.requests import BoundlessRequests
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.util import get_one_or_create
from palace.manager.util.datetime_helpers import datetime_utc
from palace.manager.util.http.exception import BadResponseException
from palace.manager.util.log import LoggerMixin


class BoundlessImporter(LoggerMixin):
    _DEFAULT_START_TIME = datetime_utc(1970, 1, 1)

    def __init__(
        self,
        db: Session,
        collection: Collection,
        registry: LicenseProvidersRegistry,
        import_all: bool = False,
        identifier_set: IdentifierSet | None = None,
        api_requests: BoundlessRequests | None = None,
    ) -> None:
        self._db = db
        self._collection = collection
        self._import_all = import_all
        self._identifier_set = identifier_set

        if not registry.equivalent(collection.protocol, BoundlessApi):
            raise PalaceValueError(
                f"Collection {collection.name} [id={collection.id} protocol={collection.protocol}] "
                f"is not a Boundless collection."
            )

        self._settings = integration_settings_load(
            BoundlessApi.settings_class(), self._collection.integration_configuration
        )
        self._api_requests = (
            BoundlessRequests(self._settings) if api_requests is None else api_requests
        )

    def _get_timestamp(self) -> Timestamp:
        timestamp, _ = get_one_or_create(
            self._db,
            Timestamp,
            service="Boundless Import",
            service_type=Timestamp.TASK_TYPE,
            collection=self._collection,
        )
        return timestamp

    def _get_start_time(self, timestamp: Timestamp) -> datetime.datetime:
        """Determine the start time for fetching new data."""
        if (
            self._import_all
            or self._identifier_set is not None
            or timestamp.start is None
        ):
            return self._DEFAULT_START_TIME
        return timestamp.start

    def _recent_activity(
        self, since: datetime.datetime
    ) -> Generator[tuple[BibliographicData, CirculationData]]:
        """Find books that have had recent activity.

        :yield: A sequence of (BibliographicData, CirculationData) 2-tuples
        """
        # If we are fetching all activity, it can take a very long time, since the Boundless API
        # provides no way to page through results. We don't want to hang in the case the server
        # never gives us a response though, so we set the timeout to 10 (!) minutes, so that we do
        # eventually give up and let the task be retried. I chose 10 minutes based on querying our
        # logs to get an idea the maximum time we've seen for a full feed response in the past.
        availability_response = self._api_requests.availability(
            since=since, timeout=10 * 60
        )
        yield from BibliographicParser.parse(availability_response)

    def _check_api_credentials(self) -> bool:
        # Try to get a bearer token, to make sure the collection is configured correctly.
        try:
            self._api_requests.refresh_bearer_token()
            return True
        except BadResponseException as e:
            if e.response.status_code == 401:
                self.log.error(
                    f"Failed to authenticate with Boundless API for collection {self._collection.name} "
                    f"(id={self._collection.id}). Please check the collection configuration."
                )
                return False
            raise

    def import_collection(
        self,
        *,
        apply_bibliographic: ApplyBibliographicCallable,
    ) -> IdentifierSet | None:
        if not self._check_api_credentials():
            return None

        timestamp = self._get_timestamp()
        start_time = self._get_start_time(timestamp)

        identifiers = []
        policy = ReplacementPolicy(
            identifiers=False,
            subjects=True,
            contributions=True,
            formats=True,
            links=True,
        )

        self.log.info(
            f"Starting process of queuing items in collection {self._collection.name} (id={self._collection.id} "
            f"for import that have changed since {start_time}. "
        )

        with timestamp.recording():
            for bibliographic, circulation in self._recent_activity(start_time):
                if bibliographic.primary_identifier_data is not None:
                    identifiers.append(bibliographic.primary_identifier_data)

                if self._import_all or bibliographic.has_changed(self._db):
                    apply_bibliographic(
                        bibliographic, collection_id=self._collection.id, replace=policy
                    )

        achievements = [f"Total items queued for import:  {len(identifiers)}."]
        if (elapsed_time := timestamp.elapsed_seconds) is not None:
            achievements.append(f"Elapsed time: {elapsed_time:.2f} seconds.")

        if self._identifier_set is not None:
            self._identifier_set.add(*identifiers)

        timestamp.achievements = "\n".join(achievements)

        self.log.info(
            f"Finished import for collection {self._collection.name} (id={self._collection.id}. "
            f"{' '.join(achievements)}"
        )

        return self._identifier_set
