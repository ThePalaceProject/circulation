import datetime
from dataclasses import dataclass
from functools import partial

from sqlalchemy.orm import Session

from palace.manager.celery.tasks.apply import (
    ApplyBibliographicCallable,
    ApplyCirculationCallable,
)
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.integration.base import integration_settings_load
from palace.manager.integration.license.boundless.api import BoundlessApi
from palace.manager.integration.license.boundless.parser import BibliographicParser
from palace.manager.integration.license.boundless.requests import BoundlessRequests
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.util import get_one_or_create
from palace.manager.util import chunks
from palace.manager.util.datetime_helpers import datetime_utc
from palace.manager.util.log import LoggerMixin


@dataclass(frozen=True)
class FeedImportResult:
    complete: bool
    active_processed: int
    inactive_processed: int
    current_page: int
    total_pages: int
    next_page: int | None = None


class BoundlessImporter(LoggerMixin):
    DEFAULT_START_TIME = datetime_utc(1970, 1, 1)

    # Because of URL length limitations, we can only request availability for a certain number
    # of titles at a time. My testing shows that 125 works reliably. The default page size is
    # 500, so 125 seemed like a good value, so we would make 4 availability calls per page of titles.
    _AVAILABILITY_CALL_MAXIMUM_IDENTIFIERS = 125

    def __init__(
        self,
        db: Session,
        collection: Collection,
        registry: LicenseProvidersRegistry,
        import_all: bool = False,
        api_requests: BoundlessRequests | None = None,
    ) -> None:
        self._db = db
        self._collection = collection
        self._import_all = import_all

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

    @classmethod
    def get_timestamp(cls, db: Session, collection: Collection) -> Timestamp:
        timestamp, _ = get_one_or_create(
            db,
            Timestamp,
            service="Boundless Import",
            service_type=Timestamp.TASK_TYPE,
            collection=collection,
        )
        return timestamp

    def _mark_inactive_titles(
        self,
        inactive_title_ids: list[str],
        apply_circulation: ApplyCirculationCallable,
    ) -> None:
        create_circulation = partial(
            CirculationData,
            data_source_name=DataSource.BOUNDLESS,
            licenses_owned=0,
            licenses_available=0,
        )

        for title_id in inactive_title_ids:
            identifier = IdentifierData(
                type=Identifier.AXIS_360_ID,
                identifier=title_id,
            )
            circulation = create_circulation(primary_identifier_data=identifier)
            apply_circulation(circulation, collection_id=self._collection.id)

        if inactive_title_ids:
            self.log.info(f"Marked {len(inactive_title_ids)} titles as inactive.")

    def _import_active_titles(
        self,
        active_title_ids: list[str],
        apply_bibliographic: ApplyBibliographicCallable,
        apply_circulation: ApplyCirculationCallable,
    ) -> None:
        policy = ReplacementPolicy(
            identifiers=False,
            subjects=True,
            contributions=True,
            formats=True,
            links=True,
        )

        bibliographic_updated = 0
        circulation_updated = 0
        no_changes = 0

        for chunk in chunks(
            active_title_ids, self._AVAILABILITY_CALL_MAXIMUM_IDENTIFIERS
        ):
            availability_response = self._api_requests.availability(title_ids=chunk)
            for bibliographic, circulation in BibliographicParser.parse(
                availability_response
            ):
                if self._import_all or bibliographic.has_changed(self._db):
                    apply_bibliographic(
                        bibliographic, collection_id=self._collection.id, replace=policy
                    )
                    bibliographic_updated += 1
                elif circulation.has_changed(self._db, collection=self._collection):
                    apply_circulation(circulation, collection_id=self._collection.id)
                    circulation_updated += 1
                else:
                    no_changes += 1

        if active_title_ids:
            self.log.info(
                f"Processed {len(active_title_ids)} active titles: "
                f"{bibliographic_updated} bibliographic updates, "
                f"{circulation_updated} circulation updates, "
                f"{no_changes} unchanged."
            )

    def import_collection(
        self,
        *,
        apply_bibliographic: ApplyBibliographicCallable,
        apply_circulation: ApplyCirculationCallable,
        page: int,
        modified_since: datetime.datetime,
    ) -> FeedImportResult:
        """
        Import a single page of titles from the Boundless Title License API.

        This method fetches one page of titles that have been modified since the given
        datetime, processes active titles by fetching their detailed availability data
        and queueing bibliographic/circulation updates, and marks inactive titles as
        having zero licenses.

        :param apply_bibliographic: Callable to queue bibliographic data for processing.
            Called for each active title that has changed or when import_all is True.
        :param apply_circulation: Callable to queue circulation data for processing.
            Called for each active title whose circulation has changed and for all
            inactive titles to mark them as unavailable.
        :param page: The page number to fetch from the API (1-indexed).
        :param modified_since: Only fetch titles modified after this datetime.
        :return: FeedImportResult containing pagination info, counts of titles processed,
            and the next page number if more pages remain.
        """
        # The timeouts for the title_license call need to be set to a higher value
        # than the default because of performance issues on Boundless' end. In my testing
        # with large accounts, it seems like 60 seconds is enough to get the call to succeed.
        # We should be able to monitor this in cloudwatch and lower the timeout / remove it
        # entirely when Boundless fixes the performance issue.
        # TODO: Remove this timeout when boundless fixes the performance issue.
        title_response = self._api_requests.title_license(
            modified_since=modified_since, page=page, timeout=60
        )

        active_title_ids = [
            title.title_id for title in title_response.titles if title.active is True
        ]
        self._import_active_titles(
            active_title_ids, apply_bibliographic, apply_circulation
        )

        inactive_title_ids = [
            title.title_id for title in title_response.titles if title.active is False
        ]
        self._mark_inactive_titles(inactive_title_ids, apply_circulation)

        current_page = title_response.pagination.current_page
        total_pages = title_response.pagination.total_page
        complete = current_page == total_pages
        next_page = current_page + 1 if not complete else None

        return FeedImportResult(
            complete=complete,
            active_processed=len(active_title_ids),
            inactive_processed=len(inactive_title_ids),
            current_page=current_page,
            total_pages=total_pages,
            next_page=next_page,
        )
