from __future__ import annotations

import datetime
import json

from celery import shared_task
from sqlalchemy import delete, select
from sqlalchemy.orm import Session, sessionmaker

from core.celery.job import Job
from core.celery.task import Task
from core.external_search import ExternalSearchIndex
from core.model import CustomList, CustomListEntry
from core.query.customlist import CustomListQueries
from core.service.celery.celery import QueueNames
from core.util.cache import CachedData


class AutoUpdateCustomListJob(Job):
    """Traverse all entries and update lists if they have auto_update_enabled"""

    def __init__(
        self,
        session_maker: sessionmaker[Session],
        search: ExternalSearchIndex,
        custom_list_id: int | None,
    ):
        super().__init__(session_maker)
        self.search = search
        self.custom_list_id = custom_list_id

    @staticmethod
    def custom_list(db: Session, custom_list_id: int | None) -> CustomList | None:
        if custom_list_id is None:
            return None
        return (
            db.execute(select(CustomList).where(CustomList.id == custom_list_id))
            .scalars()
            .one_or_none()
        )

    def _update_list_with_new_entries(
        self, db: Session, custom_list: CustomList
    ) -> None:
        """Run a search on a custom list, assuming we have auto_update_enabled with a valid query
        Only json type queries are supported right now, without any support for additional facets
        """

        start_page = 1
        json_query = None
        if custom_list.auto_update_status == CustomList.INIT:
            # We're in the init phase, we need to back-populate all titles
            # starting from page 2, since page 1 should be already populated
            start_page = 2
        elif custom_list.auto_update_status == CustomList.REPOPULATE:
            # During a repopulate phase we must empty the list
            # and start population from page 1
            db.execute(
                delete(CustomListEntry).where(CustomListEntry.list_id == custom_list.id)
            )
            custom_list.entries = []
        else:
            # Otherwise we are in an update type process, which means we only search for
            # "newer" books from the last time we updated the list
            try:
                if custom_list.auto_update_query:
                    json_query = json.loads(custom_list.auto_update_query)
                else:
                    return
            except json.JSONDecodeError as e:
                self.log.error(
                    f"Could not decode custom list({custom_list.id}) saved query: {e}"
                )
                return
            # Update availability time as a query part that allows us to filter for new licenses
            # Although the last_update should never be null, we're failsafing
            availability_time = (
                custom_list.auto_update_last_update or datetime.datetime.now()
            )
            query_part = json_query["query"]
            query_part = {
                "and": [
                    {
                        "key": "licensepools.availability_time",
                        "op": "gte",
                        "value": availability_time.timestamp(),
                    },
                    query_part,
                ]
            }
            # Update the query as such
            json_query["query"] = query_part

        CustomListQueries.populate_query_pages(
            db, self.search, custom_list, json_query=json_query, start_page=start_page
        )
        custom_list.auto_update_status = CustomList.UPDATED

    def run(self) -> None:
        with self.transaction() as db:
            CachedData.initialize(db)
            custom_list = self.custom_list(db, self.custom_list_id)
            if custom_list is None:
                self.log.error(
                    f"CustomList with id {self.custom_list_id} not found. Unable to update."
                )
                return
            if not custom_list.auto_update_enabled:
                return
            self.log.info(f"Auto updating list entries for: {custom_list.name}")
            self._update_list_with_new_entries(db, custom_list)


@shared_task(queue=QueueNames.default, bind=True)
def update_custom_list(task: Task, custom_list_id: int) -> None:
    AutoUpdateCustomListJob(
        task.session_maker, task.services.search.index(), custom_list_id=custom_list_id
    ).run()


@shared_task(queue=QueueNames.default, bind=True)
def update_custom_lists(task: Task) -> None:
    with task.session() as db:
        custom_lists = db.execute(
            select(CustomList.id).where(CustomList.auto_update_enabled == True)
        ).all()

    for custom_list in custom_lists:
        update_custom_list.delay(custom_list.id)
