from typing import Any

from palace.util.exceptions import PalaceValueError

from palace.manager.celery.tasks.custom_lists import update_custom_list_entries_sweep
from palace.manager.scripts.base import Script
from palace.manager.sqlalchemy.model.customlist import CustomList
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.util import get_one_or_create


class CustomListEntriesSweepScript(Script):
    """Manually kick off the full custom-list maintenance pipeline.

    Enqueues ``update_custom_list_entries_sweep``, which fans out entry
    updates for every auto-updating custom list and then recalculates lane
    sizes once all updates are complete.

    This is equivalent to waiting for the hourly beat-schedule run but fires
    immediately.  The script returns as soon as the task is queued; actual
    execution happens asynchronously on the Celery workers.
    """

    def do_run(self, *args: Any, **kwargs: Any) -> None:
        update_custom_list_entries_sweep.delay()
        self.log.info(
            'The "update_custom_list_entries_sweep" task has been queued for '
            "execution. See the Celery logs for details about task execution."
        )


class CustomListManagementScript(Script):
    """Maintain a CustomList whose membership is determined by a
    MembershipManager.
    """

    def __init__(
        self,
        manager_class: type[Any],
        data_source_name: str,
        list_identifier: str,
        list_name: str,
        primary_language: str | None,
        description: str | None,
        **manager_kwargs: Any,
    ) -> None:
        data_source = DataSource.lookup(self._db, data_source_name)
        if data_source is None:
            raise PalaceValueError(f"Unknown data source: {data_source_name}")
        self.custom_list, is_new = get_one_or_create(
            self._db,
            CustomList,
            data_source_id=data_source.id,
            foreign_identifier=list_identifier,
        )
        self.custom_list.primary_language = primary_language
        self.custom_list.description = description
        self.membership_manager = manager_class(self.custom_list, **manager_kwargs)

    def run(self) -> None:
        self.membership_manager.update()
        self._db.commit()
