from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from palace.manager.api.admin.controller.base import AdminPermissionsControllerMixin
from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)
from palace.manager.sqlalchemy.model.coverage import Timestamp


class TimestampsController(
    CirculationManagerController, AdminPermissionsControllerMixin
):
    """Returns a dict: each key is a type of service (script, monitor, or coverage provider);
    each value is a nested dict in which timestamps are organized by service name and then by collection ID.
    """

    def diagnostics(
        self,
    ) -> dict[str, dict[str, dict[str, list[dict[str, Any]]]]]:
        self.require_system_admin()
        timestamps = self._db.query(Timestamp).order_by(Timestamp.start)
        sorted_by_type = self._sort_by_type(timestamps)
        sorted_by_collection: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = {}
        for service_type, services in sorted_by_type.items():
            sorted_by_collection[service_type] = {}
            for service_name, service_timestamps in services.items():
                sorted_by_collection[service_type][service_name] = (
                    self._sort_by_collection(service_timestamps)
                )
        return sorted_by_collection

    def _sort_by_type(
        self, timestamps: Iterable[Timestamp]
    ) -> dict[str, dict[str, list[dict[str, Any]]]]:
        """Takes a list of Timestamp objects.  Returns a dict: each key is a type of service
        (script, monitor, or coverage provider); each value is a dict in which the keys are the names
        of services and the values are lists of timestamps."""

        result: dict[str, list[dict[str, Any]]] = {}
        for ts in timestamps:
            info = self._extract_info(ts)
            service_type = str(ts.service_type or "other")
            result.setdefault(service_type, []).append(info)

        sorted_result: dict[str, dict[str, list[dict[str, Any]]]] = {}
        for service_type, data in list(result.items()):
            sorted_result[service_type] = self._sort_by_service(data)

        return sorted_result

    def _sort_by_service(
        self, timestamps: list[dict[str, Any]]
    ) -> dict[str, list[dict[str, Any]]]:
        """Returns a dict: each key is the name of a service; each value is a list of timestamps."""

        result: dict[str, list[dict[str, Any]]] = {}
        for timestamp in timestamps:
            service_name = str(timestamp.get("service") or "unknown")
            result.setdefault(service_name, []).append(timestamp)
        return result

    def _sort_by_collection(
        self, timestamps: list[dict[str, Any]]
    ) -> dict[str, list[dict[str, Any]]]:
        """Takes a list of timestamps; turns it into a dict in which each key is a
        collection ID and each value is a list of the timestamps associated with that collection.
        """

        result: dict[str, list[dict[str, Any]]] = {}
        for timestamp in timestamps:
            collection_name = str(timestamp.get("collection_name") or "unknown")
            result.setdefault(collection_name, []).append(timestamp)
        return result

    def _extract_info(self, timestamp: Timestamp) -> dict[str, Any]:
        """Takes a Timestamp object and returns a dict"""

        duration = None
        if timestamp.start and timestamp.finish:
            duration = (timestamp.finish - timestamp.start).total_seconds()

        collection_name = "No associated collection"
        if timestamp.collection:
            collection_name = timestamp.collection.name

        return dict(
            id=timestamp.id,
            start=timestamp.start,
            duration=duration,
            exception=timestamp.exception,
            service=timestamp.service,
            collection_name=collection_name,
            achievements=timestamp.achievements,
        )
