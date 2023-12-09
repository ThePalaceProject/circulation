from __future__ import annotations

from api.admin.controller.base import AdminPermissionsControllerMixin
from api.controller.circulation_manager import CirculationManagerController
from core.model import Timestamp


class TimestampsController(
    CirculationManagerController, AdminPermissionsControllerMixin
):
    """Returns a dict: each key is a type of service (script, monitor, or coverage provider);
    each value is a nested dict in which timestamps are organized by service name and then by collection ID.
    """

    def diagnostics(self):
        self.require_system_admin()
        timestamps = self._db.query(Timestamp).order_by(Timestamp.start)
        sorted = self._sort_by_type(timestamps)
        for type, services in list(sorted.items()):
            for service in services:
                by_collection = self._sort_by_collection(sorted[type][service])
                sorted[type][service] = by_collection
        return sorted

    def _sort_by_type(self, timestamps):
        """Takes a list of Timestamp objects.  Returns a dict: each key is a type of service
        (script, monitor, or coverage provider); each value is a dict in which the keys are the names
        of services and the values are lists of timestamps."""

        result = {}
        for ts in timestamps:
            info = self._extract_info(ts)
            result.setdefault((ts.service_type or "other"), []).append(info)

        for type, data in list(result.items()):
            result[type] = self._sort_by_service(data)

        return result

    def _sort_by_service(self, timestamps):
        """Returns a dict: each key is the name of a service; each value is a list of timestamps."""

        result = {}
        for timestamp in timestamps:
            result.setdefault(timestamp.get("service"), []).append(timestamp)
        return result

    def _sort_by_collection(self, timestamps):
        """Takes a list of timestamps; turns it into a dict in which each key is a
        collection ID and each value is a list of the timestamps associated with that collection.
        """

        result = {}
        for timestamp in timestamps:
            result.setdefault(timestamp.get("collection_name"), []).append(timestamp)
        return result

    def _extract_info(self, timestamp):
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
