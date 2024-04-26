from __future__ import annotations

from palace.manager.sqlalchemy.model.base import Base


class SessionBulkOperation:
    """Bulk insert/update/operate on a session"""

    def __init__(
        self,
        session,
        batch_size,
        bulk_method: str = "bulk_save_objects",
        bulk_method_kwargs=None,
    ) -> None:
        self.session = session
        self.bulk_method = bulk_method
        self.bulk_method_kwargs = bulk_method_kwargs or {}
        self.batch_size = batch_size
        self._objects: list[Base] = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._bulk_operation()

    def add(self, object):
        self._objects.append(object)
        if len(self._objects) == self.batch_size:
            self._bulk_operation()

    def _bulk_operation(self):
        self.bulk_method, getattr(
            self.session,
            self.bulk_method,
        )(self._objects, **self.bulk_method_kwargs)
        self.session.commit()
        self._objects = []
