import datetime

import pytest
from sqlalchemy.exc import IntegrityError

from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.util import create, get_one_or_create
from palace.manager.util.datetime_helpers import strptime_utc, utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class TestCirculationEvent:
    @staticmethod
    def _event_data(**kwargs):
        for k, default in (
            ("source", DataSource.OVERDRIVE),
            ("id_type", Identifier.OVERDRIVE_ID),
            ("start", utc_now()),
            ("type", CirculationEvent.DISTRIBUTOR_LICENSE_ADD),
        ):
            kwargs.setdefault(k, default)
        if "old_value" in kwargs and "new_value" in kwargs:
            kwargs["delta"] = kwargs["new_value"] - kwargs["old_value"]
        return kwargs

    @staticmethod
    def _get_datetime(data, key):
        date = data.get(key, None)
        if not date:
            return None
        elif isinstance(date, datetime.date):
            return date
        else:
            return strptime_utc(date, CirculationEvent.TIME_FORMAT)

    @staticmethod
    def _get_int(data, key):
        value = data.get(key, None)
        if not value:
            return value
        else:
            return int(value)

    def from_dict(self, data, db: DatabaseTransactionFixture):
        # Identify the source of the event.
        source_name = data["source"]
        source = DataSource.lookup(db.session, source_name)

        # Identify which LicensePool the event is talking about.
        foreign_id = data["id"]
        identifier_type = source.primary_identifier_type
        collection = data["collection"]

        license_pool, was_new = LicensePool.for_foreign_id(
            db.session, source, identifier_type, foreign_id, collection=collection
        )

        # Finally, gather some information about the event itself.
        type = data.get("type")
        start = self._get_datetime(data, "start")
        end = self._get_datetime(data, "end")
        old_value = self._get_int(data, "old_value")
        new_value = self._get_int(data, "new_value")
        delta = self._get_int(data, "delta")
        event, was_new = get_one_or_create(
            db.session,
            CirculationEvent,
            license_pool=license_pool,
            type=type,
            start=start,
            create_method_kwargs=dict(
                old_value=old_value, new_value=new_value, delta=delta, end=end
            ),
        )
        return event, was_new

    def test_new_title(self, db: DatabaseTransactionFixture):
        # Here's a new title.
        collection = db.collection()
        data = self._event_data(
            source=DataSource.OVERDRIVE,
            id="{1-2-3}",
            type=CirculationEvent.DISTRIBUTOR_LICENSE_ADD,
            collection=collection,
            old_value=0,
            delta=2,
            new_value=2,
        )

        # Turn it into an event and see what happens.
        event, ignore = self.from_dict(data, db)

        # The event is associated with the correct data source.
        assert DataSource.OVERDRIVE == event.license_pool.data_source.name

        # The event identifies a work by its ID plus the data source's
        # primary identifier and its collection.
        assert Identifier.OVERDRIVE_ID == event.license_pool.identifier.type
        assert "{1-2-3}" == event.license_pool.identifier.identifier
        assert collection == event.license_pool.collection

        # The number of licenses has not been set to the new value.
        # The creator of a circulation event is responsible for also
        # updating the dataset.
        assert 0 == event.license_pool.licenses_owned

    def test_uniqueness_constraints_no_library(self, db: DatabaseTransactionFixture):
        # If library is null, then license_pool + type + start must be
        # unique.
        pool = db.licensepool(edition=None)
        now = utc_now()
        kwargs = dict(
            license_pool=pool,
            type=CirculationEvent.DISTRIBUTOR_TITLE_ADD,
        )
        session = db.session
        event = create(session, CirculationEvent, start=now, **kwargs)

        # Different timestamp -- no problem.
        now2 = utc_now()
        event2 = create(session, CirculationEvent, start=now2, **kwargs)
        assert event != event2

        # Reuse the timestamp and you get an IntegrityError which ruins the
        # entire transaction.
        pytest.raises(
            IntegrityError, create, session, CirculationEvent, start=now, **kwargs
        )
        session.rollback()

    def test_uniqueness_constraints_with_library(self, db: DatabaseTransactionFixture):
        # If library is provided, then license_pool + library + type +
        # start must be unique.
        pool = db.licensepool(edition=None)
        now = utc_now()
        kwargs = dict(
            license_pool=pool,
            library=db.default_library(),
            type=CirculationEvent.DISTRIBUTOR_TITLE_ADD,
        )
        event = create(db.session, CirculationEvent, start=now, **kwargs)

        # Different timestamp -- no problem.
        now2 = utc_now()
        event2 = create(db.session, CirculationEvent, start=now2, **kwargs)
        assert event != event2

        # Reuse the timestamp and you get an IntegrityError which ruins the
        # entire transaction.
        pytest.raises(
            IntegrityError, create, db.session, CirculationEvent, start=now, **kwargs
        )
        db.session.rollback()
