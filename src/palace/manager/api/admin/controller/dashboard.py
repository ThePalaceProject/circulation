from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timedelta

import flask
from sqlalchemy import desc, nullslast
from sqlalchemy.orm import Session

from palace.manager.api.admin.model.dashboard_statistics import StatisticsResponse
from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)
from palace.manager.api.local_analytics_exporter import LocalAnalyticsExporter
from palace.manager.api.util.flask import get_request_library
from palace.manager.feed.annotator.admin import AdminAnnotator
from palace.manager.sqlalchemy.model.admin import Admin
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work


class DashboardController(CirculationManagerController):
    def stats(
        self, stats_function: Callable[[Admin, Session], StatisticsResponse]
    ) -> StatisticsResponse:
        admin: Admin = getattr(flask.request, "admin")
        return stats_function(admin, self._db)

    def circulation_events(self):
        annotator = AdminAnnotator(self.circulation, get_request_library())
        num = min(int(flask.request.args.get("num", "100")), 500)

        results = (
            self._db.query(CirculationEvent)
            .join(LicensePool)
            .join(Work)
            .join(DataSource)
            .join(Identifier)
            .order_by(nullslast(desc(CirculationEvent.start)))
            .limit(num)
            .all()
        )

        events = [
            {
                "id": result.id,
                "type": result.type,
                "time": result.start,
                "book": {
                    "title": result.license_pool.work.title,
                    "url": annotator.permalink_for(
                        result.license_pool.identifier,
                    ),
                },
            }
            for result in results
        ]

        return dict({"circulation_events": events})

    def bulk_circulation_events(self, analytics_exporter=None):
        date_format = "%Y-%m-%d"

        def get_date(field):
            # Return a date or datetime object representing the
            # _beginning_ of the asked-for day, local time.
            #
            # Unlike most places in this application we do not
            # use UTC since the time was selected by a human user.
            today = date.today()
            value = flask.request.args.get(field, None)
            if not value:
                return today
            try:
                return datetime.strptime(value, date_format).date()
            except ValueError as e:
                # This won't happen in real life since the format is
                # controlled by the calendar widget. There's no need
                # to send an error message -- just use the default
                # date.
                return today

        # For the start date we should use the _beginning_ of the day,
        # which is what get_date returns.
        date_start = get_date("date")

        # When running the search, the cutoff is the first moment of
        # the day _after_ the end date. When generating the filename,
        # though, we should use the date provided by the user.
        date_end_label = get_date("dateEnd")
        date_end = date_end_label + timedelta(days=1)
        locations = flask.request.args.get("locations", None)
        library = get_request_library(default=None)
        library_short_name = library.short_name if library else None

        analytics_exporter = analytics_exporter or LocalAnalyticsExporter()
        data = analytics_exporter.export(
            self._db, date_start, date_end, locations, library
        )
        return (
            data,
            date_start.strftime(date_format),
            date_end_label.strftime(date_format),
            library_short_name,
        )
