from __future__ import annotations

from io import StringIO

from palace.manager.scripts.local_analytics import LocalAnalyticsExportScript
from tests.fixtures.database import DatabaseTransactionFixture


class TestLocalAnalyticsExportScript:
    def test_do_run(self, db: DatabaseTransactionFixture):
        class MockLocalAnalyticsExporter:
            def export(self, _db, start, end):
                self.called_with = [start, end]
                return "test"

        output = StringIO()
        cmd_args = ["--start=20190820", "--end=20190827"]
        exporter = MockLocalAnalyticsExporter()
        script = LocalAnalyticsExportScript(_db=db.session)
        script.do_run(output=output, cmd_args=cmd_args, exporter=exporter)
        assert "test" == output.getvalue()
        assert ["20190820", "20190827"] == exporter.called_with
