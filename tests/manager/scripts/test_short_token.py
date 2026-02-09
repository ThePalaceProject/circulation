from __future__ import annotations

from io import StringIO

import pytest

from palace.manager.api.adobe_vendor_id import AuthdataUtility
from palace.manager.scripts.short_token import GenerateShortTokenScript
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.util.datetime_helpers import datetime_utc
from tests.fixtures.database import DatabaseTransactionFixture


class TestGenerateShortTokenScript:
    @pytest.fixture
    def script(self, db: DatabaseTransactionFixture):
        return GenerateShortTokenScript(_db=db.session)

    @pytest.fixture
    def output(self):
        return StringIO()

    @pytest.fixture
    def authdata(self, monkeypatch):
        authdata = AuthdataUtility(
            vendor_id="The Vendor ID",
            library_uri="http://your-library.org/",
            library_short_name="you",
            secret="Your library secret - padded!!!!",
        )
        test_date = datetime_utc(2021, 5, 5)
        monkeypatch.setattr(authdata, "_now", lambda: test_date)
        return authdata

    @pytest.fixture
    def patron(self, authdata, db: DatabaseTransactionFixture):
        patron = db.patron(external_identifier="test")
        patron.authorization_identifier = "test"
        adobe_credential = db.credential(
            data_source_name=DataSource.INTERNAL_PROCESSING,
            patron=patron,
            type=authdata.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
        )
        adobe_credential.credential = "1234567"
        return patron

    @pytest.fixture
    def authentication_provider(
        self,
        db: DatabaseTransactionFixture,
    ):
        barcode = "12345"
        pin = "abcd"
        db.simple_auth_integration(db.default_library(), barcode, pin)
        return barcode, pin

    def test_run_days(
        self, script, output, authdata, patron, db: DatabaseTransactionFixture
    ):
        # Test with --days
        cmd_args = [
            f"--barcode={patron.authorization_identifier}",
            "--days=2",
            db.default_library().short_name,
        ]
        script.do_run(
            _db=db.session, output=output, cmd_args=cmd_args, authdata=authdata
        )
        assert output.getvalue().split("\n") == [
            "Vendor ID: The Vendor ID",
            "Token: YOU|1620345600|1234567|MTEUZr1ZxyF86A3DV28NzlTTTjYBDK8gbPled9EQrQY@",
            "Username: YOU|1620345600|1234567",
            "Password: MTEUZr1ZxyF86A3DV28NzlTTTjYBDK8gbPled9EQrQY@",
            "",
        ]

    def test_run_minutes(
        self, script, output, authdata, patron, db: DatabaseTransactionFixture
    ):
        # Test with --minutes
        cmd_args = [
            f"--barcode={patron.authorization_identifier}",
            "--minutes=20",
            db.default_library().short_name,
        ]
        script.do_run(
            _db=db.session, output=output, cmd_args=cmd_args, authdata=authdata
        )
        assert output.getvalue().split("\n")[2] == "Username: YOU|1620174000|1234567"

    def test_run_hours(
        self, script, output, authdata, patron, db: DatabaseTransactionFixture
    ):
        # Test with --hours
        cmd_args = [
            f"--barcode={patron.authorization_identifier}",
            "--hours=4",
            db.default_library().short_name,
        ]
        script.do_run(
            _db=db.session, output=output, cmd_args=cmd_args, authdata=authdata
        )
        assert output.getvalue().split("\n")[2] == "Username: YOU|1620187200|1234567"

    def test_no_registry(self, script, output, patron, db: DatabaseTransactionFixture):
        cmd_args = [
            f"--barcode={patron.authorization_identifier}",
            "--minutes=20",
            db.default_library().short_name,
        ]
        with pytest.raises(SystemExit) as pytest_exit:
            script.do_run(_db=db.session, output=output, cmd_args=cmd_args)
        assert pytest_exit.value.code == -1
        assert "Library not registered with library registry" in output.getvalue()

    def test_no_patron_auth_method(
        self, script, output, db: DatabaseTransactionFixture
    ):
        # Test running when the patron does not exist
        cmd_args = [
            "--barcode={}".format("1234567"),
            "--hours=4",
            db.default_library().short_name,
        ]
        with pytest.raises(SystemExit) as pytest_exit:
            script.do_run(_db=db.session, output=output, cmd_args=cmd_args)
        assert pytest_exit.value.code == -1
        assert "No methods to authenticate patron found" in output.getvalue()

    def test_patron_auth(
        self,
        script,
        output,
        authdata,
        authentication_provider,
        db: DatabaseTransactionFixture,
    ):
        barcode, pin = authentication_provider
        # Test running when the patron does not exist
        cmd_args = [
            f"--barcode={barcode}",
            f"--pin={pin}",
            "--hours=4",
            db.default_library().short_name,
        ]
        script.do_run(
            _db=db.session, output=output, cmd_args=cmd_args, authdata=authdata
        )
        assert "Token: YOU|1620187200" in output.getvalue()

    def test_patron_auth_no_patron(
        self,
        script,
        output,
        authdata,
        authentication_provider,
        db: DatabaseTransactionFixture,
    ):
        barcode = "nonexistent"
        # Test running when the patron does not exist
        cmd_args = [
            f"--barcode={barcode}",
            "--hours=4",
            db.default_library().short_name,
        ]
        with pytest.raises(SystemExit) as pytest_exit:
            script.do_run(
                _db=db.session, output=output, cmd_args=cmd_args, authdata=authdata
            )
        assert pytest_exit.value.code == -1
        assert "Patron not found" in output.getvalue()
