from __future__ import annotations

from palace.manager.api.adobe_vendor_id import AuthdataUtility
from palace.manager.scripts.adobe import AdobeAccountIDResetScript
from palace.manager.sqlalchemy.model.credential import Credential
from palace.manager.sqlalchemy.model.datasource import DataSource
from tests.fixtures.database import DatabaseTransactionFixture


class TestAdobeAccountIDResetScript:
    def test_process_patron(self, db: DatabaseTransactionFixture):
        patron = db.patron()

        # This patron has a credential that links them to a Adobe account ID
        def set_value(credential):
            credential.value = "a credential"

        # Data source doesn't matter -- even if it's incorrect, a Credential
        # of the appropriate type will be deleted.
        data_source = DataSource.lookup(db.session, DataSource.OVERDRIVE)

        # Create one Credential that will be deleted and one that will be
        # left alone.
        for type in (
            AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
            "Some other type",
        ):
            credential = Credential.lookup(
                db.session, data_source, type, patron, set_value, True
            )

        assert 2 == len(patron.credentials)

        # Run the patron through the script.
        script = AdobeAccountIDResetScript(db.session)

        # A dry run does nothing.
        script.delete = False
        script.process_patron(patron)
        db.session.commit()
        assert 2 == len(patron.credentials)

        # Now try it for real.
        script.delete = True
        script.process_patron(patron)
        db.session.commit()

        # The Adobe-related credential is gone. The other one remains.
        [credential] = patron.credentials
        assert "Some other type" == credential.type
