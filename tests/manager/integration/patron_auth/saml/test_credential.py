import datetime
from unittest.mock import MagicMock

from freezegun import freeze_time

from palace.manager.integration.patron_auth.saml.credential import SAMLCredentialManager
from tests.fixtures.database import DatabaseTransactionFixture


class TestSAMLCredentialManager:
    @freeze_time("2024-01-01 12:00:00")
    def test_invalidate_saml_token(self, db: DatabaseTransactionFixture):
        """invalidate_saml_token should immediately expire the credential."""
        credential = MagicMock()

        manager = SAMLCredentialManager()
        manager.invalidate_saml_token(db.session, credential)

        assert credential.expires == datetime.datetime(
            2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC
        )
