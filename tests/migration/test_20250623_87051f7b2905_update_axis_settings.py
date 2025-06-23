import pytest
from pytest_alembic import MigrationContext

from palace.manager.api.axis.constants import API_BASE_URLS, ServerNickname
from palace.manager.api.circulation import BaseCirculationApiSettings
from tests.migration.conftest import AlembicDatabaseFixture


# These settings classes are copied into the test file instead of imported
# because we want to freeze them at the particular version that was in the code
# when the migration was written. When this is deleted, these classes will just
# go away with the test file.
class _NewAxis360Settings(BaseCirculationApiSettings):
    username: str
    password: str
    external_account_id: str
    server_nickname: ServerNickname = ServerNickname.production
    verify_certificate: bool = True


class _OldAxis360Settings(BaseCirculationApiSettings):
    username: str
    password: str
    external_account_id: str
    url: str = "https://axis360api.baker-taylor.com/Services/VendorAPI/"
    verify_certificate: bool | None = True


@pytest.mark.parametrize(
    "nickname",
    [ServerNickname.production, ServerNickname.qa],
)
def test_update_axis_settings(
    alembic_runner: MigrationContext,
    alembic_database: AlembicDatabaseFixture,
    nickname: ServerNickname,
) -> None:
    alembic_runner.migrate_down_to("87051f7b2905")

    integration_id = alembic_database.integration(
        protocol="Axis 360",
        goal="LICENSE_GOAL",
        settings=_NewAxis360Settings(
            username="test_user",
            password="test_pass",
            external_account_id="test_account",
            server_nickname=nickname,
        ),
    )

    alembic_runner.migrate_down_one()
    settings_dict = alembic_database.fetch_integration(integration_id).settings
    assert "server_nickname" not in settings_dict
    old_settings = _OldAxis360Settings.model_validate(settings_dict)
    assert old_settings.url == API_BASE_URLS[nickname]
    assert old_settings.username == "test_user"
    assert old_settings.password == "test_pass"
    assert old_settings.external_account_id == "test_account"

    alembic_runner.migrate_up_one()
    settings_dict = alembic_database.fetch_integration(integration_id).settings
    assert "url" not in settings_dict
    new_settings = _NewAxis360Settings.model_validate(settings_dict)
    assert new_settings.username == "test_user"
    assert new_settings.password == "test_pass"
    assert new_settings.external_account_id == "test_account"
    assert new_settings.server_nickname == nickname


def test_update_axis_settings_other_url(
    alembic_runner: MigrationContext,
    alembic_database: AlembicDatabaseFixture,
) -> None:
    alembic_runner.migrate_down_to("87051f7b2905")
    alembic_runner.migrate_down_one()

    integration_id = alembic_database.integration(
        protocol="Axis 360",
        goal="LICENSE_GOAL",
        settings=_OldAxis360Settings(
            username="test_user",
            password="test_pass",
            external_account_id="test_account",
            url="https://axis360apiqa.baker-taylor.com/Services/VendorAPI.svc",
        ),
    )

    alembic_runner.migrate_up_one()
    settings_dict = alembic_database.fetch_integration(integration_id).settings
    assert "url" not in settings_dict
    settings = _NewAxis360Settings.model_validate(settings_dict)
    assert settings.username == "test_user"
    assert settings.password == "test_pass"
    assert settings.external_account_id == "test_account"
    assert settings.server_nickname == ServerNickname.qa


def test_update_axis_settings_error(
    alembic_runner: MigrationContext,
    alembic_database: AlembicDatabaseFixture,
) -> None:
    alembic_runner.migrate_down_to("87051f7b2905")
    alembic_runner.migrate_down_one()

    alembic_database.integration(
        protocol="Axis 360",
        goal="LICENSE_GOAL",
        settings=_OldAxis360Settings(
            username="test_user",
            password="test_pass",
            external_account_id="test_account",
            url="https://example.com/whatever",
        ),
    )

    with pytest.raises(
        ValueError, match="unexpected URL: https://example.com/whatever"
    ):
        alembic_runner.migrate_up_one()
