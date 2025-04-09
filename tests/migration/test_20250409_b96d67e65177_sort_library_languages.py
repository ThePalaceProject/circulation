from pytest_alembic import MigrationContext

from tests.migration.conftest import AlembicDatabaseFixture


def test_sort_languages(
    alembic_runner: MigrationContext,
    alembic_database: AlembicDatabaseFixture,
) -> None:
    alembic_runner.migrate_down_to("b96d67e65177")
    alembic_runner.migrate_down_one()

    no_languages = alembic_database.library()
    large = alembic_database.library(
        settings={"large_collection_languages": ["spa", "jpn", "chi"]}
    )
    small = alembic_database.library(
        settings={"small_collection_languages": ["ger", "eng", "fre"]}
    )
    tiny = alembic_database.library(
        settings={"tiny_collection_languages": ["rus", "ita"]}
    )

    alembic_runner.migrate_up_one()

    # Check that the languages are sorted
    no_languages_settings_dict = alembic_database.fetch_library(
        no_languages
    ).settings_dict
    assert "large_collection_languages" not in no_languages_settings_dict
    assert "small_collection_languages" not in no_languages_settings_dict
    assert "tiny_collection_languages" not in no_languages_settings_dict

    assert alembic_database.fetch_library(large).settings_dict[
        "large_collection_languages"
    ] == ["chi", "jpn", "spa"]
    assert alembic_database.fetch_library(small).settings_dict[
        "small_collection_languages"
    ] == ["eng", "fre", "ger"]
    assert alembic_database.fetch_library(tiny).settings_dict[
        "tiny_collection_languages"
    ] == ["ita", "rus"]
