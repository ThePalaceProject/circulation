from collections.abc import Callable
from functools import partial

import pytest

from palace.manager.integration.configuration.library import LibrarySettings
from palace.manager.util.problem_detail import ProblemDetailException

LibrarySettingsFixture = Callable[..., LibrarySettings]


@pytest.fixture
def library_settings() -> LibrarySettingsFixture:
    # Provide a default library settings object for tests, it just gives
    # default values for required fields, so we can construct the settings
    # without worrying about the defaults.
    return partial(
        LibrarySettings,
        website="http://library.com",
        help_web="http://library.com/help",
    )


@pytest.mark.parametrize(
    "languages,expected",
    [
        (None, None),
        ([], []),
        (["English"], ["eng"]),
        (["English", "eng", "fr", "fre", "french"], ["eng", "fre"]),
    ],
)
def test_validate_language_codes(
    languages: list[str] | None,
    expected: list[str] | None,
    library_settings: LibrarySettingsFixture,
) -> None:
    settings = library_settings(large_collection_languages=languages)
    assert settings.large_collection_languages == expected

    settings = library_settings(small_collection_languages=languages)
    assert settings.small_collection_languages == expected

    settings = library_settings(tiny_collection_languages=languages)
    assert settings.tiny_collection_languages == expected


def test_validate_language_codes_error(
    library_settings: LibrarySettingsFixture,
) -> None:
    with pytest.raises(ProblemDetailException) as excinfo:
        library_settings(large_collection_languages=["eng", "xyz"])

    assert excinfo.value.problem_detail.detail is not None
    assert '"xyz" is not a valid language code' in excinfo.value.problem_detail.detail


def test_serialize_language(library_settings: LibrarySettingsFixture) -> None:
    settings = library_settings(
        large_collection_languages=["fre", "eng"],
        small_collection_languages=["ja", "chi"],
        tiny_collection_languages=["eng", "chi"],
    )

    # When serialized, they are normalized to alpha-3 codes and sorted
    serialized = settings.model_dump()
    assert serialized["large_collection_languages"] == ["eng", "fre"]
    assert serialized["small_collection_languages"] == ["chi", "jpn"]
    assert serialized["tiny_collection_languages"] == ["chi", "eng"]
