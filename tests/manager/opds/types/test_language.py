import json

import pytest
from pydantic import TypeAdapter, ValidationError

from palace.manager.opds.types.language import LanguageCode, LanguageMap


class TestLanguageCode:

    @pytest.mark.parametrize(
        "language_code, expected",
        [
            ("ENG", "eng"),
            ("en", "eng"),
            ("fra", "fra"),
            ("fR", "fra"),
        ],
    )
    def test_validation(self, language_code: str, expected: str) -> None:
        """
        Languages are always normalized to the 3-letter ISO 639-2 code.
        """
        assert LanguageCode._validate_language_code(language_code) == expected
        assert LanguageCode(language_code) == expected
        assert TypeAdapter(LanguageCode).validate_python(language_code) == expected
        assert (
            TypeAdapter(LanguageCode).validate_json(json.dumps(language_code))
            == expected
        )

    def test_validation_failure(self) -> None:
        language_code = "foo"

        with pytest.raises(ValueError, match="Invalid language code 'foo'"):
            LanguageCode(language_code)

        with pytest.raises(ValueError, match="Invalid language code 'foo'"):
            LanguageCode._validate_language_code(language_code)

        with pytest.raises(
            ValidationError, match="Value error, Invalid language code 'foo'"
        ):
            TypeAdapter(LanguageCode).validate_python(language_code)

        with pytest.raises(
            ValidationError, match="Value error, Invalid language code 'foo'"
        ):
            TypeAdapter(LanguageCode).validate_json(json.dumps(language_code))

    def test_constructor(self) -> None:
        language_code = LanguageCode("eng")
        assert language_code == "eng"
        assert isinstance(language_code, str)
        assert isinstance(language_code, LanguageCode)

        # Can be constructed with a LanguageCode, which is a no-op
        new_language_code = LanguageCode(language_code)
        assert new_language_code is language_code


class TestLanguageMap:
    def test_constructor(self) -> None:
        # Can be constructed with a string
        test_map = LanguageMap("foo")
        assert test_map.default_language is None

        # Can be constructed with a dictionary
        test_map = LanguageMap({"en": "foo", "fr": "bar"})
        assert test_map == "foo"
        assert test_map.default_language == "eng"

        # Must provide at least one translation
        with pytest.raises(ValueError, match="Must provide at least one translation"):
            LanguageMap({})

    def test__eq__(self) -> None:
        # Can compare with strings, dictionaries, and other LanguageMap
        test_map = LanguageMap({"eng": "foo", "fr": "bar"})

        # When comparing with a string, if any of the translations match the string,
        # the comparison will return True.
        assert test_map == "foo"
        assert test_map == "bar"
        assert test_map != "baz"

        # When comparing with a dictionary, the comparison will return True
        # if the dictionary would create an equivalent LanguageMap.
        assert test_map == {"eng": "foo", "fra": "bar"}
        assert test_map == {"fr": "bar", "en": "foo"}
        assert test_map != {"en": "foo"}
        assert test_map != {"eng": "foo", "de": "bar"}
        assert test_map != {"foo": "bar"}
        assert test_map != {}
        assert test_map != []

        # When comparing with another LanguageMap, the comparison will return True
        # if the two LanguageMaps contain the same translations.
        assert test_map == test_map
        assert test_map == LanguageMap({"en": "foo", "fra": "bar"})

    def test__hash__(self) -> None:
        # language maps are immutable, so they can be hashed
        map1 = LanguageMap({"en": "foo", "fr": "bar"})
        map2 = LanguageMap({"fra": "bar", "eng": "foo"})
        assert hash(map1) == hash(map2)

    def test_get(self) -> None:
        test_map = LanguageMap({"en": "foo", "fr": "bar"})

        # Can get the default language
        assert test_map.get() == "foo"
        assert test_map.get(None) == "foo"
        assert test_map.get("") == "foo"
        assert test_map.get("eng") == "foo"
        assert test_map.get("en") == "foo"

        # Can get a specific language
        assert test_map.get("fra") == "bar"
        assert test_map.get("fr") == "bar"

        # Can get a language that doesn't exist
        assert test_map.get("de") is None

        # Can provide a default value
        assert test_map.get("de", "baz") == "baz"

        # Raises an error if the language code is invalid
        with pytest.raises(ValueError, match="Invalid language code 'foo'"):
            test_map.get("foo")

    def test__getitem__(self) -> None:
        test_map = LanguageMap({"en": "foo", "fr": "bar"})

        # Can get the default language
        assert test_map[None] == "foo"
        assert test_map[""] == "foo"
        assert test_map["eng"] == "foo"
        assert test_map["en"] == "foo"

        # Can get a specific language
        assert test_map["fra"] == "bar"
        assert test_map["fr"] == "bar"

        # Raises an error if the language code is invalid
        with pytest.raises(ValueError, match="Invalid language code 'foo'"):
            test_map["foo"]

        # Raises an error if the language code is not found
        with pytest.raises(
            KeyError, match="Language code 'de' not found in LanguageMap"
        ):
            test_map["de"]

    def test__len__(self) -> None:
        test_map = LanguageMap({"en": "foo", "fr": "bar"})
        assert len(test_map) == 2

    def test__iter__(self) -> None:
        test_map = LanguageMap({"en": "foo", "fr": "bar"})
        assert list(test_map) == ["eng", "fra"]

    def test__str__(self) -> None:
        test_map = LanguageMap({"en": "foo", "fr": "bar"})
        assert test_map == "foo"
        string_map = str(test_map)
        assert string_map == "foo"
        assert isinstance(string_map, str)
        assert not isinstance(test_map, str)

    def test__repr__(self) -> None:
        test_map = LanguageMap({"en": "foo", "fr": "bar"})
        assert repr(test_map) == '<LanguageMap: {"eng": "foo", "fra": "bar"}>'

    @pytest.mark.parametrize(
        "string, expected, default_language",
        [
            ("foo", "foo", None),
            ({"de": "foo", "fr": "bar"}, "foo", "deu"),
            ({"en": "foo", "fr": "bar"}, "foo", "eng"),
            ({"spa": "bar"}, "bar", "spa"),
        ],
    )
    def test_validate(
        self, string: str | dict[str, str], expected: str, default_language: str | None
    ) -> None:
        ta = TypeAdapter(LanguageMap)

        test_map = ta.validate_python(string)
        assert test_map == expected
        assert test_map.default_language == default_language
        assert isinstance(test_map, LanguageMap)

        assert ta.validate_json(json.dumps(string)) == test_map

        # Test round-trip to json
        assert (
            json.loads(ta.dump_json(test_map))
            == {LanguageCode(l): v for l, v in string.items()}
            if isinstance(string, dict)
            else string
        )

        # Test dump to python
        dumped = ta.dump_python(test_map)
        assert dumped == test_map
        assert isinstance(dumped, LanguageMap)

    def test_validate_failure(self) -> None:
        ta = TypeAdapter(LanguageMap)
        with pytest.raises(ValidationError, match="Invalid language code 'abcd'"):
            ta.validate_json(json.dumps({"abcd": "foo"}))
