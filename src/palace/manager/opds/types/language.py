from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from typing import Any, TypeVar, cast, overload

import pycountry
from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema


class LanguageCode(str):
    """
    LanguageCode parses 2-letter or 3-letter ISO 639-2 language codes. Making sure
    they are valid language codes and normalizing them to 3-letter codes.
    """

    __slots__ = ()

    def __new__(
        cls,
        value: str,
    ) -> LanguageCode:
        if isinstance(value, LanguageCode):
            return value
        return super().__new__(cls, cls._validate_language_code(value))

    @staticmethod
    def _validate_language_code(language_code: str) -> str:
        language = None
        if len(language_code) == 2:
            language = pycountry.languages.get(alpha_2=language_code)
        elif len(language_code) == 3:
            language = pycountry.languages.get(alpha_3=language_code)

        if language is None:
            raise ValueError(f"Invalid language code '{language_code}'")

        return language.alpha_3  # type: ignore[no-any-return]

    @classmethod
    def __get_pydantic_core_schema__(
        cls, _: type[Any], __: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        """
        Return a Pydantic CoreSchema validating a 2-letter or 3-letter ISO 639-2 language code.
        """
        return core_schema.no_info_after_validator_function(
            cls,
            core_schema.str_schema(min_length=2, max_length=3),
        )


T = TypeVar("T")


class LanguageMap(Mapping[str, str]):
    """
    A string or map of strings that provides values in multiple languages.

    This is meant to implement the multilingual strings in the RWPM standard.
    https://github.com/readium/webpub-manifest/blob/master/schema/language-map.schema.json
    """

    __slots__ = ("_mapping", "_default_language", "_hash")

    def __init__(self, value: str | Mapping[str, str]) -> None:
        """
        Create a new LanguageMap instance.

        :param value: Either a string or a dictionary with language codes as keys
            and translations as values.
        """
        self._mapping: dict[LanguageCode | None, str] = {}
        self._hash: int | None = None

        if isinstance(value, str):
            self._mapping[None] = value
        else:
            if len(value) == 0:
                raise ValueError("Must provide at least one translation")

            self._mapping = {
                LanguageCode(lang): translation for lang, translation in value.items()
            }
        self._default_language = next(iter(self._mapping.keys()))

    @property
    def default_language(self) -> LanguageCode | None:
        return self._default_language

    @staticmethod
    def _serialize(instance: LanguageMap) -> str | dict[str, str]:
        # If the default language is None, we serialize as a string
        if instance.default_language is None:
            return instance.get()

        # Otherwise, we serialize as a dictionary
        return {
            lang: translation
            for lang, translation in instance.items()
            if lang is not None
        }

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: type[Any], handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        """
        This method validates the LanguageMap type when used in
        Pydantic. It is validated as 'str | dict[str, str]' the value is
        then passed into the class constructor, which does some
        further validation, raising TypeErrors if the input is invalid.
        """
        return core_schema.no_info_after_validator_function(
            cls,
            core_schema.union_schema(
                [
                    core_schema.str_schema(),
                    core_schema.dict_schema(
                        core_schema.str_schema(),
                        core_schema.str_schema(),
                    ),
                ],
                serialization=core_schema.plain_serializer_function_ser_schema(
                    cls._serialize, when_used="json"
                ),
            ),
        )

    @overload
    def get(self) -> str: ...

    @overload
    def get(self, language: str | None) -> str | None: ...

    @overload
    def get(self, language: str | None, default: str | T) -> str | T: ...

    def get(
        self, language: str | None = None, default: str | T | None = None
    ) -> str | T | None:
        """
        Return the translation for the specified language.

        :param language: The language code to return the translation for. This can
            be either a 2-letter or 3-letter ISO 639-2 code.
        :param default: The default translation to return if the specified language
            is not found. If not provided, None is returned.

        :return: The translation for the specified language, or the default translation.

        :raises ValueError: If the language code is invalid.
        """
        language_code = LanguageCode(language) if language else self.default_language
        return self._mapping.get(language_code, default)

    def __getitem__(self, language: str | None) -> str:
        translation = self.get(language)
        if translation is None:
            raise KeyError(
                f"Language code '{language}' not found in {self.__class__.__name__}"
            )
        return translation

    def __iter__(self) -> Iterator[str]:
        if None in self._mapping:
            return iter([""])
        return cast(Iterator[str], self._mapping.__iter__())

    def __len__(self) -> int:
        return len(self._mapping)

    def __str__(self) -> str:
        return self.get()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {json.dumps(self._mapping)}>"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, LanguageMap):
            return self._mapping == other._mapping
        if isinstance(other, str):
            return other in set(self.values())
        if isinstance(other, Mapping):
            try:
                return self == LanguageMap(other)
            except ValueError:
                return False
        return NotImplemented

    def __hash__(self) -> int:
        if self._hash is None:
            self._hash = hash(frozenset(self.items()))
        return self._hash
