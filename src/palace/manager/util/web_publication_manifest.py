"""Helper classes for the Readium Web Publication Manifest format
(https://github.com/readium/webpub-manifest) and its audiobook profile
(https://github.com/HadrienGardeur/audiobook-manifest).
"""

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.licensing import LicensePool

# JSON-LD @context can be a string URI, a dict object, or a list of strings/dicts
JSONLDContext = str | dict[str, Any] | list[str | dict[str, Any]]


class JSONable:
    """An object whose Unicode representation is a JSON dump
    of a dictionary.
    """

    def __str__(self) -> str:
        return json.dumps(self.as_dict)

    @property
    def as_dict(self) -> dict[str, Any]:
        raise NotImplementedError()

    @classmethod
    def json_ready(cls, value: Any) -> Any:
        if isinstance(value, JSONable):
            return value.as_dict
        elif isinstance(value, list):
            return [cls.json_ready(x) for x in value]
        else:
            return value


class Manifest(JSONable):
    """A Python object corresponding to a Readium Web Publication
    Manifest.
    """

    BOOK_TYPE = "http://schema.org/Book"
    AUDIOBOOK_TYPE = "http://bib.schema.org/Audiobook"

    DEFAULT_CONTEXT = "http://readium.org/webpub/default.jsonld"
    DEFAULT_TYPE = BOOK_TYPE

    links: list[dict[str, Any]] = []
    readingOrder: list[dict[str, Any]] = []
    resources: list[dict[str, Any]] = []

    def __init__(
        self,
        context: JSONLDContext | None = None,
        type: str | None = None,
    ) -> None:
        self.context = context or self.DEFAULT_CONTEXT
        self.type = type or self.DEFAULT_TYPE
        self.metadata: dict[str, Any] = {"@type": self.type}

        # Initialize all component lists to the empty list.
        for name in self.component_lists:
            setattr(self, name, [])

    @property
    def as_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"@context": self.context, "metadata": self.metadata}
        for key in self.component_lists:
            value = getattr(self, key)
            if value:
                data[key] = self.json_ready(value)
        return data

    @property
    def component_lists(self) -> tuple[str, str, str]:
        return "links", "readingOrder", "resources"

    def _append(self, append_to: list[dict[str, Any]], **kwargs: Any) -> None:
        # Omit properties with None values, rather than propagating nulls to manifest.
        append_to.append({k: v for k, v in kwargs.items() if v is not None})

    def add_link(self, href: str | None, rel: str, **kwargs: Any) -> None:
        self._append(self.links, href=href, rel=rel, **kwargs)

    def add_reading_order(
        self, href: str | None, type: str, title: str, **kwargs: Any
    ) -> None:
        self._append(self.readingOrder, href=href, type=type, title=title, **kwargs)

    def add_resource(self, href: str | None, type: str, **kwargs: Any) -> None:
        self._append(self.resources, href=href, type=type, **kwargs)

    def update_bibliographic_metadata(self, license_pool: "LicensePool") -> None:
        """Update this Manifest with basic bibliographic metadata
        taken from a LicensePool object.

        Currently this assumes that there is no other source of
        bibliographic metadata, so it will overwrite any metadata that is
        already present and add a cover link even if the manifest
        already has one.
        """
        self.metadata["identifier"] = license_pool.identifier.urn

        edition = license_pool.presentation_edition
        if not edition:
            return
        self.metadata["title"] = edition.title

        self.metadata["language"] = edition.language_code

        authors = [
            author.display_name or author.sort_name
            for author in edition.author_contributors
            if author.display_name or author.sort_name
        ]
        if authors:
            self.metadata["author"] = authors

        if edition.cover_thumbnail_url:
            self.add_link(edition.cover_thumbnail_url, "cover")


class AudiobookManifest(Manifest):
    """A Python object corresponding to a Readium Web Publication
    Manifest.
    """

    DEFAULT_TYPE = Manifest.AUDIOBOOK_TYPE
