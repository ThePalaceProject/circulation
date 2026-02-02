from __future__ import annotations

from collections.abc import Callable, Generator
from typing import TYPE_CHECKING, Any, Self

from palace.manager.core.entrypoint import EntryPoint, EverythingEntryPoint
from palace.manager.feed.facets.constants import FacetConfig
from palace.manager.feed.facets.feed import Facets
from palace.manager.sqlalchemy.constants import EditionConstants
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.util.accept_language import parse_accept_language
from palace.manager.util.languages import LanguageCodes
from palace.manager.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from palace.manager.feed.worklist.base import WorkList
    from palace.manager.search.filter import Filter


class SearchFacets(Facets):
    """A Facets object designed to filter search results.

    Most search result filtering is handled by WorkList, but this
    allows someone to, e.g., search a multi-lingual WorkList in their
    preferred language.
    """

    # If search results are to be ordered by some field other than
    # score, we need a cutoff point so that marginal matches don't get
    # top billing just because they're first alphabetically. This is
    # the default cutoff point, determined empirically.
    DEFAULT_MIN_SCORE = 500

    def __init__(self, **kwargs: Any) -> None:
        languages = kwargs.pop("languages", None)
        media = kwargs.pop("media", None)

        # Our default_facets implementation will fill in values for
        # the facet groups defined by the Facets class. This
        # eliminates the need to explicitly specify a library, since
        # the library is mainly used to determine these defaults --
        # SearchFacets itself doesn't need one. However, in real
        # usage, a Library will be provided via
        # SearchFacets.from_request.
        kwargs.setdefault("library", None)
        kwargs.setdefault("availability", None)
        kwargs.setdefault("distributor", None)
        kwargs.setdefault("collection_name", None)
        order = kwargs.setdefault("order", None)

        if order in (None, self.ORDER_BY_RELEVANCE):
            # Search results are ordered by score, so there is no
            # need for a score cutoff.
            default_min_score = None
        else:
            default_min_score = self.DEFAULT_MIN_SCORE
        self.min_score: int | None = kwargs.pop("min_score", default_min_score)

        self.search_type: str = kwargs.pop("search_type", "default")
        if self.search_type not in ["default", "json"]:
            raise ValueError(f"Invalid search type: {self.search_type}")

        super().__init__(**kwargs)
        if media == Edition.ALL_MEDIUM:
            self.media: str | list[str] | None = media
        else:
            self.media = self._ensure_list(media)
        self.media_argument = media

        self.languages = self._ensure_list(languages)
        self._language_from_query: bool = kwargs.pop("language_from_query", False)

    @classmethod
    def default_facet(
        cls, ignore: Library | FacetConfig | None, group_name: str
    ) -> str | None:
        """The default facet settings for SearchFacets are hard-coded.

        By default, we will search all
        availabilities, and order by match quality rather than any
        bibliographic field.
        """
        if group_name == cls.AVAILABILITY_FACET_GROUP_NAME:
            return cls.AVAILABLE_ALL

        if group_name == cls.ORDER_FACET_GROUP_NAME:
            return cls.ORDER_BY_RELEVANCE
        return None

    def _ensure_list(self, x: str | list[str] | None) -> list[str] | None:
        """Make sure x is a list of values, if there is a value at all."""
        if x is None:
            return None
        if isinstance(x, list):
            return x
        return [x]

    @classmethod
    def from_request(
        cls,
        library: Library,
        facet_config: Library | FacetConfig,
        get_argument: Callable[[str, str | None], str | None],
        get_header: Callable[[str, str | None], str | None],
        worklist: WorkList | None,
        default_entrypoint: type[EntryPoint] | None = EverythingEntryPoint,
        **extra_kwargs: Any,
    ) -> Self | ProblemDetail:
        values = cls._values_from_request(facet_config, get_argument, get_header)
        if isinstance(values, ProblemDetail):
            return values
        extra_kwargs.update(values)
        extra_kwargs["library"] = library
        # Searches against a WorkList will use the union of the
        # languages allowed by the WorkList and the languages found in
        # the client's Accept-Language header.
        language_header = get_header("Accept-Language", None)
        language = get_argument("language", None) or None
        extra_kwargs["language_from_query"] = language is not None
        languages: str | list[str] | None
        if not language:
            if language_header:
                accept_header_languages = (
                    l.language for l in parse_accept_language(language_header)
                )
                mapped_languages = map(
                    LanguageCodes.iso_639_2_for_locale, accept_header_languages
                )
                languages = [l for l in mapped_languages if l] or None
            else:
                languages = None
        else:
            languages = language
        extra_kwargs["languages"] = languages

        # The client can request a minimum score for search results.
        if (min_score := get_argument("min_score", None)) is not None:
            try:
                extra_kwargs["min_score"] = int(min_score)
            except ValueError:
                pass

        # The client can request an additional restriction on
        # the media types to be returned by searches.

        media = get_argument("media", None)
        if media not in EditionConstants.KNOWN_MEDIA:
            media = None
        extra_kwargs["media"] = media

        search_type = get_argument("search_type", None)
        if search_type:
            extra_kwargs["search_type"] = search_type

        return cls._from_request(
            facet_config,
            get_argument,
            get_header,
            worklist,
            default_entrypoint,
            **extra_kwargs,
        )

    @classmethod
    def selectable_entrypoints(
        cls, worklist: WorkList | Library | FacetConfig | None
    ) -> list[type[EntryPoint]]:
        """If the WorkList has more than one facet, an 'everything' facet
        is added for search purposes.
        """
        if not worklist:
            return []
        entrypoints = list(worklist.entrypoints)
        if len(entrypoints) < 2:
            return entrypoints
        if EverythingEntryPoint not in entrypoints:
            entrypoints.insert(0, EverythingEntryPoint)
        return entrypoints

    def modify_search_filter(self, filter: Filter) -> Filter:
        """Modify the given external_search.Filter object
        so that it reflects this SearchFacets object.
        """
        super().modify_search_filter(filter)

        if filter.order is not None and filter.min_score is None:
            # The user wants search results to be ordered by one of
            # the data fields, not the match score; and no overriding
            # score cutoff has been provided yet. Use ours.
            filter.min_score = self.min_score

        # The incoming 'media' argument takes precedence over any
        # media restriction defined by the WorkList or the EntryPoint.
        if self.media == Edition.ALL_MEDIUM:
            # Clear any preexisting media restrictions.
            filter.media = None
        elif self.media:
            filter.media = self.media

        # The languages matched by the filter are the union of the
        # languages allowed by the WorkList (which were set to
        # filter.languages upon instantiation) and the languages
        # mentioned in the the user's Accept-Language header (which
        # were stuck into the SearchFacets object when _it_ was
        # instantiated).
        #
        # We don't rely solely on the WorkList languages because at
        # the moment it's hard for people who don't read the dominant
        # language of the circulation manager to find the right place
        # to search.
        #
        # We don't rely solely on the SearchFacets languages because a
        # lot of people read in languages other than the one they've
        # set for their device UI.
        #
        # We should only modify the langauges when we've not been asked to
        # display "all" the languages
        if self.languages != ["all"]:
            all_languages: set[str] = set()
            for language_list in (self.languages, filter.languages):
                for language in self._ensure_list(language_list) or []:
                    all_languages.add(language)
            filter.languages = sorted(all_languages) or None
        return filter

    def items(self) -> Generator[tuple[str, Any]]:
        """Yields a 2-tuple for every active facet setting.

        This means the EntryPoint (handled by the superclass)
        as well as possible settings for 'media' and "min_score".
        """
        yield from list(super().items())
        if self.media_argument:
            yield ("media", self.media_argument)

        if self.min_score is not None:
            yield ("min_score", str(self.min_score))

        if self.search_type is not None:
            yield ("search_type", self.search_type)

        # Only a language that came in from the request query should be reproduced
        if self._language_from_query and self.languages:
            yield ("language", self.languages)

    def navigate(
        self,
        entrypoint: type[EntryPoint] | None = None,
        availability: str | None = None,
        order: str | None = None,
        distributor: str | None = None,
        collection_name: str | None = None,
        min_score: int | None = None,
        **kwargs: Any,
    ) -> Self:
        if min_score is None:
            min_score = self.min_score
        new_facets = super().navigate(
            entrypoint=entrypoint,
            availability=availability,
            order=order,
            distributor=distributor,
            collection_name=collection_name,
            **kwargs,
        )
        new_facets.min_score = min_score
        return new_facets
