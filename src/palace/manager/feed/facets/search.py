from __future__ import annotations

from palace.manager.core.entrypoint import EverythingEntryPoint
from palace.manager.feed.facets.feed import Facets
from palace.manager.sqlalchemy.constants import EditionConstants
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.util.accept_language import parse_accept_language
from palace.manager.util.languages import LanguageCodes
from palace.manager.util.problem_detail import ProblemDetail


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

    def __init__(self, **kwargs):
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
        self.min_score = kwargs.pop("min_score", default_min_score)

        self.search_type = kwargs.pop("search_type", "default")
        if self.search_type not in ["default", "json"]:
            raise ValueError(f"Invalid search type: {self.search_type}")

        super().__init__(**kwargs)
        if media == Edition.ALL_MEDIUM:
            self.media = media
        else:
            self.media = self._ensure_list(media)
        self.media_argument = media

        self.languages = self._ensure_list(languages)
        self._language_from_query = kwargs.pop("language_from_query", False)

    @classmethod
    def default_facet(cls, ignore, group_name):
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

    def _ensure_list(self, x):
        """Make sure x is a list of values, if there is a value at all."""
        if x is None:
            return None
        if isinstance(x, list):
            return x
        return [x]

    @classmethod
    def from_request(
        cls,
        library,
        config,
        get_argument,
        get_header,
        worklist,
        default_entrypoint=EverythingEntryPoint,
        **extra,
    ):
        values = cls._values_from_request(config, get_argument, get_header)
        if isinstance(values, ProblemDetail):
            return values
        extra.update(values)
        extra["library"] = library
        # Searches against a WorkList will use the union of the
        # languages allowed by the WorkList and the languages found in
        # the client's Accept-Language header.
        language_header = get_header("Accept-Language")
        languages = get_argument("language") or None
        extra["language_from_query"] = languages is not None
        if not languages:
            if language_header:
                languages = parse_accept_language(language_header)
                languages = [l[0] for l in languages]
                languages = list(map(LanguageCodes.iso_639_2_for_locale, languages))
                languages = [l for l in languages if l]
            languages = languages or None
        extra["languages"] = languages

        # The client can request a minimum score for search results.
        min_score = get_argument("min_score", None)
        if min_score is not None:
            try:
                min_score = int(min_score)
            except ValueError as e:
                min_score = None
        if min_score is not None:
            extra["min_score"] = min_score

        # The client can request an additional restriction on
        # the media types to be returned by searches.

        media = get_argument("media", None)
        if media not in EditionConstants.KNOWN_MEDIA:
            media = None
        extra["media"] = media

        search_type = get_argument("search_type")
        if search_type:
            extra["search_type"] = search_type

        return cls._from_request(
            config, get_argument, get_header, worklist, default_entrypoint, **extra
        )

    @classmethod
    def selectable_entrypoints(cls, worklist):
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

    def modify_search_filter(self, filter):
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
            all_languages = set()
            for language_list in (self.languages, filter.languages):
                for language in self._ensure_list(language_list) or []:
                    all_languages.add(language)
            filter.languages = sorted(all_languages) or None

    def items(self):
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

    def navigate(self, **kwargs):
        min_score = kwargs.pop("min_score", self.min_score)
        new_facets = super().navigate(**kwargs)
        new_facets.min_score = min_score
        return new_facets
