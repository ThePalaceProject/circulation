from __future__ import annotations

from collections.abc import Callable, Generator, Iterable, Sequence
from typing import TYPE_CHECKING, Any, Self

from palace.manager.core.entrypoint import EntryPoint
from palace.manager.feed.facets.constants import FacetConfig, FacetConstants
from palace.manager.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from sqlalchemy.orm import Query, Session

    from palace.manager.feed.worklist.base import WorkList
    from palace.manager.search.filter import Filter, OpensearchDslType
    from palace.manager.sqlalchemy.model.library import Library
    from palace.manager.sqlalchemy.model.work import Work


class BaseFacets(FacetConstants):
    """Basic faceting class that doesn't modify a search filter at all.

    This is intended solely for use as a base class.
    """

    def items(self) -> Generator[tuple[str, str]]:
        """Yields a 2-tuple for every active facet setting.

        These tuples are used to generate URLs that can identify
        specific facet settings.
        """
        # Empty generator
        yield from ()

    @property
    def query_string(self) -> str:
        """A query string fragment that propagates all active facet
        settings.
        """
        return "&".join("=".join(x) for x in sorted(self.items()))

    @property
    def facet_groups(self) -> Generator[tuple[str, str, Self, bool, bool]]:
        """Yield a list of 5-tuples
        (facet group, facet value, new Facets object, selected, is_default)
        for use in building OPDS facets.

        This does not include the 'entry point' facet group,
        which must be handled separately.
        """
        yield from ()

    @classmethod
    def selectable_entrypoints(
        cls, worklist: WorkList | Library | FacetConfig | None
    ) -> Iterable[type[EntryPoint]]:
        """Ignore all entry points, even if the WorkList supports them."""
        return []

    def modify_search_filter(self, filter: Filter) -> Filter:
        """Modify an external_search.Filter object to filter out works
        excluded by the business logic of this faceting class.
        """
        return filter

    def modify_database_query(self, _db: Session, qu: Query[Work]) -> Query[Work]:
        """If necessary, modify a database query so that resulting
        items conform the constraints of this faceting object.

        The default behavior is to not modify the query.
        """
        return qu

    def scoring_functions(self, filter: Filter) -> list[OpensearchDslType]:
        """Create a list of scoring function objects that modify how
        works in the given WorkList should be ordered.

        Most subclasses will not use this because they order
        works using the 'order' feature.
        """
        return []


class FacetsWithEntryPoint(BaseFacets):
    """Basic Facets class that knows how to filter a query based on a
    selected EntryPoint.
    """

    def __init__(
        self,
        entrypoint: type[EntryPoint] | None = None,
        entrypoint_is_default: bool = False,
        **kwargs: Any,
    ) -> None:
        """Constructor.

        :param entrypoint: An EntryPoint (optional).
        :param entrypoint_is_default: If this is True, then `entrypoint`
            is a default value and was not determined by a user's
            explicit choice.
        :param kwargs: Other arguments may be supplied based on user
            input, but the default implementation is to ignore them.
        """
        self.entrypoint = entrypoint
        self.entrypoint_is_default = entrypoint_is_default
        self.constructor_kwargs = kwargs

    @classmethod
    def selectable_entrypoints(
        cls, worklist: WorkList | Library | FacetConfig | None
    ) -> Iterable[type[EntryPoint]]:
        """Which EntryPoints can be selected for these facets on this
        WorkList?

        In most cases, there are no selectable EntryPoints; this generally
        happens only at the top level.

        By default, this is completely determined by the WorkList.
        See SearchFacets for an example that changes this.
        """
        if not worklist:
            return []
        return worklist.entrypoints

    def navigate(
        self, *, entrypoint: type[EntryPoint] | None = None, **kwargs: Any
    ) -> Self:
        """Create a very similar FacetsWithEntryPoint that points to
        a different EntryPoint.

        Subclasses can override this method to accept additional keyword
        arguments for navigating to different facet settings.
        """
        return self.__class__(
            entrypoint=entrypoint,
            entrypoint_is_default=False,
            **self.constructor_kwargs,
        )

    @classmethod
    def from_request(
        cls,
        library: Library,
        facet_config: Library | FacetConfig,
        get_argument: Callable[[str, str | None], str | None],
        get_header: Callable[[str, str | None], str | None],
        worklist: WorkList | None,
        default_entrypoint: type[EntryPoint] | None = None,
        **extra_kwargs: Any,
    ) -> Self | ProblemDetail:
        """Load a faceting object from an HTTP request.

        :param facet_config: A Library (or mock of one) that knows
           which subset of the available facets are configured.

        :param get_argument: A callable that takes one argument and
           retrieves (or pretends to retrieve) a query string
           parameter of that name from an incoming HTTP request.

        :param get_header: A callable that takes one argument and
           retrieves (or pretends to retrieve) an HTTP header
           of that name from an incoming HTTP request.

        :param worklist: A WorkList associated with the current request,
           if any.

        :param default_entrypoint: Select this EntryPoint if the
           incoming request does not specify an enabled EntryPoint.
           If this is None, the first enabled EntryPoint will be used
           as the default.

        :param extra_kwargs: A dictionary of keyword arguments to pass
           into the constructor when a faceting object is instantiated.

        :return: A FacetsWithEntryPoint, or a ProblemDetail if there's
            a problem with the input from the request.
        """
        return cls._from_request(
            facet_config,
            get_argument,
            get_header,
            worklist,
            default_entrypoint,
            **extra_kwargs,
        )

    @classmethod
    def _from_request(
        cls,
        facet_config: Library | FacetConfig,
        get_argument: Callable[[str, str | None], str | None],
        get_header: Callable[[str, str | None], str | None],
        worklist: WorkList | None,
        default_entrypoint: type[EntryPoint] | None = None,
        **extra_kwargs: Any,
    ) -> Self:
        """Load a faceting object from an HTTP request.

        Subclasses of FacetsWithEntryPoint can override `from_request`,
        but call this method to load the EntryPoint and actually
        instantiate the faceting class.
        """
        entrypoint_name = get_argument(cls.ENTRY_POINT_FACET_GROUP_NAME, None)
        valid_entrypoints = list(cls.selectable_entrypoints(facet_config))
        entrypoint, is_default = cls.load_entrypoint(
            entrypoint_name, valid_entrypoints, default=default_entrypoint
        )

        return cls(
            entrypoint=entrypoint,
            entrypoint_is_default=is_default,
            **extra_kwargs,
        )

    @classmethod
    def load_entrypoint(
        cls,
        name: str | None,
        valid_entrypoints: Sequence[type[EntryPoint]],
        default: type[EntryPoint] | None = None,
    ) -> tuple[type[EntryPoint] | None, bool]:
        """Look up an EntryPoint by name, assuming it's valid in the
        given WorkList.

        :param valid_entrypoints: The EntryPoints that might be
            valid. This is probably not the value of
            WorkList.selectable_entrypoints, because an EntryPoint
            selected in a WorkList remains valid (but not selectable) for
            all of its children.

        :param default: A class to use as the default EntryPoint if
            none is specified. If no default is specified, the first
            enabled EntryPoint will be used.

        :return: A 2-tuple (EntryPoint class, is_default).
        """
        if not valid_entrypoints:
            return None, True
        if default is None:
            default = valid_entrypoints[0]
        ep = EntryPoint.BY_INTERNAL_NAME.get(name) if name else None
        if not ep or ep not in valid_entrypoints:
            return default, True
        return ep, False

    def items(self) -> Generator[tuple[str, str]]:
        """Yields a 2-tuple for every active facet setting.

        In this class that just means the entrypoint.
        """
        if self.entrypoint:
            yield (self.ENTRY_POINT_FACET_GROUP_NAME, self.entrypoint.INTERNAL_NAME)

    def modify_search_filter(self, filter: Filter) -> Filter:
        """Modify the given external_search.Filter object
        so that it reflects this set of facets.
        """
        if self.entrypoint:
            self.entrypoint.modify_search_filter(filter)
        return filter

    def modify_database_query(self, _db: Session, qu: Query[Work]) -> Query[Work]:
        """Modify the given database query so that it reflects this set of
        facets.
        """
        if self.entrypoint:
            qu = self.entrypoint.modify_database_query(_db, qu)
        return qu
