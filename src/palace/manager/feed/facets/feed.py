from __future__ import annotations

import logging
from collections.abc import Callable, Generator
from typing import TYPE_CHECKING, Any, Self

from flask_babel import lazy_gettext as _
from sqlalchemy import and_, or_, true
from sqlalchemy.orm import Query, Session

from palace.manager.core.config import Configuration
from palace.manager.core.entrypoint import EntryPoint
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.feed.facets.base import FacetGroup, FacetsWithEntryPoint
from palace.manager.feed.facets.constants import FacetConfig
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from palace.manager.feed.worklist.base import WorkList
    from palace.manager.search.filter import Filter, OpensearchDslType, RandomSeedType
    from palace.manager.sqlalchemy.model.lane import Lane
    from palace.manager.sqlalchemy.model.work import Work


class Facets(FacetsWithEntryPoint):
    """A full-fledged facet class that supports complex navigation between
    multiple facet groups.

    Despite the generic name, this is only used in 'page' type OPDS
    feeds that list all the works in some WorkList.
    """

    ORDER_BY_RELEVANCE = "relevance"

    @classmethod
    def default(
        cls,
        library: Library | None,
        collection: Collection | None = None,
        availability: str | None = None,
        order: str | None = None,
        entrypoint: type[EntryPoint] | None = None,
        distributor: str | None = None,
        collection_name: str | None = None,
    ) -> Self:
        return cls(
            library,
            collection=collection,
            availability=availability,
            order=order,
            distributor=distributor,
            collection_name=collection_name,
            entrypoint=entrypoint,
        )

    @classmethod
    def available_facets(
        cls, config: Library | FacetConfig | None, facet_group_name: str
    ) -> list[str]:
        """Which facets are enabled for the given facet group?

        You can override this to forcible enable or disable facets
        that might not be enabled in library configuration, but you
        can't make up totally new facets.

        TODO: This system would make more sense if you _could_ make up
        totally new facets, maybe because each facet was represented
        as a policy object rather than a key to code implemented
        elsewhere in this class. Right now this method implies more
        flexibility than actually exists.
        """
        available = config.enabled_facets(facet_group_name) if config else None
        if available is None:
            available = []

        # "The default facet isn't available" makes no sense. If the
        # default facet is not in the available list for any reason,
        # add it to the beginning of the list. This makes other code
        # elsewhere easier to write.
        default = cls.default_facet(config, facet_group_name)
        if default is not None and default not in available:
            available = [default] + available
        return available

    @classmethod
    def default_facet(
        cls, config: Library | FacetConfig | None, facet_group_name: str
    ) -> str | None:
        """The default value for the given facet group.

        The default value must be one of the values returned by available_facets() above.
        """
        if config is None:
            return None
        return config.default_facet(facet_group_name)

    @classmethod
    def _values_from_request(
        cls,
        config: Library | FacetConfig,
        get_argument: Callable[[str, str | None], str | None],
        get_header: Callable[[str, str | None], str | None],
    ) -> dict[str, Any] | ProblemDetail:
        g = Facets.ORDER_FACET_GROUP_NAME
        order = get_argument(g, cls.default_facet(config, g))
        order_facets = cls.available_facets(config, g)
        # Also accept reverse variants of enabled base facets
        valid_order_facets = set(order_facets)
        for base_facet in order_facets:
            reverse = cls.ORDER_FACET_TO_REVERSE_VARIANT.get(base_facet)
            if reverse:
                valid_order_facets.add(reverse)
        if order and order not in valid_order_facets:
            return INVALID_INPUT.detailed(
                _("I don't know how to order a feed by '%(order)s'", order=order), 400
            )

        g = Facets.AVAILABILITY_FACET_GROUP_NAME
        availability = get_argument(g, cls.default_facet(config, g))
        availability_facets = cls.available_facets(config, g)
        if availability and not availability in availability_facets:
            return INVALID_INPUT.detailed(
                _(
                    "I don't understand the availability term '%(availability)s'",
                    availability=availability,
                ),
                400,
            )

        g = Facets.DISTRIBUTOR_FACETS_GROUP_NAME
        distributor = get_argument(g, cls.default_facet(config, g))
        distributor_facets = cls.available_facets(config, g)
        if (
            distributor
            and distributor != "All"
            and distributor not in distributor_facets
        ):
            return INVALID_INPUT.detailed(
                _(
                    "I don't understand which distributor '%(distributor)s' refers to.",
                    distributor=distributor,
                ),
                400,
            )

        g = Facets.COLLECTION_NAME_FACETS_GROUP_NAME
        collection_name = get_argument(g, cls.default_facet(config, g))
        collection_name_facets = cls.available_facets(config, g)
        if (
            collection_name
            and collection_name != "All"
            and collection_name not in collection_name_facets
        ):
            return INVALID_INPUT.detailed(
                _(
                    "I don't understand which collection '%(collection_name)s' refers to.",
                    collection_name=collection_name,
                ),
                400,
            )

        enabled = {
            Facets.ORDER_FACET_GROUP_NAME: order_facets,
            Facets.AVAILABILITY_FACET_GROUP_NAME: availability_facets,
            Facets.DISTRIBUTOR_FACETS_GROUP_NAME: distributor_facets,
            Facets.COLLECTION_NAME_FACETS_GROUP_NAME: collection_name_facets,
        }

        return dict(
            order=order,
            availability=availability,
            distributor=distributor,
            collection_name=collection_name,
            enabled_facets=enabled,
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
        """Load a faceting object from an HTTP request."""

        values = cls._values_from_request(facet_config, get_argument, get_header)
        if isinstance(values, ProblemDetail):
            return values
        extra_kwargs.update(values)
        extra_kwargs["library"] = library

        return cls._from_request(
            facet_config,
            get_argument,
            get_header,
            worklist,
            default_entrypoint,
            **extra_kwargs,
        )

    def __init__(
        self,
        library: Library | None,
        availability: str | None,
        order: str | None,
        distributor: str | None,
        collection_name: str | None,
        order_ascending: str | bool | None = None,
        enabled_facets: dict[str, list[str]] | None = None,
        entrypoint: type[EntryPoint] | None = None,
        entrypoint_is_default: bool = False,
        **constructor_kwargs: Any,
    ) -> None:
        """Constructor.

        :param entrypoint: An EntryPoint class. The 'entry point'
        facet group is configured on a per-WorkList basis rather than
        a per-library basis.
        """
        super().__init__(entrypoint, entrypoint_is_default, **constructor_kwargs)
        availability = availability or self.default_facet(
            library, self.AVAILABILITY_FACET_GROUP_NAME
        )
        order = order or self.default_facet(library, self.ORDER_FACET_GROUP_NAME)
        if order_ascending is None:
            if order in Facets.ORDER_DESCENDING_BY_DEFAULT:
                order_ascending = self.ORDER_DESCENDING
            else:
                order_ascending = self.ORDER_ASCENDING

        if (
            availability == self.AVAILABLE_ALL
            and (library and not library.settings.allow_holds)
            and (
                self.AVAILABLE_NOW
                in self.available_facets(library, self.AVAILABILITY_FACET_GROUP_NAME)
            )
        ):
            # Under normal circumstances we would show all works, but
            # library configuration says to hide books that aren't
            # available.
            availability = self.AVAILABLE_NOW

        self.library = library
        self.availability = availability
        self.order = order
        self.distributor = distributor or self.default_facet(
            library, self.DISTRIBUTOR_FACETS_GROUP_NAME
        )
        self.collection_name = collection_name or self.default_facet(
            library, self.COLLECTION_NAME_FACETS_GROUP_NAME
        )
        if order_ascending == self.ORDER_ASCENDING:
            order_ascending = True
        elif order_ascending == self.ORDER_DESCENDING:
            order_ascending = False
        self.order_ascending = order_ascending
        self.facets_enabled_at_init = enabled_facets

    def navigate(
        self,
        *,
        entrypoint: type[EntryPoint] | None = None,
        availability: str | None = None,
        order: str | None = None,
        distributor: str | None = None,
        collection_name: str | None = None,
        **kwargs: Any,
    ) -> Self:
        """Create a slightly different Facets object from this one."""
        return self.__class__(
            library=self.library,
            availability=availability or self.availability,
            order=order or self.order,
            distributor=distributor or self.distributor,
            collection_name=collection_name or self.collection_name,
            enabled_facets=self.facets_enabled_at_init,
            entrypoint=(entrypoint or self.entrypoint),
            entrypoint_is_default=False,
        )

    def items(self) -> Generator[tuple[str, str]]:
        yield from list(super().items())
        if self.order:
            yield (self.ORDER_FACET_GROUP_NAME, self.order)
        if self.availability:
            yield (self.AVAILABILITY_FACET_GROUP_NAME, self.availability)
        if self.distributor:
            yield (self.DISTRIBUTOR_FACETS_GROUP_NAME, self.distributor)
        if self.collection_name:
            yield (self.COLLECTION_NAME_FACETS_GROUP_NAME, self.collection_name)

    @property
    def enabled_facets(self) -> Generator[list[str]]:
        """
        Yield lists of enabled facets for each facet group.

        Yields in order: order, availability, collection, distributor, collectionName

        The 'entry point' facet group is handled separately, since it
        is not always used.
        """
        if self.facets_enabled_at_init:
            # When this Facets object was initialized, a list of enabled
            # facets was passed. We'll only work with those facets.
            facet_types = [
                self.ORDER_FACET_GROUP_NAME,
                self.AVAILABILITY_FACET_GROUP_NAME,
                self.DISTRIBUTOR_FACETS_GROUP_NAME,
                self.COLLECTION_NAME_FACETS_GROUP_NAME,
            ]
            for facet_type in facet_types:
                yield self.facets_enabled_at_init.get(facet_type, [])
        else:
            library = self.library
            for group_name in (
                Facets.ORDER_FACET_GROUP_NAME,
                Facets.AVAILABILITY_FACET_GROUP_NAME,
                Facets.DISTRIBUTOR_FACETS_GROUP_NAME,
                Facets.COLLECTION_NAME_FACETS_GROUP_NAME,
            ):
                yield self.available_facets(self.library, group_name)

    @property
    def facet_groups(self) -> Generator[FacetGroup]:
        """Yield FacetGroup objects for use in building OPDS facet links.

        This does not yield anything for the 'entry point' facet group,
        which must be handled separately.
        """

        (
            order_facets,
            availability_facets,
            distributor_facets,
            collection_name_facets,
        ) = self.enabled_facets

        facet_config = FacetConfig.from_library(self.library) if self.library else None

        def is_default_facet(facet: str, facet_group_name: str) -> bool:
            if not facet_config:
                return False
            default_facet = self.default_facet(facet_config, facet_group_name)
            return default_facet == facet

        # First, the order facets.
        # For each enabled base facet, also yield its reverse variant.
        order_facet_values = list(order_facets)
        for facet in order_facets:
            reverse_facet = self.ORDER_FACET_TO_REVERSE_VARIANT.get(facet)
            if reverse_facet:
                order_facet_values.append(reverse_facet)
        if len(set(order_facet_values)) > 1:
            group = self.ORDER_FACET_GROUP_NAME
            for facet in order_facets:
                yield FacetGroup(
                    group=group,
                    value=facet,
                    facets=self.navigate(order=facet),
                    is_selected=self.order == facet,
                    is_default=is_default_facet(facet, group),
                )
                # Yield the reverse variant if one exists
                reverse_facet = self.ORDER_FACET_TO_REVERSE_VARIANT.get(facet)
                if reverse_facet:
                    yield FacetGroup(
                        group=group,
                        value=reverse_facet,
                        facets=self.navigate(order=reverse_facet),
                        is_selected=self.order == reverse_facet,
                        is_default=False,  # Reverse variants are never the default
                    )

        # Next, the availability facets.
        if len(availability_facets) > 1:
            group = self.AVAILABILITY_FACET_GROUP_NAME
            for facet in availability_facets:
                yield FacetGroup(
                    group=group,
                    value=facet,
                    facets=self.navigate(availability=facet),
                    is_selected=self.availability == facet,
                    is_default=is_default_facet(facet, group),
                )

        if len(distributor_facets) > 1:
            group = self.DISTRIBUTOR_FACETS_GROUP_NAME
            for facet in distributor_facets:
                yield FacetGroup(
                    group=group,
                    value=facet,
                    facets=self.navigate(distributor=facet),
                    is_selected=self.distributor == facet,
                    is_default=is_default_facet(facet, group),
                )

        if len(collection_name_facets) > 1:
            group = self.COLLECTION_NAME_FACETS_GROUP_NAME
            for facet in collection_name_facets:
                yield FacetGroup(
                    group=group,
                    value=facet,
                    facets=self.navigate(collection_name=facet),
                    is_selected=self.collection_name == facet,
                    is_default=is_default_facet(facet, group),
                )

    def modify_search_filter(self, filter: Filter) -> Filter:
        """Modify the given external_search.Filter object
        so that it reflects the settings of this Facets object.

        This is the Opensearch equivalent of apply(). However, the
        Opensearch implementation of (e.g.) the meaning of the
        different availabilty statuses is kept in Filter.build().
        """
        super().modify_search_filter(filter)

        if self.library:
            filter.minimum_featured_quality = (
                self.library.settings.minimum_featured_quality
            )

        filter.availability = self.availability

        # We can only have distributor and collection name facets if we have a library
        if self.library:
            _db = Session.object_session(self.library)

            if self.distributor and self.distributor != self.DISTRIBUTOR_ALL:
                distributor = DataSource.lookup(_db, self.distributor, autocreate=False)
                if distributor:
                    filter.license_datasources = [distributor.id]

            if (
                self.collection_name
                and self.collection_name != self.COLLECTION_NAME_ALL
            ):
                collection = Collection.by_name(_db, self.collection_name)
                if collection:
                    filter.collection_ids = [collection.id]

        # No order and relevance order both signify the default and,
        # thus, either should leave `filter.order` unset.
        if self.order and self.order != self.ORDER_BY_RELEVANCE:
            order = self.SORT_ORDER_TO_OPENSEARCH_FIELD_NAME.get(self.order)
            if order:
                filter.order = order
                filter.order_ascending = self.order_ascending
            else:
                logging.error("Unrecognized sort order: %s", self.order)
        return filter

    def modify_database_query(self, _db: Session, qu: Query[Work]) -> Query[Work]:
        """Restrict a query against Work+LicensePool+Edition so that it
        matches only works that fit the criteria of this Faceting object.

        Sort order facet cannot be handled in this method, but can be
        handled in subclasses that override this method.
        """

        # Apply any superclass criteria
        qu = super().modify_database_query(_db, qu)

        # sqlalchemy-stubs has issues with true() in and_/or_ expressions
        active_metered_filter = and_(  # type: ignore[type-var]
            LicensePool.metered_or_equivalent_type == true(),
            LicensePool.active_status == true(),
        )
        active_unlimited_filter = and_(  # type: ignore[type-var]
            LicensePool.unlimited_type == true(),
            LicensePool.active_status == true(),
        )

        if self.availability == self.AVAILABLE_NOW:
            availability_clause = or_(
                and_(LicensePool.licenses_available > 0, active_metered_filter),
                active_unlimited_filter,
            )
        elif self.availability == self.AVAILABLE_ALL:
            availability_clause = or_(
                active_metered_filter,
                active_unlimited_filter,
            )
        elif self.availability == self.AVAILABLE_OPEN_ACCESS:
            availability_clause = and_(
                LicensePool.open_access == true(),
                active_unlimited_filter,
            )
        elif self.availability == self.AVAILABLE_NOT_NOW:
            # The book must be licensed but currently unavailable.
            availability_clause = and_(
                LicensePool.licenses_available == 0, active_metered_filter
            )
        else:
            raise PalaceValueError(f"Unknown availability facet: {self.availability}")

        qu = qu.filter(availability_clause)

        return qu


class DefaultSortOrderFacets(Facets):
    """A faceting object that changes the default sort order.

    Subclasses must set DEFAULT_SORT_ORDER
    """

    DEFAULT_SORT_ORDER: str

    @classmethod
    def available_facets(
        cls, config: Library | FacetConfig | None, facet_group_name: str
    ) -> list[str]:
        """Make sure the default sort order is the first item
        in the list of available sort orders.
        """
        if facet_group_name != cls.ORDER_FACET_GROUP_NAME:
            return super().available_facets(config, facet_group_name)
        default = config.enabled_facets(facet_group_name) if config else []
        if default is None:
            default = []

        # Promote the default sort order to the front of the list,
        # adding it if necessary.
        order = cls.DEFAULT_SORT_ORDER
        if order in default:
            default = [x for x in default if x != order]
        return [order] + default

    @classmethod
    def default_facet(
        cls, config: Library | FacetConfig | None, facet_group_name: str
    ) -> str | None:
        if facet_group_name == cls.ORDER_FACET_GROUP_NAME:
            return cls.DEFAULT_SORT_ORDER
        return super().default_facet(config, facet_group_name)


class FeaturedFacets(FacetsWithEntryPoint):
    """A simple faceting object that configures a query so that the 'most
    featurable' items are at the front.

    This is mainly a convenient thing to pass into
    AcquisitionFeed.groups().
    """

    def __init__(
        self,
        minimum_featured_quality: float,
        entrypoint: type[EntryPoint] | None = None,
        random_seed: RandomSeedType = None,
        **kwargs: Any,
    ) -> None:
        """Set up an object that finds featured books in a given
        WorkList.

        :param kwargs: Other arguments may be supplied based on user
            input, but the default implementation is to ignore them.
        """
        super().__init__(entrypoint=entrypoint, **kwargs)
        self.minimum_featured_quality = minimum_featured_quality
        self.random_seed = random_seed

    @classmethod
    def default(cls, lane: WorkList | Library | Lane | None, **kwargs: Any) -> Self:
        library: Library | None = None
        if lane:
            if isinstance(lane, Library):
                library = lane
            else:
                library = getattr(lane, "library", None)

        if library:
            quality = library.settings.minimum_featured_quality
        else:
            quality = Configuration.DEFAULT_MINIMUM_FEATURED_QUALITY
        return cls(quality, **kwargs)

    def navigate(
        self,
        entrypoint: type[EntryPoint] | None = None,
        minimum_featured_quality: float | None = None,
        **kwargs: Any,
    ) -> Self:
        """Create a slightly different FeaturedFacets object based on this
        one.
        """
        minimum_featured_quality = (
            minimum_featured_quality or self.minimum_featured_quality
        )
        entrypoint = entrypoint or self.entrypoint
        return self.__class__(minimum_featured_quality, entrypoint)

    def modify_search_filter(self, filter: Filter) -> Filter:
        super().modify_search_filter(filter)
        filter.minimum_featured_quality = self.minimum_featured_quality
        return filter

    def scoring_functions(self, filter: Filter) -> list[OpensearchDslType]:
        """Generate scoring functions that weight works randomly, but
        with 'more featurable' works tending to be at the top.
        """
        return filter.featurability_scoring_functions(self.random_seed)
