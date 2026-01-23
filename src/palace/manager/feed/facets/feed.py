from __future__ import annotations

import logging
from typing import Any

from flask_babel import lazy_gettext as _
from sqlalchemy import and_, or_, true
from sqlalchemy.orm import Session

from palace.manager.core.config import Configuration
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.feed.facets.base import FacetsWithEntryPoint
from palace.manager.feed.facets.constants import FacetConfig
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.util.problem_detail import ProblemDetail


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
        library,
        collection=None,
        availability=None,
        order=None,
        entrypoint=None,
        distributor=None,
        collection_name=None,
    ):
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
    def available_facets(cls, config, facet_group_name):
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
        available = config.enabled_facets(facet_group_name)

        # "The default facet isn't available" makes no sense. If the
        # default facet is not in the available list for any reason,
        # add it to the beginning of the list. This makes other code
        # elsewhere easier to write.
        default = cls.default_facet(config, facet_group_name)
        if default not in available:
            available = [default] + available
        return available

    @classmethod
    def default_facet(cls, config, facet_group_name):
        """The default value for the given facet group.

        The default value must be one of the values returned by available_facets() above.
        """
        return config.default_facet(facet_group_name)

    @classmethod
    def _values_from_request(
        cls, config, get_argument, get_header
    ) -> dict[str, Any] | ProblemDetail:
        g = Facets.ORDER_FACET_GROUP_NAME
        order = get_argument(g, cls.default_facet(config, g))
        order_facets = cls.available_facets(config, g)
        if order and not order in order_facets:
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
        library,
        config,
        get_argument,
        get_header,
        worklist,
        default_entrypoint=None,
        **extra,
    ):
        """Load a faceting object from an HTTP request."""

        values = cls._values_from_request(config, get_argument, get_header)
        if isinstance(values, ProblemDetail):
            return values
        extra.update(values)
        extra["library"] = library

        return cls._from_request(
            config, get_argument, get_header, worklist, default_entrypoint, **extra
        )

    def __init__(
        self,
        library,
        availability,
        order,
        distributor,
        collection_name,
        order_ascending=None,
        enabled_facets=None,
        entrypoint=None,
        entrypoint_is_default=False,
        **constructor_kwargs,
    ):
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
        availability=None,
        order=None,
        entrypoint=None,
        distributor=None,
        collection_name=None,
    ):
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

    def items(self):
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
    def enabled_facets(self):
        """Yield a 5-tuple of lists (order, availability, collection, distributor, collectionName)
        representing facet values enabled via initialization or configuration

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
    def facet_groups(self):
        """Yield a list of 5-tuples
        (facet group, facet value, new Facets object, selected, is_default)
        for use in building OPDS facets.

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

        def is_default_facet(facets, facet, facet_group_name) -> bool:
            if not facet_config:
                return False
            default_facet = facets.default_facet(facet_config, facet_group_name)
            return default_facet == facet

        def dy(new_value):
            group = self.ORDER_FACET_GROUP_NAME
            current_value = self.order
            facets = self.navigate(order=new_value)

            return (
                group,
                new_value,
                facets,
                current_value == new_value,
                is_default_facet(facets, new_value, group),
            )

        # First, the order facets.
        if len(order_facets) > 1:
            for facet in order_facets:
                yield dy(facet)

        # Next, the availability facets.
        def dy(new_value):
            group = self.AVAILABILITY_FACET_GROUP_NAME
            current_value = self.availability
            facets = self.navigate(availability=new_value)
            return (
                group,
                new_value,
                facets,
                new_value == current_value,
                is_default_facet(facets, new_value, group),
            )

        if len(availability_facets) > 1:
            for facet in availability_facets:
                yield dy(facet)

        if len(distributor_facets) > 1:
            for facet in distributor_facets:
                group = self.DISTRIBUTOR_FACETS_GROUP_NAME
                current_value = self.distributor
                facets = self.navigate(distributor=facet)
                yield (
                    group,
                    facet,
                    facets,
                    facet == current_value,
                    is_default_facet(facets, facet, group),
                )

        if len(collection_name_facets) > 1:
            for facet in collection_name_facets:
                group = self.COLLECTION_NAME_FACETS_GROUP_NAME
                current_value = self.collection_name
                facets = self.navigate(collection_name=facet)
                yield (
                    group,
                    facet,
                    facets,
                    facet == current_value,
                    is_default_facet(facets, facet, group),
                )

    def modify_search_filter(self, filter):
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

    def modify_database_query(self, _db, qu):
        """Restrict a query against Work+LicensePool+Edition so that it
        matches only works that fit the criteria of this Faceting object.

        Sort order facet cannot be handled in this method, but can be
        handled in subclasses that override this method.
        """

        # Apply any superclass criteria
        qu = super().modify_database_query(_db, qu)

        active_metered_filter = and_(
            LicensePool.metered_or_equivalent_type == true(),
            LicensePool.active_status == true(),
        )
        active_unlimited_filter = and_(
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

    @classmethod
    def available_facets(cls, config, facet_group_name):
        """Make sure the default sort order is the first item
        in the list of available sort orders.
        """
        if facet_group_name != cls.ORDER_FACET_GROUP_NAME:
            return super().available_facets(config, facet_group_name)
        default = config.enabled_facets(facet_group_name)

        # Promote the default sort order to the front of the list,
        # adding it if necessary.
        order = cls.DEFAULT_SORT_ORDER
        if order in default:
            default = [x for x in default if x != order]
        return [order] + default

    @classmethod
    def default_facet(cls, config, facet_group_name):
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
        self, minimum_featured_quality, entrypoint=None, random_seed=None, **kwargs
    ):
        """Set up an object that finds featured books in a given
        WorkList.

        :param kwargs: Other arguments may be supplied based on user
            input, but the default implementation is to ignore them.
        """
        super().__init__(entrypoint=entrypoint, **kwargs)
        self.minimum_featured_quality = minimum_featured_quality
        self.random_seed = random_seed

    @classmethod
    def default(cls, lane, **kwargs):
        library = None
        if lane:
            if isinstance(lane, Library):
                library = lane
            else:
                library = lane.library

        if library:
            quality = library.settings.minimum_featured_quality
        else:
            quality = Configuration.DEFAULT_MINIMUM_FEATURED_QUALITY
        return cls(quality, **kwargs)

    def navigate(self, minimum_featured_quality=None, entrypoint=None):
        """Create a slightly different FeaturedFacets object based on this
        one.
        """
        minimum_featured_quality = (
            minimum_featured_quality or self.minimum_featured_quality
        )
        entrypoint = entrypoint or self.entrypoint
        return self.__class__(minimum_featured_quality, entrypoint)

    def modify_search_filter(self, filter):
        super().modify_search_filter(filter)
        filter.minimum_featured_quality = self.minimum_featured_quality

    def scoring_functions(self, filter):
        """Generate scoring functions that weight works randomly, but
        with 'more featurable' works tending to be at the top.
        """
        return filter.featurability_scoring_functions(self.random_seed)
