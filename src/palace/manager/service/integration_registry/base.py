from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Iterator
from itertools import chain
from typing import Generic, Literal, TypeVar, overload

from sqlalchemy import select
from sqlalchemy.sql import Select

from palace.manager.core.exceptions import BasePalaceException, PalaceValueError
from palace.manager.integration.goals import Goals
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.util.sentinel import SentinelType

T = TypeVar("T", covariant=True)
V = TypeVar("V")


class RegistrationException(BasePalaceException, ValueError):
    """An error occurred while registering an integration."""


class LookupException(BasePalaceException, LookupError):
    """An error occurred while looking up an integration."""


class IntegrationRegistry(Generic[T]):
    def __init__(self, goal: Goals, integrations: dict[str, type[T]] | None = None):
        """
        Initialize a new IntegrationRegistry.

        :param goal: The integration goal this registry manages (e.g., LICENSE_GOAL, METADATA_GOAL)
        :param integrations: Optional dictionary mapping protocol names to integration classes to register
        """
        self._lookup: dict[str, type[T]] = {}
        self._reverse_lookup: dict[type[T], list[str]] = defaultdict(list)
        self.goal = goal

        if integrations:
            for protocol, integration in integrations.items():
                self.register(integration, canonical=protocol)

    def register(
        self,
        integration: type[T],
        *,
        canonical: str | None = None,
        aliases: Iterable[str] | None = None,
    ) -> type[T]:
        """
        Register an integration class.

        If no canonical protocol name is provided, the integration class's
        name will be used, otherwise the class name will be added as an alias.

        Aliases are additional names that can be used to look up the integration
        class.

        :param integration: The integration class to register
        :param canonical: The canonical protocol name (defaults to integration.__name__)
        :param aliases: Additional protocol names that can be used to look up the integration
        :return: The registered integration class
        :raises RegistrationException: If a protocol name is already registered to a different integration
        """

        if canonical is None:
            canonical = integration.__name__
        # Use a dict to preserve order and ensure uniqueness of names
        names = dict.fromkeys(chain([canonical], aliases or [], [integration.__name__]))
        for protocol in names.keys():
            if protocol in self._lookup and self._lookup[protocol] != integration:
                raise RegistrationException(
                    f"Integration {protocol} already registered"
                )
            self._lookup[protocol] = integration
        self._reverse_lookup[integration] = list(names.keys())

        return integration

    @overload
    def get(self, protocol: str) -> type[T]: ...

    @overload
    def get(self, protocol: str, default: V) -> type[T] | V: ...

    def get(
        self,
        protocol: str,
        default: V | Literal[SentinelType.NotGiven] = SentinelType.NotGiven,
    ) -> type[T] | V:
        """
        Look up an integration class by protocol.

        :param protocol: Protocol name (canonical or alias) to look up
        :param default: Value to return if protocol is not found. If not provided,
                       raises LookupException when protocol is not found.
        :return: The integration class if found, otherwise the default value
        :raises LookupException: If protocol is not found and no default is provided
        """
        if protocol not in self._lookup:
            if default is SentinelType.NotGiven:
                raise LookupException(f"Integration {protocol} not found")
            return default
        return self[protocol]

    @overload
    def get_protocol(self, integration: type[T]) -> str: ...

    @overload
    def get_protocol(self, integration: type[T], default: V) -> str | V: ...

    def get_protocol(
        self,
        integration: type[T],
        default: V | Literal[SentinelType.NotGiven] = SentinelType.NotGiven,
    ) -> str | V:
        """
        Look up the canonical protocol for an integration class.

        :param integration: The integration class to look up
        :param default: Value to return if integration is not found. If not provided,
                       raises LookupException when integration is not found.
        :return: The canonical protocol name if found, otherwise the default value
        :raises LookupException: If integration is not found and no default is provided
        """
        if integration not in self._reverse_lookup:
            if default is SentinelType.NotGiven:
                raise LookupException(f"Integration {integration} not found")
            return default
        return self._reverse_lookup[integration][0]

    @overload
    def get_protocols(self, integration: type[T]) -> list[str]: ...

    @overload
    def get_protocols(self, integration: type[T], default: V) -> list[str] | V: ...

    def get_protocols(
        self,
        integration: type[T],
        default: V | Literal[SentinelType.NotGiven] = SentinelType.NotGiven,
    ) -> list[str] | V:
        """
        Look up all protocols for an integration class.

        Returns all protocol names (canonical and aliases) associated with the
        integration class. The canonical name is always first in the list.

        :param integration: The integration class to look up
        :param default: Value to return if integration is not found. If not provided,
                       raises LookupException when integration is not found.
        :return: List of protocol names (canonical first, then aliases) if found, otherwise the default value
        :raises LookupException: If integration is not found and no default is provided
        """
        if integration not in self._reverse_lookup:
            if default is SentinelType.NotGiven:
                raise LookupException(f"Integration {integration} not found")
            return default
        return self._reverse_lookup[integration]

    @property
    def integrations(self) -> set[type[T]]:
        """Return a set of all registered integration classes."""
        return set(self._reverse_lookup.keys())

    def update(self, other: IntegrationRegistry[T]) -> None:
        """
        Update this registry to include all integrations from another registry.

        All integration classes from the other registry are registered into this
        registry with their canonical names and aliases preserved.

        :param other: Another IntegrationRegistry with the same goal
        :raises RegistrationException: If registries have different goals or if registration conflicts occur
        """
        if self.goal != other.goal:
            raise RegistrationException(
                f"IntegrationRegistry's goals must be the same. (Self: {self.goal}, Other: {other.goal})"
            )

        for integration in other.integrations:
            names = other.get_protocols(integration)
            self.register(integration, canonical=names[0], aliases=names[1:])

    def canonicalize(self, protocol: str) -> str:
        """
        Return the canonical protocol name for a given protocol.

        :param protocol: A protocol name (canonical or alias)
        :return: The canonical protocol name
        :raises LookupException: If the protocol is not registered
        """
        return self.get_protocol(self[protocol])

    def equivalent(
        self, protocol1: str | type[T] | None, protocol2: str | type[T] | None
    ) -> bool:
        """
        Check whether two protocols or integration classes are equivalent.

        Two protocols are considered equivalent if they resolve to the same
        integration class. Protocol names can be canonical names or aliases.

        :param protocol1: A protocol name, integration class, or None
        :param protocol2: A protocol name, integration class, or None
        :return: True if both resolve to the same integration class, False otherwise
        """
        if isinstance(protocol1, str):
            protocol1 = self.get(protocol1, None)

        if isinstance(protocol2, str):
            protocol2 = self.get(protocol2, None)

        if protocol1 is None or protocol2 is None:
            return False

        return protocol1 is protocol2

    def configurations_query(self, *protocols_or_integrations: str | type[T]) -> Select:
        """
        Create a SQLAlchemy query to select IntegrationConfiguration records.

        This function builds a query to find all integration configurations matching
        one or more protocols or integration classes, filtering by the registry's goal.

        It takes care to make sure that protocol aliases are looked up correctly,
        so that the query can be used if the integration is saved in the database
        using an alias or the canonical name.

        :param protocols_or_integrations: One or more protocol names (str) or integration classes
        :raises PalaceValueError: If no protocols or integrations are provided
        :return: A SQLAlchemy Select query
        """
        if not protocols_or_integrations:
            raise PalaceValueError(
                "At least one protocol or integration must be provided"
            )

        integrations = {
            (
                self[protocol_or_integration]
                if isinstance(protocol_or_integration, str)
                else protocol_or_integration
            )
            for protocol_or_integration in protocols_or_integrations
        }

        protocols = set(
            chain.from_iterable(
                self.get_protocols(integration) for integration in integrations
            )
        )

        configurations_query = select(IntegrationConfiguration).where(
            IntegrationConfiguration.goal == self.goal,
        )

        if len(protocols) == 1:
            configurations_query = configurations_query.where(
                IntegrationConfiguration.protocol == next(iter(protocols))
            )
        else:
            configurations_query = configurations_query.where(
                IntegrationConfiguration.protocol.in_(protocols)
            )
        return configurations_query

    def __iter__(self) -> Iterator[tuple[str, type[T]]]:
        """
        Iterate over registered integrations.

        :return: Iterator of (canonical_protocol, integration_class) tuples
        """
        for integration, names in self._reverse_lookup.items():
            yield names[0], integration

    def __getitem__(self, protocol: str) -> type[T]:
        """
        Look up an integration class by protocol name using the [] operator.

        :param protocol: Protocol name (canonical or alias)
        :return: The integration class registered under the given protocol
        :raises LookupException: If the protocol is not registered
        """
        try:
            return self._lookup[protocol]
        except KeyError as e:
            raise LookupException(f"Integration {protocol} not found") from e

    def __len__(self) -> int:
        """Return the number of registered integration classes."""
        return len(self._reverse_lookup)

    def __contains__(self, name: str) -> bool:
        """
        Check if a protocol name is registered (supports 'in' operator).

        :param name: Protocol name to check (canonical or alias)
        :return: True if the protocol name is registered, False otherwise
        """
        return name in self._lookup

    def __repr__(self) -> str:
        """
        Return a string representation of the registry.
        """
        return f"<IntegrationRegistry: {self._lookup}>"

    def __add__(self, other: IntegrationRegistry[V]) -> IntegrationRegistry[T | V]:
        """
        Combine two registries using the + operator.

        Creates a new registry containing all integrations from both registries.
        Both registries must have the same goal.

        :param other: Another IntegrationRegistry to combine with this one
        :return: A new IntegrationRegistry containing integrations from both registries
        :raises TypeError: If other is not an IntegrationRegistry
        :raises RegistrationException: If registries have different goals
        """
        if not isinstance(other, IntegrationRegistry):
            raise TypeError(
                f"unsupported operand type(s) for +: 'IntegrationRegistry' and '{type(other).__name__}'"
            )

        new: IntegrationRegistry[T | V] = IntegrationRegistry(self.goal)
        new.update(self)
        new.update(other)
        return new
