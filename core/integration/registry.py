from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from typing import Generic, TypeVar, overload

from core.integration.goals import Goals

T = TypeVar("T", covariant=True)
V = TypeVar("V")


class IntegrationRegistryException(ValueError):
    """An error occurred while registering an integration."""


class IntegrationRegistry(Generic[T]):
    def __init__(self, goal: Goals, integrations: dict[str, type[T]] | None = None):
        """Initialize a new IntegrationRegistry."""
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
        aliases: list[str] | None = None,
    ) -> type[T]:
        """
        Register an integration class.

        If no canonical protocol name is provided, the integration class's
        name will be used, otherwise the class name will be added as an alias.

        Aliases are additional names that can be used to look up the integration
        class.
        """

        if aliases is None:
            aliases = []

        if canonical is None:
            canonical = integration.__name__
        else:
            aliases.append(integration.__name__)

        for protocol in [canonical] + aliases:
            if protocol in self._lookup and self._lookup[protocol] != integration:
                raise IntegrationRegistryException(
                    f"Integration {protocol} already registered"
                )
            self._lookup[protocol] = integration
            self._reverse_lookup[integration].append(protocol)

        return integration

    @overload
    def get(self, protocol: str, default: None = ...) -> type[T] | None:
        ...

    @overload
    def get(self, protocol: str, default: V) -> type[T] | V:
        ...

    def get(self, protocol: str, default: V | None = None) -> type[T] | V | None:
        """Look up an integration class by protocol."""
        if protocol not in self._lookup:
            return default
        return self[protocol]

    @overload
    def get_protocol(self, integration: type[T], default: None = ...) -> str | None:
        ...

    @overload
    def get_protocol(self, integration: type[T], default: V) -> str | V:
        ...

    def get_protocol(
        self, integration: type[T], default: V | None = None
    ) -> str | V | None:
        """Look up the canonical protocol for an integration class."""
        names = self.get_protocols(integration, default)
        if not isinstance(names, list):
            return default
        return names[0]

    @overload
    def get_protocols(
        self, integration: type[T], default: None = ...
    ) -> list[str] | None:
        ...

    @overload
    def get_protocols(self, integration: type[T], default: V) -> list[str] | V:
        ...

    def get_protocols(
        self, integration: type[T], default: V | None = None
    ) -> list[str] | V | None:
        """Look up all protocols for an integration class."""
        if integration not in self._reverse_lookup:
            return default
        return self._reverse_lookup[integration]

    @property
    def integrations(self) -> set[type[T]]:
        """Return a set of all registered canonical protocols."""
        return set(self._reverse_lookup.keys())

    def update(self, other: IntegrationRegistry[T]) -> None:
        """Update registry to include integrations in other."""
        if self.goal != other.goal:
            raise IntegrationRegistryException(
                f"IntegrationRegistry's goals must be the same. (Self: {self.goal}, Other: {other.goal})"
            )

        for integration in other.integrations:
            names = other.get_protocols(integration)
            assert isinstance(names, list)
            self.register(integration, canonical=names[0], aliases=names[1:])

    def __iter__(self) -> Iterator[tuple[str, type[T]]]:
        for integration, names in self._reverse_lookup.items():
            yield names[0], integration

    def __getitem__(self, protocol: str) -> type[T]:
        """Look up an integration class by protocol, using the [] operator."""
        return self._lookup[protocol]

    def __len__(self) -> int:
        """Return the number of registered integration classes."""
        return len(self._reverse_lookup)

    def __contains__(self, name: str) -> bool:
        """Return whether an integration class is registered under the given name."""
        return name in self._lookup

    def __repr__(self) -> str:
        return f"<IntegrationRegistry: {self._lookup}>"

    def __add__(self, other: IntegrationRegistry[V]) -> IntegrationRegistry[T | V]:
        if not isinstance(other, IntegrationRegistry):
            raise TypeError(
                f"unsupported operand type(s) for +: 'IntegrationRegistry' and '{type(other).__name__}'"
            )

        new: IntegrationRegistry[T | V] = IntegrationRegistry(self.goal)
        new.update(self)
        new.update(other)
        return new
