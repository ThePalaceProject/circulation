from __future__ import annotations

from collections import defaultdict
from typing import (
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    overload,
)

from core.integration.goals import Goals

T = TypeVar("T")
V = TypeVar("V")


class IntegrationRegistryException(ValueError):
    """An error occurred while registering an integration."""


class IntegrationRegistry(Generic[T]):
    def __init__(self, goal: Goals):
        """Initialize a new IntegrationRegistry."""
        self._lookup: Dict[str, Type[T]] = {}
        self._reverse_lookup: Dict[Type[T], List[str]] = defaultdict(list)

        self.goal = goal

    def register(
        self,
        integration: Type[T],
        *,
        canonical: Optional[str] = None,
        aliases: Optional[List[str]] = None,
    ) -> Type[T]:
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
    def get(self, protocol: str, default: None = ...) -> Type[T] | None:
        ...

    @overload
    def get(self, protocol: str, default: V) -> Type[T] | V:
        ...

    def get(self, protocol: str, default: V | None = None) -> Type[T] | V | None:
        """Look up an integration class by protocol."""
        if protocol not in self._lookup:
            return default
        return self[protocol]

    @overload
    def get_protocol(self, integration: Type[T], default: None = ...) -> str | None:
        ...

    @overload
    def get_protocol(self, integration: Type[T], default: V) -> str | V:
        ...

    def get_protocol(
        self, integration: Type[T], default: V | None = None
    ) -> str | V | None:
        """Look up the canonical protocol for an integration class."""
        names = self.get_protocols(integration, default)
        if not isinstance(names, list):
            return default
        return names[0]

    @overload
    def get_protocols(
        self, integration: Type[T], default: None = ...
    ) -> List[str] | None:
        ...

    @overload
    def get_protocols(self, integration: Type[T], default: V) -> List[str] | V:
        ...

    def get_protocols(
        self, integration: Type[T], default: V | None = None
    ) -> List[str] | V | None:
        """Look up all protocols for an integration class."""
        if integration not in self._reverse_lookup:
            return default
        return self._reverse_lookup[integration]

    @property
    def integrations(self) -> Set[Type[T]]:
        """Return a set of all registered canonical protocols."""
        return set(self._reverse_lookup.keys())

    def __iter__(self) -> Iterator[Tuple[str, Type[T]]]:
        for integration, names in self._reverse_lookup.items():
            yield names[0], integration

    def __getitem__(self, protocol: str) -> Type[T]:
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
