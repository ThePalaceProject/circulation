from __future__ import annotations

from itertools import chain
from typing import Any, Literal, Self

from frozendict import frozendict
from sqlalchemy import Boolean, or_
from sqlalchemy.orm import Session
from sqlalchemy.sql import ColumnElement

from palace.manager.data_layer.base.frozen import BaseFrozenData
from palace.manager.sqlalchemy.model.contributor import Contribution, Contributor
from palace.manager.util.log import LoggerMixin
from palace.manager.util.personal_names import display_name_to_sort_name
from palace.manager.util.pydantic import FrozenDict
from palace.manager.util.sentinel import SentinelType


class ContributorData(BaseFrozenData, LoggerMixin):
    sort_name: str | None = None
    display_name: str | None = None
    family_name: str | None = None
    wikipedia_name: str | None = None
    roles: tuple[str, ...] = (Contributor.Role.AUTHOR,)
    lc: str | None = None
    viaf: str | None = None
    biography: str | None = None
    aliases: tuple[str, ...] = tuple()
    extra: FrozenDict[str, str] = frozendict()
    # TODO:  consider if it's time for ContributorData to connect back to Contributions

    _cached_sort_name: str | None | Literal[SentinelType.NotGiven] = (
        SentinelType.NotGiven
    )
    """
    A cached version of the sort name. This is set in model_post_init if sort_name is given,
    otherwise it is set in find_sort_name.
    """

    def model_post_init(self, context: Any) -> None:
        if self.sort_name is not None:
            self._cached_sort_name = self.sort_name

    @classmethod
    def from_contributor(
        cls, contributor: Contributor, *, roles: list[str] | None = None
    ) -> Self:
        if roles is None:
            roles = []
        return cls(
            sort_name=contributor.sort_name,
            display_name=contributor.display_name,
            family_name=contributor.family_name,
            wikipedia_name=contributor.wikipedia_name,
            lc=contributor.lc,
            viaf=contributor.viaf,
            biography=contributor.biography,
            aliases=contributor.aliases,
            roles=roles,
            extra=contributor.extra,
        )

    @classmethod
    def from_contribution(cls, contribution: Contribution) -> Self:
        """Create a ContributorData object from a data-model Contribution
        object.
        """
        return cls.from_contributor(
            contribution.contributor,
            roles=[contribution.role],
        )

    @classmethod
    def lookup(
        cls,
        _db: Session,
        sort_name: str | None = None,
        display_name: str | None = None,
        lc: str | None = None,
        viaf: str | None = None,
    ) -> Self | None:
        """Create a (potentially synthetic) ContributorData based on
        the best available information in the database.

        :return: A ContributorData.
        """
        clauses: list[ColumnElement[Boolean]] = []
        if sort_name:
            # Mypy doesn't like this one because Contributor.sort_name is a HybridProperty, so we just
            # ignore the type check here, since it is a valid comparison.
            clauses.append(Contributor.sort_name == sort_name)  # type: ignore[arg-type, comparison-overlap]
        if display_name:
            clauses.append(Contributor.display_name == display_name)
        if lc:
            clauses.append(Contributor.lc == lc)
        if viaf:
            clauses.append(Contributor.viaf == viaf)

        if not clauses:
            raise ValueError("No Contributor information provided!")

        or_clause = or_(*clauses)
        contributors = _db.query(Contributor).filter(or_clause).all()
        if len(contributors) == 0:
            # We have no idea who this person is.
            return None

        # We found at least one matching Contributor. Let's try to
        # build a composite ContributorData for the person.
        sort_name_values = set()
        display_name_values = set()
        lc_values = set()
        viaf_values = set()

        # If all the people we found share (e.g.) a VIAF field, then
        # we can use that as a clue when doing a search -- anyone with
        # that VIAF number is probably this person, even if their display
        # name doesn't match.
        for c in contributors:
            if c.sort_name:
                sort_name_values.add(c.sort_name)
            if c.display_name:
                display_name_values.add(c.display_name)
            if c.lc:
                lc_values.add(c.lc)
            if c.viaf:
                viaf_values.add(c.viaf)

        # Use any passed-in values as default values for the
        # ContributorData. If all the Contributors we found have the
        # same value for a field, we can use it to supplement the
        # default values.
        if len(sort_name_values) == 1:
            sort_name = sort_name_values.pop()
        if len(display_name_values) == 1:
            display_name = display_name_values.pop()
        if len(lc_values) == 1:
            lc = lc_values.pop()
        if len(viaf_values) == 1:
            viaf = viaf_values.pop()

        return cls(
            roles=[], sort_name=sort_name, display_name=display_name, lc=lc, viaf=viaf
        )

    def apply(self, destination: Contributor) -> tuple[Contributor, bool]:
        """Update the passed-in Contributor-type object with this
        ContributorData's information.

        :param: destination -- the Contributor or ContributorData object to
            write this ContributorData object's metadata to.

        :return: the possibly changed Contributor object and a flag of whether it's been changed.
        """
        self.log.debug(
            "Applying %r (%s) into %r (%s)",
            self,
            self.viaf,
            destination,
            destination.viaf,
        )

        made_changes = False

        if self.sort_name and self.sort_name != destination.sort_name:
            destination.sort_name = self.sort_name
            made_changes = True

        existing_aliases = set(destination.aliases)
        new_aliases = list(destination.aliases)
        for name in chain([self.sort_name], self.aliases):
            if (
                name is not None
                and name != destination.sort_name
                and name not in existing_aliases
            ):
                new_aliases.append(name)
                made_changes = True
        if new_aliases != destination.aliases:
            destination.aliases = new_aliases
            made_changes = True

        if destination.extra != self.extra:
            destination.extra.update(self.extra)
            made_changes = True

        if self.lc and self.lc != destination.lc:
            destination.lc = self.lc
            made_changes = True
        if self.viaf and self.viaf != destination.viaf:
            destination.viaf = self.viaf
            made_changes = True
        if self.family_name and self.family_name != destination.family_name:
            destination.family_name = self.family_name
            made_changes = True
        if self.display_name and self.display_name != destination.display_name:
            destination.display_name = self.display_name
            made_changes = True
        if self.wikipedia_name and self.wikipedia_name != destination.wikipedia_name:
            destination.wikipedia_name = self.wikipedia_name
            made_changes = True

        if self.biography and self.biography != destination.biography:
            destination.biography = self.biography
            made_changes = True

        # TODO:  Contributor.merge_into also looks at
        # contributions.  Could maybe extract contributions from roles,
        # but not sure it'd be useful.

        return destination, made_changes

    def find_sort_name(self, _db: Session) -> str | None:
        """Try as hard as possible to find this person's sort name."""
        if self._cached_sort_name is not SentinelType.NotGiven:
            return self._cached_sort_name

        if not self.display_name:
            raise ValueError(
                "Cannot find sort name for a contributor with no display name!"
            )

        # Is there a contributor already in the database with this
        # exact sort name? If so, use their display name.
        # If not, take our best guess based on the display name.
        sort_name = self.display_name_to_sort_name_from_existing_contributor(
            _db, self.display_name
        )

        if not sort_name:
            # If there's still no sort name, take our best guess based
            # on the display name.
            sort_name = display_name_to_sort_name(self.display_name)

        self._cached_sort_name = sort_name
        return sort_name

    @classmethod
    def display_name_to_sort_name_from_existing_contributor(
        cls, _db: Session, display_name: str
    ) -> str | None:
        """Find the sort name for this book's author, assuming it's easy.

        'Easy' means we already have an established sort name for a
        Contributor with this exact display name.

        If we have a copy of this book in our collection (the only
        time an external list item is relevant), this will probably be
        easy.
        """
        contributors = (
            _db.query(Contributor)
            .filter(Contributor.display_name == display_name)
            .filter(Contributor.sort_name != None)
            .all()
        )
        if contributors:
            cls.logger().debug(
                "Determined that sort name of %s is %s based on previously existing contributor",
                display_name,
                contributors[0].sort_name,
            )
            return contributors[0].sort_name  # type: ignore[no-any-return]
        return None
