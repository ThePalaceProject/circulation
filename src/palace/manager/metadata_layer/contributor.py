from __future__ import annotations

import logging
from collections.abc import Sequence

from sqlalchemy import Boolean, or_
from sqlalchemy.orm import Session
from sqlalchemy.sql import ColumnElement
from typing_extensions import Self

from palace.manager.sqlalchemy.model.contributor import Contribution, Contributor
from palace.manager.util.log import LoggerMixin
from palace.manager.util.personal_names import display_name_to_sort_name


class ContributorData(LoggerMixin):
    def __init__(
        self,
        sort_name: str | None = None,
        display_name: str | None = None,
        family_name: str | None = None,
        wikipedia_name: str | None = None,
        roles: str | Sequence[str] | None = None,
        lc: str | None = None,
        viaf: str | None = None,
        biography: str | None = None,
        aliases: Sequence[str] | None = None,
        extra: dict[str, str] | None = None,
    ) -> None:
        self.sort_name = sort_name
        self.display_name = display_name
        self.family_name = family_name
        self.wikipedia_name = wikipedia_name
        if roles is None:
            roles = [Contributor.Role.AUTHOR]
        if isinstance(roles, str):
            roles = [roles]
        self.roles = list(roles)
        self.lc = lc
        self.viaf = viaf
        self.biography = biography
        self.aliases = list(aliases) if aliases is not None else []
        # extra is a dictionary of stuff like birthdates
        self.extra = extra or dict()
        # TODO:  consider if it's time for ContributorData to connect back to Contributions

    def __repr__(self) -> str:
        return (
            '<ContributorData sort="%s" display="%s" family="%s" wiki="%s" roles=%r lc=%s viaf=%s>'
            % (
                self.sort_name,
                self.display_name,
                self.family_name,
                self.wikipedia_name,
                self.roles,
                self.lc,
                self.viaf,
            )
        )

    @classmethod
    def from_contribution(cls, contribution: Contribution) -> Self:
        """Create a ContributorData object from a data-model Contribution
        object.
        """
        c = contribution.contributor
        return cls(
            sort_name=c.sort_name,
            display_name=c.display_name,
            family_name=c.family_name,
            wikipedia_name=c.wikipedia_name,
            lc=c.lc,
            viaf=c.viaf,
            biography=c.biography,
            aliases=c.aliases,
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

        existing_aliases = set(destination.aliases or [])
        new_aliases = list(destination.aliases or [])
        for name in [self.sort_name] + self.aliases:
            if name != destination.sort_name and name not in existing_aliases:
                new_aliases.append(name)
                made_changes = True
        if new_aliases != destination.aliases:
            destination.aliases = new_aliases
            made_changes = True

        for k, v in list(self.extra.items()):
            if not k in destination.extra:
                destination.extra[k] = v

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

    def find_sort_name(self, _db: Session) -> bool:
        """Try as hard as possible to find this person's sort name."""
        if self.sort_name:
            return True

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
        if sort_name:
            self.sort_name = sort_name
            return True

        # If there's still no sort name, take our best guess based
        # on the display name.
        self.sort_name = display_name_to_sort_name(self.display_name)

        return self.sort_name is not None

    @classmethod
    def display_name_to_sort_name_from_existing_contributor(
        self, _db: Session, display_name: str
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
            log = logging.getLogger("Abstract metadata layer")
            log.debug(
                "Determined that sort name of %s is %s based on previously existing contributor",
                display_name,
                contributors[0].sort_name,
            )
            return contributors[0].sort_name  # type: ignore[no-any-return]
        return None
