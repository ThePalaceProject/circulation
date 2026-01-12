from __future__ import annotations

import logging
import sys
import unicodedata
from collections.abc import Sequence
from typing import Any

from sqlalchemy.orm import Query, Session

from palace.manager.scripts.input import IdentifierInputScript, SupportsReadlines
from palace.manager.sqlalchemy.model.contributor import Contribution, Contributor
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.util.personal_names import (
    contributor_name_match_ratio,
    display_name_to_sort_name,
)


class CheckContributorNamesInDB(IdentifierInputScript):
    """Checks that contributor sort_names are display_names in
    "last name, comma, other names" format.

    Read contributors edition by edition, so that can, if necessary,
    restrict db query by passed-in identifiers, and so can find associated
    license pools to register author complaints to.

    NOTE:  There's also CheckContributorNamesOnWeb in metadata,
    it's a child of this script.  Use it to check our knowledge against
    viaf, with the newer better sort_name selection and formatting.

    TODO: make sure don't start at beginning again when interrupt while batch job is running.
    """

    COMPLAINT_SOURCE = "CheckContributorNamesInDB"
    COMPLAINT_TYPE = "http://librarysimplified.org/terms/problem/wrong-author"

    def __init__(
        self,
        _db: Session | None = None,
        cmd_args: Sequence[str | None] | None = None,
        stdin: SupportsReadlines = sys.stdin,
    ) -> None:
        super().__init__(_db=_db)

        self.parsed_args = self.parse_command_line(
            _db=self._db, cmd_args=cmd_args, stdin=stdin
        )

    @classmethod
    def make_query(
        cls,
        _db: Session,
        identifier_type: str | None,
        identifiers: Sequence[Identifier] | None,
        log: logging.Logger | None = None,
    ) -> Query[Edition]:
        query = _db.query(Edition)
        if identifiers or identifier_type:
            query = query.join(Edition.primary_identifier)

        # we only want to look at editions with license pools, in case we want to make a Complaint
        query = query.join(Edition.is_presentation_for)

        if identifiers:
            if log:
                log.info("Restricted to %d specific identifiers." % len(identifiers))
            query = query.filter(
                Edition.primary_identifier_id.in_([x.id for x in identifiers])
            )
        if identifier_type:
            if log:
                log.info('Restricted to identifier type "%s".' % identifier_type)
            query = query.filter(Identifier.type == identifier_type)

        if log:
            log.info("Processing %d editions.", query.count())

        return query.order_by(Edition.id)

    def do_run(self, batch_size: int = 10) -> None:
        self.query = self.make_query(
            self._db,
            self.parsed_args.identifier_type,
            self.parsed_args.identifiers,
            self.log,
        )

        offset = 0
        output = "ContributorID|\tSortName|\tDisplayName|\tComputedSortName|\tResolution|\tComplaintSource"
        print(output.encode("utf8"))

        while True:
            my_query = self.query.offset(offset).limit(batch_size)
            editions = my_query.all()
            if not editions:
                break

            for edition in editions:
                if edition.contributions:
                    for contribution in edition.contributions:
                        self.process_contribution_local(
                            self._db, contribution, self.log
                        )
            offset += batch_size

            self._db.commit()
        self._db.commit()

    def process_local_mismatch(self, **kwargs: Any) -> None:
        """XXX: This used to produce a Complaint, but the complaint system no longer exists..."""
        return None

    def process_contribution_local(
        self,
        _db: Session,
        contribution: Contribution | None,
        log: logging.Logger | None = None,
    ) -> None:
        if not contribution or not contribution.edition:
            return

        contributor = contribution.contributor

        identifier = contribution.edition.primary_identifier

        if contributor.sort_name and contributor.display_name:
            computed_sort_name_local_new = unicodedata.normalize(
                "NFKD", str(display_name_to_sort_name(contributor.display_name))
            )
            # Did HumanName parser produce a differet result from the plain comma replacement?
            if (
                contributor.sort_name.strip().lower()
                != computed_sort_name_local_new.strip().lower()
            ):
                error_message_detail = (
                    "Contributor[id=%s].sort_name is oddly different from computed_sort_name, human intervention required."
                    % contributor.id
                )

                # computed names don't match.  by how much?  if it's a matter of a comma or a misplaced
                # suffix, we can fix without asking for human intervention.  if the names are very different,
                # there's a chance the sort and display names are different on purpose, s.a. when foreign names
                # are passed as translated into only one of the fields, or when the author has a popular pseudonym.
                # best ask a human.

                # if the relative lengths are off by more than a stray space or comma, ask a human
                # it probably means that a human metadata professional had added an explanation/expansion to the
                # sort_name, s.a. "Bob A. Jones" --> "Bob A. (Allan) Jones", and we'd rather not replace this data
                # with the "Jones, Bob A." that the auto-algorigthm would generate.
                length_difference = len(contributor.sort_name.strip()) - len(
                    computed_sort_name_local_new.strip()
                )
                if abs(length_difference) > 3:
                    return self.process_local_mismatch(
                        _db=_db,
                        contribution=contribution,
                        computed_sort_name=computed_sort_name_local_new,
                        error_message_detail=error_message_detail,
                        log=log,
                    )

                match_ratio = contributor_name_match_ratio(
                    contributor.sort_name,
                    computed_sort_name_local_new,
                    normalize_names=False,
                )

                if match_ratio < 40:
                    # ask a human.  this kind of score can happen when the sort_name is a transliteration of the display_name,
                    # and is non-trivial to fix.
                    self.process_local_mismatch(
                        _db=_db,
                        contribution=contribution,
                        computed_sort_name=computed_sort_name_local_new,
                        error_message_detail=error_message_detail,
                        log=log,
                    )
                else:
                    # we can fix it!
                    output = "{}|\t{}|\t{}|\t{}|\tlocal_fix".format(
                        contributor.id,
                        contributor.sort_name,
                        contributor.display_name,
                        computed_sort_name_local_new,
                    )
                    print(output.encode("utf8"))
                    self.set_contributor_sort_name(
                        computed_sort_name_local_new, contribution
                    )

    @classmethod
    def set_contributor_sort_name(
        cls, sort_name: str, contribution: Contribution
    ) -> None:
        """Sets the contributor.sort_name and associated edition.author_name to the passed-in value."""
        contribution.contributor.sort_name = sort_name

        # also change edition.sort_author, if the author was primary
        # Note: I considered using contribution.edition.author_contributors, but
        # found that it's not impossible to have a messy dataset that doesn't work on.
        # For our purpose here, the following logic is cleaner-acting:
        # If this author appears as Primary Author anywhere on the edition, then change edition.sort_author.
        edition_contributions = contribution.edition.contributions
        for edition_contribution in edition_contributions:
            if (edition_contribution.role == Contributor.Role.PRIMARY_AUTHOR) and (
                edition_contribution.contributor.display_name
                == contribution.contributor.display_name
            ):
                contribution.edition.sort_author = sort_name
