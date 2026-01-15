from __future__ import annotations

import argparse
import sys
from collections.abc import Iterator, Sequence
from datetime import datetime
from decimal import Decimal
from typing import Any, TextIO

from sqlalchemy import and_, exists, select
from sqlalchemy.orm import Query, Session

from palace.manager.integration.goals import Goals
from palace.manager.scripts.base import Script
from palace.manager.scripts.input import (
    CollectionInputScript,
    IdentifierInputScript,
    LibraryInputScript,
    SupportsReadlines,
)
from palace.manager.search.external_search import ExternalSearchIndex, Filter
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import CoverageRecord
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.model.lane import Lane
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.languages import LanguageCodes


class ShowLibrariesScript(Script):
    """Show information about the libraries on a server."""

    name = "List the libraries on this server."

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--short-name",
            help="Only display information for the library with the given short name",
        )
        parser.add_argument(
            "--show-secrets",
            help="Print out secrets associated with the library.",
            action="store_true",
        )
        return parser

    def do_run(
        self,
        _db: Session | None = None,
        cmd_args: Sequence[str | None] | None = None,
        output: TextIO = sys.stdout,
    ) -> None:
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)
        if args.short_name:
            library = get_one(_db, Library, short_name=args.short_name)
            if not library:
                output.write(
                    f"Could not locate library by short name: {args.short_name}\n"
                )
                return
            libraries = [library]
        else:
            libraries = _db.query(Library).order_by(Library.name).all()
        if not libraries:
            output.write("No libraries found.\n")
        for library in libraries:
            output.write("\n".join(library.explain(include_secrets=args.show_secrets)))
            output.write("\n")


class ShowCollectionsScript(Script):
    """Show information about the collections on a server."""

    name = "List the collections on this server."

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--name",
            help="Only display information for the collection with the given name",
        )
        parser.add_argument(
            "--show-secrets",
            help="Display secret values such as passwords.",
            action="store_true",
        )
        return parser

    def do_run(
        self,
        _db: Session | None = None,
        cmd_args: Sequence[str | None] | None = None,
        output: TextIO = sys.stdout,
    ) -> None:
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)
        if args.name:
            name = args.name
            collection = Collection.by_name(_db, name)
            if collection:
                collections = [collection]
            else:
                output.write(f"Could not locate collection by name: {name}\n")
                collections = []
        else:
            collections = (
                _db.execute(
                    select(Collection)
                    .join(IntegrationConfiguration)
                    .where(IntegrationConfiguration.goal == Goals.LICENSE_GOAL)
                    .order_by(IntegrationConfiguration.name)
                )
                .scalars()
                .all()
            )
        if not collections:
            output.write("No collections found.\n")
        for collection in collections:
            output.write(
                "\n".join(collection.explain(include_secrets=args.show_secrets))
            )
            output.write("\n")


class ShowIntegrationsScript(Script):
    """Show information about the integrations on a server."""

    name = "List the integrations on this server."

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--name",
            help="Only display information for the integration with the given name or ID",
        )
        parser.add_argument(
            "--show-secrets",
            help="Display secret values such as passwords.",
            action="store_true",
        )
        return parser

    def do_run(
        self,
        _db: Session | None = None,
        cmd_args: Sequence[str | None] | None = None,
        output: TextIO = sys.stdout,
    ) -> None:
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)
        if args.name:
            name = args.name
            integration = get_one(_db, IntegrationConfiguration, name=name)
            if not integration:
                integration = get_one(_db, IntegrationConfiguration, id=name)
            if integration:
                integrations = [integration]
            else:
                output.write("Could not locate integration by name or ID: %s\n" % args)
                integrations = []
        else:
            integrations = (
                _db.query(IntegrationConfiguration)
                .order_by(IntegrationConfiguration.name)
                .all()
            )
        if not integrations:
            output.write("No integrations found.\n")
        for integration in integrations:
            output.write(
                "\n".join(integration.explain(include_secrets=args.show_secrets))
            )
            output.write("\n\n")


class ShowLanesScript(Script):
    """Show information about the lanes on a server."""

    name = "List the lanes on this server."

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--id",
            help="Only display information for the lane with the given ID",
        )
        return parser

    def do_run(
        self,
        _db: Session | None = None,
        cmd_args: Sequence[str | None] | None = None,
        output: TextIO = sys.stdout,
    ) -> None:
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)
        if args.id:
            id = args.id
            lane = get_one(_db, Lane, id=id)
            if lane:
                lanes = [lane]
            else:
                output.write(f"Could not locate lane with id: {id}")
                lanes = []
        else:
            lanes = _db.query(Lane).order_by(Lane.id).all()
        if not lanes:
            output.write("No lanes found.\n")
        for lane in lanes:
            output.write("\n".join(lane.explain()))
            output.write("\n\n")


class Explain(IdentifierInputScript):
    """Explain everything known about a given work."""

    name = "Explain everything known about a given work"

    # Where to go to get best available metadata about a work.
    METADATA_URL_TEMPLATE = "http://metadata.librarysimplified.org/lookup?urn=%s"
    TIME_FORMAT = "%Y-%m-%d %H:%M"

    def do_run(
        self,
        cmd_args: Sequence[str | None] | None = None,
        stdin: SupportsReadlines = sys.stdin,
        stdout: TextIO = sys.stdout,
    ) -> None:
        param_args = self.parse_command_line(self._db, cmd_args=cmd_args, stdin=stdin)
        identifier_ids = [x.id for x in param_args.identifiers]
        editions = self._db.query(Edition).filter(
            Edition.primary_identifier_id.in_(identifier_ids)
        )
        self.stdout = stdout

        policy = None
        for edition in editions:
            self.explain(self._db, edition, policy)
            self.write("-" * 80)

    def write(self, s: str) -> None:
        """Write a string to self.stdout."""
        if not s.endswith("\n"):
            s += "\n"
        self.stdout.write(s)

    def explain(
        self,
        _db: Session,
        edition: Edition,
        presentation_calculation_policy: Any | None = None,
    ) -> None:
        if edition.medium not in ("Book", "Audio"):
            # we haven't yet decided what to display for you
            return

        # Tell about the Edition record.
        output = "{} ({}, {}) according to {}".format(
            edition.title,
            edition.author,
            edition.medium,
            edition.data_source.name,
        )
        self.write(output)
        self.write(" Permanent work ID: %s" % edition.permanent_work_id)
        self.write(
            " Metadata URL: %s "
            % (self.METADATA_URL_TEMPLATE % edition.primary_identifier.urn)
        )

        seen: set[int] = set()
        self.explain_identifier(edition.primary_identifier, True, seen, 1, 0)

        # Find all contributions, and tell about the contributors.
        if edition.contributions:
            for contribution in edition.contributions:
                self.explain_contribution(contribution)

        # Tell about the LicensePool.
        lps = edition.license_pools
        if lps:
            for lp in lps:
                self.explain_license_pool(lp)
        else:
            self.write(" No associated license pools.")

        # Tell about the Work.
        work = edition.work
        if work:
            self.explain_work(work)
        else:
            self.write(" No associated work.")

        # Note:  Can change DB state.
        if work and presentation_calculation_policy is not None:
            print("!!! About to calculate presentation!")
            work.calculate_presentation(policy=presentation_calculation_policy)
            print("!!! All done!")
            print()
            print("After recalculating presentation:")
            self.explain_work(work)

    def explain_contribution(self, contribution: Any) -> None:
        contributor_id = contribution.contributor.id
        contributor_sort_name = contribution.contributor.sort_name
        contributor_display_name = contribution.contributor.display_name
        self.write(
            " Contributor[%s]: contributor_sort_name=%s, contributor_display_name=%s, "
            % (contributor_id, contributor_sort_name, contributor_display_name)
        )

    def explain_identifier(
        self,
        identifier: Identifier,
        primary: bool,
        seen: set[int],
        strength: float | int | Decimal | None,
        level: int,
    ) -> None:
        indent = "  " * level
        if primary:
            ident = "Primary identifier"
        else:
            ident = "Identifier"
        if primary:
            strength = 1
        self.write(
            "%s %s: %s/%s (q=%s)"
            % (indent, ident, identifier.type, identifier.identifier, strength)
        )

        _db = Session.object_session(identifier)
        classifications = Identifier.classifications_for_identifier_ids(
            _db, [identifier.id]
        )
        for classification in classifications:
            subject = classification.subject
            genre_name = subject.genre.name if subject.genre else "(!genre)"
            # print("%s  %s says: %s/%s %s w=%s" % (
            #    indent, classification.data_source.name,
            #    subject.identifier, subject.name, genre_name, classification.weight
            # ))
        seen.add(identifier.id)
        for equivalency in identifier.equivalencies:
            if equivalency.id in seen:
                continue
            seen.add(equivalency.id)
            output = equivalency.output
            self.explain_identifier(
                output, False, seen, equivalency.strength, level + 1
            )
        if primary:
            crs = identifier.coverage_records
            if crs:
                self.write("  %d coverage records:" % len(crs))
                for cr in sorted(crs, key=lambda x: x.timestamp or datetime.min):
                    self.explain_coverage_record(cr)

    def explain_license_pool(self, pool: LicensePool) -> None:
        self.write("Licensepool info:")
        if pool.collection:
            self.write(" Collection: %r" % pool.collection)
            libraries = [
                library.name
                for library in pool.collection.associated_libraries
                if library.name
            ]
            if libraries:
                self.write(" Available to libraries: %s" % ", ".join(libraries))
            else:
                self.write("Not available to any libraries!")
        else:
            self.write(" Not in any collection!")
        self.write(" Delivery mechanisms:")
        if pool.delivery_mechanisms:
            for lpdm in pool.delivery_mechanisms:
                dm = lpdm.delivery_mechanism
                if dm.default_client_can_fulfill:
                    fulfillable = "Fulfillable"
                else:
                    fulfillable = "Unfulfillable"
                available = "Available" if lpdm.available else "Unavailable"
                self.write(
                    f"  {available} {fulfillable} {dm.content_type}/{dm.drm_scheme}"
                )
        else:
            self.write(" No delivery mechanisms.")
        self.write(
            " %s owned, %d available, %d holds, %d reserves"
            % (
                pool.licenses_owned,
                pool.licenses_available,
                pool.patrons_in_hold_queue,
                pool.licenses_reserved,
            )
        )

    def explain_work(self, work: Work) -> None:
        self.write("Work info:")
        if work.presentation_edition:
            self.write(
                " Identifier of presentation edition: %r"
                % work.presentation_edition.primary_identifier
            )
        else:
            self.write(" No presentation edition.")
        self.write(" Fiction: %s" % work.fiction)
        self.write(" Audience: %s" % work.audience)
        self.write(" Target age: %r" % work.target_age)
        self.write(" %s genres." % (len(work.genres)))
        for genre in work.genres:
            self.write("  %s" % genre)
        self.write(" License pools:")
        for pool in work.license_pools:
            if pool.collection:
                collection = pool.collection.name
            else:
                collection = "!collection"
            self.write(f"  ACTIVE: {pool.identifier!r} {collection}")

    def explain_coverage_record(self, cr: CoverageRecord) -> None:
        if cr.timestamp is None:
            return
        status_value = str(cr.status) if cr.status is not None else None
        operation_value = str(cr.operation) if cr.operation is not None else None
        self._explain_coverage_record(
            cr.timestamp, cr.data_source, operation_value, status_value, cr.exception
        )

    def _explain_coverage_record(
        self,
        timestamp: datetime,
        data_source: DataSource | None,
        operation: str | None,
        status: str | None,
        exception: str | None,
    ) -> None:
        timestamp_string = timestamp.strftime(self.TIME_FORMAT)
        if data_source:
            data_source_value = data_source.name + " | "
        else:
            data_source_value = ""
        if operation:
            operation_value = operation + " | "
        else:
            operation_value = ""
        if exception:
            exception_value = " | " + exception
        else:
            exception_value = ""
        self.write(
            "   {} | {}{}{}{}".format(
                timestamp_string,
                data_source_value,
                operation_value,
                status,
                exception_value,
            )
        )


class WhereAreMyBooksScript(CollectionInputScript):
    """Try to figure out why Works aren't showing up.

    This is a common problem on a new installation or when a new collection
    is being configured.
    """

    def __init__(
        self,
        _db: Session | None = None,
        output: TextIO | None = None,
        search: ExternalSearchIndex | None = None,
    ) -> None:
        _db = _db or self._db
        super().__init__(_db)
        self.output = output or sys.stdout
        self.search = search or self.services.search.index()

    def out(self, s: str, *args: Any) -> None:
        if not s.endswith("\n"):
            s += "\n"
        self.output.write(s % args)

    def run(self, cmd_args: Sequence[str | None] | None = None) -> None:
        parsed = self.parse_command_line(self._db, cmd_args=cmd_args or [])

        # Check each library.
        libraries = self._db.query(Library).all()
        if libraries:
            for library in libraries:
                self.check_library(library)
                self.out("\n")
        else:
            self.out("There are no libraries in the system -- that's a problem.")
        self.out("\n")
        collections = parsed.collections or self._db.query(Collection)
        for collection in collections:
            self.explain_collection(collection)
            self.out("\n")

    def check_library(self, library: Library) -> None:
        """Make sure a library is properly set up to show works."""
        self.out("Checking library %s", library.name)

        # Make sure it has collections.
        if not (associated_collections := set(library.associated_collections)):
            self.out(" This library has no associated collections -- that's a problem.")
        elif not (active_collections := set(library.active_collections)):
            self.out(" This library has no active collections -- that's a problem.")
        else:
            for collection in associated_collections:
                active = collection in active_collections
                self.out(f" Associated with collection {collection.name} ({active=}).")

        # Make sure it has lanes.
        if not library.lanes:
            self.out(" This library has no lanes -- that's a problem.")
        else:
            self.out(" Associated with %s lanes.", len(library.lanes))

    def explain_collection(self, collection: Collection) -> None:
        self.out('Examining collection "%s"', collection.name)

        base = (
            self._db.query(Work)
            .join(LicensePool)
            .filter(LicensePool.collection == collection)
        )

        ready = base.filter(Work.presentation_ready == True)
        unready = base.filter(Work.presentation_ready == False)

        ready_count = ready.count()
        unready_count = unready.count()
        self.out(" %d presentation-ready works.", ready_count)
        self.out(" %d works not presentation-ready.", unready_count)

        # Check if the works have delivery mechanisms.
        LPDM = LicensePoolDeliveryMechanism
        no_delivery_mechanisms = base.filter(
            ~exists().where(
                and_(
                    LicensePool.data_source_id == LPDM.data_source_id,
                    LicensePool.identifier_id == LPDM.identifier_id,
                )
            )
        ).count()
        if no_delivery_mechanisms > 0:
            self.out(
                " %d works are missing delivery mechanisms and won't show up.",
                no_delivery_mechanisms,
            )

        # Check if the license pools are suppressed.
        suppressed = base.filter(LicensePool.suppressed == True).count()
        if suppressed > 0:
            self.out(
                " %d works have suppressed LicensePools and won't show up.", suppressed
            )

        # Check if the pools have available licenses.
        not_owned = base.filter(
            and_(LicensePool.licenses_owned == 0, ~LicensePool.open_access)
        ).count()
        if not_owned > 0:
            self.out(
                " %d non-open-access works have no owned licenses and won't show up.",
                not_owned,
            )

        filter = Filter(collections=[collection])
        count = self.search.count_works(filter)
        self.out(
            " %d works in the search index, expected around %d.", count, ready_count
        )


class LanguageListScript(LibraryInputScript):
    """List all the languages with at least one non-open access work
    in the collection.
    """

    def process_library(self, library: Library) -> None:
        print(library.short_name)
        for item in self.languages(library):
            print(item)

    def languages(self, library: Library) -> Iterator[str]:
        ":yield: A list of output lines, one per language."
        for abbreviation, count in library.estimated_holdings_by_language(
            include_open_access=False
        ).most_common():
            display_name = LanguageCodes.name_for_languageset(abbreviation)
            yield "%s %i (%s)" % (abbreviation, count, display_name)


class DisappearingBookReportScript(Script):
    """Print a TSV-format report on books that used to be in the
    collection, or should be in the collection, but aren't.
    """

    def do_run(self) -> None:
        qu = (
            self._db.query(LicensePool)
            .filter(LicensePool.open_access == False)
            .filter(LicensePool.suppressed == False)
            .filter(LicensePool.licenses_owned <= 0)
            .order_by(LicensePool.availability_time.desc())
        )
        first_row = [
            "Identifier",
            "Title",
            "Author",
            "First seen",
            "Last seen (best guess)",
            "Current licenses owned",
            "Current licenses available",
            "Changes in number of licenses",
            "Changes in title availability",
        ]
        print("\t".join(first_row))

        for pool in qu:
            self.explain(pool)

    def investigate(
        self, licensepool: LicensePool
    ) -> tuple[datetime | None, Query[CirculationEvent], Query[CirculationEvent]]:
        """Find when the given LicensePool might have disappeared from the
        collection.

        :param licensepool: A LicensePool.

        :return: a 3-tuple (last_seen, title_removal_events,
            license_removal_events).

        `last_seen` is the latest point at which we knew the book was
        circulating. If we never knew the book to be circulating, this
        is the first time we ever saw the LicensePool.

        `title_removal_events` is a query that returns CirculationEvents
        in which this LicensePool was removed from the remote collection.

        `license_removal_events` is a query that returns
        CirculationEvents in which LicensePool.licenses_owned went
        from having a positive number to being zero or a negative
        number.
        """
        # If we have absolutely no information about the book ever
        # circulating, we act like we lost track of the book
        # immediately after seeing it for the first time.
        last_seen = licensepool.availability_time

        # If there's a recorded loan or hold on the book, that can
        # push up the last time the book was known to be circulating.
        for l in (licensepool.loans, licensepool.holds):
            for item in l:
                if item.start is None:
                    continue
                if last_seen is None or item.start > last_seen:
                    last_seen = item.start

        # Now we look for relevant circulation events. First, an event
        # where the title was explicitly removed is pretty clearly
        # a 'last seen'.
        base_query = (
            self._db.query(CirculationEvent)
            .filter(CirculationEvent.license_pool == licensepool)
            .order_by(CirculationEvent.start.desc())
        )
        title_removal_events = base_query.filter(
            CirculationEvent.type == CirculationEvent.DISTRIBUTOR_TITLE_REMOVE
        )
        if title_removal_events.count():
            candidate = title_removal_events[-1].start
            if last_seen is None or candidate > last_seen:
                last_seen = candidate

        # Also look for an event where the title went from a nonzero
        # number of licenses to a zero number of licenses. That's a
        # good 'last seen'.
        license_removal_events = (
            base_query.filter(
                CirculationEvent.type == CirculationEvent.DISTRIBUTOR_LICENSE_REMOVE,
            )
            .filter(CirculationEvent.old_value > 0)
            .filter(CirculationEvent.new_value <= 0)
        )
        if license_removal_events.count():
            candidate = license_removal_events[-1].start
            if last_seen is None or candidate > last_seen:
                last_seen = candidate

        return last_seen, title_removal_events, license_removal_events

    format = "%Y-%m-%d"

    def explain(self, licensepool: LicensePool) -> None:
        edition = licensepool.presentation_edition
        identifier = licensepool.identifier
        last_seen, title_removal_events, license_removal_events = self.investigate(
            licensepool
        )

        data: list[str] = [f"{identifier.type} {identifier.identifier}"]
        if edition:
            data.extend([edition.title or "", edition.author or ""])
        if licensepool.availability_time:
            first_seen = licensepool.availability_time.strftime(self.format)
        else:
            first_seen = ""
        data.append(first_seen)
        if last_seen:
            last_seen_value = last_seen.strftime(self.format)
        else:
            last_seen_value = ""
        data.append(last_seen_value)
        data.append(str(licensepool.licenses_owned))
        data.append(str(licensepool.licenses_available))

        license_removals: list[str] = []
        for event in license_removal_events:
            if event.start is None:
                continue
            description = "{}: {}→{}".format(
                event.start.strftime(self.format),
                event.old_value,
                event.new_value,
            )
            license_removals.append(description)
        data.append(", ".join(license_removals))

        title_removals = [
            event.start.strftime(self.format)
            for event in title_removal_events
            if event.start is not None
        ]
        data.append(", ".join(title_removals))

        print("\t".join([str(x) for x in data]))
