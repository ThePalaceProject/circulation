"""Machinery for crawling an Overdrive collection's product list.

Overdrive's product list is paged by offset and publishes no unique sort key
(``id``, ``reserveId`` and ``crossRefId`` are all rejected), so a crawl has to
establish coverage structurally rather than trust the paging. The
:class:`CrawlCursor` here owns that arithmetic: where the next page is fetched,
what each fetched page must be shown to reach, and whether a finished crawl can
be trusted. Callers own everything else -- fetching pages, accumulating
identifiers, and deciding what a fault costs them. The reaper aborts on any
fault, because it acts on titles' *absence* from the crawl; a future delta
harvest can treat the same faults as advisory, because a missed update is
recovered by the next run's window.

The cursor walks the list *backwards*: the first page is fetched at offset 0 to
learn ``totalItems``, each following page steps back from the end of the list,
and the walk finishes with a fresh page at offset 0. Walking forwards --
following Overdrive's ``next`` links -- would lose a title every time one was
removed mid-crawl: removing a title shifts every title behind it down one
position, so the title sitting at the next page's starting offset is never
returned, and a title the crawl never saw is indistinguishable from one that
left the collection. Walking backwards, a removal below the cursor can only
shift a not-yet-crawled title further into the region still being walked, and a
removal above it cannot move that region at all. The step back is shorter than
a page, so consecutive pages overlap; that absorbs the mirror image of the same
problem -- a title *inserted* below the cursor pushes its neighbours up, one of
them into the region already crawled -- along with any ordering jitter within a
tie group. Overlapping is free when the identifiers are collected into a set.

Ending with a fresh page at offset 0 is what closes the walk's last seam. The
first page's snapshot of the start of the list is hours stale by the time a
long crawl gets back there, and titles only ever move *down* the
``dateAdded:asc`` ordering (removals shift them down; additions land at the
end), so a title sitting just past the first page's edge when the crawl began
can drift below it before the walk returns -- dodging every page unless the
start is re-covered at the end.
"""

from __future__ import annotations

import datetime
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Any

from palace.util.datetime_helpers import utc_now

from palace.manager.data_layer.identifier import IdentifierData

# How much of a page consecutive crawl pages share. A fraction, so that it
# scales with whatever page size Overdrive actually applies.
CRAWL_OVERLAP: float = 0.05

# How far a completed crawl may fall short of Overdrive's reported totalItems
# before it is treated as incomplete. A crawl of a large collection runs for
# hours while titles are added and removed under it, so the count is expected
# to move a little; the floor keeps small collections from being held to an
# unreachable standard. The shortfall being allowed for is churn during the
# crawl, which does not scale with collection size, so the cap keeps the
# allowance below one page -- a crawl that lost a whole page must never be
# excused.
CRAWL_ALLOWANCE: float = 0.001
CRAWL_ALLOWANCE_FLOOR: int = 10
# Capped as a fraction of the page size the crawl is actually walking in, which
# is the one Overdrive applied rather than the one asked for.
CRAWL_ALLOWANCE_PAGE_FRACTION: float = 0.25


@dataclass(frozen=True)
class ProductPage:
    """One page of a collection's product list.

    :param products: The raw product dictionaries exactly as Overdrive returned
        them, for callers that need more than identifiers -- metadata and
        availability links, say. The crawl itself reads nothing from them.
    :param listed: Identifiers for every title on the page, owned or not. The
        reaper's set difference is taken against these, so a title Overdrive
        still lists is never reaped for being absent -- if it is no longer
        owned it is marked from ``unowned`` instead.
    :param unowned: The subset of ``listed`` that Overdrive flags
        ``isOwnedByCollections: false``. Overdrive keeps weeded and expired
        titles in the product list rather than dropping them, so this is direct
        evidence that a title is gone, as opposed to the inference drawn from
        an identifier's absence.
    :param total_items: Overdrive's count of the whole result set.
    :param limit: The page size Overdrive applied, which may be smaller than
        the one requested. Always the size applied, never the number of titles
        returned, so a page at the end of the list still reports a full one.
    """

    listed: tuple[IdentifierData, ...]
    unowned: tuple[IdentifierData, ...]
    total_items: int
    limit: int
    products: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class CrawlFault:
    """A page the crawl cannot reconcile with the walk so far.

    :param reason: What the page failed to show. The wording is deliberately
        consequence-free -- it states the arithmetic, not what to do about
        it -- because the caller decides what a fault costs: the reaper aborts,
        since it acts on absence; a harvest stops the walk but keeps what it
        imported. Each caller appends its own consequence when it logs.
    """

    reason: str


@dataclass(frozen=True)
class CrawlComplete:
    """A walk that covered the whole list, pending the completeness gate.

    Structural coverage (every page reached the one before it) is established
    by the walk itself; what remains is comparing how many *distinct* titles
    the caller collected against Overdrive's count, which is the caller's to
    supply because the cursor never sees the identifiers.
    """

    total_items: int
    page_limit: int
    single_page: bool
    started_at: datetime.datetime

    @property
    def elapsed(self) -> datetime.timedelta:
        """How long the crawl has been running."""
        return utc_now() - self.started_at

    def completeness_fault(self, crawled: int) -> CrawlFault | None:
        """Judge whether the finished crawl covered the whole collection.

        :param crawled: The number of *distinct* identifiers collected, so a
            title returned twice cannot paper over one that was missed.
        :return: None when the crawl can be trusted, or a :class:`CrawlFault`
            when the shortfall exceeds what mid-crawl churn can explain. A
            collection that arrived in a single response had no window for
            churn -- its products and count were produced together -- so it is
            required to agree exactly.
        """
        allowance = (
            0
            if self.single_page
            else min(
                max(1, int(self.page_limit * CRAWL_ALLOWANCE_PAGE_FRACTION)),
                max(
                    CRAWL_ALLOWANCE_FLOOR,
                    int(self.total_items * CRAWL_ALLOWANCE),
                ),
            )
        )
        if crawled + allowance < self.total_items:
            return CrawlFault(
                f"The crawl saw {crawled} distinct titles of the "
                f"{self.total_items} Overdrive reports (allowance {allowance})."
            )
        return None


@dataclass(frozen=True)
class CrawlCursor:
    """Where a backwards crawl of a product list stands.

    A fresh cursor points at the crawl's first page (offset 0, with no paging
    facts yet); :meth:`advance` consumes each fetched page and returns the
    cursor for the next one, a :class:`CrawlComplete` once the list is
    covered, or a :class:`CrawlFault` for a page the walk cannot use. Cursors
    are immutable and JSON-serializable, so a paginated task can carry one
    across ``task.replace()`` hand-offs as a single kwarg.

    :param offset: The offset the current page is (to be) fetched at.
    :param previous_offset: Where the previous page of the backwards walk
        started, so the current one can be shown to reach it. None on the first
        backwards page, which runs to the end of the list instead.
    :param total_items: Overdrive's collection size as reported by the crawl's
        first page, carried for the completeness gate.
    :param page_limit: The page size Overdrive applied to the first page. Every
        gap check is measured in it, so the walk's arithmetic only holds while
        it stays the same.
    :param started_at: When the crawl began, for wall-clock logging: the guards
        are all sensitive to how long a crawl runs, so an abort without the
        elapsed time is half a diagnosis.
    """

    offset: int = 0
    previous_offset: int | None = None
    total_items: int | None = None
    page_limit: int | None = None
    started_at: datetime.datetime = field(default_factory=utc_now)

    @property
    def elapsed(self) -> datetime.timedelta:
        """How long the crawl has been running."""
        return utc_now() - self.started_at

    def advance(self, page: ProductPage) -> CrawlCursor | CrawlComplete | CrawlFault:
        """Consume the page fetched at this cursor and say what comes next.

        :param page: The page fetched at :attr:`offset`.
        """
        if self.total_items is None:
            # The crawl's first page. It exists to learn the paging facts --
            # coverage never depends on it, because the walk ends by fetching
            # the start of the list again -- except when the whole collection
            # arrives in this one response.
            if page.total_items <= page.limit:
                return self._complete(
                    total_items=page.total_items,
                    page_limit=page.limit,
                    single_page=True,
                )
            return replace(
                self,
                offset=page.total_items - page.limit,
                previous_offset=None,
                total_items=page.total_items,
                page_limit=page.limit,
            )

        # total_items and page_limit are always recorded together.
        assert self.page_limit is not None

        if page.limit != self.page_limit:
            # Where a page reaches, and so where the walk can stop, is measured
            # in page sizes. Overdrive echoes the size it applied, and the
            # crawl asks for the same one every time, so a change mid-crawl
            # means this arithmetic no longer describes the list.
            return CrawlFault(
                f"The page at offset {self.offset} came back with a page size "
                f"of {page.limit} after the crawl had been walking in steps of "
                f"{self.page_limit}, so the walk's arithmetic no longer "
                f"describes the list."
            )

        if self.previous_offset is None:
            # The walk's first backwards page runs to the end of the list, and
            # is the one page with nothing above it to be checked against. Its
            # reach is exactly knowable for that same reason: it has to arrive
            # at the end.
            #
            # Which end depends on what the collection did in between. Titles
            # removed since the first page shorten the list, so the smaller
            # count is the one to insist on; titles added lengthen it, and
            # under dateAdded:asc they land beyond where this page starts, so
            # they are not this page's to reach -- nor can they be falsely
            # reaped, being absent from the snapshot the chord took of what we
            # hold.
            must_reach = min(page.total_items, self.total_items)
            if self.offset + len(page.listed) < must_reach:
                return CrawlFault(
                    f"The page at offset {self.offset} returned "
                    f"{len(page.listed)} titles and stops short of the "
                    f"{must_reach} Overdrive reports, so the walk cannot be "
                    f"shown to reach the end of the list."
                )
        elif self.offset + len(page.listed) < self.previous_offset:
            # The walk steps back by a page size, but a page is free to return
            # fewer titles than that. When one does, the step overshoots and
            # leaves a strip of the list uncrawled -- which would reach the
            # completeness gate as a shortfall indistinguishable from titles
            # removed mid-crawl. Pages are only legitimately short at the end
            # of the list, so every other page is required to reach the one
            # before it.
            return CrawlFault(
                f"The page at offset {self.offset} returned "
                f"{len(page.listed)} titles and stops short of the region "
                f"already crawled at {self.previous_offset}, leaving a strip "
                f"of the list uncovered."
            )

        if self.offset == 0:
            # The walk has re-fetched the start of the list, so its coverage
            # is complete without leaning on the first page's stale snapshot.
            return self._complete(
                total_items=self.total_items,
                page_limit=self.page_limit,
                single_page=False,
            )

        step = max(1, self.page_limit - int(self.page_limit * CRAWL_OVERLAP))
        return replace(
            self,
            offset=max(0, self.offset - step),
            previous_offset=self.offset,
        )

    def _complete(
        self, *, total_items: int, page_limit: int, single_page: bool
    ) -> CrawlComplete:
        """Build the completion record for this walk."""
        return CrawlComplete(
            total_items=total_items,
            page_limit=page_limit,
            single_page=single_page,
            started_at=self.started_at,
        )

    def __json__(self) -> dict[str, Any]:
        """Serialize for a Celery task kwarg; the inverse of :meth:`from_json`.

        ``started_at`` travels as an ISO string so the round trip does not
        depend on how the task serializer treats datetimes inside dicts.
        """
        return {
            "offset": self.offset,
            "previous_offset": self.previous_offset,
            "total_items": self.total_items,
            "page_limit": self.page_limit,
            "started_at": self.started_at.isoformat(),
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> CrawlCursor:
        """Rebuild a cursor serialized by :meth:`__json__`."""
        return cls(
            offset=data["offset"],
            previous_offset=data["previous_offset"],
            total_items=data["total_items"],
            page_limit=data["page_limit"],
            started_at=datetime.datetime.fromisoformat(data["started_at"]),
        )
