"""Tests for the Overdrive product-list crawl machinery."""

from __future__ import annotations

import json
from collections.abc import Callable

import pytest

from palace.util.datetime_helpers import utc_now

from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.license.overdrive.crawl import (
    CRAWL_ALLOWANCE_FLOOR,
    CrawlComplete,
    CrawlCursor,
    CrawlFault,
    ProductPage,
)
from palace.manager.sqlalchemy.model.identifier import Identifier


def identifiers(*names: str) -> tuple[IdentifierData, ...]:
    return tuple(
        IdentifierData(type=Identifier.OVERDRIVE_ID, identifier=name) for name in names
    )


def page_of(
    *,
    listed: tuple[IdentifierData, ...] = (),
    total_items: int,
    limit: int = 2000,
) -> ProductPage:
    return ProductPage(listed=listed, unowned=(), total_items=total_items, limit=limit)


def page_from(source: list[str], offset: int, limit: int) -> ProductPage:
    """Serve the page a product list of ``source`` would return at ``offset``."""
    return page_of(
        listed=identifiers(*source[offset : offset + limit]),
        total_items=len(source),
        limit=limit,
    )


def walk(
    source: list[str],
    limit: int,
    on_page: list[Callable[[list[str]], None]] | None = None,
) -> tuple[set[str], CrawlComplete | CrawlFault, list[int]]:
    """Drive a cursor over ``source``, mutating it between pages if asked.

    :param source: The product list being crawled, as a list of title ids. Pages
        are cut from it at fetch time, so changes to it between pages behave
        like collection churn during a crawl.
    :param limit: The page size the simulated Overdrive applies.
    :param on_page: Optional callables, one applied to ``source`` after each
        page is fetched, simulating churn at that moment of the crawl. Shorter
        lists stop mutating; None mutates nothing.
    :return: The distinct ids collected, the walk's outcome, and the offsets
        fetched in order.
    """
    cursor = CrawlCursor()
    collected: set[str] = set()
    offsets: list[int] = []
    # Generous bound so a cursor bug cannot loop the test forever.
    for index in range(len(source) + 10):
        offsets.append(cursor.offset)
        page = page_from(source, cursor.offset, limit)
        collected.update(identifier.identifier for identifier in page.listed)
        step = cursor.advance(page)
        if on_page is not None and index < len(on_page):
            on_page[index](source)
        if not isinstance(step, CrawlCursor):
            return collected, step, offsets
        cursor = step
    raise AssertionError("The walk never terminated.")


class TestCrawlCursor:
    @pytest.mark.parametrize(
        "total_items,limit,expected_offset",
        [
            pytest.param(1500, 2000, None, id="first page covers the collection"),
            pytest.param(2000, 2000, None, id="first page is exactly the collection"),
            pytest.param(5000, 2000, 3000, id="first page jumps to the end"),
        ],
    )
    def test_advance_from_the_first_page(
        self, total_items: int, limit: int, expected_offset: int | None
    ) -> None:
        cursor = CrawlCursor()
        step = cursor.advance(page_of(total_items=total_items, limit=limit))

        if expected_offset is None:
            assert isinstance(step, CrawlComplete)
            assert step.single_page
            assert step.total_items == total_items
        else:
            assert isinstance(step, CrawlCursor)
            assert step.offset == expected_offset
            assert step.previous_offset is None
            assert step.total_items == total_items
            assert step.page_limit == limit
            # The crawl's start time survives the hand-off, so wall-clock
            # logging spans the whole crawl rather than one page.
            assert step.started_at == cursor.started_at

    @pytest.mark.parametrize(
        "total_items,limit",
        [
            pytest.param(1050, 100, id="ragged final page"),
            pytest.param(1000, 100, id="exact multiple"),
            pytest.param(100, 100, id="single page"),
            pytest.param(1, 100, id="single title"),
            pytest.param(101, 100, id="one title past the first page"),
        ],
    )
    def test_walk_covers_the_whole_list(self, total_items: int, limit: int) -> None:
        """Walking the pages must cover every title and terminate.

        A gap would be indistinguishable from titles that left the collection,
        and the reaper would mark them as gone.
        """
        source = [f"title-{index}" for index in range(total_items)]

        collected, outcome, offsets = walk(source, limit)

        assert isinstance(outcome, CrawlComplete)
        assert collected == set(source)
        assert outcome.completeness_fault(len(collected)) is None

    def test_walk_ends_by_recrawling_the_start(self) -> None:
        """A multi-page walk finishes with a fresh page at offset 0.

        The first page's snapshot of the start of the list goes stale over a
        long crawl, so the walk never leans on it: coverage ends where it
        began, on a page fetched last.
        """
        source = [f"title-{index}" for index in range(59)]

        _, outcome, offsets = walk(source, 20)

        assert isinstance(outcome, CrawlComplete)
        assert not outcome.single_page
        assert offsets[0] == 0
        assert offsets[-1] == 0
        # In between, the walk only ever moves towards the start of the list.
        backwards = offsets[1:]
        assert backwards == sorted(backwards, reverse=True)

    def test_walk_collects_a_title_that_drifts_below_the_first_page(self) -> None:
        """Titles only move down the dateAdded:asc ordering, and the walk
        re-covers the start of the list, so a title that drifts below the first
        page's edge mid-crawl is still collected.

        This is the seam a walk that stopped at ``offset <= limit`` would
        leave open: hard removals among the oldest titles shift a title from
        just past the first page's coverage down into a strip covered only by
        the first page's stale snapshot.
        """
        # At crawl start "drifter" sits at index 21, just past the first
        # page's coverage of [0, 20).
        source = (
            [f"title-{index}" for index in range(21)]
            + ["drifter"]
            + [f"title-{index}" for index in range(22, 59)]
        )

        def remove_two_of_the_oldest(current: list[str]) -> None:
            del current[0:2]

        collected, outcome, _ = walk(
            source,
            20,
            # The removal lands after the second page, once the walk is out at
            # the end of the list, shifting "drifter" down to index 19.
            on_page=[lambda _: None, remove_two_of_the_oldest],
        )

        assert isinstance(outcome, CrawlComplete)
        assert "drifter" in collected
        assert outcome.completeness_fault(len(collected)) is None

    @pytest.mark.parametrize(
        "reported_now,returned,faults",
        [
            pytest.param(6000, 2000, False, id="unchanged, full page"),
            pytest.param(6001, 2000, False, id="a title added since the first page"),
            pytest.param(6100, 2000, False, id="many titles added"),
            pytest.param(5990, 1990, False, id="titles removed since the first page"),
            pytest.param(6000, 1990, True, id="page truncated"),
        ],
    )
    def test_first_backwards_page_must_reach_the_end(
        self, reported_now: int, returned: int, faults: bool
    ) -> None:
        """Which end the first backwards page must reach depends on what changed.

        It starts one page back from where the collection ended when the crawl
        began, so a full page arrives exactly at that count. Titles added since
        then sit beyond it -- not this page's to reach. Titles removed shorten
        the list, and that shorter count is the one it has to reach.
        """
        cursor = CrawlCursor(offset=4000, total_items=6000, page_limit=2000)
        page = page_of(
            listed=identifiers(*(f"title-{index}" for index in range(returned))),
            total_items=reported_now,
        )

        step = cursor.advance(page)

        if faults:
            assert isinstance(step, CrawlFault)
            assert "Refusing to reap across a gap" in step.reason
        else:
            assert isinstance(step, CrawlCursor)

    def test_every_other_page_must_reach_the_one_before_it(self) -> None:
        """A page that returns fewer titles than the walk stepped back by
        leaves a strip of the list uncrawled, indistinguishable at the
        completeness gate from titles removed mid-crawl."""
        cursor = CrawlCursor(
            offset=3000, previous_offset=4000, total_items=6000, page_limit=2000
        )

        step = cursor.advance(page_of(listed=identifiers("only"), total_items=6000))

        assert isinstance(step, CrawlFault)
        assert "Refusing to reap across a gap" in step.reason
        assert "already crawled at 4000" in step.reason

    def test_page_size_change_faults(self) -> None:
        """Every gap check measures in page sizes, so one that changes
        invalidates the walk's arithmetic."""
        cursor = CrawlCursor(offset=4000, total_items=6000, page_limit=2000)

        step = cursor.advance(
            page_of(
                listed=identifiers(*(f"title-{index}" for index in range(500))),
                total_items=6000,
                limit=500,
            )
        )

        assert isinstance(step, CrawlFault)
        assert "whose paging changed underneath it" in step.reason

    def test_serialization_round_trip(self) -> None:
        """A cursor survives the JSON wire format a task kwarg travels as."""
        cursor = CrawlCursor(
            offset=3000, previous_offset=4000, total_items=6000, page_limit=2000
        )

        rebuilt = CrawlCursor.from_json(json.loads(json.dumps(cursor.__json__())))

        assert rebuilt == cursor

        fresh = CrawlCursor()
        assert CrawlCursor.from_json(json.loads(json.dumps(fresh.__json__()))) == fresh


class TestCrawlComplete:
    @pytest.mark.parametrize(
        "crawled,total_items,single_page,complete",
        [
            pytest.param(1_000, 1_000, False, True, id="exact match"),
            pytest.param(0, 5, False, True, id="floor covers a tiny collection"),
            pytest.param(0, 100, False, False, id="shortfall beyond the floor"),
            pytest.param(
                4_000_000 - 400, 4_000_000, False, True, id="churn within the cap"
            ),
            pytest.param(
                4_000_000 - 2_000,
                4_000_000,
                False,
                False,
                id="a lost page is not excused",
            ),
            pytest.param(1_000, 1_000, True, True, id="single page, counts agree"),
            pytest.param(999, 1_000, True, False, id="single page, one short"),
        ],
    )
    def test_completeness_fault(
        self, crawled: int, total_items: int, single_page: bool, complete: bool
    ) -> None:
        """The allowance absorbs churn during a crawl, but never a whole lost page.

        It is capped because the churn it exists for does not scale with
        collection size, while a proportional allowance does: 0.1% of a
        4M-title collection would be two full pages, so a crawl that silently
        dropped one would be accepted and those titles reaped. A collection
        that arrived in a single response had no window for churn at all, so
        its counts have to agree exactly.
        """
        outcome = CrawlComplete(
            total_items=total_items,
            page_limit=2000,
            single_page=single_page,
            started_at=utc_now(),
        )

        fault = outcome.completeness_fault(crawled)

        if complete:
            assert fault is None
        else:
            assert isinstance(fault, CrawlFault)
            assert "Refusing to reap on an incomplete crawl" in fault.reason

    def test_allowance_stays_under_a_page(self) -> None:
        """A lost page must never be excused, whatever page size Overdrive applied.

        The allowance is meant to absorb churn during a crawl, and the crawl's
        first backwards page has nothing above it to be checked against -- so
        if the allowance ever reached a whole page, an empty response there
        would be indistinguishable from titles removed mid-crawl.
        """
        lost_page = 10_000_000
        for page_limit in (2000, 300, 25):
            outcome = CrawlComplete(
                total_items=lost_page,
                page_limit=page_limit,
                single_page=False,
                started_at=utc_now(),
            )
            assert outcome.completeness_fault(lost_page - page_limit) is not None

    def test_floor_is_the_constant(self) -> None:
        """The floor keeps small collections from being held to an unreachable
        standard; pin it so a change is a deliberate one."""
        assert CRAWL_ALLOWANCE_FLOOR == 10
