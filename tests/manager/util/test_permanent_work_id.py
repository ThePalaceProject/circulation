import datetime

from palace.manager.util.permanent_work_id import (
    PERIODICAL_GROUPING_CATEGORY,
    periodical_grouping_category,
)


class TestPeriodicalGroupingCategory:
    def test_publication_date_is_used_when_we_have_one(self):
        # The date tells one issue from another.
        assert (
            periodical_grouping_category(published=datetime.date(2024, 3, 1))
            == "periodical/2024-03-01"
        )

        # Two issues published on different days get different categories.
        assert periodical_grouping_category(
            published=datetime.date(2024, 3, 1)
        ) != periodical_grouping_category(published=datetime.date(2024, 4, 1))

    def test_time_of_day_is_dropped(self):
        # Two sources are unlikely to agree on a time of day, so a datetime is
        # cut down to its date and still matches the same issue from a source
        # that only gave us a date.
        assert periodical_grouping_category(
            published=datetime.datetime(2024, 3, 1, 9, 30)
        ) == periodical_grouping_category(published=datetime.date(2024, 3, 1))

    def test_issue_position_is_used_when_there_is_no_date(self):
        assert periodical_grouping_category(issue_position=3) == "periodical/3"

    def test_the_date_wins_over_the_position(self):
        # We only ever use one of the two, so that an issue that reached us
        # with both still matches the same issue from a source that only gave
        # us the date.
        with_both = periodical_grouping_category(
            published=datetime.date(2024, 3, 1), issue_position=3
        )
        assert with_both == periodical_grouping_category(
            published=datetime.date(2024, 3, 1)
        )

    def test_nothing_to_go_on(self):
        # With no date and no position we have nothing to tell issues apart.
        assert periodical_grouping_category() == PERIODICAL_GROUPING_CATEGORY

    def test_a_periodical_is_never_a_book(self):
        for category in (
            periodical_grouping_category(),
            periodical_grouping_category(published=datetime.date(2024, 3, 1)),
            periodical_grouping_category(issue_position=3),
        ):
            assert category != "book"
            assert category.startswith(PERIODICAL_GROUPING_CATEGORY)
