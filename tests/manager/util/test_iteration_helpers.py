import pytest

from palace.manager.util.iteration_helpers import CountingIterator


class TestCountingIterator:
    @pytest.mark.parametrize(
        "data, expected_items, expected_count",
        [
            pytest.param([], [], 0, id="empty-list"),
            pytest.param([1], [1], 1, id="single-item-list"),
            pytest.param([1, 2, 3], [1, 2, 3], 3, id="multi-item-list"),
            pytest.param(range(5), [0, 1, 2, 3, 4], 5, id="range"),
            pytest.param(["a", "b", "c"], ["a", "b", "c"], 3, id="string"),
            pytest.param("abc", ["a", "b", "c"], 3, id="string"),
        ],
    )
    def test_counting_iterator(
        self,
        data: list[int] | str,
        expected_items: list[int | str],
        expected_count: int,
    ):
        iterator = CountingIterator(data)
        items = list(iterator)
        assert len(items) == expected_count
        assert items == expected_items
        assert iterator.get_count() == expected_count

    def test_counting_iterator_empty(self):
        iterator = CountingIterator([])
        with pytest.raises(StopIteration):
            next(iterator)
        assert iterator.get_count() == 0

    def test_counting_iterator_multiple_iterations(self):
        data = [1, 2, 3]
        iterator = CountingIterator(data)
        items1 = list(iterator)
        assert items1 == data
        assert iterator.get_count() == 3

        # Second iteration should be empty.
        items2 = list(iterator)
        assert items2 == []
        assert iterator.get_count() == 3

    def test_counting_iterator_get_count_before_iteration(self):
        data = [1, 2, 3]
        iterator = CountingIterator(data)
        assert iterator.get_count() == 0
        list(iterator)
        assert iterator.get_count() == 3

    def test_counting_iterator_get_count_during_iteration(self):
        data = [1, 2, 3]
        iterator = CountingIterator(data)
        assert iterator.get_count() == 0
        next(iterator)
        assert iterator.get_count() == 1
        next(iterator)
        assert iterator.get_count() == 2
        next(iterator)
        assert iterator.get_count() == 3
        with pytest.raises(StopIteration):
            next(iterator)
        assert iterator.get_count() == 3
