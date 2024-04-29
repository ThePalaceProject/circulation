import pytest

from palace.manager.celery.celery import Celery


class TestCelery:
    @pytest.mark.parametrize(
        "name, module, expected",
        [
            ("baz", "foo.bar", "foo.bar.baz"),
            ("task", "palace.manager.celery.tasks.test", "test.task"),
        ],
    )
    def test_gen_task_name(self, name: str, module: str, expected: str):
        celery = Celery()
        assert celery.gen_task_name(name, module) == expected
