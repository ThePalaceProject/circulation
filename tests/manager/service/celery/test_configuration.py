from collections.abc import Callable
from functools import partial

import pytest

from palace.manager.service.celery.configuration import CeleryConfiguration

CeleryConfFixture = Callable[..., CeleryConfiguration]


@pytest.fixture
def celery_configuration() -> CeleryConfFixture:
    return partial(CeleryConfiguration, broker_url="redis://test.com:6379/0")


class TestCeleryConfiguration:
    def test_dict_no_merge(
        self, celery_configuration: CeleryConfFixture, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv(
            "PALACE_CELERY_BROKER_TRANSPORT_OPTIONS_GLOBAL_KEYPREFIX", "x"
        )
        monkeypatch.setenv(
            "PALACE_CELERY_BROKER_TRANSPORT_OPTIONS_QUEUE_ORDER_STRATEGY", "y"
        )

        config = celery_configuration()
        result = config.model_dump(merge_options=False)
        assert "broker_url" in result
        assert result.get("broker_transport_options_global_keyprefix") == "x"
        assert result.get("broker_transport_options_queue_order_strategy") == "y"
        assert "broker_transport_options" not in result

    def test_dict_merge(
        self, celery_configuration: CeleryConfFixture, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv(
            "PALACE_CELERY_BROKER_TRANSPORT_OPTIONS_GLOBAL_KEYPREFIX", "x"
        )
        monkeypatch.setenv(
            "PALACE_CELERY_BROKER_TRANSPORT_OPTIONS_QUEUE_ORDER_STRATEGY", "y"
        )
        monkeypatch.setenv("PALACE_CELERY_BROKER_TRANSPORT_OPTIONS_OTHER_OPTION", "z")

        config = celery_configuration()
        result = config.model_dump()
        assert "broker_url" in result
        assert "broker_transport_options" in result
        options = result["broker_transport_options"]
        assert options.get("global_keyprefix") == "x"
        assert options.get("queue_order_strategy") == "y"
        assert options.get("other_option") == "z"
        assert "broker_transport_options_global_keyprefix" not in result
        assert "broker_transport_options_queue_order_strategy" not in result
        assert "broker_transport_options_other_option" not in result

    def test_additional_options(
        self, celery_configuration: CeleryConfFixture, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv("PALACE_CELERY_TEST", "test")

        config = celery_configuration()
        result = config.model_dump()
        assert "broker_url" in result
        assert result.get("test") == "test"
        assert "test" not in config.model_fields
