from collections.abc import Callable
from functools import partial

import pytest
from kombu.utils.json import dumps, loads

from palace.manager.opds.opds2 import Publication, PublicationFeed
from palace.manager.service.celery.configuration import CeleryConfiguration
from tests.fixtures.files import OPDS2FilesFixture

CeleryConfFixture = Callable[..., CeleryConfiguration]


@pytest.fixture
def celery_configuration() -> CeleryConfFixture:
    return partial(
        CeleryConfiguration,
        broker_url="redis://test.com:6379/0",
        result_backend="redis://test.com:6379/2",
    )


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

        monkeypatch.setenv(
            "PALACE_CELERY_RESULT_BACKEND_TRANSPORT_OPTIONS_GLOBAL_KEYPREFIX", "z"
        )

        config = celery_configuration()
        result = config.model_dump(merge_options=False)
        assert "broker_url" in result
        assert "result_backend" in result
        assert result.get("broker_transport_options_global_keyprefix") == "x"
        assert result.get("broker_transport_options_queue_order_strategy") == "y"
        assert result.get("result_backend_transport_options_global_keyprefix") == "z"
        assert "broker_transport_options" not in result
        assert "result_backend_transport_options" not in result

    def test_dict_merge(
        self, celery_configuration: CeleryConfFixture, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv(
            "PALACE_CELERY_BROKER_TRANSPORT_OPTIONS_GLOBAL_KEYPREFIX", "x"
        )
        monkeypatch.setenv(
            "PALACE_CELERY_BROKER_TRANSPORT_OPTIONS_QUEUE_ORDER_STRATEGY", "y"
        )

        monkeypatch.setenv(
            "PALACE_CELERY_RESULT_BACKEND_TRANSPORT_OPTIONS_GLOBAL_KEYPREFIX", "z"
        )

        config = celery_configuration()
        result = config.model_dump()
        assert "broker_url" in result
        assert "result_backend" in result
        assert "broker_transport_options" in result
        options = result["broker_transport_options"]
        assert options.get("global_keyprefix") == "x"
        assert options.get("queue_order_strategy") == "y"
        assert "broker_transport_options_global_keyprefix" not in result
        assert "broker_transport_options_queue_order_strategy" not in result

        options = result["result_backend_transport_options"]
        assert options.get("global_keyprefix") == "z"
        assert "result_backend_transport_options_global_keyprefix" not in result


class TestPydanticSerialization:
    def test_pydantic_object(self, opds2_files_fixture: OPDS2FilesFixture) -> None:
        """
        Test that we are able to round-trip pydantic models through the Kombu json serializer.
        """
        feed = PublicationFeed.model_validate_json(
            opds2_files_fixture.sample_data("feed.json")
        )
        serialized_feed = dumps(feed)
        deserialized_feed = loads(serialized_feed)

        assert isinstance(deserialized_feed, PublicationFeed)
        assert deserialized_feed == feed

    def test_pydantic_object_nested(
        self, opds2_files_fixture: OPDS2FilesFixture
    ) -> None:
        """
        Test that we are able to round-trip pydantic models nested inside containers through
        the Kombu json serializer.
        """
        feed = PublicationFeed.model_validate_json(
            opds2_files_fixture.sample_data("feed.json")
        )
        serialized_publications = dumps(feed.publications)
        deserialized_publications = loads(serialized_publications)

        assert isinstance(deserialized_publications, list)
        assert all(isinstance(pub, Publication) for pub in deserialized_publications)
        assert deserialized_publications == feed.publications
