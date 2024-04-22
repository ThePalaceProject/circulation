from unittest.mock import MagicMock, call

from palace import manager
from palace.manager.api.util.xray import PalaceXrayMiddleware


class TestPalaceXrayMiddleware:
    def test_put_annotations(self):
        # Type annotation set based on seg_type passed into put_annotation
        segment = MagicMock()
        PalaceXrayMiddleware.put_annotations(segment, "test")
        segment.put_annotation.assert_called_once_with("type", "test")

    def test_put_annotations_env(self, monkeypatch):
        # Annotations are made based on environment variables
        segment = MagicMock()
        monkeypatch.setenv(f"{PalaceXrayMiddleware.XRAY_ENV_ANNOTATE}TEST", "test")
        monkeypatch.setenv(
            f"{PalaceXrayMiddleware.XRAY_ENV_ANNOTATE}ANOTHER_TEST", "test123"
        )
        PalaceXrayMiddleware.put_annotations(segment)
        assert segment.put_annotation.called is True
        assert segment.put_annotation.call_count == 2
        assert segment.put_annotation.call_args_list == [
            call("test", "test"),
            call("another_test", "test123"),
        ]

    def test_put_annotations_version(self, monkeypatch):
        # The version number is added as an annotation
        segment = MagicMock()
        monkeypatch.setattr(manager, "__version__", "foo")
        PalaceXrayMiddleware.put_annotations(segment)
        segment.put_annotation.assert_called_once_with("version", "foo")
