from unittest.mock import call, MagicMock

import aws_xray_sdk

from api.util.xray import PalaceXrayMiddleware, PalaceXrayUtils
from core.config import Configuration


class TestPalaceXrayUtils:

    def test_put_annotations_none(self):
        # If no segment is passed in nothing is returned
        value = PalaceXrayUtils.put_annotations(None)
        assert value is None

    def test_put_annotations(self):
        # Type annotation set based on seg_type passed into put_annotation
        segment = MagicMock()
        PalaceXrayUtils.put_annotations(segment, "test")
        segment.put_annotation.assert_called_once_with("type", "test")

    def test_put_annotations_env(self, monkeypatch):
        # Annotations are made based on environment variables
        segment = MagicMock()
        monkeypatch.setenv(f"{PalaceXrayUtils.XRAY_ENV_ANNOTATE}TEST", "test")
        monkeypatch.setenv(f"{PalaceXrayUtils.XRAY_ENV_ANNOTATE}ANOTHER_TEST", "test123")
        PalaceXrayUtils.put_annotations(segment)
        assert segment.put_annotation.called is True
        assert segment.put_annotation.call_count == 2
        assert segment.put_annotation.call_args_list == [call("test", "test"), call("another_test", "test123")]

    def test_put_annotations_version(self, monkeypatch):
        # The version number is added as an annotation
        segment = MagicMock()
        monkeypatch.setattr(Configuration, "app_version", lambda: "foo")
        PalaceXrayUtils.put_annotations(segment)
        segment.put_annotation.assert_called_once_with("version", "foo")

    def test_configure_app(self, monkeypatch):
        mock_app = MagicMock()
        mock_middleware = MagicMock()

        monkeypatch.setattr(PalaceXrayUtils, "setup_xray", MagicMock())
        monkeypatch.setattr("api.util.xray.PalaceXrayMiddleware", mock_middleware)

        # Nothing happens if env isn't set
        PalaceXrayUtils.configure_app(mock_app)
        assert PalaceXrayUtils.setup_xray.called is False
        assert mock_middleware.called is False

        # Xray not setup is env isn't true
        monkeypatch.setenv(PalaceXrayUtils.XRAY_ENV_ENABLE, "false")
        PalaceXrayUtils.configure_app(mock_app)
        assert PalaceXrayUtils.setup_xray.called is False
        assert mock_middleware.called is False

        # Xray is setup is env is true
        monkeypatch.setenv(PalaceXrayUtils.XRAY_ENV_ENABLE, "true")
        PalaceXrayUtils.configure_app(mock_app)
        assert PalaceXrayUtils.setup_xray.called is True
        assert mock_middleware.called is True
