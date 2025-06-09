from __future__ import annotations

from palace.manager.api.axis.manifest import AxisNowManifest
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism


class TestAxisNowManifest:
    """Test the simple data format used to communicate an entry point into
    AxisNow."""

    def test_unicode(self):
        manifest = AxisNowManifest("A UUID", "An ISBN")
        assert '{"book_vault_uuid": "A UUID", "isbn": "An ISBN"}' == str(manifest)
        assert DeliveryMechanism.AXISNOW_DRM == manifest.MEDIA_TYPE
