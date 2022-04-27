from core.mirror import MirrorUploader


class TestMiscellaneous:
    def test_mirror_uploader_implementations_are_being_loaded(self):
        """
        This test verifies that the two S3 mirror implementations are being
        loaded when the MARCExporter is imported.  It was not added to
        tests/core/test_marc.py because that test causes the implementations
        to be loaded since it references the core.s3 package directly.
        """
        from core.marc import MARCExporter  # noqa: autoflake

        assert MirrorUploader.IMPLEMENTATION_REGISTRY.get("Amazon S3")
        assert MirrorUploader.IMPLEMENTATION_REGISTRY.get("MinIO")
