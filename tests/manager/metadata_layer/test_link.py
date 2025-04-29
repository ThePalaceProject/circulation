from palace.manager.metadata_layer.link import LinkData
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation


class TestLinkData:
    def test_guess_media_type(self):
        rel = Hyperlink.IMAGE

        # Sometimes we have no idea what media type is at the other
        # end of a link.
        unknown = LinkData(rel=rel, href="http://foo/bar.unknown")
        assert None == unknown.guessed_media_type

        # Sometimes we can guess based on the file extension.
        jpeg = LinkData(rel=rel, href="http://foo/bar.jpeg")
        assert Representation.JPEG_MEDIA_TYPE == jpeg.guessed_media_type

        # An explicitly known media type takes precedence over
        # something we guess from the file extension.
        png = LinkData(
            rel=rel,
            href="http://foo/bar.jpeg",
            media_type=Representation.PNG_MEDIA_TYPE,
        )
        assert Representation.PNG_MEDIA_TYPE == png.guessed_media_type

        description = LinkData(rel=Hyperlink.DESCRIPTION, content="Some content")
        assert None == description.guessed_media_type
