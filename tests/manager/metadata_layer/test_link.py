from functools import partial

import pytest
from pydantic import ValidationError

from palace.manager.metadata_layer.link import LinkData
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation


class TestLinkData:
    def test_hash(self) -> None:
        """Test that the hash of a LinkData object is consistent."""
        link1 = LinkData(rel=Hyperlink.IMAGE, href="http://example.com/image.jpg")
        link2 = LinkData(rel=Hyperlink.IMAGE, href="http://example.com/image.jpg")
        assert hash(link1) == hash(link2)

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

    def test__thumbnail_has_correct_rel(self):
        # Test that the thumbnail link has the correct rel
        image = partial(
            LinkData, rel=Hyperlink.IMAGE, href="http://example.com/image.jpg"
        )

        # Create a image link with a correct thumbnail
        correct_thumbnail = image(
            thumbnail=LinkData(
                rel=Hyperlink.THUMBNAIL_IMAGE, href="http://example.com/thumbnail.jpg"
            )
        )
        assert correct_thumbnail.thumbnail.href == "http://example.com/thumbnail.jpg"
        assert correct_thumbnail.href == "http://example.com/image.jpg"

        # We set the thumbnail to none if there is an incorrect rel
        incorrect_thumbnail = image(
            thumbnail=LinkData(
                rel="non_thumbnail", href="http://example.com/non_thumbnail.jpg"
            )
        )
        assert incorrect_thumbnail.thumbnail is None
        assert incorrect_thumbnail.href == "http://example.com/image.jpg"

    def test_require_href_or_content(self):
        # Test that a LinkData object requires either href or content
        with pytest.raises(
            ValidationError, match="Either 'href' or 'content' is required"
        ):
            LinkData(rel=Hyperlink.IMAGE)
