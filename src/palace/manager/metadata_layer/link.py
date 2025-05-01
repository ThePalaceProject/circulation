from __future__ import annotations

from palace.manager.sqlalchemy.model.resource import Representation


class LinkData:
    def __init__(
        self,
        rel: str | None,
        href: str | None = None,
        media_type: str | None = None,
        content: bytes | str | None = None,
        thumbnail: LinkData | None = None,
        rights_uri: str | None = None,
        rights_explanation: str | None = None,
        original: LinkData | None = None,
        transformation_settings: dict[str, str] | None = None,
    ) -> None:
        if not rel:
            raise ValueError("rel is required")

        if not href and not content:
            raise ValueError("Either href or content is required")
        self.rel = rel
        self.href = href
        self.media_type = media_type
        self.content = content
        self.thumbnail = thumbnail
        # This handles content sources like unglue.it that have rights for each link
        # rather than each edition, and rights for cover images.
        self.rights_uri = rights_uri
        self.rights_explanation = rights_explanation
        # If this LinkData is a derivative, it may also contain the original link
        # and the settings used to transform the original into the derivative.
        self.original = original
        self.transformation_settings = transformation_settings or {}

    @property
    def guessed_media_type(self) -> str | None:
        """If the media type of a link is unknown, take a guess."""
        if self.media_type:
            # We know.
            return self.media_type

        if self.href:
            # Take a guess.
            return Representation.guess_url_media_type_from_path(self.href)  # type: ignore[no-any-return]

        # No idea.
        # TODO: We might be able to take a further guess based on the
        # content and the link relation.
        return None

    def __repr__(self) -> str:
        if self.content:
            content = ", %d bytes content" % len(self.content)
        else:
            content = ""
        if self.thumbnail:
            thumbnail = ", has thumbnail"
        else:
            thumbnail = ""
        return '<LinkData: rel="{}" href="{}" media_type={!r}{}{}>'.format(
            self.rel,
            self.href,
            self.media_type,
            thumbnail,
            content,
        )
