from __future__ import annotations

from functools import cached_property
from typing import Annotated, Self

from frozendict import frozendict
from pydantic import Field, constr, field_validator, model_validator

from palace.manager.data_layer.base.frozen import BaseFrozenData
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.util.log import LoggerMixin
from palace.manager.util.pydantic import FrozenDict


class LinkData(BaseFrozenData, LoggerMixin):
    rel: Annotated[str, constr(min_length=1)]
    href: str | None = None
    media_type: str | None = None
    content: bytes | str | None = Field(None, repr=False)
    thumbnail: LinkData | None = Field(None, repr=False)
    rights_uri: str | None = Field(None, repr=False)
    rights_explanation: str | None = Field(None, repr=False)
    original: LinkData | None = Field(None, repr=False)
    transformation_settings: FrozenDict[str, str] = Field(
        default_factory=frozendict, repr=False
    )

    @model_validator(mode="after")
    def _check_href_or_content(self) -> Self:
        if not self.href and not self.content:
            raise ValueError("Either 'href' or 'content' is required")
        return self

    @field_validator("thumbnail")
    @classmethod
    def _thumbnail_has_correct_rel(cls, value: LinkData | None) -> LinkData | None:
        if value is not None:
            if value.rel != Hyperlink.THUMBNAIL_IMAGE:
                cls.logger().error(
                    f"Thumbnail link {value!r} does not have the thumbnail link relation! Not acceptable as a thumbnail."
                )
                return None
        return value

    @cached_property
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

    def set_thumbnail(self, thumbnail: LinkData) -> Self:
        return self.model_copy(update={"thumbnail": thumbnail})
