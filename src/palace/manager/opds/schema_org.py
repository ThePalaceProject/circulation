"""
Schema.org vocabulary models for OPDS2 feeds.

This module contains Pydantic models and enums representing schema.org
types used in OPDS2 feeds, particularly those sent by DeMarque.

https://schema.org/
"""

from enum import StrEnum

from pydantic import Field

from palace.manager.opds.base import BaseOpdsModel


class PublicationTypes(StrEnum):
    """
    https://schema.org/CreativeWork
    """

    book = "http://schema.org/Book"
    audiobook = "http://schema.org/Audiobook"


class BookFormat(StrEnum):
    """
    https://schema.org/BookFormatType
    """

    hardcover = "http://schema.org/Hardcover"
    paperback = "http://schema.org/Paperback"
    ebook = "http://schema.org/EBook"
    audiobook = "http://schema.org/AudiobookFormat"
    graphic_novel = "http://schema.org/GraphicNovel"


class WorkExample(BaseOpdsModel):
    """
    This isn't documented, but we see this information coming back in DeMarque feeds.

    This gives extra information about related works, including their format and ISBN,
    which is useful information to have.

    The examples we see look like this:
        "schema:workExample": [
          {
            "@type": "http://schema.org/Book",
            "schema:bookFormat": "http://schema.org/Hardcover",
            "schema:isbn": "urn:ISBN:9781541600775"
          }
        ]

    https://schema.org/workExample
    """

    # TODO: I believe that type should be a PublicationType, and bookFormat should be
    #   a BookFormat, but we are seeing failures on some items coming in due to this,
    #   so we need to confirm with DeMarque what we should be expecting here, before
    #   switching the types.
    type: str | None = Field(None, alias="@type")
    book_format: str | None = Field(None, alias="schema:bookFormat")
    isbn: str | None = Field(None, alias="schema:isbn")


class PublicationMetadata(BaseOpdsModel):
    """
    Schema.org extensions to OPDS2 metadata.

    These are sent in some OPDS2 feeds, especially from DeMarque, to provide
    additional information about the publication.
    """

    encoding_format: str | None = Field(None, alias="schema:encodingFormat")
    """
    https://schema.org/encodingFormat
    """

    work_example: list[WorkExample] = Field(
        default_factory=list, alias="schema:workExample"
    )
    """
    https://schema.org/workExample
    """
