from pydantic import Field, field_validator

from palace.manager.opds.base import BaseOpdsModel
from palace.manager.opds.schema_org import PublicationTypes, WorkExample
from palace.manager.util.log import LoggerMixin


class PalacePublicationMetadata(BaseOpdsModel, LoggerMixin):
    """
    Palace extensions / requirements for OPDS 2.0 publication metadata.
    """

    # While OPDS2 and RWPM only require a title, we require an identifier and type as well.
    identifier: str

    # TODO: This isn't well specified by the OPDS 2.0 spec, but since we make decisions about the
    #   type of publication based on the type set, it would be nice to do some additional validation here
    #   and constrain this to PublicationTypes. Right now the Palace Bookshelf feed uses
    #   'https://schema.org/EBook' (which is not a valid type) both because it starts with
    #   https:// (schema.org uses http://) and because its a Format, not a Type. Once we get
    #   this sorted out, we should add validation here. For now we just accept any string but
    #   log a warning if it's not a valid PublicationType.
    type: str = Field(..., alias="@type")

    # See: https://www.notion.so/lyrasis/palaceproject-io-terms-namespace-572089bd44404cf395f02b6b78361fe4
    time_tracking: bool = Field(
        False, alias="http://palaceproject.io/terms/timeTracking"
    )

    # Schema.org extensions
    # These are sent in some OPDS2 feeds, especially from DeMarque to provide some
    # additional information about the publication.
    encoding_format: str | None = Field(None, alias="schema:encodingFormat")
    work_example: list[WorkExample] = Field(
        default_factory=list, alias="schema:workExample"
    )

    @field_validator("type")
    @classmethod
    def warning_when_type_is_not_valid(cls, type_: str) -> str:
        if type_ not in list(PublicationTypes):
            cls.logger().warning(f"@type '{type_}' is not a valid PublicationType.")
        return type_
