from palace.manager.core.classifier import Classifier
from palace.manager.feed.worklist.base import WorkList


class DynamicLane(WorkList):
    """A WorkList that's used to from an OPDS lane, but isn't a Lane
    in the database."""


class WorkBasedLane(DynamicLane):
    """A lane that shows works related to one particular Work."""

    DISPLAY_NAME: str | None = None
    ROUTE: str | None = None

    def __init__(self, library, work, display_name=None, children=None, **kwargs):
        self.work = work
        self.edition = work.presentation_edition

        # To avoid showing the same book in other languages, the value
        # of this lane's .languages is always derived from the
        # language of the work.  All children of this lane will be put
        # under a similar restriction.
        self.source_language = self.edition.language
        kwargs["languages"] = [self.source_language]

        # To avoid showing inappropriate material, the value of this
        # lane's .audiences setting is always derived from the
        # audience of the work. All children of this lane will be
        # under a similar restriction.
        self.source_audience = self.work.audience
        kwargs["audiences"] = self.audiences_list_from_source()

        display_name = display_name or self.DISPLAY_NAME

        children = children or list()

        super().initialize(
            library, display_name=display_name, children=children, **kwargs
        )

    @property
    def url_arguments(self):
        if not self.ROUTE:
            raise NotImplementedError()
        identifier = self.edition.primary_identifier
        kwargs = dict(identifier_type=identifier.type, identifier=identifier.identifier)
        return self.ROUTE, kwargs

    def audiences_list_from_source(self):
        if (
            not self.source_audience
            or self.source_audience in Classifier.AUDIENCES_ADULT
        ):
            return Classifier.AUDIENCES
        if self.source_audience == Classifier.AUDIENCE_YOUNG_ADULT:
            return Classifier.AUDIENCES_JUVENILE
        else:
            return [Classifier.AUDIENCE_CHILDREN]

    def append_child(self, worklist):
        """Add another Worklist as a child of this one and change its
        configuration to make sure its results fit in with this lane.
        """
        super().append_child(worklist)
        worklist.languages = self.languages
        worklist.audiences = self.audiences

    def accessible_to(self, patron):
        """In addition to the restrictions imposed by the superclass, a lane
        based on a specific Work is accessible to a Patron only if the
        Work itself is age-appropriate for the patron.

        :param patron: A Patron
        :return: A boolean
        """
        superclass_ok = super().accessible_to(patron)
        return superclass_ok and (
            not self.work or self.work.age_appropriate_for_patron(patron)
        )
