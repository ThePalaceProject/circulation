from flask_babel import lazy_gettext as _
from sqlalchemy.orm.session import Session

from core.model import CirculationEvent, ExternalIntegration, create, get_one
from core.util.log import LoggerMixin


class LocalAnalyticsProvider(LoggerMixin):
    NAME = _("Local Analytics")

    DESCRIPTION = _("Store analytics events in the 'circulationevents' database table.")

    # A given site can only have one analytics provider.
    CARDINALITY = 1

    # Where to get the 'location' of an analytics event.
    LOCATION_SOURCE = "location_source"

    # The 'location' of an analytics event is the 'neighborhood' of
    # the request's authenticated patron.
    LOCATION_SOURCE_NEIGHBORHOOD = "neighborhood"

    # Analytics events have no 'location'.
    LOCATION_SOURCE_DISABLED = None

    def __init__(self, config):
        self.location_source = config.location_source

    def collect_event(
        self,
        library,
        license_pool,
        event_type,
        time,
        old_value=None,
        new_value=None,
        **kwargs
    ):
        if not library and not license_pool:
            raise ValueError("Either library or license_pool must be provided.")
        if library:
            _db = Session.object_session(library)
        else:
            _db = Session.object_session(license_pool)

        neighborhood = None
        if self.location_source == self.LOCATION_SOURCE_NEIGHBORHOOD:
            neighborhood = kwargs.pop("neighborhood", None)

        return CirculationEvent.log(
            _db,
            license_pool,
            event_type,
            old_value,
            new_value,
            start=time,
            library=library,
            location=neighborhood,
        )

    @classmethod
    def initialize(cls, _db):
        """Find or create a local analytics service."""

        # If a local analytics service already exists, return it.
        local_analytics = get_one(
            _db,
            ExternalIntegration,
            protocol=cls.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )

        # If a local analytics service already exists, don't create a
        # default one. Otherwise, create it with default name of
        # "Local Analytics".
        if not local_analytics:
            local_analytics, ignore = create(
                _db,
                ExternalIntegration,
                protocol=cls.__module__,
                goal=ExternalIntegration.ANALYTICS_GOAL,
                name=str(cls.NAME),
            )
        return local_analytics


# The Analytics class looks for the name "Provider".
Provider = LocalAnalyticsProvider
