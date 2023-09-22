import re
import unicodedata
import urllib.parse
import uuid

from flask_babel import lazy_gettext as _

from core.model import ConfigurationSetting, ExternalIntegration, Session
from core.service.container import Services
from core.util.http import HTTP

from .config import CannotLoadConfiguration


class GoogleAnalyticsProvider:

    NAME = _("Google Analytics")
    DESCRIPTION = _("How to Configure a Google Analytics Integration")
    INSTRUCTIONS = _(
        "<p>In order to track usage statistics, you can configure the Palace Collection Manager "
        + "to connect to Google Analytics.</p>"
        + "<p>Create a <a href='https://analytics.google.com/analytics/web/provision/?authuser=0#/provision' "
        + "rel='noopener' rel='noreferer' target='_blank'>Google Analytics</a> account, "
        + "or sign into your existing one.</p>"
        + "<p>To capture data from the Palace Collection Manager in your Google Analytics account, "
        + "you must set up a property in Google Analytics for Palace Collection Manager.  In your Google Analytics "
        + "account, on the administration page for the property, go to Custom Definitions > Custom Dimensions, "
        + "and add the following dimensions, in this order: <ol>"
        + "<li>time</li>"
        + "<li>identifier</li>"
        + "<li>identifier_type</li>"
        + "<li>title</li>"
        + "<li>author</li>"
        + "<li>fiction</li>"
        + "<li>audience</li>"
        + "<li>target_age</li>"
        + "<li>publisher</li>"
        + "<li>language</li>"
        + "<li>genre</li>"
        + "<li>open_access</li>"
        + "<li>distributor</li>"
        + "<li>medium</li>"
        + "<li>library</li>"
        + "</ol></p>"
        + "<p>Each dimension should have the scope set to 'Hit' and the 'Active' box checked.</p>"
        + "<p>Then go to Tracking Info and get the tracking id for the property.  Select your "
        + "library from the dropdown below, and enter the tracking id into the form.</p>"
    )

    TRACKING_ID = "tracking_id"
    DEFAULT_URL = "http://www.google-analytics.com/collect"

    SETTINGS = [
        {
            "key": ExternalIntegration.URL,
            "label": _("URL"),
            "default": DEFAULT_URL,
            "required": True,
            "format": "url",
        },
    ]

    LIBRARY_SETTINGS = [
        {"key": TRACKING_ID, "label": _("Tracking ID"), "required": True},
    ]

    def __init__(self, integration, services: Services, library=None):
        _db = Session.object_session(integration)
        if not library:
            raise CannotLoadConfiguration(
                "Google Analytics can't be configured without a library."
            )
        url_setting = ConfigurationSetting.for_externalintegration(
            ExternalIntegration.URL, integration
        )
        self.url = url_setting.value or self.DEFAULT_URL
        self.tracking_id = ConfigurationSetting.for_library_and_externalintegration(
            _db,
            self.TRACKING_ID,
            library,
            integration,
        ).value
        if not self.tracking_id:
            raise CannotLoadConfiguration(
                "Missing tracking id for library %s" % library.short_name
            )

    def collect_event(self, library, license_pool, event_type, time, **kwargs):

        # Explicitly destroy any neighborhood information -- we don't
        # want to send this to third-party sources.
        kwargs.pop("neighborhood", None)

        client_id = uuid.uuid4()
        fields = {
            "v": 1,
            "tid": self.tracking_id,
            "cid": client_id,
            "aip": 1,  # anonymize IP
            "ds": "Circulation Manager",
            "t": "event",
            "ec": "circulation",
            "ea": event_type,
            "cd1": time,
        }

        if license_pool:
            fields.update(
                {
                    "cd2": license_pool.identifier.identifier,
                    "cd3": license_pool.identifier.type,
                }
            )

            work = license_pool.work
            edition = license_pool.presentation_edition
            if work and edition:
                fields.update(
                    {
                        "cd4": edition.title,
                        "cd5": edition.author,
                        "cd6": "fiction" if work.fiction else "nonfiction",
                        "cd7": work.audience,
                        "cd8": work.target_age_string,
                        "cd9": edition.publisher,
                        "cd10": edition.language,
                        "cd11": work.top_genre(),
                        "cd12": "true" if license_pool.open_access else "false",
                    }
                )

            # Backwards compatibility requires that new dimensions be
            # added to the end of the list. For the sake of
            # consistency, this code that sets values for those new
            # dimensions runs after the original implementation.
            fields.update({"cd13": license_pool.data_source.name})
            if work and edition:
                fields.update({"cd14": edition.medium})
        if library:
            fields.update({"cd15": library.short_name})

        if license_pool and work and edition:
            fields.update({"cd16": license_pool.collection.name})

        # urlencode doesn't like unicode strings so we convert them to utf8
        fields = {
            k: unicodedata.normalize("NFKD", str(v)).encode("utf8")
            for k, v in list(fields.items())
        }

        params = re.sub(r"=None(&?)", r"=\1", urllib.parse.urlencode(fields))
        self.post(self.url, params)

    def post(self, url, params):
        response = HTTP.post_with_timeout(url, params)


# The Analytics class looks for the name "Provider".
Provider = GoogleAnalyticsProvider
