import logging
import os
import urllib.parse

from flask import Flask
from flask_babel import Babel
from flask_sqlalchemy_session import flask_scoped_session
from redmail import EmailSender

from api.config import Configuration
from core.log import LogConfiguration
from core.model import SessionManager
from core.util import LanguageCodes

from .util.profilers import (
    PalaceCProfileProfiler,
    PalacePyInstrumentProfiler,
    PalaceXrayProfiler,
)

app = Flask(__name__)
app._db = None
app.config["BABEL_DEFAULT_LOCALE"] = LanguageCodes.three_to_two[
    Configuration.localization_languages()[0]
]
app.config["BABEL_TRANSLATION_DIRECTORIES"] = "../translations"
babel = Babel(app)

# We use URIs as identifiers throughout the application, meaning that
# we never want werkzeug's merge_slashes feature.
app.url_map.merge_slashes = False

# Optionally setup any profilers that are enabled
PalacePyInstrumentProfiler.configure(app)
PalaceCProfileProfiler.configure(app)
PalaceXrayProfiler.configure(app)


@app.before_first_request
def initialize_database(autoinitialize=True):
    testing = "TESTING" in os.environ

    db_url = Configuration.database_url()
    if autoinitialize:
        SessionManager.initialize(db_url)
    session_factory = SessionManager.sessionmaker(db_url)
    _db = flask_scoped_session(session_factory, app)
    app._db = _db

    log_level = LogConfiguration.initialize(_db, testing=testing)
    debug = log_level == "DEBUG"
    app.config["DEBUG"] = debug
    app.debug = debug
    _db.commit()
    logging.getLogger().info("Application debug mode==%r" % app.debug)


def setup_email_configuration():
    mail_server = os.environ.get("MAIL_SERVER")
    mail_port = int(os.environ.get("MAIL_PORT", "25"))
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")

    return EmailSender(
        host=mail_server,
        port=mail_port,
        username=mail_username,
        password=mail_password,
        use_starttls=False,
    )


app.mail = setup_email_configuration()


from . import routes  # noqa
from .admin import routes  # noqa


def run(url=None):
    base_url = url or "http://localhost:6500/"
    scheme, netloc, path, parameters, query, fragment = urllib.parse.urlparse(base_url)
    if ":" in netloc:
        host, port = netloc.split(":")
        port = int(port)
    else:
        host = netloc
        port = 80

    # Required for subdomain support.
    app.config["SERVER_NAME"] = netloc

    debug = True

    # Workaround for a "Resource temporarily unavailable" error when
    # running in debug mode with the global socket timeout set by isbnlib
    if debug:
        import socket

        socket.setdefaulttimeout(None)

    logging.info("Starting app on %s:%s", host, port)
    sslContext = "adhoc" if scheme == "https" else None
    app.run(debug=debug, host=host, port=port, threaded=True, ssl_context=sslContext)
