import logging
import sys
import urllib.parse

from palace.manager.api.app import initialize_application
from palace.manager.scripts.initialization import InstanceInitializationScript


def run(url=None):
    base_url = url or "http://localhost:6500/"
    scheme, netloc, path, parameters, query, fragment = urllib.parse.urlparse(base_url)
    if ":" in netloc:
        host, port = netloc.split(":")
        port = int(port)
    else:
        host = netloc
        port = 80

    debug = True

    # Workaround for a "Resource temporarily unavailable" error when
    # running in debug mode with the global socket timeout set by isbnlib
    if debug:
        import socket

        socket.setdefaulttimeout(None)

    # Setup database by initializing it or running migrations
    InstanceInitializationScript().run()
    app = initialize_application()

    # Required for subdomain support.
    app.config["SERVER_NAME"] = netloc

    # Allow non-HTTPS cookies in local development
    if debug:
        app.config["SESSION_COOKIE_SECURE"] = False

    logging.info("Starting app on %s:%s", host, port)

    sslContext = "adhoc" if scheme == "https" else None
    app.run(debug=debug, host=host, port=port, threaded=True, ssl_context=sslContext)


if __name__ == "__main__":
    url = sys.argv.pop() if len(sys.argv) > 1 else None
    run(url)
