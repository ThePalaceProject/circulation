from palace.manager.api.sip import SIPClient
from palace.manager.api.sip.client import Constants


class MockSIPClient(SIPClient):
    """A SIP client that relies on canned responses rather than a socket
    connection.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.read_count = 0
        self.write_count = 0
        self.requests = []
        self.responses = []
        self.status = []

    def queue_response(self, response):
        if isinstance(response, str):
            # Make sure responses come in as bytestrings, as they would
            # in real life.
            response = response.encode(Constants.DEFAULT_ENCODING)
        self.responses.append(response)

    def connect(self):
        # Since there is no socket, do nothing but reset the local
        # connection-specific variables.
        self.status.append("Creating new socket connection.")
        self.reset_connection_state()
        return None

    def do_send(self, data):
        self.write_count += 1
        self.requests.append(data)

    def read_message(self, max_size=1024 * 1024):
        """Read a response message off the queue."""
        self.read_count += 1
        response = self.responses[0]
        self.responses = self.responses[1:]
        return response

    def disconnect(self):
        pass
