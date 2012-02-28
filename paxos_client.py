from message_server import default_serialize_message, default_deserialize_message

import socket
import uuid
import json

class PaxosClient(object):
    def __init__(self, host, port,
                 serialize_message=default_serialize_message,
                 deserialize_message=default_deserialize_message):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client_name = str(uuid.uuid4())
        self.host = host
        self.port = port
        self.serialize_message = serialize_message
        self.deserialize_message = deserialize_message

    def propose(self, value):
        assert isinstance(value, basestring), \
                "Proposed value must be of basestring type."
        message = self.serialize_message(self.client_name,
                                         "proposer",
                                         {"message_type": "propose", "value": value})
        self.sock.sendto(message, (self.host, self.port))
