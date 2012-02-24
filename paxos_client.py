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

    def request(self, value):
        message = self.serialize_message(self.client_name,
                                         "proposer",
                                         {"message_type": "propose", "value": value})
        self.sock.sendto(message, (self.host, self.port))
        response = None
        while not response:
            raw_response, learner = self.sock.recvfrom(1024)
            sender_name, recipient, response_message = self.deserialize_message(raw_response)
            if response_message['value'] == value:
                return response_message
