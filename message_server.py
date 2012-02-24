import asyncore
import socket
import json

def default_serialize_message(sender_name, recipient, message):
    return json.dumps([sender_name, recipient, message])

def default_deserialize_message(serialized_message):
    sender_name, recipient, message = json.loads(serialized_message)
    return sender_name, recipient, message

class MessageServer(asyncore.dispatcher):
    def __init__(self, host, port, message_handlers,
                 serialize_message=default_serialize_message,
                 deserialize_message=default_deserialize_message):
        asyncore.dispatcher.__init__(self)

        self.message_handlers = {}
        self.write_buffer = ''
        self.buffer_recipient = None

        self.host = host
        self.port = port

        self.deserialize_message = deserialize_message
        self.serialize_message = serialize_message

        self.create_socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.bind((host, port))

        for handler_name in message_handlers:
            handler_object = message_handlers[handler_name]
            self.set_handler(handler_name, handler_object)
        self.message_queue = []

    def set_handler(self, handler_name, handler_object):
        handler_object._initialize_message_handler(handler_name, self)
        self.message_handlers[handler_name] = handler_object

    def handle_read(self):
        raw_message, (host, port) = self.recvfrom(1024)
        sender_name, recipient, message = self.deserialize_message(raw_message)
        sender = (host, port, sender_name)
        handler = self.message_handlers.get(recipient)
        #print "HANDLING WRITE!!"
        #print self.message_queue
        #print "HANDLING WRITE!!"
        if handler:
            handler.handle_message(sender, message)

    def writable(self):
        return self.write_buffer or self.message_queue

    def handle_write(self):
        if not self.write_buffer:
            self.buffer_recipient, self.write_buffer = self.message_queue.pop()
        else:
            sent_bytes = self.sendto(self.write_buffer, self.buffer_recipient)
            self.write_buffer = self.write_buffer[sent_bytes:]

    def queue_message(self, sender_name, recipient, message):
        host, port, recipient_name = recipient
        self.message_queue.append((
                (host, port),
                self.serialize_message(sender_name, recipient_name, message)))

    def start(self):
        asyncore.loop()
