#!/usr/bin/env python
import collections
import asyncore
import socket
import heapq
import time
import uuid
import json
import sys

def default_serialize_message(sender_name, recipient, message, callback_id=None, is_response=False):
    # callback_id is only for messages with timeout
    return json.dumps([sender_name, recipient, message, callback_id, is_response])

def default_deserialize_message(serialized_message):
    sender_name, recipient, message, callback_id, is_response = json.loads(serialized_message)
    return sender_name, recipient, message, callback_id, is_response

class MessageHandler(object):
    """
    The MessageHandler (or any object which inherits from MessageHandler) must
    implement handle_message and send_message.
    """
    def handle_message(self, sender, message):
        raise NotImplemented

    def send_message(self, recipient, message):
        raise NotImplemented

    def _initialize_message_handler(self, message_handler_id, message_server):
        self._message_handler_id = message_handler_id
        self._message_server = message_server

    def queue_message(self, recipient, message, timeout=None,
                      timeout_func=None, callback_func=None, is_response=None,
                      callback_id=None):
        # Either they're all None, or none of them are None!
        #assert all(map(lambda x: x is not None, [timeout, timeout_func, callback_func])) or all(map(lambda x: x is None, [timeout, timeout_func, callback_func]))
        self._message_server.queue_message(self._message_handler_id,
                                           recipient,
                                           message,
                                           timeout,
                                           timeout_func,
                                           callback_func,
                                           is_response,
                                           callback_id)

    def register_callback(self, sender, callback_id):
        self.callback_sender = sender
        self.next_callback_id = callback_id

    def respond(self, message={}):
        self.queue_message(self.callback_sender,
                           message,
                           callback_id=self.next_callback_id,
                           is_response=True)

class MessageServer(asyncore.dispatcher):
    """
    A select()-based async message-passing server. This 
    """
    def __init__(self, host, port, message_handlers,
                 serialize_message=default_serialize_message,
                 max_message_size=8192, poll_interval=.1,
                 deserialize_message=default_deserialize_message):
        asyncore.dispatcher.__init__(self)

        self.max_message_size = max_message_size
        self.message_handlers = {}
        self.write_buffer = ''
        self.buffer_recipient = None
        self.poll_interval = poll_interval
        self.callbacks = {}
        self.timeouts = []

        self.host = host
        self.port = port

        self.deserialize_message = deserialize_message
        self.serialize_message = serialize_message

        self.create_socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.bind((self.host, self.port))

        self.next_timeout = None

        for handler_name in message_handlers:
            handler_object = message_handlers[handler_name]
            self.set_handler(handler_name, handler_object)
        self.message_queue = collections.deque()

    def set_handler(self, handler_name, handler_object):
        handler_object._initialize_message_handler(handler_name, self)
        self.message_handlers[handler_name] = handler_object

    def handle_read(self):
        raw_message, (host, port) = self.recvfrom(self.max_message_size)
        try:
            sender_name, recipient, message, callback_id, is_response = self.deserialize_message(raw_message)
        except Exception as err:
            print >> sys.stderr, "Error on message deserialzation: %s" % err
        else:
            if is_response:
                assert callback_id is not None
                if callback_id in self.callbacks:
                    self.callbacks[callback_id]['callback'](message)
                    del self.callbacks[callback_id]
            else:
                sender = (host, port, sender_name)
                handler = self.message_handlers.get(recipient)
                if handler:
                    if callback_id is not None:
                        handler.register_callback(sender, callback_id)
                    handler.handle_message(sender, message)

    # Called every N seconds, where N is the 'timeout' arg to asyncore.loop().
    # If it returns True, handle_write is called.
    def writable(self):
        return self.write_buffer or self.message_queue or \
               (self.timeouts and time.time() >= heapq.nsmallest(1, self.timeouts)[0][0])

    # handle_write is called whenever a write needs to be handled or when a
    # message's timeout has been reached.
    def handle_write(self):
        if self.timeouts and time.time() >= self.timeouts[0][0]:
            _, callback_id = heapq.heappop(self.timeouts)
            if callback_id in self.callbacks:
                try:
                    self.callbacks[callback_id]['timeout_func']()
                finally:
                    del self.callbacks[callback_id]
        else:
            if not self.write_buffer:
                self.buffer_recipient, self.write_buffer = self.message_queue.popleft()
            else:
                sent_bytes = self.sendto(self.write_buffer, self.buffer_recipient)
                self.write_buffer = self.write_buffer[sent_bytes:]

    def queue_message(self, sender_name, recipient, message, timeout, timeout_func, callback_func, is_response=False, callback_id=None):
        host, port, recipient_name = recipient

        if timeout is not None:
            callback_id = str(uuid.uuid4())
            failure_time = time.time() + timeout
            self.callbacks[callback_id] = {'callback': callback_func,
                                           'timeout': failure_time,
                                           'timeout_func': timeout_func}
            heapq.heappush(self.timeouts, (failure_time, callback_id))
            self.next_timeout = self.timeouts[0][0]

        self.message_queue.append((
                (host, port),
                self.serialize_message(sender_name, recipient_name, message, callback_id, is_response)))

    def start(self):
        asyncore.loop(self.poll_interval)
