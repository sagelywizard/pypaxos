import json
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

    def queue_message(self, recipient, message):
        self._message_server.queue_message(self._message_handler_id,
                                           recipient,
                                           message)
