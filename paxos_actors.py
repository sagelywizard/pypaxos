from handlers import MessageHandler

import json

class PaxosActor(MessageHandler):
    def __init__(self):
        self.handlers = {}

    def handle_message(self, sender, message):
        message_type = message['message_type']
        del message['message_type']

        handler = self.handlers.get(message_type)

        handler(sender, **message)

    def send_message(self, recipient, message_type, proposal_id=None, **kwargs):
        message = kwargs
        message['message_type'] = message_type
        if proposal_id:
            message['proposal_id'] = proposal_id
        self.queue_message(recipient, message)

class Proposer(PaxosActor):
    def __init__(self, accepters=[], proposer_to_join=None):
        self.accepters = set(accepters)
        self.last_seen_proposal_id = 0
        self.handlers = {
            'join': self.receive_join,
            'list_of_accepters': self.receive_list_of_accepters,
            'propose': self.receive_propose,
            'promise': self.receive_promise,
            'accepted': self.receive_accepted,
        }

    def join_group(self, proposer):
        self.send_message(proposer, "join")

    def receive_join(self, proposer):
        self.send_message(proposer, "list_of_accepters", list_of_accepters=self.accepters)

    def receive_list_of_accepters(self, proposer, list_of_accepters):
        for accepter in list_of_accepters:
            self.add_accepter(accepter)

    def add_accepter(self, accepter):
        self.accepters.add(accepter)

    def receive_propose(self, client, value):
        if self.accepters:
            self.propose(client, value)
        else:
            # TODO: SEND NACK
            pass

    def propose(self, client, value):
        self.proposed_value = value
        self.client_host, self.client_port, self.client_name = client
        self.last_seen_proposal_id += 1
        self.send_prepare(self.last_seen_proposal_id)

    def send_prepare(self, proposal_id):
        self.quorum = set([])
        self.quorum_reached = False
        for accepter in self.accepters:
            self.send_message(accepter, "prepare", proposal_id)

    def receive_promise(self, promiser, proposal_id, last_seen_proposal_id):
        self.quorum.add(promiser)
        if last_seen_proposal_id > self.last_seen_proposal_id:
            self.last_seen_proposal_id = last_seen_proposal_id
        if len(self.quorum) >= (len(self.accepters) > 2):
            if not self.quorum_reached:
                self.quorum_reached = True
                for accepter in self.quorum:
                    self.send_accept(accepter, self.last_seen_proposal_id, self.proposed_value)

    def send_accept(self, accepter, proposal_id, value):
        self.send_message(accepter, "accept", proposal_id, value=value, client_host=self.client_host, client_port=self.client_port, client_name=self.client_name)

    def receive_accepted(self, accepter, proposal_id, value):
        pass

class Accepter(PaxosActor):
    def __init__(self, learners=[]):
        self.learners = set(learners)
        self.last_seen_proposal_id = 0
        self.handlers = {
            "join": self.receive_join,
            "list_of_learners": self.receive_list_of_learners,
            "accept": self.receive_accept,
            "prepare": self.receive_prepare,
        }

    def join_group(self, proposer):
        self.send_message(proposer, "join")

    def receive_join(self, proposer):
        self.send_message(proposer, "list_of_learners", list_of_learners=self.learners)

    def receive_list_of_learners(self, proposer, list_of_learners):
        for learner in list_of_learners:
            self.add_learner(learner)

    def add_learner(self, learner):
        self.learners.add(learner)

    def receive_propose(self, client, value):
        self.propose(client, value)

    def receive_prepare(self, proposer, proposal_id):
        if proposal_id > self.last_seen_proposal_id:
            self.promise(proposer, proposal_id)
        else:
            pass #TODO: send nack

    def promise(self, proposer, proposal_id):
        self.send_message(proposer, "promise", proposal_id, last_seen_proposal_id=self.last_seen_proposal_id)
        self.last_seen_proposal_id = proposal_id

    def receive_accept(self, proposer, proposal_id, value, client_host, client_port, client_name):
        if proposal_id >= self.last_seen_proposal_id:
            for learner in self.learners:
                self.send_accepted(learner, proposal_id, value, client_host=client_host, client_port=client_port, client_name=client_name)
            self.send_accepted(proposer, proposal_id, value)

    def send_accepted(self, actor, proposal_id, value, **kwargs):
        self.send_message(actor, "accepted", proposal_id, value=value, **kwargs)

class Learner(PaxosActor):
    def __init__(self, call_on_learn):
        self.handlers = {
            'accepted': self.receive_accepted,
        }
        self.call_on_learn = call_on_learn

    def receive_accepted(self, accepter, proposal_id, value, client_host, client_port, client_name):
        client = (client_host, client_port, client_name)
        self.send_message(client, "learnt", proposal_id, value=value)
        self.call_on_learn(proposal_id, value)
