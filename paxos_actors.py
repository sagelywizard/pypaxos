"""
This file contains the actors for the Paxos algorithm. These actors are
passed to a message handling server (implemented in message_server.py), where
they perform as message handlers.
"""
from collections import defaultdict

from message_server import MessageHandler

class PaxosActor(MessageHandler):
    """
    The base class for all Paxos actors.
    """
    handlers = {}
    def handle_message(self, sender, message):
        message_type = message['message_type']
        del message['message_type']

        handler = self.handlers.get(message_type)

        handler(sender, **message)

    def send_message(self, recipient, message_type,
                     instance_id=None, ballot_id=None, value=None, **kwargs):
        message = kwargs
        message['message_type'] = message_type
        if instance_id:
            message['instance_id'] = instance_id
        if ballot_id:
            message['ballot_id'] = ballot_id
        if value:
            message['value'] = value
        self.queue_message(recipient, message)

class Proposer(PaxosActor):
    def __init__(self, accepters=[]):
        self.accepters = set(accepters)
        self.instances = defaultdict(lambda: {'ballot_id': 1,
                                              'quorum': set([]),
                                              'highest_accepted_ballot_id': 0,
                                              'accepted_value': None,
                                              'quorum_reached': False})
        self.current_instance_id = 0
        self.handlers = {
            'propose': self.receive_propose,
            'promise': self.receive_promise,
            'accepted': self.receive_accepted,
        }

    def receive_propose(self, client, value):
        if self.accepters:
            self.current_instance_id += 1
            self.instances['client'] = client
            self.propose(value, self.current_instance_id)
        else:
            # TODO: SEND NACK
            pass

    def propose(self, value, instance_id):
        instance = self.instances[instance_id]
        instance['value'] = value
        self.send_prepare(instance_id, instance['ballot_id'])

    def send_prepare(self, instance_id, ballot_id):
        for accepter in self.accepters:
            self.send_message(accepter, "prepare", instance_id, ballot_id)

    def receive_promise(self, promiser, instance_id, ballot_id,
                        accepted_ballot_id, accepted_value):
        instance = self.instances[instance_id]
        if accepted_ballot_id is not None:
            if accepted_ballot_id > instance['highest_accepted_ballot_id']:
                instance['accepted_value'] = accepted_value
                instance['highest_accepted_ballot_id'] = accepted_ballot_id

        instance['quorum'].add(promiser)
        if len(instance['quorum']) >= (len(self.accepters)/2+1):

            if instance['highest_accepted_ballot_id'] > 0:
                ballot_id = instance['highest_accepted_ballot_id']
                value = instance['highest_accepted_value']
            else:
                ballot_id = instance['ballot_id']
                value = instance['value']

            if not instance['quorum_reached']:
                instance['quorum_reached'] = True
                for accepter in self.accepters:
                    self.send_accept(accepter, instance_id, ballot_id, value)

    def send_accept(self, accepter, instance_id, ballot_id, value):
        self.send_message(accepter, "accept", instance_id, ballot_id, value)

    def receive_accepted(self, accepter, instance_id, ballot_id, value):
        pass

class Accepter(PaxosActor):
    def __init__(self, learners=[]):
        self.learners = set(learners)
        self.instances = defaultdict(lambda: {'highest_ballot_id': 0,
                                              'accepted_value': None,
                                              'accepted_ballot_id': None})
        self.handlers = {
            "accept": self.receive_accept,
            "prepare": self.receive_prepare,
        }

    def receive_prepare(self, proposer, instance_id, ballot_id):
        if ballot_id >= self.instances[instance_id]['highest_ballot_id']:
            self.promise(proposer, instance_id, ballot_id)
        else:
            pass #TODO: send nack

    def promise(self, proposer, instance_id, ballot_id):
        instance = self.instances[instance_id]
        accepted_ballot_id = instance['accepted_ballot_id']
        accepted_value = instance['accepted_value']
        self.send_message(proposer, "promise", instance_id, ballot_id,
                          accepted_ballot_id=accepted_ballot_id,
                          accepted_value=accepted_value)
        instance['highest_ballot_id'] = ballot_id

    def receive_accept(self, proposer, instance_id, ballot_id, value):
        instance = self.instances[instance_id]
        if ballot_id >= instance['highest_ballot_id']:
            instance['accepted_ballot_id'] = ballot_id
            instance['highest_ballot_id'] = ballot_id
            instance['accepted_value'] = value
            for learner in self.learners:
                self.send_accepted(learner, instance_id, ballot_id, value)
            self.send_accepted(proposer, instance_id, ballot_id, value)
        else:
            pass # TODO?: Send nack?

    def send_accepted(self, actor, instance_id, ballot_id, value):
        self.send_message(actor, "accepted", instance_id, ballot_id, value)

class Learner(PaxosActor):
    def __init__(self, call_on_learn, accepters=[]):
        self.accepters = set(accepters)
        self.num_of_accepters = len(self.accepters)
        self.instances = defaultdict(lambda: {'accepters': {},
                                              'learned': False,
                                              'values': defaultdict(lambda: 0)})
        self.handlers = {
            'accepted': self.receive_accepted,
        }
        self.call_on_learn = call_on_learn

    def receive_accepted(self, accepter, instance_id, ballot_id, value):
        instance = self.instances[instance_id]
        if not instance['learned']:
            if accepter in instance['accepters']:
                old_value = instance['accepters'][accepter]
                instance['values'][old_value] -= 1

            instance['values'][value] += 1
            instance['accepters'][accepter] = value

            # If the instance hasn't already been learned and it has been
            # accepted by a majority of the accepters.
            if instance['values'][value] >= (len(self.accepters)/2+1):
                instance['learned'] = True
                self.learn(instance_id, value)

    def learn(self, instance_id, value):
        self.call_on_learn(value)
