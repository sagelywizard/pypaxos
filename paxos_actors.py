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
        if instance_id is not None:
            message['instance_id'] = instance_id
        if ballot_id is not None:
            message['ballot_id'] = ballot_id
        if value is not None:
            message['value'] = value
        self.queue_message(recipient, message)

class Proposer(PaxosActor):
    def __init__(self, host, port, name, proposers=[], accepters=[]):
        self.accepters = set(accepters)
        # In order to achieve disjoint ballot IDs for different proposers,
        # I order the set of all (host, port, name) groups, find the index in
        # the sorted list of this proposer's (host, port, name) group and use
        # that as the mod for the ballot IDs for this proposer.
        proposers = sorted(set(proposers + [(host, port, name)]))
        ballot_mod = proposers.index((host, port, name))
        self.instances = defaultdict(lambda: {'ballot_id': ballot_mod,
                                              'quorum': set([]),
                                              'highest_accepted_ballot_id': 0,
                                              'highest_accepted_value': None,
                                              'quorum_reached': False})
        self.current_instance_id = 0
        self.handlers = {
            'propose': self.receive_propose,
            'promise': self.receive_promise,
            'accepted': self.receive_accepted,
            'nack_prepare': self.receive_nack_prepare,
        }

    def receive_propose(self, client, value):
        if self.accepters:
            self.instances['client'] = client
            self.propose(value)
        else:
            # TODO: SEND NACK
            pass

    def propose(self, value):
        self.current_instance_id += 1
        instance = self.instances[self.current_instance_id]
        instance['value'] = value
        self.send_prepare(self.current_instance_id, instance['ballot_id'])

    def send_prepare(self, instance_id, ballot_id):
        for accepter in self.accepters:
            self.send_message(accepter, "prepare", instance_id, ballot_id)

    def receive_promise(self, promiser, instance_id, ballot_id,
                        accepted_ballot_id, accepted_value,
                        highest_instance_id):
        if highest_instance_id > self.current_instance_id:
            self.current_instance_id = highest_instance_id
        instance = self.instances[instance_id]
        if accepted_ballot_id is not None:
            if accepted_ballot_id > instance['highest_accepted_ballot_id']:
                instance['highest_accepted_value'] = accepted_value
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

    def receive_nack_prepare(self, accepter, failed_instance_id, highest_instance_id):
        if highest_instance_id > self.current_instance_id:
            self.current_instance_id = highest_instance_id

        value = self.instances[failed_instance_id]['value']
        self.propose(value)

    def send_accept(self, accepter, instance_id, ballot_id, value):
        self.send_message(accepter, "accept", instance_id, ballot_id, value)

    def receive_accepted(self, accepter, instance_id, ballot_id, value):
        pass

# I am making the Accepter respond with the highest instance ID as well, so as to make proposers which propose on a lower proposal ID know to propose above the highest seen instance.

class Accepter(PaxosActor):
    def __init__(self, learners=[]):
        self.learners = set(learners)
        self.highest_instance_id = 0
        self.instances = defaultdict(lambda: {'highest_ballot_id': 0,
                                              'accepted_value': None,
                                              'accepted_ballot_id': None})
        self.handlers = {
            "accept": self.receive_accept,
            "prepare": self.receive_prepare,
        }

    def receive_prepare(self, proposer, instance_id, ballot_id):
        if ballot_id >= self.instances[instance_id]['highest_ballot_id']:
            if instance_id > self.highest_instance_id:
                self.highest_instance_id = instance_id
            self.promise(proposer, instance_id, ballot_id)
        else:
            self.send_message(proposer,
                              "nack_prepare",
                              failed_instance_id=instance_id,
                              highest_instance_id=self.highest_instance_id)

    def promise(self, proposer, instance_id, ballot_id):
        instance = self.instances[instance_id]
        accepted_ballot_id = instance['accepted_ballot_id']
        accepted_value = instance['accepted_value']
        self.send_message(proposer, "promise", instance_id, ballot_id,
                          accepted_ballot_id=accepted_ballot_id,
                          accepted_value=accepted_value,
                          highest_instance_id=self.highest_instance_id)
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
        self.call_on_learn(instance_id, value)
