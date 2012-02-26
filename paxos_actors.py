from collections import defaultdict
import json

from handlers import MessageHandler

def multicompare(func, one, two):
    assert len(one) == len(two), [one, two]
    for i in xrange(len(one)):
        if one[i] == two[i]:
            continue
        else:
            return func(one[i], two[i])

class PaxosActor(MessageHandler):
    handlers = {}
    def set_handler(self, **kwargs):
        for key in kwargs:
            self.handlers[key] = kwargs[key]

    def handle_message(self, sender, message):
        message_type = message['message_type']
        del message['message_type']

        handler = self.handlers.get(message_type)

        handler(sender, **message)

    def send_message(self, recipient, message_type, instance_id=None, ballot_id=None, **kwargs):
        message = kwargs
        message['message_type'] = message_type
        if instance_id:
            message['instance_id'] = instance_id
        if ballot_id:
            message['ballot_id'] = ballot_id
        self.queue_message(recipient, message)

class Proposer(PaxosActor):
    def __init__(self, accepters=[]):
        self.accepters = set(accepters)
        self.instances = defaultdict(lambda: {'ballot_id': 1, 'quorum': set([]), 'highest_accepted_ballot_id': 0, 'accepted_value': None, 'quorum_reached': False})
        self.current_instance_id = 0
        self.handlers = {
            'propose': self.receive_propose,
            'promise': self.receive_promise,
            'accepted': self.receive_accepted,
        }

    def add_accepter(self, accepter):
        self.accepters.add(accepter)

    def receive_propose(self, client, value):
        if self.accepters:
            self.current_instance_id += 1
            self.propose(value, self.current_instance_id)
        else:
            # TODO: SEND NACK
            pass

    def propose(self, value, instance_id):
        self.instances[instance_id]['value'] = value
        self.send_prepare(instance_id, self.instances[instance_id]['ballot_id'])

    def send_prepare(self, instance_id, ballot_id):
        for accepter in self.accepters:
            self.send_message(accepter, "prepare", instance_id, ballot_id)

    def receive_promise(self, promiser, instance_id, ballot_id, accepted_ballot_id, accepted_value):
        if accepted_ballot_id is not None:
            if accepted_ballot_id > self.instances[instance_id]['highest_accepted_ballot_id']:
                self.instances[instance_id]['accepted_value'] = accepted_value
                self.instances[instance_id]['highest_accepted_ballot_id'] = accepted_ballot_id

        self.instances[instance_id]['quorum'].add(promiser)
        if len(self.instances[instance_id]['quorum']) >= (len(self.accepters)/2+1):

            if self.instances[instance_id]['highest_accepted_ballot_id'] > 0:
                ballot_id = self.instances[instance_id]['highest_accepted_ballot_id']
                value = self.instances[instance_id]['highest_accepted_value']
            else:
                ballot_id = self.instances[instance_id]['ballot_id']
                value = self.instances[instance_id]['value']

            if not self.instances[instance_id]['quorum_reached']:
                self.instances[instance_id]['quorum_reached'] = True
                for accepter in self.accepters:
                    self.send_accept(accepter, instance_id, ballot_id, value)

    def send_accept(self, accepter, instance_id, ballot_id, value):
        self.send_message(accepter, "accept", instance_id, ballot_id, value=value)

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
            "return_accepted_instance_value": self.receive_accepted_instance_request,
        }

    def receive_accepted_instance_request(self, learner, instance_id):
        self.send_message(learner, "learn", self.instances[instance_id])

    def receive_prepare(self, proposer, instance_id, ballot_id):
        if ballot_id >= self.instances[instance_id]['highest_ballot_id']:
            self.promise(proposer, instance_id, ballot_id)
        else:
            pass #TODO: send nack

    def promise(self, proposer, instance_id, ballot_id):
        accepted_ballot_id = self.instances[instance_id]['accepted_ballot_id']
        accepted_value = self.instances[instance_id]['accepted_value']
        self.send_message(proposer, "promise", instance_id, ballot_id, accepted_ballot_id=accepted_ballot_id, accepted_value=accepted_value)
        self.instances[instance_id]['highest_ballot_id'] = ballot_id

    def receive_accept(self, proposer, instance_id, ballot_id, value):
        if ballot_id >= self.instances[instance_id]['highest_ballot_id']:
            self.instances[instance_id]['accepted_ballot_id'] = ballot_id
            self.instances[instance_id]['highest_ballot_id'] = ballot_id
            self.instances[instance_id]['accepted_value'] = value
            for learner in self.learners:
                self.send_accepted(learner, instance_id, ballot_id, value)
            self.send_accepted(proposer, instance_id, ballot_id, value)
        else:
            pass # TODO?: Send nack?

    def send_accepted(self, actor, instance_id, ballot_id, value):
        self.send_message(actor, "accepted", instance_id, ballot_id, value=value)

class Learner(PaxosActor):
    def __init__(self, call_on_learn, accepters=[]):
        self.accepters = set(accepters)
        self.num_of_accepters = len(self.accepters)
        self.instances = defaultdict(lambda: {'accepters': {}, 'learned': False, 'values': defaultdict(lambda: 0)})
        self.handlers = {
            'accepted': self.receive_accepted,
        }
        self.call_on_learn = call_on_learn

    def request_accepted_instance_value(self, instance_id):
        for accepter in self.accepters:
            self.send_message(accepter, "return_accepted_instance_value", instance_id)

    def receive_accepted(self, accepter, instance_id, ballot_id, value):
        if not self.instances[instance_id]['learned']:
            if accepter in self.instances[instance_id]['accepters']:
                old_value = self.instances[instance_id]['accepters'][accepter]
                self.instances[instance_id]['values'][old_value] -= 1

            self.instances[instance_id]['values'][value] += 1
            self.instances[instance_id]['accepters'][accepter] = value

            # If the instance hasn't already been learned and it has been accepted
            # by a majority of the accepters.
            if self.instances[instance_id]['values'][value] >= (len(self.accepters)/2+1):
                self.instances[instance_id]['learned'] = True
                self.learn(instance_id, value)

    def learn(self, instance_id, value):
        print instance_id, value
        ## In case there is a hole in
        #if self.instances[instance_id - 1]['learned']:
        #    self.instances[instance_id]['learned'] = True
        #    self.call_on_learn(value)
        #    self.learn(instance_id+1, value)
        #else:
        #    self.request_accepted_instance_value(instance_id - 1)
