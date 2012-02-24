from message_server import MessageServer
from paxos_actors import Proposer, Accepter, Learner

def print_stuff(proposal_id, value):
    print "%s: %s" % (proposal_id, value)

class MultiPaxos(MessageServer):
    def __init__(self, host, port):
        message_handlers = {
            'proposer': Proposer(accepters=[(host, port, 'accepter')]),
            'accepter': Accepter(learners=[(host, port, 'learner')]),
            'learner': Learner(print_stuff),
        }

        MessageServer.__init__(self, host, port, message_handlers)

if __name__ == '__main__':
    paxos = MultiPaxos('localhost', 9898)
    paxos.start()
