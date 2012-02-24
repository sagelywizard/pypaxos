from message_server import MessageServer
from paxos_actors import Proposer, Accepter, Learner
import sys

def print_stuff(proposal_id, value):
    print "%s: %s" % (proposal_id, value)

class TestPaxos(MessageServer):
    def __init__(self, port_a, port_b):
        host = 'localhost'
        message_handlers = {
            'proposer': Proposer(accepters=[(host, port_a, 'accepter'), (host, port_b, 'accepter')]),
            'accepter': Accepter(learners=[(host, port_a, 'learner'), (host, port_b, 'learner')]),
            'learner': Learner(print_stuff),
        }

        MessageServer.__init__(self, host, port_a, message_handlers)

if __name__ == '__main__':
    port_a = int(sys.argv[1])
    port_b = int(sys.argv[2])
    paxos = TestPaxos(port_a, port_b)
    paxos.start()
