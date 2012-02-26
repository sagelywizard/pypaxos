from message_server import MessageServer
from paxos_actors import Proposer, Accepter, Learner
import sys

def print_stuff(value):
    print "LEARNED STUFF"
    print value

class TestPaxos(MessageServer):
    def __init__(self, *ports):
        host = 'localhost'
        message_handlers = {
            'proposer': Proposer(accepters=[(host, port, 'accepter') for port in ports]),
            'accepter': Accepter(learners=[(host, port, 'learner') for port in ports]),
            'learner': Learner(print_stuff),
        }

        MessageServer.__init__(self, host, ports[0], message_handlers)

if __name__ == '__main__':
    ports = map(int, sys.argv[1:])
    paxos = TestPaxos(*ports)
    paxos.start()
