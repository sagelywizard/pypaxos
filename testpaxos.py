from message_server import MessageServer
from paxos_actors import Proposer, Accepter, Learner
import sys

def print_stuff(value):
    print "LEARNED STUFF"
    print value

class TestPaxos(MessageServer):
    def __init__(self, *hosts):
        host, port = hosts[0]
        message_handlers = {
            'proposer': Proposer(accepters=[(host, port, 'accepter') for host, port in hosts]),
            'accepter': Accepter(learners=[(host, port, 'learner') for host, port in hosts]),
            'learner': Learner(print_stuff),
        }

        MessageServer.__init__(self, host, port, message_handlers)

if __name__ == '__main__':
    hosts = [(host.split(':', 1)[0], int(host.split(':', 1)[1])) for host in sys.argv[1:]]
    paxos = TestPaxos(*hosts)
    paxos.start()
