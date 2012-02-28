from message_server import MessageServer
from paxos_actors import Proposer, Accepter, Learner
import sys

def print_stuff(instance_id, value):
    print "Instance: %s, Value: %s" % (instance_id, value)

class TestPaxos(MessageServer):
    def __init__(self, *hosts):
        local_host, local_port = hosts[0]
        message_handlers = {
            'proposer': Proposer(
                local_host,
                local_port,
                'proposer',
                proposers=[(host, port, 'proposer') for host, port in hosts],
                accepters=[(host, port, 'accepter') for host, port in hosts]),
            'accepter': Accepter(
                learners=[(host, port, 'learner') for host, port in hosts]),
            'learner': Learner(print_stuff),
        }

        MessageServer.__init__(self, local_host, local_port, message_handlers)

if __name__ == '__main__':
    hosts = [(host.split(':', 1)[0], int(host.split(':', 1)[1])) for host in sys.argv[1:]]
    paxos = TestPaxos(*hosts)
    paxos.start()
