In order to play around with this Paxos nonsense, try this:

    $ python testpaxos.py localhost:7777 localhost:8888 localhost:9999
    $ python testpaxos.py localhost:8888 localhost:9999 localhost:7777
    $ python testpaxos.py localhost:9999 localhost:7777 localhost:8888
    $ python
    >>> import paxos_client
    >>> cli = paxos_client.PaxosClient('localhost', 8888)
    >>> cli.request('hello!')
