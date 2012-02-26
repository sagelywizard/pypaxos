In order to play around with this Paxos nonsense, try this:

    $ python testpaxos.py 7777 8888 9999
    $ python testpaxos.py 8888 9999 8888
    $ python testpaxos.py 9999 7777 8888
    $ python
    >>> import paxos_client
    >>> cli = paxos_client.PaxosClient('localhost', 8888)
    >>> cli.request('hello!')
