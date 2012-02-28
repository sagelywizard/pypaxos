This is a toy implementation of the Paxos algorithm. There are some fairly important features yet to implement, which you can look at in TODO. I'll probably implement more of these features over the weekend.

Example usage:

    $ python testpaxos.py localhost:7777 localhost:8888 localhost:9999
    $ python testpaxos.py localhost:8888 localhost:9999 localhost:7777
    $ python testpaxos.py localhost:9999 localhost:7777 localhost:8888
    $ python
    >>> import paxos_client
    >>> cli = paxos_client.PaxosClient('localhost', 8888)
    >>> cli.propose('hello!')
