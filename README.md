Not really sure what this is yet...

But it involves...
     _______  _______           _______  _______
    (  ____ )(  ___  )|\     /|(  ___  )(  ____ \
    | (    )|| (   ) |( \   / )| (   ) || (    \/
    | (____)|| (___) | \ (_) / | |   | || (_____
    |  _____)|  ___  |  ) _ (  | |   | |(_____  )
    | (      | (   ) | / ( ) \ | |   | |      ) |
    | )      | )   ( |( /   \ )| (___) |/\____) |
    |/       |/     \||/     \|(_______)\_______)

Werd up, my gangsters. Tell your homies, Paxos is here.

paxos_actors.py are where the Paxos "actors" live.

message_server.py is where the async message passing server and handlers are. This should probably be renamed at some point. One, because it's boring and, two, because the base message handler class lives in there too.

In order to play around with this Paxos nonsense, try this:

    $ python testpaxos.py 8888 9999
    $ python testpaxos.py 9999 8888
    $ python
    >>> import paxos_client
    >>> cli = paxos_client.PaxosClient('localhost', 8888)
    >>> cli.request('hello!')
