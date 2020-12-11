"""

TODO: FINISH IMPLEMENTATION FOR NEPTUNE AND GREMLIN BY USING:

https://github.com/opencypher/cypher-for-gremlin/tree/master/tinkerpop/cypher-gremlin-server-plugin#gremlin-python


"""

import urllib.parse as pr

from typing import Iterable

from gremlin_python.driver.client import Client
from gremlin_python.driver.request import RequestMessage
from gremlin_python.driver.serializer import GraphSONMessageSerializer

from .shared import ResultElement


def gremlin_match_parser(res) -> Iterable[ResultElement or str]:
    ## TODO
    raise NotImplementedError()


def gremlin_graph_connection(connection_string: str):
    parsed = pr.urlparse(connection_string)

    if parsed.query:
        qp = parsed.query.split("&", maxsplit=1)[0]
        p_name, p_value = qp.split("=")
        if p_name != "graph":
            db_graph = "sqerzo"
        else:
            db_graph = p_value
    else:
        db_graph = "sqerzo"

    port = parsed.port
    if port is None:
        port = 8182


    serializer = GraphSONMessageSerializer()
    # workaround to avoid exception on any opProcessor other than `standard`
    # or `traversal`:
    serializer.cypher = serializer.standard

    client = Client(f'ws://{parsed.hostname}:{port}/gremlin', 'g',
                    message_serializer=serializer)

    def connection(c):
        def _query(query: str):
            message = RequestMessage('cypher', 'eval', {'gremlin': query})
            return c.submit(message).all().result()

        return _query

    return connection(client)


__all__ = ("gremlin_graph_connection", "gremlin_match_parser")
