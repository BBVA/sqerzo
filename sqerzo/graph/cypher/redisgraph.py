import redis
import urllib.parse as pr

from typing import Iterable, List, Type, Tuple

from redisgraph import Graph

from ...exceptions import *
from ..model import GraphElement, GraphNode
from .transaction import CypherSQErzoTransaction
from .interfaces import ResultElement, CypherSQErzoGraphConnection
from ..interfaces import SQErzoQueryResponse, SQErzoGraphConnection


class RedisGraphSQErzoQueryResponse(SQErzoQueryResponse):

    def __init__(self, graph: SQErzoGraphConnection, query: str, **kwargs):
        self.query = query
        self.graph = graph
        self.params = kwargs

    def __iter__(self):
        for res in self.graph.connection.query(
                self.query, self.params
        ).result_set:

            for node in res:
                yield ResultElement(
                    id=node.__dict__["id"],
                    labels=[node.__dict__["label"]],
                    properties=node.__dict__["properties"]
                )

class RedisSQErzoTransaction(CypherSQErzoTransaction):
    SUPPORTED_TYPES = ("str", "int", "float", "bool")

    def dump_data(self,
                  partial_query_nodes: dict,
                  partial_edged: dict):
        if partial_query_nodes:
            node_query = f"CREATE {', '.join(partial_query_nodes.values())}"
            self.graph.db_engine.query(node_query)

        #
        # Get al Edges
        #
        for edges in partial_edged.values():

            # Get first element for complete query
            edge = edges[0]

            #
            # Build bach information.
            #
            # We use indexed data batch, instead of dict, because not all
            # DB Engines support list of dict, but all supports indexed
            # data
            #
            batch = [
                [
                    b.source.make_identity(),
                    b.destination.make_identity(),
                    b.make_identity()
                ]
                for b in edges
            ]

            q = f"""
            UNWIND $batch as row
            MATCH (from:{edge.source.labels()} {{ identity: row[0] }})
            MATCH (to:{edge.destination.labels()} {{ identity: row[1] }})
            CREATE (from)-[:{edge.labels()} {{ identity: row[2] }}]->(to)
            """

            self.graph.db_engine.query(q, batch=batch)


class RedisSQErzoGraphConnection(CypherSQErzoGraphConnection):

    def __init__(self, connection_string: str):
        self.connection = self._parse_connection_string(
            connection_string
        )

    def update_element(self, graph_element: GraphElement) \
            -> None or SQErzoElementExistException:
        """Redis doesn't support multiple labels -> we'll simulate then"""

        def update_fixed_label_nodes(n) -> Iterable[Tuple[bool, GraphElement]]:
            if len(n.__labels__) > 1:
                #
                # If multiple labels are not supported, then create
                # net node clone with only new labels
                #
                for lb in n.__labels__[1:]:
                    new_node = n.clone(exclude=["identity"])
                    new_node.__labels__ = [lb]

                    yield True, new_node

            else:
                yield False, n

        for need_create, n in update_fixed_label_nodes(graph_element):

            # No multiple labels supported. Need to be created a new node
            if need_create:
                q = n.query_create()

            # Multiple labels supported -> update node
            else:
                q = n.query_update()

            self.query(q)

            n.__dirty_properties__.clear()

    def save_element(self, graph_element: GraphElement) \
            -> None or SQErzoElementExistException:

        node_id = graph_element.make_identity()

        if isinstance(graph_element, GraphNode):
            if self.get_node_by_id(node_id):
                raise SQErzoElementExistException(
                    f"Graph element with id '{node_id}' already exits"
                )

        super(RedisSQErzoGraphConnection, self).save_element(graph_element)

    @property
    def transaction_class(self) -> Type:
        return RedisSQErzoTransaction

    def query(self, query: str, **kwargs):
        self.connection.query(query, kwargs)

    def query_response(self, query: str, **kwargs) -> Iterable[ResultElement]:
        return RedisGraphSQErzoQueryResponse(self, query, **kwargs)

    def create_constraints_nodes(self, key: str, labels: List[str]):
        """Redis doesn't support constraints -> do nothing"""

    def create_constraints_edges(self, key: str, labels: List[str]):
        """Redis doesn't support constraints -> do nothing"""

    def create_indexes(self, attribute: str, labels: List[str]):
        #
        # Redis doesn't support multi-label nodes -> create two index. One
        # for each node
        #
        for label in labels:
            q = f"""
            CREATE INDEX ON :{label}({attribute})
            """

            self.query(q)


    def _parse_connection_string(self, cs: str) -> Graph:
        parsed = pr.urlparse(cs)

        if parsed.path:
            db = parsed.path[1:]

            if not db:
                db = 0
            else:
                db = int(db)
        else:
            db = 0

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
            port = 6379

        r = redis.Redis(
            host=parsed.hostname,
            port=port,
            username=parsed.username,
            password=parsed.password,
            db=db
        )

        return Graph(db_graph, r)

__all__ = ("RedisSQErzoGraphConnection",)
