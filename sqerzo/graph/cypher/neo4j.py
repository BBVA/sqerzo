import urllib.parse as pr

from typing import List, Iterable

from neo4j import GraphDatabase

from ..model import GraphElement
from ..interfaces import ResultElement, SQErzoQueryResponse
from ... import SQErzoGraphConnection
from ...exceptions import SQErzoElementExistException
from .interfaces import CypherSQErzoGraphConnection


class Neo4jSQErzoQueryResponse(SQErzoQueryResponse):

    def __init__(self, graph: SQErzoGraphConnection, query: str, **kwargs):
        self.query = query
        self.graph = graph
        self.params = kwargs

    def __iter__(self):
        with self.graph.connection.session() as session:
            ret = session.run(self.query, **self.params)

            for node in ret.value():
                yield ResultElement(
                    id=node.id,
                    labels=list(node.labels),
                    properties=node._properties
                )


class Neo4JSQErzoGraphConnection(CypherSQErzoGraphConnection):
    SUPPORTED_TYPES = ("str", "int", "float", "bool", "datetime")

    def __init__(self, connection_string: str):
        self.connection = self._parse_connection_string(connection_string)
        self.enterprise: bool = False

    def query_with_response(self,
                            query: str,
                            **kwargs) -> Iterable[ResultElement]:
        with self.connection.session() as session:
            session.run(query, **kwargs)

    def query_response(self, query: str, **kwargs) -> Iterable[ResultElement]:
        return Neo4jSQErzoQueryResponse(self, query, **kwargs)

    def query(self, query: str, **kwargs) -> None:
        with self.connection.session() as session:
            session.run(query, **kwargs)

    def save_element(self, graph_element: GraphElement) -> None or SQErzoElementExistException:
        try:
            super(Neo4JSQErzoGraphConnection, self).save_element(graph_element)
        except Exception:
            raise SQErzoElementExistException(
                f"Graph element with id '{graph_element.identity}' already exits"
            )

    def create_constraints_nodes(self, key: str, labels: List[str]):
        constrain_name = self.plain_string(self.labels_to_name(labels))
        label = self.labels_to_name(labels)

        q = f"""
        CREATE CONSTRAINT {constrain_name}_unique_{key} IF NOT EXISTS
        ON (p:{label}) ASSERT p.{key} IS UNIQUE
        """

        self.query(q)

    def create_constraints_edges(self, key: str, labels: List[str]):
        if not self.enterprise:
            return

        constrain_name = self.plain_string(self.labels_to_name(labels))
        label = self.labels_to_name(labels)

        q = f"""
        CREATE CONSTRAINT {constrain_name}_unique_{key} IF NOT EXISTS
        ON ()-[p:{label}]-() ASSERT EXISTS (p.{key})
        """

        self.query(q)

    def create_indexes(self, attribute: str, labels: List[str]):
        label = self.labels_to_name(labels)
        index_suffix = self.plain_string(label)
        index_prefix = self.plain_string(attribute)

        q = f"""
        CREATE INDEX index_{index_prefix}_{index_suffix} IF NOT EXISTS FOR (n:{label})
        ON (n.{attribute})
        """

        self.query(q)

    def _parse_connection_string(self, cs: str) -> GraphDatabase:
        parsed = pr.urlparse(cs)

        host = parsed.hostname
        if not host:
            host = "127.0.0.1"

        port = parsed.port
        if port is None:
            port = 7687

        config = {"uri": f"bolt://{host}:{port}"}

        if parsed.username and parsed.password:
            config["auth"] = (parsed.username, parsed.password)

        if "enterprise" in parsed.scheme:
            self.enterprise = True

        return GraphDatabase.driver(**config)


__all__ = ("Neo4JSQErzoGraphConnection",)
