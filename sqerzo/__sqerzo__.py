from __future__ import annotations

import logging
import urllib.parse as pr

from typing import Type, List, Iterable

from .cache import *
from .config import *
from .exceptions import *
from .graph.model import *
from .graph.transaction import SQErzoTransaction
from .graph.interfaces import SQErzoGraphConnection
from .graph.cypher.neo4j import Neo4JSQErzoGraphConnection
from .graph.cypher.redisgraph import RedisSQErzoGraphConnection

log = logging.getLogger("sqerzo")

_SUPPORTED_TYPES = ("str",)
_SUPPORTED_TYPES_REDIS = ("str", "int", "float", "bool")
_SUPPORTED_TYPES_NEO4J = ("str", "int", "float", "bool", "datetime")


class SQErzoGraph:

    def __init__(self, connection_string: str = None, cache: str = None):
        self.connection_string = self._fix_connection_string(connection_string)
        self.db_engine: SQErzoGraphConnection = self._setup_db(self.connection_string)
        self.cache = self._setup_cache(cache)

        self._create_constrains()

    def transaction(self) -> SQErzoTransaction:
        return self.db_engine.transaction_class(self)

    def update(self, node: GraphElement):
        self.db_engine.update_element(node)

    def get_node_by_id(self, node_id: str, map_class: Type[GraphNode]) \
            -> Type or None:
        if not node_id:
            return

        # Check if is in cache
        if node_cache := self.cache.get_id(node_id):
            return node_cache

        if found := self.db_engine.get_node_by_id(node_id):
            node = map_class.from_query_results(found)

            self.cache.save_id(node_id, node)

            return node

    def save(self, graph_element: GraphElement) \
            -> None or SQErzoElementExistException:
        self.db_engine.save_element(graph_element)


    def truncate(self):
        """Remove all nodes and edges of database"""
        self.db_engine.truncate()

    def raw_query(self, query: str, **kwargs):
        self.db_engine.query(query, **kwargs)

    def fetch_many(self,
                   node_type: Type[GraphElement] or GraphElement,
                   **kwargs: dict) \
            -> Iterable[GraphElement] or SQErzoException:
        for x in self.db_engine.fetch_nodes(node_type, **kwargs):
            yield x

    def fetch_one(self,
                   node_type: Type[GraphElement] or GraphElement,
                   **kwargs: dict) \
            -> Iterable[GraphElement] or SQErzoException:

        for res in self.fetch_many(node_type, **kwargs):
            return res

    # -------------------------------------------------------------------------
    # Private methods
    # -------------------------------------------------------------------------
    def _setup_cache(self, cache_config: str):
        if cache_config is None or cache_config == "memory://":
            return MemoryGraphNodeCache()
        else:
            raise ValueError(f"Invalid cache config: '{cache_config}'")

    def _fix_connection_string(self, cs: str) -> str:

        parsed = pr.urlparse(cs)

        if parsed.path == cs:
            return f"{cs}://"
        else:
            return cs

    def _setup_db(self, connection: str) -> SQErzoGraphConnection:

        parsed = pr.urlparse(connection)

        # -------------------------------------------------------------------------
        # Update connection_string
        # -------------------------------------------------------------------------
        if parsed.scheme.startswith("redis"):
            SQErzoConfig.SUPPORTED_TYPES = _SUPPORTED_TYPES_REDIS
            SQErzoConfig.SUPPORTED_INDEXES = True
            SQErzoConfig.SUPPORTED_CONSTRAINTS = False
            SQErzoConfig.SUPPORTED_MULTIPLE_LABELS = False

            return RedisSQErzoGraphConnection(self.connection_string)

        elif parsed.scheme.startswith(("enterprise+neo4j", "neo4j")):
            SQErzoConfig.SUPPORTED_TYPES = _SUPPORTED_TYPES_NEO4J
            SQErzoConfig.SUPPORTED_INDEXES = True
            SQErzoConfig.SUPPORTED_CONSTRAINTS = True
            SQErzoConfig.SUPPORTED_MULTIPLE_LABELS = True

            return Neo4JSQErzoGraphConnection(self.connection_string)

        elif parsed.scheme.startswith("neptune"):
            raise NotImplementedError("Neptune is not implemented yet")

        elif parsed.scheme.startswith("gremlin"):
            raise NotImplementedError("Neptune is not implemented yet")

        else:
            raise ValueError("Invalid db engine")

    def _create_constrains(self):

        nodes: List[GraphNode] = SQErzoConfig.SETUP_OBJECTS

        for n in nodes:
            labels = n.__labels__

            #
            # For Edge types
            #
            if issubclass(n, GraphEdge):

                for key in n.__keys__:
                    self.db_engine.create_constraints_edges(key, labels)

                for key in n.__unique__:
                    self.db_engine.create_constraints_nodes(key, labels)

            elif issubclass(n, GraphNode):

                for key in n.__keys__:
                    self.db_engine.create_constraints_nodes(key, labels)

                for key in n.__unique__:
                    self.db_engine.create_constraints_nodes(key, labels)

                for index_attr in n.__indexes__:
                    self.db_engine.create_indexes(index_attr, labels)

            else:
                raise ValueError(f"Invalid Node type: {type(n)}")

__all__ = ("SQErzoGraph",)
