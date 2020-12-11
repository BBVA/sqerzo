from __future__ import annotations

import urllib.parse as pr
from typing import Callable, Type, Tuple

from .cache import *
from .config import *
from .graph import *
from .helpers import *
from .model import *

log = logging.getLogger("sqerzo")

_SUPPORTED_TYPES = ("str",)
_SUPPORTED_TYPES_REDIS = ("str", "int", "float", "bool")
_SUPPORTED_TYPES_NEO4J = ("str", "int", "float", "bool", "datetime")


def get_from_param(o: GraphNode, param: str, value: str) -> str:
    labels = ":".join(o.__labels__)
    prop = ''.join(prepare_params({param: value}, operation="query"))

    return f"""
    MATCH (a:{labels})
    WHERE {prop}
    RETURN a
    """


class _SQErzoGraph:

    def __init__(self):
        self.connection_string = None
        self.con_read: Callable[[str], List[ResultElement]] = None
        self.con_write: Callable[[str], None] = None
        self.match_parser: Callable = None

    def setup(self, connection: str):

        self.connection_string = connection

        parsed = pr.urlparse(connection)
        if parsed.path == connection:
            self.connection_string = f"{connection}://"

        # -------------------------------------------------------------------------
        # Update connection_string
        # -------------------------------------------------------------------------
        if parsed.scheme.startswith("redis"):
            SQErzoConfig.SUPPORTED_TYPES = _SUPPORTED_TYPES_REDIS
            SQErzoConfig.SUPPORTED_CONSTRAINTS = False
            SQErzoConfig.SUPPORTED_MULTIPLE_LABELS = False

            self.match_parser = redis_match_parser
            self.con_write = self.con_read = redis_graph_connection(
                self.connection_string
            )

        elif parsed.scheme.startswith("neo4j"):
            SQErzoConfig.SUPPORTED_TYPES = _SUPPORTED_TYPES_NEO4J
            SQErzoConfig.SUPPORTED_CONSTRAINTS = True
            SQErzoConfig.SUPPORTED_MULTIPLE_LABELS = True

            self.match_parser = neo4j_match_parser
            self.con_write, self.con_read = neo4j_graph_connection(
                self.connection_string
            )

        elif parsed.scheme.startswith(("gremlin", "neptune")):
            SQErzoConfig.SUPPORTED_TYPES = _SUPPORTED_TYPES_NEO4J
            SQErzoConfig.SUPPORTED_CONSTRAINTS = False
            SQErzoConfig.SUPPORTED_MULTIPLE_LABELS = True

            self.match_parser = gremlin_match_parser
            self.con_write = self.con_read = gremlin_graph_connection(
                self.connection_string
            )

        else:
            raise ValueError("Invalid db engine")

        # -------------------------------------------------------------------------
        # Update constraints
        # -------------------------------------------------------------------------
        self._create_constrains()

    def _save(self, graph_element: GraphElement):
        q = graph_element.query_create()

        try:

            # Check if is in cache
            if node_cache := MemoryGraphNodeCache.get(graph_element):
                graph_element.identity = node_cache.identity
                return

            MemoryGraphNodeCache.get_or_create(graph_element)

            return self.con_write(q)
        except ValueError as e:
            #
            # This exception raises when node already exits
            #

            # First -> try to recover from cache
            if node_cache := MemoryGraphNodeCache.get(graph_element):
                graph_element.identity = node_cache.identity

            # Otherwise -> Try to recover from database
            else:
                if found := self._read(graph_element.query_get_node()):
                    graph_element.identity = found[0]["a.identity"]
                    MemoryGraphNodeCache.get_or_create(graph_element)
                else:
                    raise ValueError("Can't find node")

    def _read(self, q: str):
        return self.con_read(q)

    def _create_constrains(self):

        def labels_to_name(labels: List[str]) -> str:
            return "_".join(l.replace(":", "_").lower() for l in labels)

        def constraint_node(key, labels) -> str:
            constrain_name = labels_to_name(labels)

            return f"""
            CREATE CONSTRAINT {constrain_name}_unique_{key} IF NOT EXISTS
            ON (p:{':'.join(labels)}) ASSERT p.identity IS UNIQUE
            """

        def constraint_edge(key, labels) -> str:
            # TODO
            constrain_name = labels_to_name(labels)

            return f"""
            CREATE CONSTRAINT edge_{constrain_name} IF NOT EXISTS
            ON ()-[p:{labels}]-() ASSERT EXISTS (p.{key})
            """

        if SQErzoConfig.SUPPORTED_CONSTRAINTS is False:
            return

        nodes: List[GraphNode] = SQErzoConfig.SETUP_OBJECTS

        for n in nodes:
            labels = n.__labels__

            #
            # For Edge types
            #
            if issubclass(n, GraphEdge):
                # TODO: Edge constraints
                # cs = constraint_edge(key, labels)  # TODO
                continue

            elif issubclass(n, GraphNode):
                # Create constraints
                for key in n.__keys__:
                    self.con_write(constraint_node(key, labels))

                # TODO: Indexes

                # TODO: Uniques

            else:
                raise ValueError(f"Invalid Node type: {type(n)}")


    def update(self, node: GraphElement):

        def update_fixed_label_nodes(n) -> Iterable[Tuple[bool, GraphElement]]:
            if len(node.__labels__) > 1 and not SQErzoConfig.SUPPORTED_MULTIPLE_LABELS:
                #
                # If multiple labels are not supported, then create
                # net node clone with only new labels
                #
                for i, lb in enumerate(node.__labels__):
                    if i == 0:
                        continue

                    new_node = node.clone(exclude=["identity"])
                    new_node.__labels__ = [lb]

                    yield True, new_node

            else:
                yield False, n

        for need_create, n in update_fixed_label_nodes(node):

            # No multiple labels supported. Need to be created a new node
            if need_create:
                q = n.query_create()

            # Multiple labels supported -> update node
            else:
                q = n.query_update()

            self.con_write(q)

            n.__dirty_properties__.clear()

    def get_or_create(self, graph_element: GraphElement) \
            -> GraphElement or None:

        q = graph_element.query_create()

        try:

            # Check if is in cache
            if node_cache := MemoryGraphNodeCache.get(graph_element):
                return node_cache

            else:

                # Only enter in this case if not supported constraints.
                # Otherwise we'll use constrain exceptions
                if SQErzoConfig.SUPPORTED_CONSTRAINTS is False:
                    if node_db := self.fetch_one(
                            graph_element,
                            {"identity": graph_element.make_identity()}
                    ):
                        return node_db

                #
                # This case: Not found in cache and constraints supported
                #
                MemoryGraphNodeCache.save(graph_element)
                self.con_write(q)

                return graph_element

        except ValueError as e:
            #
            # This exception raises when node already exits
            #
            if found := self._read(graph_element.query_get_node()):
                graph_element.identity = found[0]["a.identity"]

                # Add to cache
                MemoryGraphNodeCache.save(graph_element)

                # TODO
                return found

    def _get(self, query: str):
        if found := self.con_read(query):
            for r in self.match_parser(found):
                yield r

    def fetch_one(self,
                  node_type: Type[GraphElement] or GraphElement,
                  properties: dict) -> GraphElement:
        for n in self.fetch_many(node_type, properties):
            return n

    def fetch_many(self,
                   node_type: Type[GraphElement] or GraphElement,
                   properties: dict) \
            -> Iterable[GraphElement] or SQErzoException:

        if type(properties) is not dict:
            raise SQErzoException("fetch_many expect 'properties' as dict")

        #
        # Detect if 'node_type' is and object instance or a class definition
        #
        if "__module__" in node_type.__dict__:
            # Is a class definition
            labels = ":".join(node_type.__labels__)
            class_constructor = node_type
        else:

            # Is an object instance
            if not issubclass(type(node_type), GraphElement):
                raise SQErzoException(
                    "'node_type' must be 'GraphElement' subclass"
                )

            labels = node_type.labels()
            class_constructor = node_type.__class__

        tmp_prop = " and ".join(prepare_params(properties, operation="query"))

        query = f"""
        MATCH (a:{labels})
        WHERE {tmp_prop}
        RETURN a
        """

        for x in self._get(query):
            identity = x.properties.pop("identity")
            properties = x.properties.pop("properties", {})

            yield class_constructor(identity=identity,
                                    properties=DirtyDict(properties),
                                    **x.properties)


    def truncate(self):
        """Remove all nodes and edges of database"""
        self.con_write("match (p) detach delete p")

SQErzoGraph = _SQErzoGraph()


__all__ = ("SQErzoGraph",)
