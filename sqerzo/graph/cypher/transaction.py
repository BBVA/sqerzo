from collections import defaultdict

from .lang import create_query
from ..transaction import SQErzoTransaction
from .model import GraphElement, GraphNode, GraphEdge

class CypherSQErzoTransaction(SQErzoTransaction):

    def __init__(self, graph):
        self.graph = graph
        self._commit = []

    def add(self, element: GraphElement):
        self._commit.append(element)


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
            batch = [
                {
                    "identity_from": b.source.make_identity(),
                    "identity_to": b.destination.make_identity(),
                    "identity_edge": b.make_identity()
                }
                for b in edges
            ]

            q = f"""
            UNWIND $batch as row
            MATCH (from:{edge.source.labels()} {{ identity: row.identity_from }})
            MATCH (to:{edge.destination.labels()} {{ identity: row.identity_to }})
            CREATE (from)-[:{edge.labels()} {{ identity: row.identity_edge }}]->(to)
            """

            self.graph.db_engine.query(q, batch=batch)


    def commit(self):

        counter = 0
        processed = 0
        partial_query_nodes = {}
        partial_query_nodes_processed = set()
        partial_edges = defaultdict(list)
        chunk_size = self.graph.db_engine.batch_size

        #
        # Get all nodes and create the query
        #
        # Use this approach instead of a function that split list in chunks
        # to avoid the memory of the chunked list, the creation of
        # a new list and scroll through the list 2 times. For big commit
        # list it should be a problem.
        #
        while self._commit:

            element = self._commit.pop()

            if isinstance(element, GraphNode):
                key = "".join(getattr(element, x) for x in element.__keys__)

                if key in partial_query_nodes_processed:
                    continue

                partial_query_nodes_processed.add(key)
                partial_query_nodes[key] = create_query(element, partial=True)

            elif isinstance(element, GraphEdge):
                label_edge = element.labels()
                label_source = element.source.labels()
                label_destination = element.destination.labels()
                key = f"{label_source}#{label_edge}#{label_destination}"

                partial_edges[key].append(element)

            counter += 1
            processed += 1

            if counter >= chunk_size:
                self.dump_data(partial_query_nodes, partial_edges)

                #
                # Reset
                #
                counter = 0
                partial_edges.clear()  # Reset list
                partial_query_nodes.clear()  # Reset list

        # Check if there are some remain data to dump
        self.dump_data(partial_query_nodes, partial_edges)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()

__all__ = ("CypherSQErzoTransaction",)
