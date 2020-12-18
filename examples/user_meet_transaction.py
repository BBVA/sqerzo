import logging

from dataclasses import dataclass

from sqerzo import GraphEdge, GraphNode, SQErzoGraph

log = logging.getLogger("sqerzo")

class MeetEdge(GraphEdge):
    pass


@dataclass
class UserNode(GraphNode):
    __keys__ = ["name"]

    name: str = None


def create_graph(connection_string: str, count: int = 500):
    gh = SQErzoGraph(connection_string)
    gh.truncate()  # Drop database

    with gh.transaction() as tx:

        for n in range(count):
            u1 = UserNode(name=f"UName-{n}")
            d1 = UserNode(name=f"DName-{n}")

            tx.add(u1)
            tx.add(d1)

            u1_meet_g1 = MeetEdge(
                source=u1,
                destination=d1
            )
            tx.add(u1_meet_g1)


if __name__ == '__main__':
    print("Redis...")
    create_graph("redis://127.0.0.1:7000/?graph=email")
    print("Neo4j...")
    create_graph("neo4j://neo4j:s3cr3t@127.0.0.1:7687/?graph=email")
