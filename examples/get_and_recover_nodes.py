import logging

from dataclasses import dataclass

from sqerzo import GraphEdge, GraphNode, SQErzoGraph
from sqerzo.exceptions import SQErzoElementExistException

log = logging.getLogger("sqerzo")

class MeetEdge(GraphEdge):
    pass


@dataclass
class UserNode(GraphNode):
    __keys__ = ["name"]

    name: str = None


def create_graph(connection_string: str):
    gh = SQErzoGraph(connection_string)
    gh.truncate()  # Drop database


    nodes_ids = []

    # Insert some nodes / relations
    for n in range(20):
        u1 = UserNode(name=f"UName-{n}")
        u2 = UserNode(name=f"UName-two{n}")

        gh.save(u1)
        gh.save(u2)

        nodes_ids.append(u1.make_identity())

        try:
            gh.save(u2)
        except SQErzoElementExistException:
             #When you insert the same node twice an exception will raise
            pass

        u1_meet_g1 = MeetEdge(
            source=u1,
            destination=u2
        )
        gh.save(u1_meet_g1)

    # Update some nodes
    for node_id in nodes_ids:
        n: UserNode = gh.get_node_by_id(node_id, UserNode)
        gh.update(n)



if __name__ == '__main__':
    print("Redis...")
    create_graph("redis://127.0.0.1:7000/?graph=email")
    # print("Neo4j...")9
    # create_graph("neo4j://neo4j:s3cr3t@127.0.0.1:7687/?graph=email")
    # create_graph("enterprise+neo4j://neo4j:s3cr3t@127.0.0.1:7687/?graph=email")
