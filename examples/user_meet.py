from dataclasses import dataclass

from sqerzo import GraphEdge, GraphNode, SQErzoGraph


class MeetEdge(GraphEdge):
    pass


@dataclass
class UserNode(GraphNode):
    name: str = None


def create_graph(connection_string: str):
    gh = SQErzoGraph(connection_string)
    gh.truncate()  # Drop database

    # Creates 20 relations
    for n in range(20):
        u1 = UserNode(name=f"UName-{n}")
        gh.save(u1)

        d1 = UserNode(name=f"DName-{n}")
        gh.save(d1)

        u1_meet_g1 = MeetEdge(
            source=u1,
            destination=d1
        )
        gh.save(u1_meet_g1)


if __name__ == '__main__':
    create_graph("redis://127.0.0.1:7000/?graph=email")
    create_graph("neo4j://neo4j:s3cr3t@127.0.0.1:7687/?graph=email")
