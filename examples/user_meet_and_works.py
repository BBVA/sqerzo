import time
import logging

from dataclasses import dataclass

from sqerzo import GraphEdge, GraphNode, SQErzoGraph

log = logging.getLogger("sqerzo")

class MeetEdge(GraphEdge):
    pass

class WorksWithEdge(GraphEdge):
    pass


@dataclass
class UserNode(GraphNode):
    __keys__ = "email"

    name: str = None
    email: str = None


def create_graph(connection_string: str, nodes_count = 500):
    gh = SQErzoGraph(connection_string)
    gh.truncate()  # Drop database

    nodes = []

    #
    # Add some data and relations: User1 -[meet]-> User 2
    #
    with gh.transaction() as tx:

        for n in range(nodes_count):
            u1_name = f"uname{n}"
            d1_name = f"dname{n}"

            u1 = UserNode(name=u1_name, email=f"{u1_name}@{u1_name}.com")
            d1 = UserNode(name=d1_name, email=f"{d1_name}@{d1_name}.com")

            nodes.append(u1)
            nodes.append(d1)

            tx.add(u1)
            tx.add(d1)

            u2_meet_u1 = MeetEdge(
                source=u1,
                destination=d1
            )
            u1_meet_u2 = MeetEdge(
                source=d1,
                destination=u1
            )
            tx.add(u1_meet_u2)
            tx.add(u2_meet_u1)

    #
    # Add some works relations: User 1 -[WorksWith]-> User 2
    #
    with gh.transaction() as tx:
        for i in range(0, len(nodes), 4):
            u1 = nodes[i]
            u2 = nodes[i + 2]

            work_1 = WorksWithEdge(
                source=u1,
                destination=u2
            )
            work_2 = WorksWithEdge(
                source=u2,
                destination=u1
            )

            tx.add(work_1)
            tx.add(work_2)

    q = gh.Query.raw(
        "match (u1:User)-[:Meet]->(u2:User) return u1, u2"
    ).execute(map_to={"u1": UserNode, "u2": UserNode})

    print(q)

if __name__ == '__main__':

    count = 2

    # This values are from logic of program to avoid a lot of combinations
    total_nodes = (count * 2) + int(count / 4)
    total_edges = (total_nodes * 2) + int(count / 2)

    total = total_edges + total_edges

    print(f"[*] Inserting {total} nodes/edges in Redis...", end='', flush=True)

    start = time.time()
    create_graph("redis://127.0.0.1:7000/?graph=email", nodes_count=count)
    end = time.time()
    print(f"{end - start} seconds")

    print(f"[*] Inserting {total} nodes/edges in Neo4j...", end='', flush=True)
    start = time.time()
    create_graph("neo4j://neo4j:s3cr3t@127.0.0.1:7687/?graph=email", nodes_count=count)
    end = time.time()
    print(f"{end - start} seconds")
