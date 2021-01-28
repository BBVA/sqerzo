from dataclasses import dataclass

from sqerzo import GraphEdge, GraphNode, SQErzoGraph


@dataclass
class UserNode(GraphNode):
    name: str = None
    age: int = None


def create_graph(connection_string: str):
    gh = SQErzoGraph(connection_string)
    gh.truncate()  # Drop database

    u1 = UserNode(name="Eustaquio", age=22)
    gh.save(u1)
    u2 = UserNode(name="Guachinche", age=22)
    gh.save(u2)
    u3 = UserNode(name="Emiliano", age=40)
    gh.save(u3)

    # First argument: node ID we want to recover
    # Second argument: node class in which we want to map the result
    for n in gh.fetch_many(UserNode, age=22):
        print(n)

if __name__ == '__main__':

    print("Redis:")
    create_graph("redis://127.0.0.1:7000/?graph=email")
    print("Neo4j:")
    create_graph("neo4j://neo4j:s3cr3t@127.0.0.1:7687/?graph=email")
