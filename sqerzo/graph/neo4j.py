import urllib.parse as pr

from typing import List, Iterable

import neo4j.exceptions

from neo4j import GraphDatabase

from .shared import ResultElement


def neo4j_match_parser(results) -> Iterable[ResultElement or str]:


    for res in results:
        for node_res, node_val in res.items():
            yield ResultElement(
                id=node_val.id,
                labels=list(node_val.labels),
                properties=node_val._properties
            )

def neo4j_graph_connection(connection_string: str):

    def insert(driver):

        def _insert(query: str):
            # def get_or_create(tx):
            #     tx.run(query)

            with driver.session() as session:
                try:
                    session.run(query)
                except neo4j.exceptions.ConstraintError as e:
                    #
                    # Get original node
                    #
                    raise ValueError(e)

        return _insert

    def read(driver):

        def _read(query: str) -> List[dict]:
            with driver.session() as session:
                return [
                    dict(x)
                    for x in session.run(query)
                ]

        return _read

    parsed = pr.urlparse(connection_string)

    host = parsed.hostname
    if not host:
        host = "127.0.0.1"

    port = parsed.port
    if port is None:
        port = 7687

    config = {"uri": f"bolt://{host}:{port}"}

    if parsed.username and parsed.password:
        config["auth"] = (parsed.username, parsed.password)

    driver = GraphDatabase.driver(**config)

    return insert(driver), read(driver)


__all__ = ("neo4j_graph_connection", "neo4j_match_parser")
