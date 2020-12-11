import urllib.parse as pr

from typing import Iterable

import redis

from redisgraph import Graph

from .shared import ResultElement


def redis_match_parser(res) -> Iterable[ResultElement or str]:
    tmp_res = res.result_set

    if type(tmp_res) is list:
        results = tmp_res
    else:
        results = [tmp_res]

    for t in results:
        for r in t:
            if hasattr(r, "__dict__"):
                yield ResultElement(
                    id=r.__dict__["id"],
                    labels=[r.__dict__["label"]],
                    properties=r.__dict__["properties"]
                )
            else:
                yield r


def redis_graph_connection(connection_string: str):
    parsed = pr.urlparse(connection_string)

    if parsed.path:
        db = parsed.path[1:]

        if not db:
            db = 0
        else:
            db = int(db)
    else:
        db = 0

    if parsed.query:
        qp = parsed.query.split("&", maxsplit=1)[0]
        p_name, p_value = qp.split("=")
        if p_name != "graph":
            db_graph = "sqerzo"
        else:
            db_graph = p_value
    else:
        db_graph = "sqerzo"

    port = parsed.port
    if port is None:
        port = 6379

    r = redis.Redis(
        host=parsed.hostname,
        port=port,
        username=parsed.username,
        password=parsed.password,
        db=db
    )

    redis_graph = Graph(db_graph, r)

    return redis_graph.query





__all__ = ("redis_graph_connection", "redis_match_parser")
