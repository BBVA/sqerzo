from typing import Dict, Type

from ..exceptions import SQErzoException

class Query:

    def __init__(self, graph):
        self.graph = graph
        self.query = None

    def raw(self, query: str = None):
        self.query = query

        return self

    def execute(self, map_to: Dict[str, Type] = None):
        if self.query:
            with self.graph.db_engine.query_response(self.query) as responses:
                results = []

                for row in responses:
                    tmp = []

                    if map_to:
                        for column in row:

                            try:
                                tmp.append(
                                    map_to[column.alias].from_query_results(column)
                                )
                            except KeyError:
                                raise SQErzoException(
                                    f"Can't find class for mapping alias '{row.alias}'"
                                )

                        results.append(tmp)

                    else:
                        results.append(
                            row
                        )

                return results
