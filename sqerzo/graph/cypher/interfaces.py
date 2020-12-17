from __future__ import annotations

import abc

from typing import List, Iterable, Type

from .lang import create_query, prepare_params
from .model import GraphElement, DirtyDict
from ...exceptions import SQErzoElementExistException, SQErzoException
from ..interfaces import SQErzoGraphConnection, ResultElement


class CypherSQErzoGraphConnection(SQErzoGraphConnection):

    def plain_string(self, text: str) -> str:
        return text.replace("-", "_").replace(":", "_").lower()

    def labels_to_name(self, labels: List[str]) -> str:
        return "_".join(l.replace(":", "_") for l in labels)

    def get_node_by_id(self, node_id: str) \
            -> GraphElement or None:

        if not node_id:
            return

        q = f"""
        MATCH (n {{ identity: '{node_id}' }})
        RETURN n
        """
        for res in self.query(q):
            return res

    def save_element(self, graph_element: GraphElement) \
            -> None or SQErzoElementExistException:

        self.query(create_query(graph_element))

    def update_element(self, graph_element: GraphElement) \
            -> None or SQErzoElementExistException:

        self.query(graph_element.query_update())

        graph_element.__dirty_properties__.clear()

    def fetch_nodes(self,
                    node_type: Type[GraphElement] or GraphElement,
                    **kwargs) \
            -> Iterable[GraphElement] or SQErzoException:

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

        tmp_prop = " and ".join(
            prepare_params(kwargs, operation="insert")
        )

        query = f"""
        MATCH (a:{labels} {{ {tmp_prop} }})
        RETURN a
        """

        with self.query_response(query) as responses:
            for res in responses:
                yield class_constructor.from_query_results(res)

    @property
    def batch_size(self) -> int:
        return 1000

    @property
    def transaction_class(self) -> Type:
        from .transaction import CypherSQErzoTransaction

        return CypherSQErzoTransaction

    def truncate(self):
        self.query("match (p) detach delete p")

__all__ = ("CypherSQErzoGraphConnection", "ResultElement")
