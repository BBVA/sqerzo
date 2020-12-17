from __future__ import annotations

import abc

from typing import List, Iterable, Type
from dataclasses import dataclass

from .model import GraphElement
from ..exceptions import SQErzoElementExistException, SQErzoException


@dataclass
class ResultElement:
    id: str = None  # DB Id
    labels: List[str] = None
    properties: dict = None

class SQErzoQueryResponse:

    @abc.abstractmethod
    def __init__(self, graph: SQErzoGraphConnection, query: str, **kwargs):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abc.abstractmethod
    def __iter__(self):
        raise NotImplementedError()


class SQErzoGraphConnection(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def __init__(self, connection_string: str):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_node_by_id(self, node_id: str) \
            -> GraphElement or None:
        raise NotImplementedError()

    @abc.abstractmethod
    def save_element(self, graph_element: GraphElement) \
            -> None or SQErzoElementExistException:
        raise NotImplementedError()

    @abc.abstractmethod
    def update_element(self, graph_element: GraphElement) \
            -> None or SQErzoElementExistException:
        raise NotImplementedError()

    @property
    def batch_size(self) -> int:
        return 1000

    @property
    @abc.abstractmethod
    def transaction_class(self) -> Type:
        raise NotImplementedError()

    @abc.abstractmethod
    def query(self, query: str, **kwargs) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def query_response(self, query: str, **kwargs) -> Iterable[ResultElement]:
        raise NotImplementedError()

    @abc.abstractmethod
    def fetch_nodes(self,
                    node_type: Type[GraphElement] or GraphElement,
                    **kwargs) \
            -> Iterable[GraphElement] or SQErzoException:
        raise NotImplementedError()

    @abc.abstractmethod
    def create_indexes(self, attribute: str, labels: List[str]):
        raise NotImplementedError()

    @abc.abstractmethod
    def create_constraints_nodes(self, key: str, labels: List[str]):
        raise NotImplementedError()

    @abc.abstractmethod
    def create_constraints_edges(self, key: str, labels: List[str]):
        raise NotImplementedError()

    @abc.abstractmethod
    def truncate(self):
        raise NotImplementedError()

__all__ = ("ResultElement", "SQErzoGraphConnection",)
