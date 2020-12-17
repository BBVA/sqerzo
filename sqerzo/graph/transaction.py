import abc

from .model import GraphElement


class SQErzoTransaction:

    @abc.abstractmethod
    def add(self, element: GraphElement):
        raise NotImplementedError()

    @abc.abstractmethod
    def dump_data(self, **kwargs):
        raise NotImplementedError()

    @abc.abstractmethod
    def commit(self):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()


__all__ = ("SQErzoTransaction",)

