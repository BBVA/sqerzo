from __future__ import annotations

import abc
import hashlib
import logging

from typing import List
from collections import Iterable
from dataclasses import dataclass, field

from ..exceptions import SQErzoException
from .helpers import get_class_properties, guuid

log = logging.getLogger("sqerzo")

class DirtyDict(dict):
    __slots__ = ()
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__

    def __init__(self, *args, **kwargs):
        super(DirtyDict, self).__init__(*args, **kwargs)

        self.__dirty_properties__ = {}

    def __setitem__(self, key, value):
        try:
            old_value = self.__dict__[key]
        except KeyError:
            old_value = None

        self.__dirty_properties__[key] = old_value

        super(DirtyDict, self).__setitem__(key, value)


    def items(self):
        for k, v in super(DirtyDict, self).items():
            if k.startswith("_"):
                continue

            yield k, v

class GraphElementMetaClass(type):
    """
    This is the metaclass for each GraphNode or GraphEdge subclass. It checks
    that all nodes have these properties when created:

    - __keys__
    - __labels__
    - __unique__
    - __indexes__

    Something important things:

    **__labels__**

    If not property __label__ was set into class, it will try to deduce from
    class name. Valid class names for deducing must ends with 'Node' word. For
    example: MyCustomNode

    **__keys__**

    If not __keys__ property was set the default value are: 'identity' property

    **__unique__**

    It's an iterable value.

    **__indexes__**

    It's an iterable value.
    """

    def __new__(cls, class_name: str, bases, dct: dict):

        o = type.__new__(cls, class_name, bases, dct)

        if class_name in ("GraphElement", "GraphNode", "GraphEdge"):
            return o

        iterable_types = (list, set, tuple)

        #
        # Ensures that all nodes has the property: __label__
        #
        if not hasattr(o, "__labels__"):
            #
            # Try to find label by class name
            #
            log.debug(f"Trying to deduce Node label from class name: "
                      f"'{class_name}'")

            if (index := class_name.rfind("Node")) != -1:
                label = class_name[:index]

            elif (index := class_name.rfind("Edge")) != -1:
                label = class_name[:index]
            else:
                label = None

            if not label:
                raise SQErzoException(
                    f"Class name '{class_name}' must has property __label__ or "
                    f"the class name must ends with 'Node' or 'Edge' name. "
                    f"Examples: MyCustomNode, MyCustomEdge "
                )
            else:
                o.__labels__ = {label}

        else:
            # Check types
            if not isinstance(o.__labels__, iterable_types):
                o.__labels__ = [o.__labels__]

            o.__labels__ = set(o.__labels__)

        try:
            class_props = get_class_properties(o)
        except KeyError:
            raise SQErzoException(
                "Currently only support Python Data Classes"
            )

        if not hasattr(o, "__keys__"):
            o.__keys__ = {"identity"}

        else:
            # Check types
            if not isinstance(o.__keys__, iterable_types):
                o.__keys__ = [o.__keys__]

            o.__keys__ = set(o.__keys__)

            #
            # Check attribute values are in class properties
            #
            if missing := o.__keys__ - class_props:
                raise SQErzoException(
                    f"__keys__ has a value missing in class property "
                    f"'{class_name}': {missing} "
                )


        if hasattr(o, "__unique__"):

            # Check types
            if not isinstance(o.__unique__, iterable_types):
                o.__unique__ = [o.__unique__]

            # Fix duplicates
            o.__unique__ = set(o.__unique__)

            #
            # Check attribute values are in class properties
            #
            if missing := o.__unique__ - class_props:
                raise SQErzoException(
                    f"__unique__ has a value missing in class property "
                    f"'{class_name}': {missing} "
                )
        else:
            o.__unique__ = set()

        if hasattr(o, "__indexes__"):

            # Check types
            if not isinstance(o.__indexes__, iterable_types):
                o.__indexes__ = [o.__indexes__]

            # Fix duplicates
            o.__indexes__ = set(o.__indexes__)
            o.__indexes__.add("identity")  # Ensures identity

            #
            # Check attribute values are in class properties
            #
            if missing := o.__indexes__ - class_props:
                raise SQErzoException(
                    f"__indexes__ has a value missing in class property "
                    f"'{class_name}': {missing} "
                )
        else:
            o.__indexes__ = {"identity"}

        #
        # Track object
        #
        from sqerzo.config import SQErzoConfig

        SQErzoConfig.SETUP_OBJECTS.append(o)

        # Attack meta properties
        o.__dirty_properties__ = {}
        o.__is_instance__ = False

        return o

    def __call__(cls, *args, **kwargs):
        o = super().__call__(*args, **kwargs)
        o.__is_instance__ = True

        return o

#
# Base classes
#
class GraphElement(metaclass=GraphElementMetaClass):

    def __setattr__(self, key, value):
        if self.__is_instance__ and key != "__is_instance__":

            # Save old value
            self.__dirty_properties__[key] = getattr(self, key, None)

        super(GraphElement, self).__setattr__(key, value)

    def add_label(self, label_name: str):
        if not label_name:
            return

        self.__labels__.add(label_name)

    @abc.abstractmethod
    def clone(self, exclude: List[str] = None) -> GraphElement:
        raise NotImplementedError()

    @abc.abstractmethod
    def query_create(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def query_update(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def query_get_node(self) -> str:
        raise NotImplementedError()

    def labels(self) -> str:
        return ":".join(self.__labels__)


@dataclass
class GraphNode(GraphElement):
    properties: DirtyDict = field(default_factory=DirtyDict)
    identity: str = None

    @classmethod
    def from_query_results(cls, result_data: object):
        properties = {
            k:v for k, v in result_data.properties.items()
            if k not in ("identifier",)
        }
        custom_class_properties = {
            k: result_data.properties[k]
            for k in cls.__annotations__.keys()
        }

        config = {
            **custom_class_properties,
            "identity": result_data.properties["identity"],
            "properties": DirtyDict(properties)
        }

        return cls(**config)

    def make_identity(self) -> str:
        if self.identity:
            return self.identity

        l = self.labels()

        key_builders = [*self.__keys__]

        if not (set(self.__keys__) - {"identity"}) and not self.identity:
            key_builders.append(guuid())

        q = "#".join(
            f"{k}:{v}"
            for k, v in self.__dict__.items() if k in key_builders
        )

        v =  hashlib.sha512(f"{l}#{q}".encode()).hexdigest()

        self.identity = v

        return v

@dataclass
class GraphEdge(GraphElement):
    source: GraphNode
    destination: GraphNode
    properties: dict = field(default_factory=dict)
    identity: str = None

    def make_identity(self) -> str:
        if self.identity:
            return self.identity

        l = self.labels()
        n = f"{self.source.make_identity()}#{self.source.make_identity()}"

        v =  hashlib.sha512(f"{l}#{n}".encode()).hexdigest()

        self.identity = v

        return v


__all__ = ("GraphNode", "GraphEdge", "GraphElement", "DirtyDict")
