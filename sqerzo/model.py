from __future__ import annotations

import abc
import uuid
import hashlib
import logging

from collections import Iterable
from dataclasses import dataclass, field

from .graph.lang import *
from .exceptions import SQErzoException
from .helpers import get_class_properties
from .config import SQErzoConfig

log = logging.getLogger("sqerzo")

guuid = lambda: uuid.uuid4().hex


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
                o.__labels__ = [label]

        else:
            # Check types
            if not isinstance(o.__labels__, Iterable):
                raise SQErzoException(
                    f"__labels__ property of '{class_name}' must be a iterable"
                )

            o.__labels__ = list(o.__labels__)

        try:
            class_props = get_class_properties(o)
        except KeyError:
            raise SQErzoException(
                "Currently only support Python Data Classes"
            )

        if not hasattr(o, "__keys__"):
            o.__keys__ = ("identity",)
        else:

            #
            # Check key values are in class properties
            #
            if missing := set(o.__keys__) - class_props:
                raise SQErzoException(
                    f"__keys__ has a value missing in class property "
                    f"'{class_name}': {missing} "
                )


        if hasattr(o, "__unique__"):
            # Fix duplicates
            o.__unique__ = tuple(set(o.__unique__))

            #
            # Check key values are in class properties
            #
            if missing := set(o.__unique__) - class_props:
                raise SQErzoException(
                    f"__unique__ has a value missing in class property "
                    f"'{class_name}': {missing} "
                )

        if hasattr(o, "__indexes__"):
            # Fix duplicates
            o.__indexes__ = tuple(set(o.__indexes__))

            #
            # Check key values are in class properties
            #
            if missing := set(o.__indexes__) - class_props:
                raise SQErzoException(
                    f"__indexes__ has a value missing in class property "
                    f"'{class_name}': {missing} "
                )

        #
        # Track object
        #
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

        try:
            l = [label_name, *self.__labels__]

            self.__labels__ = list(set(l))
        except TypeError as e:
            pass

    def clone(self, exclude: List[str] = None) -> GraphElement:
        public_attrs = {
            k: v
            for k, v in self.__dict__.items()
            if not k.startswith("_") and k not in exclude
        }

        o = self.__class__(**public_attrs)

        # Setup private properties
        for k, v in self.__dict__.items():
            if not k.startswith("_") or k in exclude:
                continue

            setattr(o, k, v)

        return o

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

    def make_identity(self) -> str:
        l = self.labels()

        if self.__keys__ == ("identity",) and not self.identity:
            self.identity = guuid()

        q = "#".join(
            f"{k}:{v}"
            for k, v in self.__dict__.items() if k in self.__keys__
        )

        res = f"{l}#{q}"

        return hashlib.sha512(res.encode()).hexdigest()

@dataclass
class GraphNode(GraphElement):
    properties: DirtyDict = field(default_factory=DirtyDict)
    identity: str = None

    def query_get_node(self) -> str:
        """Get current node from db graph"""
        labels = ":".join(self.__labels__)

        tmp_prop = prepare_params({
            k: getattr(self, k)
            for k in self.__keys__
        }, operation="query")

        prop = f"{' and '.join(tmp_prop)}"

        return f"""
        MATCH (a:{labels})
        WHERE {prop}
        RETURN a.identity
        """

    def query_create(self) -> str:
        if not self.identity:
            self.identity = self.make_identity()

            # Do not include in dirty properties
            del self.__dirty_properties__["identity"]

        labels = self.labels()

        tmp_prop = [f"identity: '{self.identity}'"]
        tmp_prop.extend(prepare_params(self.properties))
        tmp_prop.extend(prepare_params({
            k: v
            for k, v in self.__dict__.items()
            if k not in ("properties","identity") and not k.startswith("_")
        }))

        prop = f"{{{', '.join(tmp_prop)}}}"

        return f"CREATE (:{labels} {prop})"

    def query_update(self) -> str:
        # Get properties that was modified

        #
        # Find only properties that was modified
        #
        sets = []
        nodes = []

        #
        # Find changes in properties of class with simple types: int, str...
        #
        for prop, old_value in self.__dirty_properties__.items():

            if prop == "__labels__":
                nodes.append(f"p:{':'.join(old_value)}")
                sets.append(f"p:{self.labels()}")
            else:
                sets.extend(prepare_params(
                    {
                        prop: getattr(self, prop)
                    },
                    operation="update",
                    node_name="p"
                ))

        #
        # Find changes in 'properties' that is a DirtyDict, a custom dictionary
        #
        for prop, old_value in self.properties.__dirty_properties__.items():
            sets.extend(prepare_params(
                {
                    prop: self.properties[prop]
                },
                operation="update",
                node_name="p"
            ))

        q_nodes = ", ".join(f"({x})" for x in nodes)
        q_sets = ", ".join(sets)

        q = f"""
        MATCH {q_nodes}
        WHERE p.identity = '{self.identity}'
        SET {q_sets}
        """

        return q


@dataclass
class GraphEdge(GraphElement):
    source: GraphNode
    destination: GraphNode
    properties: dict = field(default_factory=dict)
    identity: str = None

    def query_create(self) -> str:
        if not self.identity:
            self.identity = self.make_identity()

        labels = self.labels()

        tmp_prop = [f"identity: '{self.identity}'"]
        tmp_prop.extend(prepare_params(self.properties))

        source_labels = self.source.labels()
        dest_labels = self.destination.labels()

        prop = f"{{{', '.join(tmp_prop)}}}"

        q = f"""
        MATCH (a:{source_labels}),(b:{dest_labels})
        WHERE a.identity = '{self.source.identity}' AND b.identity = '{self.destination.identity}'
        CREATE (a)-[r:{labels} {prop}]->(b)
        RETURN type(r)
        """

        return q
