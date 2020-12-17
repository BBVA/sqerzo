from __future__ import annotations

import logging

from typing import Tuple
from dataclasses import dataclass, field

from .lang import *
from ..model import *
from ..helpers import *

log = logging.getLogger("sqerzo")

# #
# # Base classes
# #
# class CypherGraphElement(GraphElement):
#
#     def clone(self, exclude: List[str] = None) -> GraphElement:
#         public_attrs = {
#             k: v
#             for k, v in self.__dict__.items()
#             if not k.startswith("_") and k not in exclude
#         }
#
#         o = self.__class__(**public_attrs)
#
#         # Setup private properties
#         for k, v in self.__dict__.items():
#             if not k.startswith("_") or k in exclude:
#                 continue
#
#             setattr(o, k, v)
#
#         return o
#
#
# @dataclass
# class CypherGraphNode(GraphElement):
#     properties: DirtyDict = field(default_factory=DirtyDict)
#     identity: str = None
#
#     def query_create(self, partial: bool = False) -> str:
#         if not self.identity:
#             self.make_identity()
#
#             # Do not include in dirty properties
#             del self.__dirty_properties__["identity"]
#
#         labels = self.labels()
#
#         tmp_prop = [f"identity: '{self.identity}'"]
#         tmp_prop.extend(prepare_params(self.properties))
#         tmp_prop.extend(prepare_params({
#             k: v
#             for k, v in self.__dict__.items()
#             if k not in ("properties","identity") and not k.startswith("_")
#         }))
#
#         prop = f"{{{', '.join(tmp_prop)}}}"
#
#         return f"{'' if partial else 'CREATE '} (:{labels} {prop})"
#
#     def query_update(self) -> str:
#         # Get properties that was modified
#
#         #
#         # Find only properties that was modified
#         #
#         sets = []
#         nodes = []
#
#         #
#         # Find changes in properties of class with simple types: int, str...
#         #
#         for prop, old_value in self.__dirty_properties__.items():
#
#             if prop == "__labels__":
#                 nodes.append(f"p:{':'.join(old_value)}")
#                 sets.append(f"p:{self.labels()}")
#             else:
#                 sets.extend(prepare_params(
#                     {
#                         prop: getattr(self, prop)
#                     },
#                     operation="update",
#                     node_name="p"
#                 ))
#
#         #
#         # Find changes in 'properties' that is a DirtyDict, a custom dictionary
#         #
#         for prop, old_value in self.properties.__dirty_properties__.items():
#             sets.extend(prepare_params(
#                 {
#                     prop: self.properties[prop]
#                 },
#                 operation="update",
#                 node_name="p"
#             ))
#
#         q_nodes = ", ".join(f"({x})" for x in nodes)
#         q_sets = ", ".join(sets)
#
#         q = f"""
#         MATCH {q_nodes}
#         WHERE p.identity = '{self.identity}'
#         SET {q_sets}
#         """
#
#         return q
#
#
# @dataclass
# class CypherGraphEdge(GraphElement):
#     source: GraphNode
#     destination: GraphNode
#     properties: dict = field(default_factory=dict)
#     identity: str = None
#
#     def query_create(self, random_node_alias: bool = False) -> str:
#         if not self.identity:
#             self.make_identity()
#
#         if random_node_alias:
#             source_alias = rtext()
#             dest_alias = rtext()
#         else:
#             source_alias = "a"
#             dest_alias = "b"
#
#         labels = self.labels()
#
#         tmp_prop = [f"identity: '{self.identity}'"]
#         tmp_prop.extend(prepare_params(self.properties))
#
#         source_labels = self.source.labels()
#         dest_labels = self.destination.labels()
#
#         prop = f"{{{', '.join(tmp_prop)}}}"
#
#         q = f"""
#         MATCH ({source_alias}:{source_labels}),({dest_alias}:{dest_labels})
#         WHERE {source_alias}.identity = '{self.source.identity}' AND {dest_alias}.identity = '{self.destination.identity}'
#         CREATE ({source_alias})-[r:{labels} {prop}]->({dest_alias})
#         """
#         return q
#
#     def query_create_partial(self) -> Tuple[str, str]:
#         if not self.identity:
#             self.identity = self.make_identity()
#
#         source_alias = rtext()
#         dest_alias = rtext()
#         label_alias = rtext()
#
#         labels = self.labels()
#
#         tmp_prop = [f"identity: '{self.identity}'"]
#         tmp_prop.extend(prepare_params(self.properties))
#
#         source_labels = self.source.labels()
#         dest_labels = self.destination.labels()
#
#         prop = f"{{{', '.join(tmp_prop)}}}"
#
#         q_match = f"""({source_alias}:{source_labels} {{ identity: '{self.source.identity}' }}),({dest_alias}:{dest_labels} {{identity: '{self.destination.identity}' }})"""
#         q_create = f"""({source_alias})-[{label_alias}:{labels} {prop}]->({dest_alias})"""
#
#         return q_match, q_create
