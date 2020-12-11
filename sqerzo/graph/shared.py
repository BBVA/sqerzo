from typing import List
from dataclasses import dataclass


@dataclass
class ResultElement:
    id: str
    labels: List[str]
    properties: dict


__all__ = ("ResultElement",)
