from enum import IntEnum
from typing import List, Type

class _SQErzoConfig(dict):
    __slots__ = ()
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__

    def __init__(self, *args, **kwargs):
        super(_SQErzoConfig, self).__init__(*args, **kwargs)

        self["DB_ENGINE"]: DBEngine = None
        self["SUPPORTED_CONSTRAINTS"]: bool = False
        self["SUPPORTED_INDEXES"]: bool = False
        self["SUPPORTED_MULTIPLE_LABELS"]: bool = False
        self["SUPPORTED_TYPES"]: List[str] = []
        self["SETUP_OBJECTS"]: List[Type] = []

SQErzoConfig = _SQErzoConfig()

__all__ = ("SQErzoConfig",)
