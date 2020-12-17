import uuid
import random

from collections import Iterable


def get_class_properties(c) -> Iterable:
    props = set()
    for m in c.mro():
        if m.__dict__.get("__annotations__", None):
            props.update(list(m.__dict__["__annotations__"].keys()))

    return props


guuid = lambda: uuid.uuid4().hex
rtext = lambda: "".join(random.choice(string.ascii_letters) for _ in range(80))


__all__ = ("get_class_properties", "guuid", "rtext")
