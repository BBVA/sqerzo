class MemoryGraphNodeCache:

    def __init__(self):
        self._cache = {}

    def make_key(self, node):

        if k := node.identity:
            return k
        else:
            k = "#".join(
                getattr(node, k)
                for k in node.__keys__
            )
            return k

    def get_id(self, node_id: str) -> object or None:
        try:
            return self._cache[node_id]
        except KeyError:
            return None

    def save_id(self, node_id: str, obj: object) -> object or None:
        self._cache[node_id] = obj

    def get(self, node: object) -> object or None:
        try:
            return self._cache[node.make_identity()]
        except KeyError:
            return None

    def save(self, node: object):
        key = self.make_key(node)
        self._cache[key] = node


__all__ = ("MemoryGraphNodeCache",)
