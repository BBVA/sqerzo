class _GraphNodeCache:

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

    def get(self, node: object) -> object or None:
        # key = self.make_key(node)

        try:
            return self._cache[node.make_identity()]
        except KeyError:
            return None

    def save(self, node: object):
        key = self.make_key(node)
        self._cache[key] = node

MemoryGraphNodeCache = _GraphNodeCache()

__all__ = ("MemoryGraphNodeCache",)
