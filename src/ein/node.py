from typing import Dict


class Node:
    """Node object from the SQLite db."""

    def __init__(self, id: str, body: Dict):
        """Representation of schema from {{schema_name}}_nodes."""
        self._id = id
        self._body = body

    def get_id(self):
        return self._id

    def get_body(self):
        return self._body

