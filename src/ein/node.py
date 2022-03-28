from typing import Dict, List

from .edge import Edge


class Node:
    """Node object from the SQLite db."""

    def __init__(self, id: str, body: Dict, edges: List[Edge]):
        """Representation of schema from {{schema_name}}_nodes."""
        self.id = id
        self.body = body
        self.edges = edges

    def update_id(self, id: str) -> None:
        self.id = id

    def update_body(self, body: Dict) -> None:
        self.body = body

