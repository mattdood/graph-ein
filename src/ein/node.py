from typing import Dict, List, Optional

from .edge import Edge


class Node:
    """Node object from the SQLite db."""

    def __init__(self,
                 schema_name: str,
                 id: str,
                 body: Dict,
                 edges: Optional[List[Edge]] = None) -> None:
        """Representation of 'node' from {{schema_name}}_nodes."""
        self.schema_name = schema_name
        self.id = id
        self.body = body
        self.edges = edges

