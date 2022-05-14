from typing import Dict, Optional

from .node import Node


class Edge:
    """Edge object from the SQLite db."""

    def __init__(self,
                 schema_name: str,
                 source_id: Node,
                 target_id: Node,
                 properties: Optional[Dict] = None) -> None:
        """Representation of 'edge' from {{schema_name}}_edges."""
        self.schema_name = schema_name
        self.source_id = source_id
        self.target_id = target_id
        self.properties = properties

    def __eq__(self, source_id: str, target_id: str) -> bool:
        return (
            self.source_id == source_id
            and
            self.target_id == target_id
        )

