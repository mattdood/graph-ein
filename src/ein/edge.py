from typing import Dict, Optional

from src.ein.node import Node


class Edge:
    """Edge object from the SQLite db."""

    def __init__(self,
                 schema_name: str,
                 source: Node,
                 target: Node,
                 properties: Optional[Dict] = None) -> None:
        """Representation of 'edge' from {{schema_name}}_edges."""
        self.schema_name = schema_name
        self.source = source
        self.target = target
        self.properties = properties

    def __eq__(self, source: Optional[Node] = None, target: Optional[Node] = None) -> bool:

        if source and target:
            return (
                self.source.id == source.id
                and
                self.target.id == target.id
            )
        else:
            super().__eq__(self)


