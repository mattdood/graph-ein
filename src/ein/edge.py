from typing import Dict, Optional

from .node import Node


class Edge:
    """Edge object from the SQLite db."""

    def __init__(self,
                 schema_name: str,
                 source: Node,
                 target: Node,
                 properties: Optional[Dict] = None) -> None:
        """Representation of 'edge' from {{schema_name}}_edges.

        Params:
            schema_name (str): Schema name for edge table.
            source (Node): Origin `Node` of the edge.
            target (Node): Direction of the `Node` link in the edge.
            properties (Optional[Dict]): Properties of the link (e.g., weights)

        Returns:
            None
        """
        self.schema_name = schema_name
        self.source = source
        self.source_schema_name = source.schema_name
        self.target = target
        self.target_schema_name = target.schema_name
        self.properties = properties

    def __eq__(self, source: Optional[Node] = None, target: Optional[Node] = None) -> bool:

        if source and target:
            return (
                self.source.id == source.id and self.target.id == target.id
            )
        else:
            super().__eq__(self)


