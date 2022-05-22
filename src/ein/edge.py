from typing import Dict, Optional

from src.ein.node import Node


class Edge:
    """Edge object from the SQLite db."""

    def __init__(self,
                 schema_name: str,
                 source: Node,
                 target: Node,
                 source_schema_name: Optional[str] = None,
                 target_schema_name: Optional[str] = None,
                 properties: Optional[Dict] = None) -> None:
        """Representation of 'edge' from {{schema_name}}_edges.

        Params:
            schema_name (str): Schema name for edge table.
            source (Node): Origin `Node` of the edge.
            target (Node): Direction of the `Node` link in the edge.
            source_schema_name (Optional[str]): Name to prepend to tables,
                defaults to the `schema_name` if not given. This is for
                multi-table nodes (separate schemas).
            target_schema_name (Optional[str]): Name to prepend to tables,
                defaults to the `schema_name` if not given. This is for
                multi-table nodes (separate schemas).
            properties (Optional[Dict]): Properties of the link (e.g., weights)

        Returns:
            None
        """
        self.schema_name = schema_name
        self.source = source
        self.source_schema_name = source_schema_name if source_schema_name else schema_name
        self.target = target
        self.target_schema_name = target_schema_name if target_schema_name else schema_name
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


