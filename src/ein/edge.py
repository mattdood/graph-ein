from typing import Dict

from .node import Node


class Edge:
    """Edge object from the SQLite db."""

    def __init__(self, source_id: Node, target_id: Node, properties: Dict=None):
        """Representation of 'edge' from {{schema_name}}_edges."""
        self.source_id = source_id
        self.target_id = target_id
        self.properties = properties

    def update_source_id(self, source_id: Node) -> None:
        self.source_id = source_id

    def update_target_id(self, target_id: Node) -> None:
        self.target_id = target_id

    def update_properties(self, properties: Dict) -> None:
        self.properties = properties

