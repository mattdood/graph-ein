from typing import Dict, List, Union

from .database import Database
from .edge import Edge
from .node import Node


class Graph:
    """Graph representation from SQLite db."""

    def __init__(self, db_path: str, nodes: Dict[str, Node], edges: List[Edge]):
        self.database = Database(db_path)
        self.nodes = nodes
        self.edges = edges

    def add_node(self, schema_name: str, json_data: Dict) -> None:
        self.database.add_node(schema_name=schema_name, json_data=json_data)
        node = self.database.get_node(schema_name=schema_name, node_id=json_data["id"])
        edges = self.database.get_edges(
            schema_name=schema_name,
            source_id=json_data["id"],
            target_id=json_data["id"],
        )
        self.nodes[json_data["id"]] = Node(id=json_data["id"], body=json_data["body"], edges=edges)

    def add_edge(self, schema_name: str, source_id: str=None, target_id: str=None) -> None:
        """Add an edge"""
        kwargs = dict(source_id=source_id, target_id=target_id)
        self.database.add_edge(
            schema_name=schema_name,
            **{key: value for key, value in kwargs.items() if value is not None}
        )

    def update_node(self, schema_name: str, node_id: str, json_data: Dict) -> None:
        self.database.update_node(
            schema_name=schema_name,
            node_id=node_id,
            json_data=json_data
        )
        updated_node_data = self.database.get_node(schema_name=schema_name, node_id=node_id))
        updated_node_edges = self.database.get_edges(schema_name=schema_name, source_id=node_id)
        # fix this
        updated_node = Node(id=updated_node_data[0], body=updated_node_data[1], updated_node_edges[0])
        self.nodes[node_id] =

    def get_node(self, node_id: str) -> Union[Node, None]:
        """Fetch a node."""
        if self.nodes[node_id]:
            return self.nodes[node_id]
        else:
            return None

    def get_edge(self, source_id: str, target_id: str) -> Union[Edge, None]:
        """Fetch an edge."""
        for edge in self.edges:
            if edge.source_id == source_id and edge.target_id == target_id:
                return edge
        return None

    def delete_node(self, schema_name: str, node_id: str) -> None:
        self.database.delete_node(schema_name=schema_name, node_id=node_id)

    def delete_edge(self, schema_name: str, source_or_target_id: str) -> None:
        self.database.delete_edge(schema_name=schema_name, source_or_target_id=source_or_target_id)

