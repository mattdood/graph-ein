import json
import sqlite3
from typing import Dict, List, Optional, Union

from .database import Database
from .edge import Edge
from .node import Node


class Graph:
    """Graph representation from SQLite db."""

    def __init__(self, db_path: str) -> None:
        """Database initialization from new or existing path.

        Params:
            db_path (str): Path to a new SQLite database
                or existing database.
        """
        self.database = Database(db_path=db_path, row_factory=True)
        self.nodes = self._all_schema_nodes()
        self.edges = self._all_schema_edges()

    def _all_schema_nodes(self) -> Dict[str, Node]:
        pass

    def _all_schema_edges(self) -> List[Edge]:
        pass

    def add_schema(self, schema_name: str) -> None:
        self.database.add_schema(schema_name=schema_name)

    def add_node(self, schema_name: str, json_data: Dict) -> None:
        self.database.add_node(schema_name=schema_name, json_data=json_data)
        node = self.database.get_node(schema_name=schema_name, node_id=json_data["id"])
        edges_data = self.database.get_edges(
            schema_name=schema_name,
            source_id=json_data["id"],
            target_id=json_data["id"],
        )

        edges = [self._create_edge(schema_name=schema_name, edge_row=edge) for edge in edges_data]

        self.nodes[node["id"]] = Node(
            schema_name=schema_name,
            id=node["id"],
            body=node["body"],
            edges=edges,
        )

    def add_edge(self, schema_name: str, source_id: str, target_id: str, properties: Optional[Dict]) -> None:
        """Add an edge"""
        self.database.add_edge(
            schema_name=schema_name,
            source_id=source_id,
            target_id=target_id,
            properties=properties,
        )
        edge_data = self.database.get_edge(
            schema_name=schema_name,
            source_id=source_id,
            target_id=target_id,
        )
        self.edges.append(self._create_edge(schema_name=schema_name, edge_row=edge_data))

    def update_node(self, node: Node, updated_body: Dict) -> None:
        self.database.update_node(
            schema_name=node.schema_name,
            node_id=node.id,
            node_body=updated_body,
        )
        updated_node_data = self.database.get_node(schema_name=node.schema_name, node_id=node.id)
        updated_node_edges = self.database.get_edges(
            schema_name=node.schema_name,
            source_id=updated_node_data["id"],
            target_id=updated_node_data["id"],
        )

        updated_node = self._create_node(
            schema_name=node.schema_name,
            node_row=updated_node_data,
            node_edges=updated_node_edges,
        )

        self.nodes[updated_node.id] = updated_node

    def update_edge(self, edge: Edge, properties: Optional[Dict] = None) -> None:
        self.database.update_edge(
            schema_name=edge.schema_name,
            source_id=edge.source_id.id,
            target_id=edge.target_id.id,
            properties=properties,
        )
        updated_edge_data = self.database.get_edge(
            schema_name=edge.schema_name,
            source_id=edge.source_id.id,
            target_id=edge.target_id.id,
        )
        updated_edge = self._create_edge(schema_name=edge.schema_name, edge_row=updated_edge_data)
        for edge in self.edges:
            if edge == updated_edge:
                self.edges.remove(edge)

        self.edges.append(updated_edge)

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

    def delete_node(self, node: Node) -> None:
        self.database.delete_node(schema_name=node.schema_name, node_id=node.id)
        self.nodes.pop(node.id)

    def delete_edge(self, edge: Edge) -> None:
        self.database.delete_edge(
            schema_name=edge.schema_name,
            source_id=edge.source_id.id,
            target_id=edge.target_id.id,
        )
        self.edges.remove(edge)

    def _create_node(self,
                     schema_name: str,
                     node_row: sqlite3.Row,
                     node_edges: List[sqlite3.Row]) -> Node:
        edges = [self._create_edge(schema_name, edge) for edge in node_edges]
        return Node(
            schema_name=schema_name,
            id=node_row["id"],
            body=json.loads(node_row["body"]),
            edges=edges,
        )

    def _create_edge(self, schema_name: str, edge_row: sqlite3.Row) -> Edge:
        return Edge(
            schema_name=schema_name,
            source_id=edge_row["source_id"],
            target_id=edge_row["target_id"],
            properties=json.loads(edge_row["properties"]),
        )

