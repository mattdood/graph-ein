import json
import sqlite3
from typing import Dict, List, Optional, Set, Union

from src.ein.database import Database
from src.ein.edge import Edge
from src.ein.node import Node


class Graph:
    """Graph representation from SQLite db."""

    def __init__(self, db_path: str) -> None:
        """Database initialization from new or existing path.

        Params:
            db_path (str): Path to a new SQLite database
                or existing database.
        """
        self.database = Database(db_path=db_path, row_factory=True)
        self.schemas = self._all_schemas()
        self.nodes = self._all_schema_nodes()
        self.edges = self._all_schema_edges()

    def _all_schemas(self) -> Set[str]:
        pass

    def _all_schema_nodes(self) -> Dict[str, Node]:
        pass

    def _all_schema_edges(self) -> List[Edge]:
        pass

    def add_schema(self, schema_name: str) -> None:
        self.database.add_schema(schema_name=schema_name)

    def add_node(self, schema_name: str, node: Node) -> None:
        self.database.add_node(
            schema_name=schema_name,
            node_id=node.id,
            node_body=node.body
        )
        new_node = self.database.get_node(schema_name=schema_name, node_id=node.id)
        self.nodes[new_node["id"]] = self._create_node(
            schema_name=schema_name,
            node_row=new_node,
        )

    def add_edge(self, edge: Edge) -> None:
        """Add an edge"""
        self.database.add_edge(
            schema_name=edge.schema_name,
            source_id=edge.source.id,
            target_id=edge.target.id,
            properties=edge.properties,
        )
        edge_data = self.database.get_edge(
            schema_name=edge.schema_name,
            source_id=edge.source.id,
            target_id=edge.target.id,
        )
        self.edges.append(
            self._create_edge(
                schema_name=edge.schema_name,
                edge_row=edge_data,
            )
        )

    def update_node(self, node: Node, updated_body: Dict) -> None:
        self.database.update_node(
            schema_name=node.schema_name,
            node_id=node.id,
            node_body=updated_body,
        )
        updated_node_data = self.database.get_node(
            schema_name=node.schema_name,
            node_id=node.id
        )
        updated_node = self._create_node(
            schema_name=node.schema_name,
            node_row=updated_node_data,
        )

        self.nodes[updated_node.id] = updated_node

    def update_edge(self, edge: Edge, properties: Optional[Dict] = None) -> None:
        self.database.update_edge(
            schema_name=edge.schema_name,
            source_id=edge.source.id,
            target_id=edge.target.id,
            properties=properties,
        )
        updated_edge_data = self.database.get_edge(
            schema_name=edge.schema_name,
            source_id=edge.source.id,
            target_id=edge.target.id,
        )
        updated_edge = self._create_edge(
            schema_name=edge.schema_name,
            edge_row=updated_edge_data,
        )

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

    def get_edge(self, source: Node, target: Node) -> Union[Edge, None]:
        """Fetch an edge."""
        for edge in self.edges:
            if edge.__eq__(source=source, target=target):
                return edge
        return None

    def delete_node(self, node: Node) -> None:
        self.database.delete_node(schema_name=node.schema_name, node_id=node.id)
        self.nodes.pop(node.id)

    def delete_edge(self, edge: Edge) -> None:
        self.database.delete_edge(
            schema_name=edge.schema_name,
            source_id=edge.source.id,
            target_id=edge.target.id,
        )
        self.edges.remove(edge)

    def _create_node(self, schema_name: str, node_row: sqlite3.Row) -> Node:
        return Node(
            schema_name=schema_name,
            id=node_row["id"],
            body=json.loads(node_row["body"]),
        )

    def _create_edge(self, schema_name: str, edge_row: sqlite3.Row) -> Edge:
        return Edge(
            schema_name=schema_name,
            source=self.get_node(edge_row["source_id"]),
            target=self.get_node(edge_row["target_id"]),
            properties=json.loads(edge_row["properties"]),
        )

