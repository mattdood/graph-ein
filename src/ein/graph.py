import json
import sqlite3
from typing import Dict, List, Set, Union

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
        self.db_path = db_path
        self.database = Database(db_path=db_path, row_factory=True)
        self.schemas = self._all_schemas()
        self.nodes = self._all_schema_nodes()
        self.edges = self._all_schema_edges()

    def _all_schemas(self) -> Set[str]:
        """Fetch all schemas on init.

        If the database already exists then we
        need a `Set` of all schema names that exist.
        This requires de-duping the `<schema_name>_nodes`
        and `<schema_name>_edges`. We then split the
        prefix out of the table name.
        """
        schema_rows = self.database.get_schemas()
        schema_list = [schema["name"] for schema in schema_rows]
        return set(
            [schema_name.split("_")[0] for schema_name in schema_list]
        )

    def _all_schema_nodes(self) -> Dict[str, Node]:
        """Fetch all nodes for all schemas.

        Gets all nodes from every schema that we have.

        Params:
            None

        Returns:
            nodes (Dict[str, Node]): Dict of node IDs and node objects
                or an empty dictionary.
        """
        nodes = {}
        for schema_name in self.schemas:
            node_rows = self.database.get_all_nodes(schema_name=schema_name)
            for node_row in node_rows:
                node = self._create_node(schema_name=schema_name, node_row=node_row)
                nodes[node.id] = node
        return nodes

    def _all_schema_edges(self) -> List[Edge]:
        """Fetch all edges for all schemas.

        Gets all edges from every schema that we have.

        Params:
            None

        Returns:
            edges (List[Edge]): List of edge objects or an empty list.
        """
        edges = []
        for schema_name in self.schemas:
            edge_rows = self.database.get_all_edges(schema_name=schema_name)
            for edge_row in edge_rows:
                edge = self._create_edge(schema_name=schema_name, edge_row=edge_row)
                edges.append(edge)
        return edges

    def add_schema(self, schema_name: str) -> None:
        """Adds a schema.

        Params:
            schema_name (str): Schema name for the DB.

        Returns:
            None
        """
        self.database.add_schema(schema_name=schema_name)
        self.schemas.add(schema_name)

    def add_node(self, schema_name: str, node: Node) -> None:
        """Adds a node.

        Params:
            schema_name (str): Schema name for the DB.
            node (Node): A node instance to add to the db
                and graph.

        Returns:
            None
        """
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

    def add_nodes(self, schema_name: str, nodes: List[Node]) -> None:
        """Adds many nodes.

        Inserts many nodes to the database, does not support multiple
        schemas in one bulk insert.

        TODO:
            * Change nodes addition to do a
                `SELECT ... FROM ... WHERE node_id IN (...)`
                Because I still like the idea of only ever having the true DB representation
                of data in the Graph obj.

        Params:
            schema_name (str): Schema name for the DB.
            nodes (List[Node]): Nodes to add to the database.

        Returns:
            None
        """
        self.database.add_nodes(
            schema_name=schema_name,
            nodes=[(node.id, json.dumps(node.body)) for node in nodes],
        )
        for node in nodes:
            self.nodes[node.id] = node

    def add_edge(self, edge: Edge) -> None:
        """Adds an edge.

        Params:
            schema_name (str): Schema name for the DB.
            edge (Edge): An edge instance to add to the db
                and graph.

        Returns:
            None
        """
        self.database.add_edge(
            schema_name=edge.schema_name,
            source_id=edge.source.id,
            target_id=edge.target.id,
            source_schema_name=edge.source_schema_name,
            target_schema_name=edge.target_schema_name,
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

    def add_edges(self, schema_name: str, edges: List[Edge]) -> None:
        """Adds many edges.

        Add many edges to one schema. This is limited to the single
        source schema of the edges, all source schemas
        should match. JSON data for properties is dumped as string
        here because we're passing a tuple into the database function.

        We don't pull freshly added edges from the DB for the
        updated `self.edges` list like we do on single adds/updates/etc.
        It would be more cumbersome to select, so just avoided doing it.

        TODO:
            * Change edges addition to do a
                `SELECT ... FROM ... WHERE source_id IN (...) OR target_id IN (...)`
                Because I still like the idea of only ever having the true DB representation
                of data in the Graph obj.

        Params:
            schema_name (str): Schema name for the DB table.
            edges (List[Edge]): List of edges to add.

        Returns:
            None
        """
        self.database.add_edges(
            schema_name=schema_name,
            edges=[(
                   edge.source.id, edge.source_schema_name,
                   edge.target.id, edge.target_schema_name,
                   json.dumps(edge.properties),
                   ) for edge in edges],
        )
        self.edges += edges

    def update_node(self, node: Node) -> None:
        """Updates a node in the DB and graph.

        Params:
            node (Node): Updates a node in the DB.
                Be sure to update the `body` before
                passing it to be updated.

        Returns:
            None
        """
        self.database.update_node(
            schema_name=node.schema_name,
            node_id=node.id,
            node_body=node.body,
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

    def update_edge(self, edge: Edge) -> None:
        """Updates a edge in the DB and graph.

        Does not change the `source` or `target`
        of the edge.

        Params:
            edge (Edge): Updates a edge in the DB.
                Be sure to update the `properties`
                before passing it to be updated.

        Returns:
            None
        """
        self.database.update_edge(
            schema_name=edge.schema_name,
            source_id=edge.source.id,
            target_id=edge.target.id,
            properties=edge.properties,
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
            if edge.__eq__(updated_edge.source, updated_edge.target):
                self.edges.remove(edge)

        self.edges.append(updated_edge)

    def get_schema(self, schema_name: str) -> Union[str, None]:
        """Fetch a schema.

        Gets a schema from the graph. If it
        exists it is returned.

        Params:
            schema_name (str): Schema name to check.

        Returns:
            schema_name (str | None): A schema name, if exists.
        """
        if schema_name in self.schemas:
            return schema_name
        else:
            return None

    def get_node(self, node_id: str) -> Union[Node, None]:
        """Fetch a node.

        Get a node by the ID.

        Params:
            node_id (str): ID of a node.

        Returns:
            node (Node | None): Node object, if exists.
        """
        if self.nodes[node_id]:
            return self.nodes[node_id]
        else:
            return None

    def get_edge(self, source: Node, target: Node) -> Union[Edge, None]:
        """Fetch a edge.

        Get a edge by the ID of the
        source and target nodes.

        Params:
            source (Node): Source node of the edge.
            target (Node): Target node of the edge.

        Returns:
            edge (Edge | None): Edge object, if exists.
        """
        for edge in self.edges:
            if edge.__eq__(source=source, target=target):
                return edge
        return None

    def delete_schema(self, schema_name: str) -> None:
        """Remove a schema from the DB and graph."""
        self.database.delete_schema(schema_name=schema_name)
        self.schemas.remove(schema_name)

    def delete_node(self, node: Node) -> None:
        """Remove a node from the DB and graph."""
        self.database.delete_node(schema_name=node.schema_name, node_id=node.id)
        self.nodes.pop(node.id)

    def delete_edge(self, edge: Edge) -> None:
        """Remove an edge from the DB and graph."""
        self.database.delete_edge(
            schema_name=edge.schema_name,
            source_id=edge.source.id,
            target_id=edge.target.id,
        )
        for e in self.edges:
            if edge.__eq__(e.source, e.target):
                self.edges.remove(e)

    def _create_node(self, schema_name: str, node_row: sqlite3.Row) -> Node:
        """A Node constructor.

        Returns a `Node` object from a database
        `Row` object.

        Params:
            schema_name (str): Schema name to use.
            node_row (sqlite3.Row): A SQLite database row.

        Returns:
            node (Node): A node object.
        """
        return Node(
            schema_name=schema_name,
            id=node_row["id"],
            body=json.loads(node_row["body"]),
        )

    def _create_edge(self, schema_name: str, edge_row: sqlite3.Row) -> Edge:
        """A Edge constructor.

        Constructs nodes and an edge
        given the data from the database.

        Params:
            schema_name (str): Schema name to use.
            edge_row (sqlite3.Row): A SQLite database row.

        Returns:
            edge (Edge): A edge object.
        """
        return Edge(
            schema_name=schema_name,
            source=self.get_node(edge_row["source"]),
            target=self.get_node(edge_row["target"]),
            properties=json.loads(edge_row["properties"]),
        )

