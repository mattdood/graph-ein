import json
import pathlib
import sqlite3
from typing import Dict, List


class Database:
    """Database interface to SQLite DB."""

    def __init__(self, db_path: str):
        """Establish connection to new or existing db."""
        self.connection = sqlite3.connect(db_path)
        self.cursor = self.connection.cursor()

    def _read_sql_file(self, file_name: str, schema_name: str=None):
        """Read `.sql` utility files.

        Reads project SQL utility files and inserts the
        `schema_name` variable to create a pseudo-schema
        object.

        Params:
            file_name (str): Name of the file to read.
            schema_name (str): Name of the schema to use.

        Returns:
            sql_text (str): SQL text with the schema added.
        """
        with open(pathlib.Path.cwd() / ".." / "sql" / file_name) as file:
            sql_text = file.read()

        if schema_name:
            sql_text.replace("{{schema_name}}", schema_name)
        return sql_text

    def add_schema(self, schema_name: str) -> None:
        """Adds a 'schema' to SQLite db.

        Schemas aren't directly supported so we
        use a naming scheme that allows us to use
        schemas.

        Params:
            schema_name (str): Name to prepend to tables.

        Returns:
            None
        """
        sql_text = self._read_sql_file("create-schema.sql", schema_name)
        self.cursor.execute(sql_text)

    def add_node(self, schema_name: str, json_data: Dict):
        """Adds a 'node' to SQLite db.

        Params:
            schema_name (str): Name to prepend to tables.
            json_data (Dict): The data representing a node.

        Returns:
            None
        """
        sql_text = self._read_sql_file("insert-node.sql", schema_name)
        self.cursor.execute(sql_text, (json_data["id"], json_data))

    def add_edge(self, schema_name: str, source: str, target: str):
        pass

    def delete_schema(self, schema_name: str):
        pass

    def delete_node(self, schema_name: str):
        pass

    def delete_edge(self, schema_name: str, source: str):
        pass

    def get_schemas(self, param: List=None):
        pass

    def get_nodes(self, param: List=None):
        pass

    def get_edges(self, param: List=None):
        pass

