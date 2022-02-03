import json
import pathlib
import sqlite3
from typing import Dict


class IncompleteStatementError(Exception):
    """An incomplete SQL statement was used."""

    def __init__(self):
        self.message = """An incomplete SQL statement was used.

        Statements must have termination with semicolons `;`
        and require closed string literals.
        """
        super().__init__(self.message)


class Database:
    """Database interface to SQLite DB."""

    def __init__(self, db_path: str):
        """Establish connection to new or existing db."""
        self._connection = sqlite3.connect(db_path)
        self._cursor = self._connection.cursor()

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
        with open(pathlib.Path.cwd() / "src/ein/sql" / file_name) as file:
            sql_text = file.read()

        if schema_name:
            return sql_text.replace("{{schema_name}}", schema_name)
        else:
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
        self._cursor.execute(sql_text)

    def add_node(self, schema_name: str, json_data: Dict):
        """Adds a 'node' to SQLite db.

        Params:
            schema_name (str): Name to prepend to tables.
            json_data (Dict): The data representing a node.

        Returns:
            None
        """
        sql_text = self._read_sql_file("insert-node.sql", schema_name)
        self._cursor.execute(sql_text, (json_data["id"], json_data))

    def add_edge(self, schema_name: str, source_id: str, target_id: str, properties: Dict=None):
        """Adds a 'edge' to SQLite db.

        Params:
            schema_name (str): Name to prepend to tables.
            source_id (str): Origin ID of the edge.
            target_id (str): Direction of the link in the edge.
            properties (Dict): Properties of the link (e.g., weights)

        Returns:
            None
        """
        sql_text = self._read_sql_file("insert-edge.sql", schema_name)
        self._cursor.execute(sql_text, (source_id, target_id, properties))

    def delete_schema(self, schema_name: str):
        """Removes a 'schema' from the SQLite db.

        Params:
            schema_name (str): Name to prepend to the table.

        Returns:
            None
        """
        sql_text = self._read_sql_file("delete-schema.sql", schema_name)
        self._cursor.execute(sql_text)

    def delete_node(self, schema_name: str, node_id: str):
        """Removes a 'schema' from the SQLite db.

        Params:
            schema_name (str): Name to prepend to the table.

        Returns:
            None
        """
        sql_text = self._read_sql_file("delete-node.sql", schema_name)
        self._cursor.execute(sql_text, node_id)

    def delete_edge(self, schema_name: str, source_or_target_id: str):
        """Removes all 'edge' rows from the SQLite db.

        Params:
            schema_name (str): Name to prepend to the table.
            source_or_target_id (str): Source or target ID of a row.

        Returns:
            None
        """
        sql_text = self._read_sql_file("delete-edge.sql", schema_name)
        self._cursor.execute(sql_text, source_or_target_id)

    def get_schemas(self, schema_name: str=None):
        """Retrieves all schemas matching schema name.

        Executes a `LIKE` operation on the `sqlite_master`
        table to retrieve table names (schemas).

        Params:
            schema_name (str|None): Schema to search for.
                Wildcard allows for finding `_nodes` and `_edges` tables.
        """
        sql_text = self._read_sql_file("select-schemas.sql")
        if schema_name:
            schema_name = schema_name + "%"
        return self._cursor.execute(sql_text, (schema_name,)).fetchall()

    def get_nodes(self, schema_name: str, params: Dict):
        """Retrieves all nodes matching schema name and params.

        Executes a `LIKE` operation on an included `body` in params,
        will execute an `=` operation on `id` in params. The
        """
        sql_text = self._read_sql_file("select-nodes.sql", schema_name)

        sql_params = ""

        # ugly but it works
        if params["id"] and params["body"]:
            sql_params += "id = " + params["id"] + " OR "
            sql_params += "body LIKE " + json.dumps(params["body"])
        else:
            if params["id"]:
                sql_params += "id = " + params["id"]
            if params["body"]:
                sql_params += "body LIKE " + json.dumps(params["body"])

        return self._cursor.execute(sql_text, (sql_params, sql_params,)).fetchall()

    def get_edges(self, schema_name: str, params: Dict):
        """Retrieves all edges matching schema name and params.

        Executes an `=` operation on `source_id`, `target_id`, or both
        given a schema name.
        """
        sql_text = self._read_sql_file("select-edges.sql", schema_name)

        sql_params = ""

        # ugly but it works
        if params["source_id"] and params["target_id"]:
            sql_params += "source_id = " + params["source_id"] + " OR "
            sql_params += "target_id = " + params["target_id"]
        else:
            if params["source_id"]:
                sql_params += "source_id = " + params["source_id"]
            if params["target_id"]:
                sql_params += "target_id = " + params["target_id"]

        return self._cursor.execute(sql_text, (schema_name, sql_params,)).fetchall()

    def execute_sql(self, sql_text: str):
        """Executes arbitrary SQL.

        Only use this is you know what you're
        doing.

        Params:
            sql_text (str): Query to execute.
        """
        return self._cursor.execute(sql_text).fetchall()


