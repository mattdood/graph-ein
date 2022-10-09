import json
import os
import pathlib
import sqlite3
from typing import Dict, List, Optional, Tuple, Union

ALLOWED_OPERATORS = {"and", "not", "or"}


class IncompleteStatementError(Exception):
    """An incomplete SQL statement was used."""
    pass


class DisallowedOperatorError(Exception):
    """Disallowed operator, controlled by `ALLOWED_OPERATORS`."""
    pass


class Database:
    """Database interface to SQLite DB."""

    def __init__(self, db_path: str, row_factory: bool = True) -> None:
        """Establish connection to new or existing db.

        Params:
            db_path (str): Path to a database. If it doesn't exist
                it will be created.
            row_factory (bool): Uses the `sqlite3.Row` object to return
                from queries. This gives dictionary-like key access to
                column names. Defaults to `True`.

        Returns:
            None
        """
        self.db_path = db_path
        self._connection = sqlite3.connect(db_path)

        # Returns items as a sqlite3.Row
        # this allows reference to columns with
        # a dictionary key
        if row_factory:
            self._connection.row_factory = sqlite3.Row

        self._cursor = self._connection.cursor()

    def _read_sql_file(self,
                       file_name: str,
                       schema_name: Optional[str] = None) -> str:
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
        with open(pathlib.Path(os.path.dirname(__file__)) / "sql" / file_name) as file:
            sql_text = file.read()

        if schema_name:
            sql_text = sql_text.replace("{{schema_name}}", schema_name)

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
        self._cursor.executescript(sql_text)
        self._connection.commit()

    def add_node(self, schema_name: str, node_id: str, node_body: Dict) -> None:
        """Adds a 'node' to SQLite db.

        Params:
            schema_name (str): Name to prepend to tables.
            node_id (str): ID of a node.
            node_body (Dict): The data representing a node.

        Returns:
            None
        """
        sql_text = self._read_sql_file("insert-node.sql", schema_name)
        self._cursor.execute(sql_text, (node_id, json.dumps(node_body)))
        self._connection.commit()

    def add_nodes(self, schema_name: str, nodes: List[Tuple[str, str]]) -> None:
        """Adds many 'node' objects to SQLite db.

        Takes a list of node data and a schema to generate a
        large amount of nodes at once.

        Params:
            schema_name (str): Schema name for all nodes.
            nodes (List[Tuple[str, Dict]]): All nodes in a list of tuples
                that have `(node_id, node_body)` format.

        Returns:
            None
        """
        # bulk insert operation
        sql_text = self._read_sql_file("insert-node.sql", schema_name)
        self._cursor.executemany(sql_text, nodes)
        self._connection.commit()

    def add_edge(self,
                 schema_name: str,
                 source_id: str,
                 target_id: str,
                 source_schema_name: Optional[str] = None,
                 target_schema_name: Optional[str] = None,
                 properties: Optional[Dict] = None) -> None:
        """Adds a 'edge' to SQLite db.

        Params:
            schema_name (str): Schema name for edge table.
            source_id (str): Origin ID of the edge.
            target_id (str): Direction of the link in the edge.
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
        sql_text = self._read_sql_file("insert-edge.sql", schema_name)
        self._cursor.execute(sql_text, (
            source_id,
            source_schema_name if source_schema_name else schema_name,
            target_id,
            target_schema_name if target_schema_name else schema_name,
            json.dumps(properties)
        ))
        self._connection.commit()

    def add_edges(self,
                  schema_name: str,
                  edges: List[Tuple]) -> None:
        """Adds a 'edge' to SQLite db.

        Params:
            schema_name (str): Schema name for edge table.
            edges (List[Tuple]): A list of edges to add to the SQLite database.
                Requires `source_id`, `source_schema_name`, `target_id`,
                `target_schema_name`, and `properties` to be present in the
                tuple.

        Returns:
            None
        """
        sql_text = self._read_sql_file("insert-edge.sql", schema_name)
        self._cursor.executemany(sql_text, edges)
        self._connection.commit()

    def update_schema(self, schema_name: str, new_schema_name: str) -> None:
        """Updates a 'schema' in the SQLite db.

        Params:
            schema_name(str): Name to prepend to tables.
            new_schema_name(str): New name to change tables to.

        Returns:
            None
        """
        sql_text = self._read_sql_file("update-schema.sql", schema_name)
        sql_text = sql_text.replace("{{new_schema_name}}", new_schema_name)
        self._cursor.executescript(sql_text)
        self._connection.commit()

    def update_node(self,
                    schema_name: str,
                    node_id: str,
                    node_body: Dict) -> None:
        """Updates a 'node' in the SQLite db.

        Params:
            schema_name(str): Name to prepend to tables.
            node_id (str): Node ID.
            node_body (Dict): Data to update.

        Returns:
            None
        """
        sql_text = self._read_sql_file("update-node.sql", schema_name)
        self._cursor.execute(sql_text, (json.dumps(node_body), node_id))
        self._connection.commit()

    def update_edge(self,
                    schema_name: str,
                    source_id: str,
                    target_id: str,
                    properties: Optional[Dict] = None) -> None:
        """Updates an 'edge' in the SQLite db.

        Params:
            schema_name(str): Name to prepend to tables.
            source_id (str): Origin ID of the edge.
            target_id (str): Direction of the link in the edge.
            properties (Dict): Properties of the link (e.g., weights) to update.

        Returns:
            None
        """
        sql_text = self._read_sql_file("update-edge.sql", schema_name)
        self._cursor.execute(sql_text, (json.dumps(properties), source_id, target_id))
        self._connection.commit()

    def delete_schema(self, schema_name: str) -> None:
        """Removes a 'schema' from the SQLite db.

        Params:
            schema_name (str): Name to prepend to the table.

        Returns:
            None
        """
        sql_text = self._read_sql_file("delete-schema.sql", schema_name)
        self._cursor.executescript(sql_text)
        self._connection.commit()

    def delete_node(self, schema_name: str, node_id: str) -> None:
        """Removes a 'node' from the SQLite db.

        Params:
            schema_name (str): Name to prepend to the table.

        Returns:
            None
        """
        sql_text = self._read_sql_file("delete-node.sql", schema_name)
        self._cursor.execute(sql_text, (node_id,))
        self._connection.commit()

    def delete_nodes(self, schema_name: str, node_ids: List[str]) -> None:
        """This bulk deletes 'nodes' in one transaction."""
        sql_text = self._read_sql_file("delete-nodes.sql", schema_name)
        node_list = (", ").join(f"'{node}'" for node in node_ids)
        nodes = f"({node_list})"
        sql_text = sql_text.replace("{{node_ids}}", nodes)
        self._cursor.execute(sql_text)
        self._connection.commit()

    def delete_edge(self, schema_name: str, source_id: str, target_id: str) -> None:
        """Removes one 'edge' row from the SQLite db.

        Params:
            schema_name (str): Name to prepend to the table.
            source_id (str): Source ID of a row.
            target_id (str): Target ID of a row.

        Returns:
            None
        """
        sql_text = self._read_sql_file("delete-edge.sql", schema_name)
        self._cursor.execute(sql_text, (source_id, target_id,))
        self._connection.commit()

    def delete_edges(self, schema_name: str, source_or_target_id: str) -> None:
        """Removes all 'edge' rows from the SQLite db.

        Params:
            schema_name (str): Name to prepend to the table.
            source_or_target_id (str): Source or target ID of a row.

        Returns:
            None
        """
        sql_text = self._read_sql_file("delete-edges.sql", schema_name)
        self._cursor.execute(sql_text, (source_or_target_id, source_or_target_id,))
        self._connection.commit()

    def get_schemas(self, schema_name: Optional[str] = None) -> List[Union[Tuple, sqlite3.Row]]:
        """Retrieves all schemas matching schema name.

        Executes a `LIKE` operation on the `sqlite_master`
        table to retrieve table names (schemas).

        Params:
            schema_name (str|None): Schema to search for.
                Wildcard allows for finding `_nodes` and `_edges` tables.

        Returns:
            results (List(Tuple)): List of all matching schemas.
        """
        sql_text = self._read_sql_file("select-schemas.sql", schema_name)

        # sqlite will turn `None` to `null`, so we use an emptry string for the
        # concat operation in the `LIKE` clause
        return self._cursor.execute(sql_text, (schema_name or "",)).fetchall()

    def get_node(self, schema_name: str, node_id: str) -> Union[Tuple, sqlite3.Row]:
        """Retrieve one node from the database.

        Params:
            schema_name (str): Schema name to search with.
            node_id (str): Node ID.

        Returns:
            result (Tuple): Tuple of the row fetched.
        """
        sql_text = self._read_sql_file("select-node.sql", schema_name)
        return self._cursor.execute(sql_text, (node_id,)).fetchone()

    def get_all_nodes(self, schema_name: str) -> List[Union[Tuple, sqlite3.Row]]:
        """Retrieve all nodes from a schema name."""
        sql_text = self._read_sql_file("select-all-nodes.sql", schema_name)
        return self._cursor.execute(sql_text).fetchall()

    def get_nodes(self,
                  schema_name: str,
                  node_id: Optional[str] = None,
                  node_body: Optional[Dict] = None,
                  operator: str = "or") -> List[Union[Tuple, sqlite3.Row]]:
        """Retrieves all nodes matching schema name and params.

        Executes a `LIKE` operation on an included `body` in params,
        will execute an `=` operation on `id` in params. The

        Params:
            schema_name (str): Name to prepend to tables.
            node_id: (str|None): Node ID to pass for search.
            node_body (Dict): The data representing a node to search on.
            operator (str): Operator to use for `node_id` and `node_body`, defaults to "or".
                Options include: "or", "and", "not". Note: Cannot use "not" on `node_id`.

        Returns:
            None
        """

        if operator.lower() not in ALLOWED_OPERATORS:
            msg = f"Illegal operator passed to query: {operator}"
            raise DisallowedOperatorError(msg)

        sql_text = self._read_sql_file("select-nodes.sql", schema_name)

        sql_params = []

        if node_id:
            sql_params.append("id = '{node_id}'".format(node_id=node_id))

        if node_body:
            json_search = 'json_extract(body, "$") LIKE \'%{node_body}%\''.format(
                node_body=json.dumps(node_body),
            )
            # sanitize Dict input from string dump to
            # remove trailing curly braces and compress to
            # database stored format

            # replace "%{"data": "here"}%"
            # to "%"data": "here"%"
            json_search = json_search.replace("%{", "%")
            json_search = json_search.replace("}%", "%")

            # remove ": ", they're stored without spaces
            json_search = json_search.replace(": ", ":")

            sql_params.append(json_search)

        sql_params = f" {operator} ".join(sql_params)
        sql_text = sql_text.replace("{{params}}", sql_params)

        return self._cursor.execute(sql_text).fetchall()

    def get_edge(self,
                 schema_name: str,
                 source_id: str,
                 target_id: str) -> Union[Tuple, sqlite3.Row]:
        """Retrieve one edge.

        Get a single edge from the database.

        Params:
            schema_name (str): Schema name to search.
            source_id (str): Source ID of the edge.
            target_id (str): Target ID of the edge.

        Returns:
            result (Tuple): First result matching both source and
                target IDs.
        """

        sql_text = self._read_sql_file("select-edge.sql", schema_name)
        edge = self._cursor.execute(sql_text, (source_id, target_id,)).fetchone()
        return edge

    def get_all_edges(self, schema_name: str) -> List[Union[Tuple, sqlite3.Row]]:
        """Retrieve all edges from a schema name."""
        sql_text = self._read_sql_file("select-all-edges.sql", schema_name)
        return self._cursor.execute(sql_text).fetchall()

    def get_edges(self,
                  schema_name: str,
                  source_id: Optional[str] = None,
                  target_id: Optional[str] = None,
                  properties: Optional[Dict] = None) -> List[Union[Tuple, sqlite3.Row]]:
        """Retrieves all edges matching schema name and params.

        Executes an `=` operation on `source`, `target`, or both
        given a schema name.

        Params:
            schema_name (str): Schema name to search.
            source_id (str): Optional source ID of the edge.
            target_id (str): Optional target ID of the edge.
            properties (Dict): Optional list of properties to search on.

        Returns:
            result (List): All results matching the parameters passed.
                Uses an `OR` operation on each parameter.
        """
        sql_text = self._read_sql_file("select-edges.sql", schema_name)

        sql_params = []

        # create a param set for each param sent in
        # columns need to be in double quotes
        if source_id:
            sql_params.append(""" "source" = '{source_id}'""".format(source_id=source_id))
        if target_id:
            sql_params.append(""" "target" = '{target_id}'""".format(target_id=target_id))
        if properties:
            sql_params.append(""" "properties" = '{properties}'""".format(properties=properties))

        # add `OR`s between each param
        sql_params = " OR ".join(sql_params)

        # for some reason the `?` variable in SQLite does something weird
        # to double quoted params passed as a string
        # I don't know why, so we replace a placeholder (which is bad, but whatever)
        sql_text = sql_text.replace("{{params}}", sql_params)

        return self._cursor.execute(sql_text).fetchall()

    def execute_sql(self, sql_text: str) -> Optional[List]:
        """Executes arbitrary SQL.

        Only use this if you know what you're
        doing.

        Params:
            sql_text (str): Query to execute.

        Returns:
            results (List): A list of SQLite rows.

        Raises:
            IncompleteStatementError: An incomplete
                SQL statement was used.
        """
        if sqlite3.complete_statement(sql_text):

            try:
                self._cursor.execute(sql_text)

                if "select" in sql_text.lower():
                    return self._cursor.fetchall()

            except sqlite3.Error as e:
                print("SQLite raised an exception: ", e)
        else:
            raise IncompleteStatementError

    def execute_sql_script(self, sql_text: str) -> Optional[List]:
        """Executes arbitrary SQL scripts.

        Only use this if you know what you're
        doing.

        Params:
            sql_text (str): Queries to execute.

        Returns:
            results (List): A list of SQLite rows.

        Raises:
            IncompleteStatementError: An incomplete
                SQL statement was used.
        """
        if sqlite3.complete_statement(sql_text):

            try:
                self._cursor.executescript(sql_text)

                if "select" in sql_text.lower():
                    return self._cursor.fetchall()

            except sqlite3.Error as e:
                print("SQLite raised an exception: ", e.args[0])
        else:
            raise IncompleteStatementError

