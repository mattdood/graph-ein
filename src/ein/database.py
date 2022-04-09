import json
import pathlib
import sqlite3
from typing import Dict, List, Optional

ALLOWED_OPERATORS = {"and", "not", "or"}


class IncompleteStatementError(Exception):
    """An incomplete SQL statement was used."""

    def __init__(self) -> None:
        self.message = """An incomplete SQL statement was used.

        Statements must have termination with semicolons `;`
        and require closed string literals.
        """
        super().__init__(self.message)


class DisallowedOperatorError(Exception):
    """Disallowed operator, controlled by `ALLOWED_OPERATORS`."""
    pass


class Database:
    """Database interface to SQLite DB."""

    def __init__(self, db_path: str) -> None:
        """Establish connection to new or existing db."""
        self._connection = sqlite3.connect(db_path)
        self._cursor = self._connection.cursor()

    def _read_sql_file(self,
                       file_name: str,
                       schema_name: Optional[str]=None) -> str:
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

    def add_node(self, schema_name: str, json_data: Dict) -> None:
        """Adds a 'node' to SQLite db.

        Params:
            schema_name (str): Name to prepend to tables.
            json_data (Dict): The data representing a node.

        Returns:
            None
        """
        sql_text = self._read_sql_file("insert-node.sql", schema_name)
        self._cursor.execute(sql_text, (json_data["id"], json.dumps(json_data)))
        self._connection.commit()

    def add_edge(self, schema_name: str,
                source_id: str, target_id: str, properties: Optional[Dict]=None) -> None:
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

    def delete_edge(self, schema_name: str, source_or_target_id: str) -> None:
        """Removes all 'edge' rows from the SQLite db.

        Params:
            schema_name (str): Name to prepend to the table.
            source_or_target_id (str): Source or target ID of a row.

        Returns:
            None
        """
        sql_text = self._read_sql_file("delete-edge.sql", schema_name)
        self._cursor.execute(sql_text, (source_or_target_id, source_or_target_id,))
        self._connection.commit()

    def get_schemas(self, schema_name: Optional[str]=None) -> List:
        """Retrieves all schemas matching schema name.

        Executes a `LIKE` operation on the `sqlite_master`
        table to retrieve table names (schemas).

        Params:
            schema_name (str|None): Schema to search for.
                Wildcard allows for finding `_nodes` and `_edges` tables.
        """
        sql_text = self._read_sql_file("select-schemas.sql", schema_name)

        # sqlite will turn `None` to `null`, so we use an emptry string for the
        # concat operation in the `LIKE` clause
        return self._cursor.execute(sql_text, (schema_name or "",)).fetchall()

    def get_node(self, schema_name: str, node_id: str) -> Dict:
        """Retrieve one node from the database.

        TODO:
            * Test this
            * Ensure return structure is consistent
        """
        sql_text = self._read_sql_file("select-node.sql", schema_name)
        return self._cursor.execute(sql_text, (node_id,)).fetchone()

    def get_nodes(self, schema_name: str, node_id: Optional[str]=None,
                node_body: Optional[Dict]=None,
                operator: str="or"
        ) -> List:
        """Retrieves all nodes matching schema name and params.

        Executes a `LIKE` operation on an included `body` in params,
        will execute an `=` operation on `id` in params. The
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

    def get_edge(self, schema_name: str, source_id: str, target_id: str) -> Dict:
        """Retrieve one edge.

        Get a single edge from the database.

        TODO:
            * Test this
            * Ensure output format is consistent to Dict
        """

        sql_text = self._read_sql_file("select-edge.sql", schema_name)
        edge = self._cursor.execute(sql_text, (source_id, target_id,)).fetchone()
        return edge

    def get_edges(self, schema_name: str, source_id: Optional[str]=None,
                target_id: Optional[str]=None, properties: Optional[Dict]=None) -> List:
        """Retrieves all edges matching schema name and params.

        Executes an `=` operation on `source`, `target`, or both
        given a schema name.
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

        Only use this is you know what you're
        doing.

        Params:
            sql_text (str): Query to execute.

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

        Only use this is you know what you're
        doing.

        Params:
            sql_text (str): Queries to execute.

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

