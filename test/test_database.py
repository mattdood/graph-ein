import pytest
import sqlite3

from conftest import TEST_DB, TEST_SCHEMA, db_setup
from src.ein.database import Database, IncompleteStatementError


def test_database_init(db_setup):
    """`Database` object init has proper connection properties."""

    db = Database(TEST_DB)

    assert isinstance(db._connection, sqlite3.Connection)
    assert isinstance(db._cursor, sqlite3.Cursor)


def test_database_executes_arbitrary_sql(db_setup):
    """Arbitrary SQL text executes. Incomplete statements don't execute."""

    # also tests fetchall()
    complete_statement = db_setup.execute_sql("SELECT 'test';")
    expected_complete_statement_result = [("test",)]
    assert complete_statement == expected_complete_statement_result

    incomplete_statement = "SELECT 'test'"
    with pytest.raises(IncompleteStatementError):
        db_setup.execute_sql(incomplete_statement)


def test_database_reads_sql_file(db_setup):
    """`Database` objects can read SQL files and insert schemas."""

    sql_text = db_setup._read_sql_file("create-schema.sql", TEST_SCHEMA)

    expected_sql_text = """
    CREATE TABLE IF NOT EXISTS test_nodes (
        id TEXT NOT NULL PRIMARY KEY,
        body JSON
    );

    CREATE INDEX IF NOT EXISTS idx_id ON test_nodes(id);

    CREATE TABLE IF NOT EXISTS test_edges (
        source TEXT,
        target TEXT,
        properties TEXT,
        UNIQUE(source, target, properties) ON CONFLICT REPLACE,
        FOREIGN KEY(source) REFERENCES nodes(id),
        FOREIGN KEY(target) REFERENCES nodes(id)
    );

    CREATE INDEX IF NOT EXISTS idx_source ON test_edges(source);
    CREATE INDEX IF NOT EXISTS idx_target ON test_edges(target);
    """

    # sanitize both strings to compare
    sql_text = " ".join(sql_text.split())
    expected_sql_text = " ".join(expected_sql_text.split())

    assert TEST_SCHEMA in sql_text
    assert sql_text == expected_sql_text


def test_database_add_schema(db_setup):
    """Schemas are added successfully with node and edge tables."""

    db_setup.add_schema(TEST_SCHEMA)

    check_schema_tables_sql = """
    SELECT
        name
    FROM
        sqlite_master
    WHERE
        type = 'table'
        AND
        tbl_name LIKE 'test%';
    """

    check_schema_indexes_sql = """
    SELECT
        name
    FROM
        sqlite_master
    WHERE
        type = 'index'
        AND
        tbl_name LIKE 'test%';
    """

    schema_tables = db_setup.execute_sql(check_schema_tables_sql)
    schema_indexes = db_setup.execute_sql(check_schema_indexes_sql)

    expected_schema_tables = [("test_nodes",), ("test_edges",)]
    expected_indexes = [("sqlite_autoindex_test_nodes_1",), ("idx_id",), ("sqlite_autoindex_test_edges_1",), ("idx_source",), ("idx_target",)]

    assert schema_tables == expected_schema_tables
    assert schema_indexes == expected_indexes


def test_database_add_node(db_setup):
    """Add a node to a new schema and retrieve it."""

    db_setup.add_schema(TEST_SCHEMA)

    node = {
        "id": "add-node-test",
        "body": {
            "id": "add-node-test",
            "other-data": [
                "string-one",
                "string-two"
            ]
        }
    }

    db_setup.add_node(TEST_SCHEMA, node)

    check_node_sql = """
    SELECT * FROM {schema_name}_nodes;
    """.format(schema_name=TEST_SCHEMA)

    nodes_data = db_setup.execute_sql(check_node_sql)

    expected_nodes_data = [('add-node-test', '{"id":"add-node-test","body":{"id":"add-node-test","other-data":["string-one","string-two"]}}')]

    assert nodes_data == expected_nodes_data


def test_database_add_edge(db_setup):
    """Add an edge to a new schema and retrieve it."""

    db_setup.add_schema(TEST_SCHEMA)

    node_one = {
        "id": "add-edge-test",
        "body": {
            "id": "add-edge-test",
            "other-data": [
                "string-one",
                "string-two"
            ]
        }
    }
    node_two = {
        "id": "add-edge-test2",
        "body": {
            "id": "add-edge-test2",
            "other-data": [
                "string-one",
                "string-two"
            ]
        }
    }

    db_setup.add_node(TEST_SCHEMA, node_one)
    db_setup.add_node(TEST_SCHEMA, node_two)

    db_setup.add_edge(TEST_SCHEMA, node_one["id"], node_two["id"])

    check_edge_sql = """
    SELECT * FROM {schema_name}_edges;
    """.format(schema_name=TEST_SCHEMA)

    edges_data = db_setup.execute_sql(check_edge_sql)

    expected_edges_data = [(node_one["id"], node_two["id"], None)]

    assert edges_data == expected_edges_data


# @pytest.mark.skip(reason="Work on json query params")
def test_database_get_nodes(db_setup):
    """Selects nodes based on params.

    TODO:
        * Finish testing

    Working query (manual):

        db.add_node("test", {"id": "test-id", "body": "test-body"})

        db.execute_sql("select * from test_nodes where json_extract(body, '$.body.other-data') LIKE '%str
        ing-two%';")
    """


    db_setup.add_schema(TEST_SCHEMA)

    node_one = {
        "id": "select-node-test",
        "body": "selected-body"
    }

    node_two = {
        "id": "select-node-test-2",
        "body": "selected-body"
    }

    db_setup.add_node(TEST_SCHEMA, node_one)
    db_setup.add_node(TEST_SCHEMA, node_two)

    params = {
        "body": "selected-body"
    }

    results = db_setup.get_nodes(TEST_SCHEMA, node_body=params)

    assert "selected-body" in results


def test_database_get_edges(db_setup):
    """Queries for edges."""

    db_setup.add_schema(TEST_SCHEMA)

    node_one = {
        "id": "select-edge-test",
        "body": "selected-body"
    }

    node_two = {
        "id": "select-edge-test-2",
        "body": "selected-body"
    }

    db_setup.add_node(TEST_SCHEMA, node_one)
    db_setup.add_node(TEST_SCHEMA, node_two)

    db_setup.add_edge(TEST_SCHEMA, node_one["id"], node_two["id"])

    selected_edges = db_setup.get_edges(TEST_SCHEMA, node_one["id"], node_two["id"])
    expected_edges = [("select-edge-test", "select-edge-test-2", None)]

    assert selected_edges == expected_edges


def test_database_get_schemas(db_setup):
    """Queries for schemas."""

    db_setup.add_schema(TEST_SCHEMA)

    selected_schemas = db_setup.get_schemas(TEST_SCHEMA)
    expected_schemas = [("test_nodes",), ("test_edges",)]

    assert selected_schemas == expected_schemas

    selected_schemas = db_setup.get_schemas()
    assert selected_schemas == expected_schemas


def test_database_delete_node(db_setup):
    """Deletes a test node."""

    db_setup.add_schema(TEST_SCHEMA)

    node_one = {
        "id": "select-edge-test",
        "body": "selected-body"
    }

    db_setup.add_node(TEST_SCHEMA, node_one)

    db_setup.delete_node(TEST_SCHEMA, node_one["id"])

    node_count_sql = """
    SELECT count(*) FROM {schema_name}_nodes;
    """.format(schema_name=TEST_SCHEMA)

    node_count = db_setup.execute_sql(node_count_sql)
    expected_node_count = [(0,)]

    assert node_count == expected_node_count


def test_database_delete_schema(db_setup):
    """Deletes a test schema."""

    db_setup.add_schema(TEST_SCHEMA)

    db_setup.delete_schema(TEST_SCHEMA)

    selected_schemas = db_setup.get_schemas()
    assert selected_schemas == []


def test_database_delete_edge(db_setup):

    db_setup.add_schema(TEST_SCHEMA)

    node_one = {
        "id": "select-edge-test",
        "body": "selected-body"
    }

    node_two = {
        "id": "select-edge-test-2",
        "body": "selected-body"
    }

    db_setup.add_node(TEST_SCHEMA, node_one)
    db_setup.add_node(TEST_SCHEMA, node_two)

    db_setup.add_edge(TEST_SCHEMA, node_one["id"], node_two["id"])

    db_setup.delete_edge(TEST_SCHEMA, node_one["id"])

    edge_count_sql = """
    SELECT count(*) FROM {schema_name}_edges;
    """.format(schema_name=TEST_SCHEMA)

    edge_count = db_setup.execute_sql(edge_count_sql)
    expected_edge_count = [(0,)]

    assert edge_count == expected_edge_count

