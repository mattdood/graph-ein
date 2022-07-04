import json
import sqlite3

import pytest

from conftest import TEST_DB, TEST_SCHEMA
from src.ein.database import (Database, DisallowedOperatorError,
                              IncompleteStatementError)


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

    sql_text = db_setup._read_sql_file("insert-node.sql", TEST_SCHEMA)

    expected_sql_text = f"""
    INSERT INTO {TEST_SCHEMA}_nodes
    (
        id,
        body
    )
    VALUES (
        ?,
        json(?)
    )
    ;
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
    expected_indexes = [("sqlite_autoindex_test_nodes_1",), ("idx_id",), ("sqlite_autoindex_test_edges_1",), ("idx_source_target",)]

    assert schema_tables == expected_schema_tables
    assert schema_indexes == expected_indexes


def test_database_add_nodes(db_setup):
    """Add nodes to a new schema and retrieve it."""

    db_setup.add_schema(TEST_SCHEMA)

    node_one = {
        "id": "add-nodes-test1",
        "body": {
            "id": "add-nodes-test1",
            "other-data": [
                "string-one",
                "string-two"
            ]
        }
    }
    node_two = {
        "id": "add-nodes-test2",
        "body": {
            "id": "add-nodes-test2",
            "other-data": [
                "string-one",
                "string-two"
            ]
        }
    }

    db_setup.add_nodes(
        TEST_SCHEMA,
        [
            (node_one["id"], json.dumps(node_one["body"])),
            (node_two["id"], json.dumps(node_two["body"])),
        ],
    )

    check_node_sql = """
    SELECT * FROM {schema_name}_nodes;
    """.format(schema_name=TEST_SCHEMA)

    nodes_data = db_setup.execute_sql(check_node_sql)

    expected_nodes_data = [
        (
            node_one["id"],
            json.dumps(node_one["body"]).replace(" ", ""),
        ),
        (
            node_two["id"],
            json.dumps(node_two["body"]).replace(" ", ""),
        ),
    ]

    assert nodes_data == expected_nodes_data


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

    db_setup.add_node(TEST_SCHEMA, node["id"], node)

    check_node_sql = """
    SELECT * FROM {schema_name}_nodes;
    """.format(schema_name=TEST_SCHEMA)

    nodes_data = db_setup.execute_sql(check_node_sql)

    expected_nodes_data = [('add-node-test', '{"id":"add-node-test","body":{"id":"add-node-test","other-data":["string-one","string-two"]}}')]

    assert nodes_data == expected_nodes_data


def test_database_update_node(db_setup):
    """Update a node in a schema and retrieve it."""

    db_setup.add_schema(TEST_SCHEMA)

    node_one = {
        "id": "update-edge-test",
        "body": {
            "id": "add-edge-test",
            "other-data": [
                "string-one",
                "string-two"
            ]
        }
    }

    updated_body = {
        "id": "add-edge-test",
        "some-other-data": [
            "string-three",
            "string-four"
        ]
    }

    db_setup.add_node(TEST_SCHEMA, node_one["id"], node_one)

    db_setup.update_node(TEST_SCHEMA, node_one["id"], updated_body)

    check_edge_sql = """
    SELECT * FROM {schema_name}_nodes;
    """.format(schema_name=TEST_SCHEMA)

    node_data = db_setup.execute_sql(check_edge_sql)

    expected_node_data = [(node_one["id"], json.dumps(updated_body).replace(" ", ""))]

    assert node_data == expected_node_data


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

    db_setup.add_node(TEST_SCHEMA, node_one["id"], node_one)
    db_setup.add_node(TEST_SCHEMA, node_two["id"], node_two)

    db_setup.add_edge(TEST_SCHEMA, node_one["id"], node_two["id"])

    check_edge_sql = """
    SELECT * FROM {schema_name}_edges;
    """.format(schema_name=TEST_SCHEMA)

    edges_data = db_setup.execute_sql(check_edge_sql)

    expected_edges_data = [
        (
            node_one["id"],
            TEST_SCHEMA,
            node_two["id"],
            TEST_SCHEMA,
            'null'
        )
    ]

    assert edges_data == expected_edges_data

    # Test unique index on FK constraints
    with pytest.raises(Exception) as e:  # noqa F841
        db_setup.add_edge(TEST_SCHEMA, node_one["id"], node_two["id"])


def test_database_add_edges(db_setup):
    """Add edges to a new schema and retrieve it."""

    db_setup.add_schema(TEST_SCHEMA)

    node_one = {
        "id": "add-edges-test",
        "body": {
            "id": "add-edges-test",
            "other-data": [
                "string-one",
                "string-two"
            ]
        }
    }
    node_two = {
        "id": "add-edges-test2",
        "body": {
            "id": "add-edges-test2",
            "other-data": [
                "string-one",
                "string-two"
            ]
        }
    }
    node_three = {
        "id": "add-edges-test3",
        "body": {
            "id": "add-edges-test3",
            "other-data": [
                "string-one",
                "string-two"
            ]
        }
    }

    db_setup.add_node(TEST_SCHEMA, node_one["id"], node_one)
    db_setup.add_node(TEST_SCHEMA, node_two["id"], node_two)
    db_setup.add_node(TEST_SCHEMA, node_three["id"], node_three)

    db_setup.add_edges(
        TEST_SCHEMA,
        [
            (node_one["id"], TEST_SCHEMA, node_two["id"], TEST_SCHEMA, json.dumps({})),
            (node_two["id"], TEST_SCHEMA, node_three["id"], TEST_SCHEMA, json.dumps({})),
        ],
    )

    check_edge_sql = """
    SELECT * FROM {schema_name}_edges;
    """.format(schema_name=TEST_SCHEMA)

    edges_data = db_setup.execute_sql(check_edge_sql)

    expected_edges_data = [
        (
            node_one["id"],
            TEST_SCHEMA,
            node_two["id"],
            TEST_SCHEMA,
            '{}'
        ),
        (
            node_two["id"],
            TEST_SCHEMA,
            node_three["id"],
            TEST_SCHEMA,
            '{}'
        )
    ]

    assert edges_data == expected_edges_data

    # Test unique index on FK constraints
    with pytest.raises(Exception) as e:  # noqa F841
        db_setup.add_edge(TEST_SCHEMA, node_one["id"], node_two["id"])


def test_database_add_multi_schema_edge(db_setup):
    """Add an edge that has nodes in 2 places."""
    schema_one = "test1"
    schema_two = "test2"

    db_setup.add_schema(schema_one)
    db_setup.add_schema(schema_two)

    node_one = {
        "id": "add-multi-schema-edge-test",
        "body": {
            "other-data": [
                "string-one",
                "string-two"
            ]
        }
    }
    node_two = {
        "id": "add-multi-schema-edge-test2",
        "body": {
            "other-data": [
                "string-one",
                "string-two"
            ]
        }
    }

    db_setup.add_node(schema_one, node_one["id"], node_one)
    db_setup.add_node(schema_two, node_two["id"], node_two)

    db_setup.add_edge(
        schema_name=schema_one,
        source_id=node_one["id"],
        source_schema_name=schema_one,
        target_id=node_two["id"],
        target_schema_name=schema_two,
    )

    check_edge_sql = """
    SELECT * FROM {schema_name}_edges;
    """.format(schema_name=schema_one)

    edges_data = db_setup.execute_sql(check_edge_sql)

    expected_edges_data = [
        (
            node_one["id"],
            schema_one,
            node_two["id"],
            schema_two,
            'null'
        )
    ]

    assert edges_data == expected_edges_data


def test_database_update_edge(db_setup):
    """Update an edge in a schema and retrieve it."""

    db_setup.add_schema(TEST_SCHEMA)

    node_one = {
        "id": "update-edge-test",
        "body": {
            "id": "update-edge-test",
            "other-data": [
                "string-one",
                "string-two"
            ]
        }
    }
    node_two = {
        "id": "update-edge-test2",
        "body": {
            "id": "update-edge-test2",
            "other-data": [
                "string-one",
                "string-two"
            ]
        }
    }

    property = {
        "find-me": "something"
    }

    db_setup.add_node(TEST_SCHEMA, node_one["id"], node_one)
    db_setup.add_node(TEST_SCHEMA, node_two["id"], node_two)

    db_setup.add_edge(TEST_SCHEMA, node_one["id"], node_two["id"])

    db_setup.update_edge(TEST_SCHEMA, node_one["id"], node_two["id"], property)

    check_edge_sql = """
    SELECT * FROM {schema_name}_edges;
    """.format(schema_name=TEST_SCHEMA)

    edges_data = db_setup.execute_sql(check_edge_sql)

    expected_edges_data = [
        (
            node_one["id"],
            TEST_SCHEMA,
            node_two["id"],
            TEST_SCHEMA,
            json.dumps(property).replace(" ", "")
        )
    ]

    assert edges_data == expected_edges_data


def test_database_update_schema(db_setup):
    """Update a schema in a database and retrieve it."""

    db_setup.add_schema(TEST_SCHEMA)

    db_setup.update_schema(TEST_SCHEMA, "new_name")

    check_schema_tables_sql = """
    SELECT
        name
    FROM
        sqlite_master
    WHERE
        type = 'table'
        AND
        tbl_name LIKE 'new_name%';
    """

    schema_data = db_setup.execute_sql(check_schema_tables_sql)

    expected_schema_tables = [("new_name_nodes",), ("new_name_edges",)]

    assert schema_data == expected_schema_tables


def test_database_get_node(db_setup):
    """Selects node based on id."""

    db_setup.add_schema(TEST_SCHEMA)

    node_one = {
        "id": "select-node-test",
        "body": "selected-body"
    }

    db_setup.add_node(TEST_SCHEMA, node_one["id"], node_one)

    results_body = db_setup.get_node(
        TEST_SCHEMA,
        node_id=node_one["id"]
    )

    assert "select-node-test" in str(results_body)
    # tuple of 2 columns
    assert len(results_body) == 2


def test_database_get_all_nodes(db_setup):
    """Selects all nodes in a schema."""

    db_setup.add_schema(TEST_SCHEMA)

    node_one = {
        "id": "select-all-nodes-test",
        "body": "selected-body"
    }

    db_setup.add_node(TEST_SCHEMA, node_one["id"], node_one)

    results_body = db_setup.get_all_nodes(TEST_SCHEMA)

    assert "select-all-nodes-test" in str(results_body)
    # tuple of 2 columns
    assert len(results_body) == 1


def test_database_get_nodes(db_setup):
    """Selects nodes based on params."""

    db_setup.add_schema(TEST_SCHEMA)

    node_one = {
        "id": "select-nodes-test",
        "body": "selected-body"
    }

    node_two = {
        "id": "select-nodes-test-2",
        "body": "selected-body"
    }

    db_setup.add_node(TEST_SCHEMA, node_one["id"], node_one)
    db_setup.add_node(TEST_SCHEMA, node_two["id"], node_two)

    params_body = {
        "body": "selected-body"
    }

    results_body = db_setup.get_nodes(TEST_SCHEMA, node_body=params_body)

    assert "selected-body" in str(results_body)
    assert len(results_body) == 2

    params_id_body = {
        "body": "selected-body"
    }

    results_id_body = db_setup.get_nodes(
        TEST_SCHEMA,
        node_id="select-node-test-2",
        node_body=params_id_body,
    )

    assert "select-nodes-test-2" in str(results_id_body)
    assert len(results_id_body) == 2

    params_operator = {
        "body": "selected-body"
    }

    results_operator = db_setup.get_nodes(
        TEST_SCHEMA,
        node_id="select-nodes-test-2",
        node_body=params_operator,
        operator="and",
    )

    assert "select-nodes-test-2" in str(results_operator)
    assert len(results_operator) == 1


def test_database_get_nodes_errors(db_setup):
    db_setup.add_schema(TEST_SCHEMA)

    node_one = {
        "id": "select-node-test",
        "body": "selected-body"
    }

    node_two = {
        "id": "select-node-test-2",
        "body": "selected-body"
    }

    db_setup.add_node(TEST_SCHEMA, node_one["id"], node_one)
    db_setup.add_node(TEST_SCHEMA, node_two["id"], node_two)

    params_operator = {
        "body": "selected-body"
    }

    with pytest.raises(DisallowedOperatorError) as e:  # noqa F841

        db_setup.get_nodes(
            TEST_SCHEMA,
            node_id="select-node-test-2",
            node_body=params_operator,
            operator="something-wrong",
        )


def test_database_get_edge(db_setup):
    """Queries for edge with a source ID and target ID."""

    db_setup.add_schema(TEST_SCHEMA)

    node_one = {
        "id": "select-edge-test",
        "body": "selected-body"
    }

    node_two = {
        "id": "select-edge-test-2",
        "body": "selected-body"
    }

    db_setup.add_node(TEST_SCHEMA, node_one["id"], node_one)
    db_setup.add_node(TEST_SCHEMA, node_two["id"], node_two)

    db_setup.add_edge(TEST_SCHEMA, node_one["id"], node_two["id"])

    selected_edge = db_setup.get_edge(
        TEST_SCHEMA,
        node_one["id"],
        node_two["id"]
    )
    expected_edge = (node_one["id"], TEST_SCHEMA, node_two["id"], TEST_SCHEMA, 'null')

    assert selected_edge == expected_edge


def test_database_get_all_edges(db_setup):
    """Queries for all edges."""

    db_setup.add_schema(TEST_SCHEMA)

    node_one = {
        "id": "select-all-edges-test",
        "body": "selected-body"
    }

    node_two = {
        "id": "select-all-edges-test-2",
        "body": "selected-body"
    }

    db_setup.add_node(TEST_SCHEMA, node_one["id"], node_one)
    db_setup.add_node(TEST_SCHEMA, node_two["id"], node_two)

    db_setup.add_edge(TEST_SCHEMA, node_one["id"], node_two["id"])

    selected_edges = db_setup.get_all_edges(TEST_SCHEMA)
    expected_edges = [
        (
            node_one["id"],
            TEST_SCHEMA,
            node_two["id"],
            TEST_SCHEMA,
            'null'
        )
    ]

    assert selected_edges == expected_edges


def test_database_get_edges(db_setup):
    """Queries for edges."""

    db_setup.add_schema(TEST_SCHEMA)

    node_one = {
        "id": "select-edges-test",
        "body": "selected-body"
    }

    node_two = {
        "id": "select-edges-test-2",
        "body": "selected-body"
    }

    db_setup.add_node(TEST_SCHEMA, node_one["id"], node_one)
    db_setup.add_node(TEST_SCHEMA, node_two["id"], node_two)

    db_setup.add_edge(TEST_SCHEMA, node_one["id"], node_two["id"])

    selected_edges = db_setup.get_edges(
        TEST_SCHEMA,
        node_one["id"],
        node_two["id"]
    )
    expected_edges = [
        (
            node_one["id"],
            TEST_SCHEMA,
            node_two["id"],
            TEST_SCHEMA,
            'null'
        )
    ]

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
        "id": "delete-node-test",
        "body": "selected-body"
    }

    db_setup.add_node(TEST_SCHEMA, node_one["id"], node_one)

    db_setup.delete_node(TEST_SCHEMA, node_one["id"])

    node_count_sql = """
    SELECT count(*) FROM {schema_name}_nodes;
    """.format(schema_name=TEST_SCHEMA)

    node_count = db_setup.execute_sql(node_count_sql)
    expected_node_count = [(0,)]

    assert node_count == expected_node_count


def test_database_delete_nodes(db_setup):
    """Deletes a test node."""

    db_setup.add_schema(TEST_SCHEMA)

    node_one = {
        "id": "delete-nodes-test",
        "body": "selected-body"
    }

    node_two = {
        "id": "delete-nodes-test-2",
        "body": "selected-body"
    }

    db_setup.add_node(TEST_SCHEMA, node_one["id"], node_one)
    db_setup.add_node(TEST_SCHEMA, node_two["id"], node_two)

    db_setup.delete_nodes(TEST_SCHEMA, [node_one["id"], node_two["id"]])

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
        "id": "delete-edge-test",
        "body": "selected-body"
    }

    node_two = {
        "id": "delete-edge-test-2",
        "body": "selected-body"
    }

    db_setup.add_node(TEST_SCHEMA, node_one["id"], node_one)
    db_setup.add_node(TEST_SCHEMA, node_two["id"], node_two)

    db_setup.add_edge(TEST_SCHEMA, node_one["id"], node_two["id"])

    db_setup.delete_edge(TEST_SCHEMA, node_one["id"], node_two["id"])

    edge_count_sql = """
    SELECT count(*) FROM {schema_name}_edges;
    """.format(schema_name=TEST_SCHEMA)

    edge_count = db_setup.execute_sql(edge_count_sql)
    expected_edge_count = [(0,)]

    assert edge_count == expected_edge_count


def test_database_delete_edges(db_setup):

    db_setup.add_schema(TEST_SCHEMA)

    node_one = {
        "id": "delete-edges-test",
        "body": "selected-body"
    }

    node_two = {
        "id": "delete-edges-test-2",
        "body": "selected-body"
    }

    db_setup.add_node(TEST_SCHEMA, node_one["id"], node_one)
    db_setup.add_node(TEST_SCHEMA, node_two["id"], node_two)

    db_setup.add_edge(TEST_SCHEMA, node_one["id"], node_two["id"])

    db_setup.delete_edges(TEST_SCHEMA, node_one["id"])

    edge_count_sql = """
    SELECT count(*) FROM {schema_name}_edges;
    """.format(schema_name=TEST_SCHEMA)

    edge_count = db_setup.execute_sql(edge_count_sql)
    expected_edge_count = [(0,)]

    assert edge_count == expected_edge_count

