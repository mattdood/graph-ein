import json
import os

import pytest

from conftest import TEST_DB, TEST_SCHEMA
from src.ein.edge import Edge
from src.ein.graph import Graph
from src.ein.node import Node


def test_graph_init():
    graph = Graph(TEST_DB)

    assert isinstance(graph, Graph)
    assert os.path.exists(TEST_DB)


def test_graph_init_existing_db(graph_setup):
    graph_setup.add_schema(TEST_SCHEMA)
    node_one = Node(
        schema_name=TEST_SCHEMA,
        id="graph-add-edge-test",
        body={"test": "value"},
    )
    node_two = Node(
        schema_name=TEST_SCHEMA,
        id="graph-add-edge-test2",
        body={"test": "value"},
    )
    edge = Edge(
        schema_name=TEST_SCHEMA,
        source=node_one,
        target=node_two,
    )

    graph_setup.add_node(schema_name=node_one.schema_name, node=node_one)
    graph_setup.add_node(schema_name=node_two.schema_name, node=node_two)
    graph_setup.add_edge(edge=edge)

    assert graph_setup.schemas == {TEST_SCHEMA}
    assert len(graph_setup.nodes) == 2
    assert len(graph_setup.edges) == 1

    existing_db_graph = Graph(TEST_DB)
    assert existing_db_graph.schemas == {TEST_SCHEMA}
    assert len(existing_db_graph.nodes) == 2
    assert len(existing_db_graph.edges) == 1


def test_graph_add_schema(db_setup, graph_setup):
    graph_setup.add_schema(TEST_SCHEMA)

    check_schema_tables_sql = f"""
    SELECT
        name
    FROM
        sqlite_master
    WHERE
        type = 'table'
        AND
        tbl_name LIKE '{TEST_SCHEMA}%';
    """

    check_schema_indexes_sql = f"""
    SELECT
        name
    FROM
        sqlite_master
    WHERE
        type = 'index'
        AND
        tbl_name LIKE '{TEST_SCHEMA}%';
    """

    schema_tables = db_setup.execute_sql(check_schema_tables_sql)
    schema_indexes = db_setup.execute_sql(check_schema_indexes_sql)

    expected_schema_tables = [("test_nodes",), ("test_edges",)]
    expected_indexes = [("sqlite_autoindex_test_nodes_1",), ("idx_id",), ("sqlite_autoindex_test_edges_1",), ("idx_source_target",)]

    assert schema_tables == expected_schema_tables
    assert schema_indexes == expected_indexes
    assert graph_setup.schemas == {TEST_SCHEMA}


def test_graph_add_node(db_setup_row_factory, graph_setup_row_factory):
    graph_setup_row_factory.add_schema(TEST_SCHEMA)

    node = Node(
        schema_name=TEST_SCHEMA,
        id="graph-add-node-test",
        body={"test": "value"},
    )
    graph_setup_row_factory.add_node(schema_name=node.schema_name, node=node)

    expected = db_setup_row_factory.get_node(schema_name=TEST_SCHEMA, node_id=node.id)

    assert node.id == expected["id"]
    assert node.body == json.loads(expected["body"])


def test_graph_add_nodes(db_setup_row_factory, graph_setup_row_factory):
    graph_setup_row_factory.add_schema(TEST_SCHEMA)

    node_one = Node(
        schema_name=TEST_SCHEMA,
        id="graph-add-nodes-test1",
        body={"test": "value"},
    )
    node_two = Node(
        schema_name=TEST_SCHEMA,
        id="graph-add-nodes-test2",
        body={"test": "value"},
    )
    graph_setup_row_factory.add_nodes(
        schema_name=TEST_SCHEMA,
        nodes=[node_one, node_two]
    )

    expected = db_setup_row_factory.get_all_nodes(schema_name=TEST_SCHEMA)

    assert len(expected) == 2
    assert node_one.id == expected[0]["id"]
    assert node_one.body == json.loads(expected[0]["body"])
    assert node_two.id == expected[1]["id"]
    assert node_two.body == json.loads(expected[1]["body"])


def test_graph_add_edge(db_setup_row_factory, graph_setup_row_factory):
    graph_setup_row_factory.add_schema(TEST_SCHEMA)

    node_one = Node(
        schema_name=TEST_SCHEMA,
        id="graph-add-edge-test",
        body={"test": "value"},
    )
    node_two = Node(
        schema_name=TEST_SCHEMA,
        id="graph-add-edge-test2",
        body={"test": "value"},
    )
    graph_setup_row_factory.add_node(schema_name=node_one.schema_name, node=node_one)
    graph_setup_row_factory.add_node(schema_name=node_two.schema_name, node=node_two)

    edge = Edge(
        schema_name=TEST_SCHEMA,
        source=node_one,
        target=node_two,
    )
    graph_setup_row_factory.add_edge(edge=edge)

    expected = db_setup_row_factory.get_edge(schema_name=TEST_SCHEMA, source_id=edge.source.id, target_id=edge.target.id)
    assert edge.source.id == expected["source"]
    assert edge.target.id == expected["target"]


def test_graph_add_edge(db_setup_row_factory, graph_setup_row_factory):
    graph_setup_row_factory.add_schema(TEST_SCHEMA)

    node_one = Node(
        schema_name=TEST_SCHEMA,
        id="graph-add-edges-test",
        body={"test": "value"},
    )
    node_two = Node(
        schema_name=TEST_SCHEMA,
        id="graph-add-edges-test2",
        body={"test": "value"},
    )
    node_three = Node(
        schema_name=TEST_SCHEMA,
        id="graph-add-edges-test3",
        body={"test": "value"},
    )
    graph_setup_row_factory.add_node(schema_name=node_one.schema_name, node=node_one)
    graph_setup_row_factory.add_node(schema_name=node_two.schema_name, node=node_two)
    graph_setup_row_factory.add_node(schema_name=node_three.schema_name, node=node_three)

    edge_one = Edge(
        schema_name=TEST_SCHEMA,
        source=node_one,
        target=node_two,
    )
    edge_two = Edge(
        schema_name=TEST_SCHEMA,
        source=node_two,
        target=node_three,
    )
    graph_setup_row_factory.add_edges(schema_name=TEST_SCHEMA, edges=[edge_one, edge_two])

    expected = db_setup_row_factory.get_all_edges(schema_name=TEST_SCHEMA)
    assert edge_one.source.id == expected[0]["source"]
    assert edge_one.target.id == expected[0]["target"]
    assert edge_two.source.id == expected[1]["source"]
    assert edge_two.target.id == expected[1]["target"]


def test_graph_add_multi_schema_edge(db_setup_row_factory, graph_setup_row_factory):
    schema_one = "test1"
    schema_two = "test2"

    graph_setup_row_factory.add_schema(schema_one)
    graph_setup_row_factory.add_schema(schema_two)

    node_one = Node(
        schema_name=schema_one,
        id="graph-add-multi-schema-edge-test",
        body={"test": "value"},
    )
    node_two = Node(
        schema_name=schema_two,
        id="graph-add-multi-schema-edge-test2",
        body={"test": "value"},
    )
    graph_setup_row_factory.add_node(schema_name=node_one.schema_name, node=node_one)
    graph_setup_row_factory.add_node(schema_name=node_two.schema_name, node=node_two)

    edge = Edge(
        schema_name=schema_one,
        source=node_one,
        target=node_two,
    )
    graph_setup_row_factory.add_edge(edge=edge)

    expected = db_setup_row_factory.get_edge(
        schema_name=schema_one,
        source_id=edge.source.id,
        target_id=edge.target.id,
    )
    assert edge.schema_name == schema_one
    assert edge.source.id == expected["source"]
    assert edge.source_schema_name == expected["source_schema"]
    assert edge.source.schema_name == expected["source_schema"]
    assert edge.target.id == expected["target"]
    assert edge.target_schema_name == expected["target_schema"]
    assert edge.target.schema_name == expected["target_schema"]


def test_graph_update_node(db_setup_row_factory, graph_setup_row_factory):
    graph_setup_row_factory.add_schema(TEST_SCHEMA)

    node = Node(
        schema_name=TEST_SCHEMA,
        id="graph-update-node-test",
        body={"test": "value"},
    )
    graph_setup_row_factory.add_node(schema_name=node.schema_name, node=node)

    node.body = {"test-something": "value"}
    graph_setup_row_factory.update_node(node=node)

    expected = db_setup_row_factory.get_node(schema_name=node.schema_name, node_id=node.id)
    assert node.id == expected["id"]
    assert node.body == json.loads(expected["body"])


def test_graph_update_edge(db_setup_row_factory, graph_setup_row_factory):
    graph_setup_row_factory.add_schema(TEST_SCHEMA)

    node_one = Node(
        schema_name=TEST_SCHEMA,
        id="graph-update-edge-test",
        body={"test": "value"},
    )
    node_two = Node(
        schema_name=TEST_SCHEMA,
        id="graph-update-edge-test2",
        body={"test": "value"},
    )
    graph_setup_row_factory.add_node(schema_name=node_one.schema_name, node=node_one)
    graph_setup_row_factory.add_node(schema_name=node_two.schema_name, node=node_two)

    edge = Edge(
        schema_name=TEST_SCHEMA,
        source=node_one,
        target=node_two,
    )
    graph_setup_row_factory.add_edge(edge=edge)

    edge.properties = {"test-property": "some-property"}
    graph_setup_row_factory.update_edge(edge=edge)

    expected = db_setup_row_factory.get_edge(schema_name=TEST_SCHEMA, source_id=edge.source.id, target_id=edge.target.id)
    assert edge.source.id == expected["source"]
    assert edge.target.id == expected["target"]
    assert edge.properties == json.loads(expected["properties"])


def test_graph_delete_schema(db_setup, graph_setup):
    graph_setup.add_schema(TEST_SCHEMA)

    check_schema_tables_sql = f"""
    SELECT
        name
    FROM
        sqlite_master
    WHERE
        type = 'table'
        AND
        tbl_name LIKE '{TEST_SCHEMA}%';
    """

    graph_setup.delete_schema(schema_name=TEST_SCHEMA)

    schema_tables = db_setup.execute_sql(check_schema_tables_sql)
    expected_schema_tables = []

    assert schema_tables == expected_schema_tables
    assert len(graph_setup.schemas) == 0


def test_graph_delete_node(db_setup_row_factory, graph_setup_row_factory):
    graph_setup_row_factory.add_schema(TEST_SCHEMA)

    node = Node(
        schema_name=TEST_SCHEMA,
        id="graph-delete-node-test",
        body={"test": "value"},
    )
    graph_setup_row_factory.add_node(schema_name=node.schema_name, node=node)
    graph_setup_row_factory.delete_node(node=node)
    assert graph_setup_row_factory.nodes == {}

    expected = db_setup_row_factory.get_node(schema_name=node.schema_name, node_id=node.id)
    assert expected is None


def test_graph_delete_edge(db_setup_row_factory, graph_setup_row_factory):
    graph_setup_row_factory.add_schema(TEST_SCHEMA)

    node_one = Node(
        schema_name=TEST_SCHEMA,
        id="graph-delete-edge-test",
        body={"test": "value"},
    )
    node_two = Node(
        schema_name=TEST_SCHEMA,
        id="graph-delete-edge-test2",
        body={"test": "value"},
    )
    graph_setup_row_factory.add_node(schema_name=node_one.schema_name, node=node_one)
    graph_setup_row_factory.add_node(schema_name=node_two.schema_name, node=node_two)

    edge = Edge(
        schema_name=TEST_SCHEMA,
        source=node_one,
        target=node_two,
    )
    graph_setup_row_factory.add_edge(edge=edge)
    assert len(graph_setup_row_factory.edges) == 1

    graph_setup_row_factory.delete_edge(edge=edge)
    assert len(graph_setup_row_factory.edges) == 0

