import os

import pytest

from src.ein.database import Database
from src.ein.graph import Graph

TEST_DB = "test.db"
TEST_SCHEMA = "test"


@pytest.fixture()
def db_setup():
    """Creates a workspace DB on use.

    We don't want to return `sqlite3.Row` objects
    because that's difficult to check values in, so
    we return `Tuple` objects instead for hard comparisons.
    """
    return Database(db_path=TEST_DB, row_factory=False)


@pytest.fixture()
def graph_setup(db_setup):
    """A `Graph` instance with no row factory."""
    return Graph(db_path=TEST_DB)


@pytest.fixture()
def db_setup_row_factory():
    """Creates a workspace DB on use.

    We want to return `sqlite3.Row` objects for
    `Graph`, `Node`, and `Edge` tests.
    """
    return Database(db_path=TEST_DB, row_factory=True)


@pytest.fixture()
def graph_setup_row_factory(db_setup_row_factory):
    """A `Graph` instance with a row factory."""
    return Graph(db_path=TEST_DB)


@pytest.fixture(autouse=True)
def cleanup_run():
    """Removes workspaces on teardown."""
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

