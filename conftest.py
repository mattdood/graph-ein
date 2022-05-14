import os
import pytest
import shutil

from src.ein.database import Database

TEST_DB = "test.db"
TEST_SCHEMA = "test"

@pytest.fixture()
def db_setup():
    """Creates a workspace DB on startup.

    We don't want to return `sqlite3.Row` objects
    because that's difficult to check values in, so
    we return `Tuple` objects instead for hard comparisons.
    """
    return Database(db_path=TEST_DB, row_factory=False)


@pytest.fixture(autouse=True)
def cleanup_run():
    """Removes workspaces on teardown."""
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

