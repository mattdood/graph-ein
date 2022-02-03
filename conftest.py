import os
import pytest
import shutil

from src.ein.database import Database

TEST_DB = "test.db"
TEST_SCHEMA = "test"

@pytest.fixture()
def db_setup():
    """Creates a workspace DB on startup."""
    return Database(db_path=TEST_DB)


@pytest.fixture(autouse=True)
def cleanup_run():
    """Removes workspaces on teardown."""
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

