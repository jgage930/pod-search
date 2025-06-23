import pytest
import sqlite3
from database import DbTable
import os


class UserTable(DbTable):
    __name__ = "users"

    name: str
    password: str


@pytest.fixture
def db_conn():
    db_path = 'tests/test_data.db'
    conn = sqlite3.connect(db_path) 
    UserTable.create_table(conn, 'users')

    yield conn

    os.remove(db_path)


def test_insert(db_conn):
    test_user_id = UserTable(name="Test User", password="supersecure123").insert(db_conn)
    assert test_user_id == 1

