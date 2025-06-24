import pytest
import sqlite3
import database as db
import os


class UserTable(db.DbTable):
    __table_name__ = "users"

    name: str
    password: str


@pytest.fixture
def db_conn():
    db_path = 'tests/test_data.db'
    conn = sqlite3.connect(db_path) 
    db.create_table(conn, UserTable)

    yield conn

    os.remove(db_path)


def test_insert(db_conn):
    test_user = UserTable(name="Test User", password="supersecure123")
    test_id = db.insert(db_conn, test_user)
    assert test_id == 1


def test_bulk_insert(db_conn):
    test_users = [
        UserTable(name="Test User", password="supersecure123"),
        UserTable(name="Test User 1", password="supersecure123"),
        UserTable(name="Test User 2", password="supersecure123"),
        UserTable(name="Test User 3", password="test123"),
        UserTable(name="Test User 4", password="123"),
    ]
    db.bulk_insert(db_conn, test_users)


def test_select(db_conn):
    test_user = UserTable(name="Test User 4", password="123")
    id = db.insert(db_conn, test_user)

    rows = db.select(db_conn, UserTable, {'name': 'Test User 4'})
    assert rows == [UserTable(id=1, name="Test User 4", password="123")]
