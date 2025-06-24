import sqlite3
from prefect import task
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, Type, TypeVar
from typing_extensions import Self
from pypika import Query, Table


@task 
def db_connect() -> sqlite3.Connection:
    return sqlite3.connect('database.db')    


class DbTable(BaseModel):
    # Name of table in db
    __table_name__ = 'Default'

    id: int = None 
    #last_updated: datetime = Field(default_factory=datetime.now)

    def insert(self, conn: sqlite3.Connection) -> int:
        # Exclude 'id' if it's None (assuming auto-incremented)
        data = self.model_dump(exclude_unset=True)
        if data.get("id") is None:
            data.pop("id", None)

        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        values = list(data.values())

        sql = f"""
            INSERT INTO {self.__name__}
            ({columns})
            VALUES ({placeholders})
        """

        cursor = conn.cursor()
        cursor.execute(sql, values)
        conn.commit()

        return cursor.lastrowid

    @classmethod
    def select(self, conn: sqlite3.Connection, attrs: dict) -> Self:
        table_name = self.__name__

        sql = f"""
            SELECT * 
        """


    @classmethod
    def create_table(cls, conn: sqlite3.Connection, name: str):
        sql_types = {
            str: 'TEXT',
            int: 'INTEGER',
            datetime: 'TEXT',
            Optional[str]: 'Text'
        }

        columns = [
            'id INTEGER PRIMARY KEY AUTOINCREMENT',
            *[f'{name} {sql_types[type_.annotation]}' for name, type_ in cls.model_fields.items() if name != 'id']
        ]
        columns_sql = ',\n'.join(columns).removesuffix(',\n')

        sql = f"""
            CREATE TABLE IF NOT EXISTS {name} (
            {columns_sql}
            )
        """

        print(sql)
        conn.execute(sql)
        conn.commit()


def create_table(conn: sqlite3.Connection, table: Type[DbTable]):
    sql_types = {
        str: 'TEXT',
        int: 'INTEGER',
        datetime: 'TEXT',
        Optional[str]: 'Text'
    }

    columns = [
        'id INTEGER PRIMARY KEY AUTOINCREMENT',
        *[f'{name} {sql_types[type_.annotation]}' for name, type_ in table.model_fields.items() if name != 'id']
    ]
    columns_sql = ',\n'.join(columns).removesuffix(',\n')

    sql = f"""
        CREATE TABLE IF NOT EXISTS {table.__table_name__} (
        {columns_sql}
        )
    """

    conn.execute(sql)
    conn.commit()


def insert(conn: sqlite3.Connection, row: DbTable) -> int:
    data = row.model_dump(exclude_unset=True)
    if data.get("id") is None:
        data.pop("id", None)

    columns = ', '.join(data.keys())
    placeholders = ', '.join(['?'] * len(data))
    values = list(data.values())

    sql = f"""
        INSERT INTO {row.__table_name__}
        ({columns})
        VALUES ({placeholders})
    """

    cursor = conn.cursor()
    cursor.execute(sql, values)
    conn.commit()

    return cursor.lastrowid


def bulk_insert(conn: sqlite3.Connection, rows: list[DbTable]):
    for row in rows:
        insert(conn, row)


def dict_factory(cursor, row):
    colnames = [c[0] for c in cursor.description]
    return dict(zip(colnames, row))

T = TypeVar('T')
def select(conn: sqlite3.Connection, model: Type[T], attrs: dict) -> list[T]:
    table = Table(model.__table_name__)
    q = Query.from_(table).select('*')

    for col, val in attrs.items():
        q = q.where(getattr(table, col) == val)

    conn.row_factory = dict_factory
    cursor = conn.cursor()
    sql = str(q)

    cursor.execute(sql)
    return [model(**row) for row in cursor.fetchall()]
