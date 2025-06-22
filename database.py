import sqlite3
from prefect import task
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional


@task 
def db_connect() -> sqlite3.Connection:
    return sqlite3.connect('database.db')    


class DbTable(BaseModel):
    # Name of table in db

    id: int = None 
    last_updated: datetime = Field(default_factory=datetime.now)

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

        return cursor.lastrowid()

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





