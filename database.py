import sqlite3
from prefect import task
from pydantic import BaseModel, Field
from datetime import datetime


@task 
def db_connect() -> sqlite3.Connection:
    return sqlite3.connect('database.db')    


class DbTable(BaseModel):
    # Name of table in db
    __name__ = "Default"

    id: int
    last_updated: datetime = Field(default_factory=datetime.now)

    def insert(self, conn: sqlite3.Connection) -> int:
        pass

    @classmethod
    def create_table(cls, conn: sqlite3.Connection):
        sql_types = {
            str: 'TEXT',
            int: 'INTEGER',
            datetime: 'TEXT'
        }

        columns = [
            'id INTEGER PRIMARY KEY AUTOINCREMENT',
            *[f'{name} {sql_types[type_.annotation]}' for name, type_ in cls.model_fields.items() if name != 'id']
        ]
        columns_sql = ',\n'.join(columns).removesuffix(',\n')

        sql = f"""
            CREATE TABLE IF NOT EXISTS {cls.__name__} (
            {columns_sql}
            )
        """

        print(sql)
        conn.execute(sql)
        conn.commit()





