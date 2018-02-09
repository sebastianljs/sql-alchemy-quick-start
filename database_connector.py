import sqlalchemy
from pprint import pprint
from sqlalchemy import Table, Column, Integer, String, ForeignKey


def connect_to_postgres_db(user: str, password: str, db: str, host: str="localhost", port: int=5432) -> tuple:
    url_str_kwargs = {
        "user": user,
        "password": password,
        "db": db,
        "host": host,
        "port": port
    }
    url = "postgresql://{user}:{password}@{host}:{port}/{db}".format(**url_str_kwargs)
    connection = sqlalchemy.create_engine(url, client_encoding="utf-8")
    meta = sqlalchemy.MetaData(bind=connection, reflect=True)

    return connection, meta


def create_slams_table(meta):
    Table('slams', meta,
          Column('name', String, primary_key=True),
          Column('country', String))


def create_results_table(meta):
    Table('results', meta,
          Column('slam', String, ForeignKey('slams.name')),
          Column('year', Integer),
          Column('result', String))


if __name__ == "__main__":
    tennis_db_connection, tennis_db_meta = connect_to_postgres_db('federer', 'grandslam', 'tennis')
    pprint(tennis_db_connection)
    pprint(tennis_db_meta)
    create_slams_table(tennis_db_meta)
    create_results_table(tennis_db_meta)
    tennis_db_meta.create_all(tennis_db_connection)
