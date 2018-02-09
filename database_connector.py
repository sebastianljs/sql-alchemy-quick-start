import sqlalchemy
from pprint import pprint
from sqlalchemy import Table, Column, Integer, String, ForeignKey
from sqlalchemy.exc import InvalidRequestError


def connect_to_postgres_db(user: str, password: str,
                           db: str, host: str="localhost",
                           port: int=5432) -> (sqlalchemy.engine.Engine, sqlalchemy.MetaData):
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


def create_slams_table(meta: sqlalchemy.MetaData) -> sqlalchemy.Table:
    """
    Create slams table
    @param meta: Metadata containing all schemas and constructs
    @type meta: sqlalchemy.MetaData
    @return: Slams table
    @rtype: sqlalchemy.Table
    """
    table = Table('slams', meta,
                  Column('name', String, primary_key=True),
                  Column('country', String))
    return table


def create_results_table(meta: sqlalchemy.MetaData) -> sqlalchemy.Table:
    """
    Create results table
    @param meta: Metadata containing all schemas and constructs
    @type meta: sqlalchemy.MetaData
    @return: Results table
    @rtype: sqlalchemy.Table
    """
    table = Table('results', meta,
                  Column('slam', String, ForeignKey('slams.name')),
                  Column('year', Integer),
                  Column('result', String))
    return table


def insert_into_table(table: sqlalchemy.Table,
                      db_connection: sqlalchemy.engine.Engine,
                      key_value_dict: dict):
    assert isinstance(table, sqlalchemy.Table)
    insert_clause = table.insert().values(**key_value_dict)
    db_connection.execute(insert_clause)


if __name__ == "__main__":
    tennis_db_connection, tennis_db_meta = connect_to_postgres_db('federer', 'grandslam', 'tennis')
    pprint(tennis_db_connection)
    pprint(tennis_db_meta)
    try:
        create_slams_table(tennis_db_meta)
        create_results_table(tennis_db_meta)
        tennis_db_meta.create_all(tennis_db_connection)
    except InvalidRequestError:
        pass
    slams_table = tennis_db_meta.tables["slams"]
    results_table = tennis_db_meta.tables["results"]
    pprint(slams_table)
    pprint(results_table)
    insert_into_table(slams_table, tennis_db_connection, {"name": "Wimbledon", "country": "United Kingdom"})
