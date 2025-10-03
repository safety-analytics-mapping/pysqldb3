import os

import configparser
import pandas as pd

from .. import pysqldb3 as pysqldb
from . import helpers

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                       server=config.get('PG_DB', 'SERVER'),
                       database=config.get('PG_DB', 'DB_NAME'),
                       user=config.get('PG_DB', 'DB_USER'),
                       password=config.get('PG_DB', 'DB_PASSWORD'))

sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                        server=config.get('SQL_DB', 'SERVER'),
                        database=config.get('SQL_DB', 'DB_NAME'),
                        user=config.get('SQL_DB', 'DB_USER'),
                        password=config.get('SQL_DB', 'DB_PASSWORD'))

table_name = 'test_table_{}'.format(db.user)
pg_schema = 'working'
sql_schema = 'dbo'

def test_column_types_basic_pg():
    db.query(f"drop table if exists {pg_schema}.{table_name}")
    db.query(f"""
        create table {pg_schema}.{table_name} (
            id int,
            name_ varchar(27),
            cit citext,
            dte date,
            geom geometry(MultiPoint,2263)
        )
    """)
    assert db.table_exists(table_name, schema=pg_schema)

    cols = db.get_table_columns(table_name, schema=pg_schema)
    assert  cols == [('id', 'integer'), ('name_', 'character varying (27)'), ('cit', 'citext'), ('dte', 'date'),  ('geom', 'geometry')]

    db.query(f"drop table if exists {pg_schema}.{table_name}")

def test_column_types_basic_funcy_names_pg():
    db.query(f"drop table if exists {pg_schema}.{table_name}")
    db.query(f"""
        create table {pg_schema}.{table_name} (
            "123id" int,
            "nam e_" varchar(27),
            dte date
        )
    """)
    assert db.table_exists(table_name, schema=pg_schema)

    cols = db.get_table_columns(table_name, schema=pg_schema)
    assert  cols == [('123id', 'integer'), ('nam e_', 'character varying (27)'), ('dte', 'date')]

    db.query(f"drop table if exists {pg_schema}.{table_name}")

def test_column_types_array_pg():
    db.query(f"drop table if exists {pg_schema}.{table_name}")
    db.query(f"""
        create table {pg_schema}.{table_name} (
            id int,
            name_ varchar(27),
            list_ varchar[],
            list2_ int[],
            dte date
        )
    """)
    assert db.table_exists(table_name, schema=pg_schema)

    cols = db.get_table_columns(table_name, schema=pg_schema)
    assert  cols == [('id', 'integer'), ('name_', 'character varying (27)'),  ('list_', 'character varying[]'),
                     ('list2_', 'integer[]'), ('dte', 'date')]

    db.query(f"drop table if exists {pg_schema}.{table_name}")


def test_query_and_table_same_result_pg():

    # create table
    db.query(f"drop table if exists {pg_schema}.{table_name}")
    db.query(f"""
        create table {pg_schema}.{table_name} (
            id int,
            name_ varchar(27),
            list_ varchar[],
            list2_ int[],
            dte date);

        insert into {pg_schema}.{table_name} values (1, 'lala', array['a', 'b'], array[54, 77], '2019-01-01');

    """)
    assert db.table_exists(table_name, schema=pg_schema)
    cols = db.get_table_columns(table_name, schema=pg_schema)

    # create query
    query_for_testing = f"select id, name_, list_, list2_, dte from {pg_schema}.{table_name}"
    query_cols = db.get_table_columns(query_for_testing, is_query = True, schema = pg_schema)

    # assert that they're equal
    assert cols == query_cols
    # clean tables
    db.drop_table(schema = pg_schema, table = table_name)

def test_column_types_basic_ms():
    sql.drop_table(sql_schema, table_name)
    sql.query(f"""
        create table {sql_schema}.{table_name} (
            id int,
            name_ varchar(27),
            dte date,
            geom geometry
        )
    """)
    assert sql.table_exists(table_name, schema=sql_schema)

    cols = sql.get_table_columns(table_name, schema=sql_schema)
    assert  cols == [('id', 'int'), ('name_', 'varchar (27)'), ('dte', 'date'), ('geom', 'geometry (-1)')]

    sql.drop_table(sql_schema, table_name)

def test_column_types_basic_funcy_names_ms():
    sql.drop_table(sql_schema, table_name)
    sql.query(f"""
        create table {sql_schema}.{table_name} (
            "123id" int,
            [nam e_] varchar(27),
            dte date
        )
    """)
    assert sql.table_exists(table_name, schema=sql_schema)

    cols = sql.get_table_columns(table_name, schema=sql_schema)
    assert cols == [('123id', 'int'), ('nam e_', 'varchar (27)'), ('dte', 'date')]

    sql.drop_table(sql_schema, table_name)

def test_query_and_table_same_result_ms():

  # create table
    sql.query(f"drop table if exists {sql_schema}.{table_name}")
    sql.query(f"""
         create table {sql_schema}.{table_name} (
            "123id" int,
            [nam e_] varchar(27),
            dte date );

    """)
    assert sql.table_exists(table_name, schema=sql_schema)
    cols = sql.get_table_columns(table_name, schema=sql_schema)

    # create query and assert that it matches the table's columns
    query_for_testing = f'select "123id", dte from {sql_schema}.{table_name}'
    query_cols = sql.get_table_columns(query_for_testing, is_query = True, schema = sql_schema)
    assert [cols[0], cols[2]] == query_cols # assert that all columns but varchar are equal

    # assert that varchar datatype has max limit that we set for undefined
    varchar_query_for_testing = f'select [nam e_] from {sql_schema}.{table_name}'
    query_varchar_cols = sql.get_table_columns(varchar_query_for_testing, is_query = True, schema = sql_schema)
    assert query_varchar_cols == [('nam e_', 'varchar (3000)')]

    # clean tables
    sql.drop_table(schema = sql_schema, table = table_name)

# no array type in sql
# def test_column_types_array_ms():
#     pass

