import os
import csv
import configparser
import pandas as pd

from .. import pysqldb3 as pysqldb
from .. import data_io
from .. import util as util
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



table_name = f'_tables_created_testing_{db.user}_'
pg_schema = 'working'
sql_schema = 'dbo'

def data_set_up():
    # small csv with empty column headers
    data = [
        ["id", "col1", 'col2'],
        [1, 2, None],
        [2, 3, 9,],
        [35, 36, 456]
    ]
    with open(helpers.DIR + "\\_test.csv", 'w', newline='') as csvfile:
        w = csv.writer(csvfile, delimiter=',')
        for row in data:
            w.writerow(row)

    for i in range(100000):
        data.append([35 + i, 3, 999])
        i += 1

    with open(helpers.DIR + "\\_test_bulk.csv", 'w', newline='') as csvfile:
        w = csv.writer(csvfile, delimiter=',')
        for row in data:
            w.writerow(row)

def cleanup_test_files():
    os.remove(helpers.DIR + "\\_test.csv")
    os.remove(helpers.DIR + "\\_test_bulk.csv")

class TestTablesCreatedPG:
    @classmethod
    def setup_class(cls):
        # helpers.set_up_test_table_pg(db)
        data_set_up()
        helpers.set_up_shapefile()

    def test_query_to_table(self):
        db.drop_table(pg_schema, table_name)
        db.query(f"""
            create table {pg_schema}.{table_name} as 
            select 1 as id_, 2 as val; 
        """)
        assert db.tables_created == [(db.server, db.database, pg_schema, table_name)]
        db.drop_table(pg_schema, table_name)
        assert not db.tables_created == [(db.server, db.database, pg_schema, table_name)]

    def test_csv_to_table(self):
        db.drop_table(pg_schema, table_name)
        db.csv_to_table(input_file=helpers.DIR + "\\_test.csv", schema=pg_schema, table=table_name)
        assert db.table_exists(table_name, schema=pg_schema)
        assert db.tables_created == [(db.server, db.database, pg_schema, table_name)]
        db.drop_table(pg_schema, table_name)

    def test_bulk_csv_to_table(self):
        db.drop_table(pg_schema, table_name)
        db.csv_to_table(input_file=helpers.DIR + "\\_test_bulk.csv", schema=pg_schema, table=table_name)
        assert db.table_exists(table_name, schema=pg_schema)
        assert db.tables_created == [(db.server, db.database, pg_schema, table_name)]
        db.drop_table(pg_schema, table_name)

    def test_shp_to_table(self):
        db.drop_table(pg_schema, table_name)
        db.shp_to_table(path=helpers.DIR + "\\test.shp", schema=pg_schema, table=table_name)
        assert db.table_exists(table_name, schema=pg_schema)
        assert db.tables_created == [(db.server, db.database, pg_schema, table_name)]
        db.drop_table(pg_schema, table_name)

    def test_shp_from_zip_to_table(self):
        db.drop_table(pg_schema, table_name)
        db.shp_to_table(path=helpers.DIR + "\\test.zip", shp_name='test.shp', schema=pg_schema, table=table_name)
        assert db.table_exists(table_name, schema=pg_schema)
        assert db.tables_created == [(db.server, db.database, pg_schema, table_name)]
        db.drop_table(pg_schema, table_name)

    def test_df_to_table(self):
        db.drop_table(pg_schema, table_name)
        data = {
            'gid': {0: 1, 1: 2},
            'WKT': {0: 'POINT(-73.88782477721676 40.75343453961836)', 1: 'POINT(-73.88747073046778 40.75149365677327)'},
            'some_value': {0: 'test1', 1: 'test2'}
        }
        df = pd.DataFrame(data)
        db.dataframe_to_table(df, table_name, schema=pg_schema)
        assert db.table_exists(table_name, schema=pg_schema)
        assert db.tables_created == [(db.server, db.database, pg_schema, table_name)]
        db.drop_table(pg_schema, table_name)

    def test_data_io_create_table(self):
        src_table_name = table_name+'_src_'
        db.drop_table(pg_schema, table_name)
        db.drop_table(pg_schema, src_table_name)
        data = {
            'gid': {0: 1, 1: 2},
            'WKT': {0: 'POINT(-73.88782477721676 40.75343453961836)', 1: 'POINT(-73.88747073046778 40.75149365677327)'},
            'some_value': {0: 'test1', 1: 'test2'}
        }
        df = pd.DataFrame(data)
        db.dataframe_to_table(df, src_table_name, schema=pg_schema)
        data_io.pg_to_pg(db, db, src_table_name, org_schema=pg_schema, dest_schema=pg_schema, dest_table=table_name, spatial=False)

        assert db.table_exists(src_table_name, schema=pg_schema)
        assert db.tables_created == [(db.server, db.database, pg_schema, src_table_name), (db.server, db.database, pg_schema, table_name)]
        db.drop_table(pg_schema, table_name)
        db.drop_table(pg_schema, src_table_name)


    @classmethod
    def teardown_class(cls):
        cleanup_test_files()
        helpers.clean_up_shapefile()


class TestTablesCreatedMS:
    @classmethod
    def setup_class(cls):
        # helpers.set_up_test_table_pg(db)
        data_set_up()
        helpers.set_up_shapefile()

    def test_query_to_table(self):
        sql.drop_table(sql_schema, table_name)
        sql.query(f""" 
        create table {sql_schema}.{table_name}
        ( id_ int, val int); 
        """)
        assert sql.table_exists(table_name, schema=sql_schema)
        assert sql.tables_created == [(sql.server, sql.database, sql_schema, table_name)]
        sql.drop_table(sql_schema, table_name)
        assert not sql.tables_created == [(sql.server, sql.database, sql_schema, table_name)]

        assert not sql.table_exists(table_name, schema=sql_schema)
        sql.query(f""" 
        select 1 as id_, 2 as val into {sql_schema}.{table_name} ;
        """)
        assert sql.table_exists(table_name, schema=sql_schema)
        assert sql.tables_created == [(sql.server, sql.database, sql_schema, table_name)]
        sql.drop_table(sql_schema, table_name)
        assert not sql.tables_created == [(sql.server, sql.database, sql_schema, table_name)]


    def test_csv_to_table(self):
        sql.drop_table(sql_schema, table_name)
        sql.csv_to_table(input_file=helpers.DIR + "\\_test.csv", schema=sql_schema, table=table_name)
        assert sql.table_exists(table_name, schema=sql_schema)
        assert sql.tables_created == [(sql.server, sql.database, sql_schema, table_name)]
        sql.drop_table(sql_schema, table_name)


    def test_bulk_csv_to_table(self):
        sql.drop_table(sql_schema, table_name)
        sql.csv_to_table(input_file=helpers.DIR + "\\_test_bulk.csv", schema=sql_schema, table=table_name)
        assert sql.table_exists(table_name, schema=sql_schema)
        assert sql.tables_created == [(sql.server, sql.database, sql_schema, table_name)]
        sql.drop_table(sql_schema, table_name)

    def test_shp_to_table(self):
        sql.drop_table(sql_schema, table_name)
        sql.shp_to_table(path=helpers.DIR + "\\test.shp", schema=sql_schema, table=table_name)
        assert sql.table_exists(table_name, schema=sql_schema)
        assert sql.tables_created == [(sql.server, sql.database, sql_schema, table_name)]
        sql.drop_table(sql_schema, table_name)

    def test_shp_from_zip_to_table(self):
        sql.drop_table(sql_schema, table_name)
        sql.shp_to_table(path=helpers.DIR + "\\test.zip", shp_name='test.shp', schema=sql_schema, table=table_name)
        assert sql.table_exists(table_name, schema=sql_schema)
        assert sql.tables_created == [(sql.server, sql.database, sql_schema, table_name)]
        sql.drop_table(sql_schema, table_name)

    def test_df_to_table(self):
        sql.drop_table(sql_schema, table_name)
        data = {
            'gid': {0: 1, 1: 2},
            'WKT': {0: 'POINT(-73.88782477721676 40.75343453961836)', 1: 'POINT(-73.88747073046778 40.75149365677327)'},
            'some_value': {0: 'test1', 1: 'test2'}
        }
        df = pd.DataFrame(data)
        sql.dataframe_to_table(df, table_name, schema=sql_schema)
        assert sql.table_exists(table_name, schema=sql_schema)
        assert sql.tables_created == [(sql.server, sql.database, sql_schema, table_name)]
        sql.drop_table(sql_schema, table_name)


    def test_data_io_create_table(self):
        src_table_name = table_name+'_src_'
        sql.drop_table(sql_schema, table_name)
        sql.drop_table(sql_schema, src_table_name)
        data = {
            'gid': {0: 1, 1: 2},
            'WKT': {0: 'POINT(-73.88782477721676 40.75343453961836)', 1: 'POINT(-73.88747073046778 40.75149365677327)'},
            'some_value': {0: 'test1', 1: 'test2'}
        }
        df = pd.DataFrame(data)
        sql.dataframe_to_table(df, src_table_name, schema=sql_schema)
        data_io.sql_to_sql(sql, sql, src_table_name, org_schema=sql_schema, dest_schema=sql_schema, dest_table=table_name, spatial=False)

        assert sql.table_exists(src_table_name, schema=sql_schema)
        assert sql.tables_created == [
            (sql.server, sql.database, sql_schema, src_table_name),
            (sql.server, sql.database, sql_schema, table_name)]
        sql.drop_table(sql_schema, table_name)
        sql.drop_table(sql_schema, src_table_name)


    @classmethod
    def teardown_class(cls):
        cleanup_test_files()
        helpers.clean_up_shapefile()
