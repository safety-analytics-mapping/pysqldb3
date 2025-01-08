import os

import configparser

from .. import pysqldb3 as pysqldb

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                       server=config.get('PG_DB', 'SERVER'),
                       database=config.get('PG_DB', 'DB_NAME'),
                       user=config.get('PG_DB', 'DB_USER'),
                       password=config.get('PG_DB', 'DB_PASSWORD'),
                       allow_temp_tables=True)

sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                        server=config.get('SQL_DB', 'SERVER'),
                        database=config.get('SQL_DB', 'DB_NAME'),
                        user=config.get('SQL_DB', 'DB_USER'),
                        password=config.get('SQL_DB', 'DB_PASSWORD'),
                        allow_temp_tables=True)

test_table = f'pytest_{db.user}'
test_table2 = f'Pytest_{db.user}'
pg_schema = 'working'
sql_schema ='dbo'


class TestGetTablesPG():
    db = db
    schema = pg_schema

    def test_get_my_tables(self):
        # make sure table isnt there
        self.db.drop_table(self.schema, test_table)
        assert not self.db.table_exists(test_table, schema=self.schema)
        # check table not in my tables
        tbls = self.db.my_tables(self.schema)
        assert not test_table in [i for i in tbls.tablename]
        # create table
        self.db.query(f"create table {self.schema}.{test_table} (id_ int)")
        # check table is now in my tables list
        tbls = self.db.my_tables(self.schema)
        assert test_table in [i for i in tbls.tablename]
        # clean up
        self.db.drop_table(self.schema, test_table)

    def test_get_schema_tables(self):
        # make sure table isnt there
        self.db.drop_table(self.schema, test_table)
        assert not self.db.table_exists(test_table, schema=self.schema)
        # check table not in my tables
        tbls = self.db.schema_tables(self.schema)
        assert not test_table in [i for i in tbls.tablename]
        # create table
        self.db.query(f"create table {self.schema}.{test_table} (id_ int)")
        # check table is now in my tables list
        tbls = self.db.schema_tables(self.schema)
        assert test_table in [i for i in tbls.tablename]
        # clean up
        self.db.drop_table(self.schema, test_table)

class TestGetTablesSQL():
    db = sql
    schema = sql_schema

    def test_get_my_tables(self):
        # sql serve my tables is not a function
        tbls = self.db.my_tables(self.schema)
        assert tbls is None


    def test_get_schema_tables(self):
        # make sure table isnt there
        self.db.drop_table(self.schema, test_table)
        assert not self.db.table_exists(test_table, schema=self.schema)
        # check table not in my tables
        tbls = self.db.schema_tables(self.schema)
        assert not test_table in [i for i in tbls.tablename]
        # create table
        self.db.query(f"create table {self.schema}.{test_table} (id_ int)")
        # check table is now in my tables list
        tbls = self.db.schema_tables(self.schema)
        assert test_table in [i for i in tbls.tablename]
        # clean up
        self.db.drop_table(self.schema, test_table)
