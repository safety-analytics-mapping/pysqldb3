import configparser

from .. import pysqldb3 as pysqldb
from ..data_io import *

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

test_clean_up_new_table = f'test_new_table_testing_{db.user}'
test_clean_up_new_table2 = f'test_new_table_testing_{db.user}_2'
pg_schema = 'public'
ms_schema = sql.default_schema

class TestCleanUpNewTablesPg:
    def test_clean_up_new_tables_basic(self):
        # make sure table doesnt exist
        db.drop_table(table=test_clean_up_new_table, schema=pg_schema)
        # create table
        db.query(f"""
            CREATE TABLE {test_clean_up_new_table} (
                id int,
                column2 text,
                column3 timestamp
            );
        """)
        # make sure table was created
        assert db.table_exists(test_clean_up_new_table, schema=pg_schema)
        # make sure table was added to log
        db.query(f"select * from {pg_schema}.__temp_log_table_{db.user}__ where table_name = '{test_clean_up_new_table}'")
        assert db.data
        # drop table
        db.drop_table(table=test_clean_up_new_table, schema=pg_schema)
        # check table is no longer in the log
        db.query(f"select * from {pg_schema}.__temp_log_table_{db.user}__ where table_name = '{test_clean_up_new_table}'")
        assert not db.data

    def test_clean_up_new_tables_schema(self):
        # make sure table doesnt exist
        db.drop_table(table=test_clean_up_new_table, schema=pg_schema)
        # create table
        db.query(f"""
            CREATE TABLE {pg_schema}.{test_clean_up_new_table} (
                id int,
                column2 text,
                column3 timestamp
            );
        """)
        # make sure table was created
        assert db.table_exists(test_clean_up_new_table, schema=pg_schema)
        # make sure table was added to log
        db.query(f"select * from {pg_schema}.__temp_log_table_{db.user}__ where table_name = '{test_clean_up_new_table}'")
        assert db.data
        # drop table
        db.drop_table(table=test_clean_up_new_table, schema=ms_schema)
        # check table is no longer in the log
        db.query(f"select * from {ms_schema}.__temp_log_table_{db.user}__ where table_name = '{test_clean_up_new_table}'")
        assert not db.data

    def test_clean_up_new_tables_schema_single_statement(self):
        # create table-drop-create
        db.query(f"""
            drop table if exists {pg_schema}.{test_clean_up_new_table};
            CREATE TABLE {pg_schema}.{test_clean_up_new_table} (
                id int,
                column2 text,
                column3 timestamp
            );
            drop table if exists {pg_schema}.{test_clean_up_new_table};
        """)
        # make sure table was dropped
        assert not db.table_exists(test_clean_up_new_table, schema=pg_schema)
        # check table is no longer in the log
        db.query(f"select * from {pg_schema}.__temp_log_table_{db.user}__ where table_name = '{test_clean_up_new_table}'")
        assert not db.data

    def test_clean_up_new_tables_rename(self):
        # make sure table doesnt exist
        db.drop_table(table=test_clean_up_new_table, schema=pg_schema)
        # create table
        db.query(f"""
            CREATE TABLE {test_clean_up_new_table} (
                id int,
                column2 text,
                column3 timestamp
            );
        """)
        # make sure table was created
        assert db.table_exists(test_clean_up_new_table, schema=pg_schema)
        # make sure table was added to log
        db.query(f"select * from {pg_schema}.__temp_log_table_{db.user}__ where table_name = '{test_clean_up_new_table}'")
        assert db.data
        # rename table
        db.drop_table(pg_schema, test_clean_up_new_table2)
        db.query(f"alter table {pg_schema}.{test_clean_up_new_table} rename to {test_clean_up_new_table2}" )
        # make sure table was created
        assert db.table_exists(test_clean_up_new_table2, schema=pg_schema)
        # make sure log was updated
        db.query(f"select * from {pg_schema}.__temp_log_table_{db.user}__ where table_name = '{test_clean_up_new_table}'")
        assert not db.data

        db.query(f"select * from {pg_schema}.__temp_log_table_{db.user}__ where table_name = '{test_clean_up_new_table2}'")
        assert db.data

        # drop table
        db.drop_table(table=test_clean_up_new_table2, schema=pg_schema)
        # check table is no longer in the log
        db.query(f"select * from {pg_schema}.__temp_log_table_{db.user}__ where table_name = '{test_clean_up_new_table2}'")
        assert not db.data

    def test_clean_up_new_tables_temp(self):
        db.query(f"""
            CREATE TEMPORARY TABLE {test_clean_up_new_table} (
                id int,
                column2 text,
                column3 timestamp
            );
        """)
        db.query(f""" INSERT INTO {test_clean_up_new_table}
            VALUES (1, 'test', now())
            """)

        # make sure table was created - table exists wont work on temp tables
        db.query(f"select * from %s" % test_clean_up_new_table)
        assert db.data
        # make sure table was not added to log
        db.query(f"select * from {pg_schema}.__temp_log_table_{db.user}__ where table_name = '{test_clean_up_new_table}'")
        assert not db.data

    def test_clean_up_rename_tables_temp(self):

        db.query("drop table if exists %s" % test_clean_up_new_table)
        db.query(f"""
            CREATE TEMPORARY TABLE {test_clean_up_new_table} (
                id int,
                column2 text,
                column3 timestamp
            );
        """)
        db.query(f""" INSERT INTO {test_clean_up_new_table}
            VALUES (1, 'test', now())
            """)

        # make sure table was created - table exists wont work on temp tables
        db.query("select * from %s" % test_clean_up_new_table)
        assert db.data
        # make sure table was not added to log
        db.query(f"select * from {ms_schema}.__temp_log_table_{db.user}__ where table_name = '{test_clean_up_new_table}'")
        assert not db.data

        # rename table
        db.query(f"alter table {test_clean_up_new_table} rename to {test_clean_up_new_table2}")
        # make sure table was created - table exists wont work on temp tables
        db.query(f"select * from %s" % test_clean_up_new_table2)
        assert db.data
        # make sure table was not added to log
        db.query(f"select * from {ms_schema}.__temp_log_table_{db.user}__ where table_name = '{test_clean_up_new_table2}'")
        assert not db.data

    def cleanup_tables():
        db.cleanup_new_tables()

class TestCleanUpNewTablesMs:
    def test_clean_up_new_tables_basic(self):
        # make sure table doesnt exist
        sql.drop_table(table=test_clean_up_new_table, schema=ms_schema)
        # create table
        sql.query(f"""
            CREATE TABLE {test_clean_up_new_table} (
                id int,
                column2 text,
                column3 datetime
            );
        """)
        # make sure table was created
        assert sql.table_exists(test_clean_up_new_table, schema=ms_schema)
        # make sure table was added to log
        sql.query(f"select * from {ms_schema}.__temp_log_table_{sql.user}__ where table_name = '{test_clean_up_new_table}'")
        assert sql.data
        # drop table
        sql.drop_table(table=test_clean_up_new_table, schema=ms_schema)
        # check table is no longer in the log
        sql.query(f"select * from {ms_schema}.__temp_log_table_{sql.user}__ where table_name = '{test_clean_up_new_table}'")
        assert not sql.data

    def test_clean_up_new_tables_schema(self):
        # make sure table doesnt exist
        sql.drop_table(table=test_clean_up_new_table, schema=ms_schema)
        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_clean_up_new_table} (
                id int,
                column2 text,
                column3 datetime
            );
        """)
        # make sure table was created
        assert sql.table_exists(test_clean_up_new_table, schema=ms_schema)
        # make sure table was added to log
        sql.query(f"select * from {ms_schema}.__temp_log_table_{sql.user}__ where table_name = '{test_clean_up_new_table}'")
        assert sql.data
        # drop table
        sql.drop_table(table=test_clean_up_new_table, schema=ms_schema)
        # check table is no longer in the log
        sql.query(f"select * from {ms_schema}.__temp_log_table_{sql.user}__ where table_name = '{test_clean_up_new_table}'")
        assert not sql.data

    def test_clean_up_new_tables_rename(self):

        # make sure table doesnt exist
        sql.drop_table(table=test_clean_up_new_table, schema=ms_schema)
        # create table
        sql.query(f"""
            CREATE TABLE {test_clean_up_new_table} (
                id int,
                column2 text,
                column3 datetime
            );
        """)
        # make sure table was created
        assert sql.table_exists(test_clean_up_new_table, schema=ms_schema)
        # make sure table was added to log
        sql.query(f"select * from {ms_schema}.__temp_log_table_{sql.user}__ where table_name = '{test_clean_up_new_table}'")
        assert sql.data
        # rename table
        sql.drop_table(ms_schema, test_clean_up_new_table2)
        sql.query(f"exec sp_rename '{ms_schema}.{test_clean_up_new_table}', '{test_clean_up_new_table2}'")

        # make sure log was updated
        sql.query(f"select * from {ms_schema}.__temp_log_table_{sql.user}__ where table_name = '{test_clean_up_new_table}'")
        assert not sql.data

        sql.query(f"select * from {ms_schema}.__temp_log_table_{sql.user}__ where table_name = '{test_clean_up_new_table2}'")
        assert sql.data

        # drop table
        sql.drop_table(table=test_clean_up_new_table2, schema=ms_schema)
        # check table is no longer in the log
        sql.query(f"select * from {ms_schema}.__temp_log_table_{sql.user}__ where table_name = '{test_clean_up_new_table2}'")
        assert not sql.data

    def test_clean_up_new_tables_temp(self):
        sql.query(f"""
            CREATE TABLE #{test_clean_up_new_table} (
                id int,
                column2 text,
                column3 datetime
            );
        """)
        sql.query(f""" INSERT INTO #{test_clean_up_new_table}
            VALUES (1, 'test', current_timestamp)
            """)

        # make sure table was created - table exists wont work on temp tables
        sql.query("select * from #%s" % test_clean_up_new_table)
        assert sql.data
        # make sure table was not added to log
        sql.query(f"select * from {ms_schema}.__temp_log_table_{sql.user}__ where table_name = '{test_clean_up_new_table}'")
        assert not sql.data

    def test_clean_up_new_tables_schema_single_statement(self):
        
        # create initial table
        sql.query(f"""
                   CREATE TABLE {ms_schema}.{test_clean_up_new_table} (
                       id int
                   );""")
        # create table-drop-create
        sql.query(f"""
            drop table {ms_schema}.{test_clean_up_new_table};
            CREATE TABLE {ms_schema}.{test_clean_up_new_table} (
                id int,
                column2 text,
                column3 datetime
            );
            drop table {ms_schema}.{test_clean_up_new_table};
        """)
        # make sure table was dropped
        assert not sql.table_exists(test_clean_up_new_table, schema=ms_schema)
        # check table is no longer in the log
        sql.query(f"select * from {ms_schema}.__temp_log_table_{sql.user}__ where table_name = '{test_clean_up_new_table}'")
        assert not sql.data

    def cleanup_tables():
        sql.cleanup_new_tables()