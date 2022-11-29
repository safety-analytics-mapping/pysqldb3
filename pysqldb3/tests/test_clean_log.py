from .. import pysqldb3 as pysqldb
from ..data_io import *

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

pg_dbconn = pysqldb.DbConnect(db_type=config.get('PG_DB', 'TYPE'),
                              host=config.get('PG_DB', 'SERVER'),
                              db_name=config.get('PG_DB', 'DB_NAME'),
                              username=config.get('PG_DB', 'DB_USER'),
                              password=config.get('PG_DB', 'DB_PASSWORD'),
                              allow_temp_tables=True)

ms_dbconn = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                              host=config.get('SQL_DB', 'SERVER'),
                              db_name=config.get('SQL_DB', 'DB_NAME'),
                              username=config.get('SQL_DB', 'DB_USER'),
                              password=config.get('SQL_DB', 'DB_PASSWORD'),
                              allow_temp_tables=True)

test_clean_up_new_table = 'test_new_table_testing_{username}'.format(username=pg_dbconn.username)
test_clean_up_new_table2 = 'test_new_table_testing_{username}_2'.format(username=pg_dbconn.username)


class TestCleanUpNewTablesPg:
    def test_clean_up_new_tables_basic(self):
        schema_name = 'public'
        # make sure table doesnt exists
        pg_dbconn.drop_table(table_name=test_clean_up_new_table, schema_name=schema_name)
        # create table
        pg_dbconn.query("""
            CREATE TABLE {tcnt} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(tcnt=test_clean_up_new_table))
        # make sure table was created
        assert pg_dbconn.table_exists(table_name=test_clean_up_new_table, schema_name=schema_name)
        # make sure table was added to log
        pg_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{tcn_table}'".format(
            schema=schema_name,
            username=pg_dbconn.username,
            tcn_table=test_clean_up_new_table))
        assert pg_dbconn.data
        # drop table
        pg_dbconn.drop_table(table_name=test_clean_up_new_table, schema_name=schema_name)
        # check table is no longer in the log
        pg_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{tcn_table}'".format(
            schema=schema_name,
            username=pg_dbconn.username,
            tcn_table=test_clean_up_new_table
        ))
        assert not pg_dbconn.data

    def test_clean_up_new_tables_schema(self):
        schema_name = 'public'
        # make sure table doesnt exists
        pg_dbconn.drop_table(table_name=test_clean_up_new_table, schema_name=schema_name)
        # create table
        pg_dbconn.query("""
            CREATE TABLE {schema}.{tcn_table} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(schema=schema_name,
                   tcn_table=test_clean_up_new_table))
        # make sure table was created
        assert pg_dbconn.table_exists(test_clean_up_new_table, schema=schema_name)
        # make sure table was added to log
        pg_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{tcn_table}'".format(
            schema=schema_name,
            username=pg_dbconn.username,
            tcn_table=test_clean_up_new_table
        ))
        assert pg_dbconn.data
        # drop table
        pg_dbconn.drop_table(table_name=test_clean_up_new_table, schema_name=schema_name)
        # check table is no longer in the log
        pg_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{tcn_table}'".format(
            schema=schema_name,
            username=pg_dbconn.username,
            tcn_table=test_clean_up_new_table
        ))
        assert not pg_dbconn.data

    def test_clean_up_new_tables_schema_single_statement(self):
        schema_name = 'public'
        # create table-drop-create
        pg_dbconn.query("""
            drop table if exists {schema}.{tcn_table};
            CREATE TABLE {schema}.{tcn_table} (
                id int,
                column2 text,
                column3 timestamp
            );
            drop table if exists {schema}.{tcn_table};
        """.format(schema=schema_name, tcn_table=test_clean_up_new_table))
        # make sure table was dropped
        assert not pg_dbconn.table_exists(table_name=test_clean_up_new_table, schema_name=schema_name)
        # check table is no longer in the log
        pg_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{tcn_table}'".format(
            schema=schema_name,
            username=pg_dbconn.username,
            tcn_table=test_clean_up_new_table
        ))
        assert not pg_dbconn.data

    def test_clean_up_new_tables_rename(self):
        schema_name = 'public'
        # make sure table doesnt exists
        pg_dbconn.drop_table(table_name=test_clean_up_new_table, schema_name=schema_name)
        # create table
        pg_dbconn.query("""
            CREATE TABLE {tcn_table} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(tcn_table=test_clean_up_new_table))

        # make sure table was created
        assert pg_dbconn.table_exists(table_name=test_clean_up_new_table, schema_name=schema_name)

        # make sure table was added to log
        pg_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{table}'".format(
            schema=schema_name,
            username=pg_dbconn.username,
            table=test_clean_up_new_table))
        assert pg_dbconn.data

        # rename table
        pg_dbconn.drop_table(schema_name, test_clean_up_new_table2)
        pg_dbconn.query("alter table {schema}.{table} rename to {table2}".format(
            schema=schema_name,
            table=test_clean_up_new_table,
            table2=test_clean_up_new_table2))

        # make sure table was created
        assert pg_dbconn.table_exists(table_name=test_clean_up_new_table2, schema_name=schema_name)

        # make sure log was updated
        pg_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{table}'".format(
            schema=schema_name,
            username=pg_dbconn.username,
            table=test_clean_up_new_table
        ))
        assert not pg_dbconn.data

        pg_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{table2}'".format(
            schema=schema_name,
            username=pg_dbconn.username,
            table2=test_clean_up_new_table2
        ))
        print(pg_dbconn.queries[-1].renamed_tables)
        print(pg_dbconn.data)
        assert pg_dbconn.data

        # drop table
        pg_dbconn.drop_table(table_name=test_clean_up_new_table2, schema_name=schema_name)
        # check table is no longer in the log
        pg_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{table2}'".format(
            schema=schema_name,
            username=pg_dbconn.username,
            table2=test_clean_up_new_table2
        ))
        assert not pg_dbconn.data

    def test_clean_up_new_tables_temp(self):
        schema = 'public'
        table_name = 'test_new_table_92820_testing'
        pg_dbconn.query("""
            CREATE TEMPORARY TABLE {table} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(table=table_name))
        pg_dbconn.query(""" INSERT INTO {table}
            VALUES (1, 'test', now())
            """.format(table=table_name))

        # make sure table was created - table exists wont work on temp tables
        pg_dbconn.query(f"select * from {table_name}")
        assert pg_dbconn.data
        # make sure table was not added to log
        pg_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{table}'".format(
            schema=schema,
            username=pg_dbconn.username,
            table=table_name
        ))
        assert not pg_dbconn.data

    def test_clean_up_rename_tables_temp(self):
        schema = 'public'
        table_name = 'test_new_table_92820_testing'
        pg_dbconn.query("drop table if exists %s" % table_name)
        pg_dbconn.query("""
            CREATE TEMPORARY TABLE {table} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(table=table_name))
        pg_dbconn.query(""" INSERT INTO {}
            VALUES (1, 'test', now())
            """.format(table_name))

        # make sure table was created - table exists wont work on temp tables
        pg_dbconn.query("select * from %s" % table_name)
        assert pg_dbconn.data
        # make sure table was not added to log
        pg_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{table}'".format(
            schema=schema,
            username=pg_dbconn.username,
            table=table_name
        ))
        assert not pg_dbconn.data

        # rename table
        pg_dbconn.query("alter table {table} rename to {table}_2".format(table=table_name))
        # make sure table was created - table exists wont work on temp tables
        pg_dbconn.query(f"select * from {table_name}_2")
        assert pg_dbconn.data
        # make sure table was not added to log
        pg_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{table}'".format(
            schema=schema,
            username=pg_dbconn.username,
            table=f'{table_name}_2'
        ))
        assert not pg_dbconn.data


class TestCleanUpNewTablesMs:
    def test_clean_up_new_tables_basic(self):
        schema = ms_dbconn.default_schema
        # make sure table doesnt exists
        ms_dbconn.drop_table(table_name=test_clean_up_new_table, schema_name=schema)
        # create table
        ms_dbconn.query("""
            CREATE TABLE {table} (
                id int,
                column2 text,
                column3 datetime
            );
        """.format(table=test_clean_up_new_table))
        # make sure table was created
        assert ms_dbconn.table_exists(test_clean_up_new_table, schema=schema)
        # make sure table was added to log
        ms_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{table}'".format(
            schema=schema,
            username=ms_dbconn.username,
            table=test_clean_up_new_table
        ))
        assert ms_dbconn.data
        # drop table
        ms_dbconn.drop_table(table_name=test_clean_up_new_table, schema_name=schema)
        # check table is no longer in the log
        ms_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{table}'".format(
            schema=schema,
            username=ms_dbconn.username,
            table=test_clean_up_new_table
        ))
        assert not ms_dbconn.data

    def test_clean_up_new_tables_schema(self):
        schema = ms_dbconn.default_schema
        # make sure table doesnt exists
        ms_dbconn.drop_table(table_name=test_clean_up_new_table, schema_name=schema)
        # create table
        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (
                id int,
                column2 text,
                column3 datetime
            );
        """.format(schema=schema, table=test_clean_up_new_table))
        # make sure table was created
        assert ms_dbconn.table_exists(test_clean_up_new_table, schema=schema)
        # make sure table was added to log
        ms_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{table}'".format(
            schema=schema,
            username=ms_dbconn.username,
            table=test_clean_up_new_table
        ))
        assert ms_dbconn.data
        # drop table
        ms_dbconn.drop_table(table_name=test_clean_up_new_table, schema_name=schema)
        # check table is no longer in the log
        ms_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{table}'".format(
            schema=schema,
            username=ms_dbconn.username,
            table=test_clean_up_new_table
        ))
        assert not ms_dbconn.data

    def test_clean_up_new_tables_rename(self):
        schema = ms_dbconn.default_schema
        # make sure table doesnt exists
        ms_dbconn.drop_table(table_name=test_clean_up_new_table, schema_name=schema)
        # create table
        ms_dbconn.query("""
            CREATE TABLE {table} (
                id int,
                column2 text,
                column3 datetime
            );
        """.format(table=test_clean_up_new_table))
        # make sure table was created
        assert ms_dbconn.table_exists(table_name=test_clean_up_new_table, schema_name=schema)
        # make sure table was added to log
        ms_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{table}'".format(
            schema=schema,
            username=ms_dbconn.username,
            table=test_clean_up_new_table
        ))
        assert ms_dbconn.data
        # rename table
        ms_dbconn.drop_table(schema, test_clean_up_new_table2)
        ms_dbconn.query("exec sp_rename '{schema}.{table}', '{table2}'".format(
            schema=schema,
            table=test_clean_up_new_table,
            table2=test_clean_up_new_table2))

        # make sure log was updated
        ms_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{table}'".format(
            schema=schema,
            username=ms_dbconn.username,
            table=test_clean_up_new_table
        ))
        assert not ms_dbconn.data

        ms_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{table}'".format(
            schema=schema,
            username=ms_dbconn.username,
            table=test_clean_up_new_table2
        ))
        assert ms_dbconn.data

        # drop table
        ms_dbconn.drop_table(table_name=test_clean_up_new_table2, schema_name=schema)
        # check table is no longer in the log
        ms_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{table}'".format(
            schema=schema,
            username=ms_dbconn.username,
            table=test_clean_up_new_table2
        ))
        assert not ms_dbconn.data

    def test_clean_up_new_tables_temp(self):
        schema = ms_dbconn.default_schema
        table_name = 'test_new_table_92820_testing'
        ms_dbconn.query("""
            CREATE TABLE #{table} (
                id int,
                column2 text,
                column3 datetime
            );
        """.format(table=table_name))
        ms_dbconn.query(""" INSERT INTO #{table}
            VALUES (1, 'test', current_timestamp)
            """.format(table=table_name))

        # make sure table was created - table exists wont work on temp tables
        ms_dbconn.query(f"select * from #{table_name}")
        assert ms_dbconn.data
        # make sure table was not added to log
        ms_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{table}'".format(
            schema=schema,
            username=ms_dbconn.username,
            table=table_name
        ))
        assert not ms_dbconn.data

    def test_clean_up_new_tables_schema_single_statement(self):
        schema = ms_dbconn.default_schema
        # create initial table
        ms_dbconn.query("""
                   CREATE TABLE {schema}.{table} (
                       id int
                   );""".format(schema=schema, table=test_clean_up_new_table))
        # create table-drop-create
        ms_dbconn.query("""
            drop table {schema}.{table};
            CREATE TABLE {schema}.{table} (
                id int,
                column2 text,
                column3 datetime
            );
            drop table {schema}.{table};
        """.format(schema=schema, table=test_clean_up_new_table))
        # make sure table was dropped
        assert not ms_dbconn.table_exists(table_name=test_clean_up_new_table, schema_name=schema)
        # check table is no longer in the log
        ms_dbconn.query("select * from {schema}.__temp_log_table_{username}__ where table_name = '{table}'".format(
            schema=schema,
            username=ms_dbconn.username,
            table=test_clean_up_new_table
        ))
        print(ms_dbconn.data)
        assert not ms_dbconn.data
