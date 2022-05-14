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

test_clean_up_new_table = 'test_new_table_testing_{}'.format(db.user)
test_clean_up_new_table2 = 'test_new_table_testing_{}_2'.format(db.user)


class TestCleanUpNewTablesPg:
    def test_clean_up_new_tables_basic(self):
        schema = 'public'
        # make sure table doesnt exists
        db.drop_table(table=test_clean_up_new_table, schema=schema)
        # create table
        db.query("""
            CREATE TABLE {} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(test_clean_up_new_table))
        # make sure table was created
        assert db.table_exists(test_clean_up_new_table, schema=schema)
        # make sure table was added to log
        db.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=db.user, t=test_clean_up_new_table
        ))
        assert db.data
        # drop table
        db.drop_table(table=test_clean_up_new_table, schema=schema)
        # check table is no longer in the log
        db.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=db.user, t=test_clean_up_new_table
        ))
        assert not db.data

    def test_clean_up_new_tables_schema(self):
        schema = 'public'
        # make sure table doesnt exists
        db.drop_table(table=test_clean_up_new_table, schema=schema)
        # create table
        db.query("""
            CREATE TABLE {}.{} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(schema, test_clean_up_new_table))
        # make sure table was created
        assert db.table_exists(test_clean_up_new_table, schema=schema)
        # make sure table was added to log
        db.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=db.user, t=test_clean_up_new_table
        ))
        assert db.data
        # drop table
        db.drop_table(table=test_clean_up_new_table, schema=schema)
        # check table is no longer in the log
        db.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=db.user, t=test_clean_up_new_table
        ))
        assert not db.data

    def test_clean_up_new_tables_schema_single_statement(self):
        schema = 'public'
        # create table-drop-create
        db.query("""
            drop table if exists {s}.{t};
            CREATE TABLE {s}.{t} (
                id int,
                column2 text,
                column3 timestamp
            );
            drop table if exists {s}.{t};
        """.format(s=schema, t=test_clean_up_new_table))
        # make sure table was dropped
        assert not db.table_exists(test_clean_up_new_table, schema=schema)
        # check table is no longer in the log
        db.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=db.user, t=test_clean_up_new_table
        ))
        assert not db.data

    def test_clean_up_new_tables_rename(self):
        schema = 'public'
        # make sure table doesnt exists
        db.drop_table(table=test_clean_up_new_table, schema=schema)
        # create table
        db.query("""
            CREATE TABLE {} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(test_clean_up_new_table))
        # make sure table was created
        assert db.table_exists(test_clean_up_new_table, schema=schema)
        # make sure table was added to log
        db.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=db.user, t=test_clean_up_new_table
        ))
        assert db.data
        # rename table
        db.drop_table(schema, test_clean_up_new_table2)
        db.query("alter table {s}.{t1} rename to {t2}".format(s=schema, t1=test_clean_up_new_table,
                                                              t2=test_clean_up_new_table2))
        # make sure table was created
        assert db.table_exists(test_clean_up_new_table2, schema=schema)
        # make sure log was updated
        db.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=db.user, t=test_clean_up_new_table
        ))
        assert not db.data

        db.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=db.user, t=test_clean_up_new_table2
        ))
        print(db.queries[-1].renamed_tables)
        print(db.data)
        assert db.data

        # drop table
        db.drop_table(table=test_clean_up_new_table2, schema=schema)
        # check table is no longer in the log
        db.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=db.user, t=test_clean_up_new_table2
        ))
        assert not db.data

    def test_clean_up_new_tables_temp(self):
        schema = 'public'
        table_name = 'test_new_table_92820_testing'
        db.query("""
            CREATE TEMPORARY TABLE {} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(table_name))
        db.query(""" INSERT INTO {}
            VALUES (1, 'test', now())
            """.format(table_name))

        # make sure table was created - table exists wont work on temp tables
        db.query("select * from %s" % table_name)
        assert db.data
        # make sure table was not added to log
        db.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=db.user, t=table_name
        ))
        assert not db.data

    def test_clean_up_rename_tables_temp(self):
        schema = 'public'
        table_name = 'test_new_table_92820_testing'
        db.query("drop table if exists %s" % table_name)
        db.query("""
            CREATE TEMPORARY TABLE {} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(table_name))
        db.query(""" INSERT INTO {}
            VALUES (1, 'test', now())
            """.format(table_name))

        # make sure table was created - table exists wont work on temp tables
        db.query("select * from %s" % table_name)
        assert db.data
        # make sure table was not added to log
        db.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=db.user, t=table_name
        ))
        assert not db.data

        # rename table
        db.query("alter table {} rename to {}".format(table_name, table_name+'_2'))
        # make sure table was created - table exists wont work on temp tables
        db.query("select * from %s" % table_name+'_2')
        assert db.data
        # make sure table was not added to log
        db.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=db.user, t=table_name+'_2'
        ))
        assert not db.data


class TestCleanUpNewTablesMs:
    def test_clean_up_new_tables_basic(self):
        schema = sql.default_schema
        # make sure table doesnt exists
        sql.drop_table(table=test_clean_up_new_table, schema=schema)
        # create table
        sql.query("""
            CREATE TABLE {} (
                id int,
                column2 text,
                column3 datetime
            );
        """.format(test_clean_up_new_table))
        # make sure table was created
        assert sql.table_exists(test_clean_up_new_table, schema=schema)
        # make sure table was added to log
        sql.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=sql.user, t=test_clean_up_new_table
        ))
        assert sql.data
        # drop table
        sql.drop_table(table=test_clean_up_new_table, schema=schema)
        # check table is no longer in the log
        sql.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=sql.user, t=test_clean_up_new_table
        ))
        assert not sql.data

    def test_clean_up_new_tables_schema(self):
        schema = sql.default_schema
        # make sure table doesnt exists
        sql.drop_table(table=test_clean_up_new_table, schema=schema)
        # create table
        sql.query("""
            CREATE TABLE {}.{} (
                id int,
                column2 text,
                column3 datetime
            );
        """.format(schema, test_clean_up_new_table))
        # make sure table was created
        assert sql.table_exists(test_clean_up_new_table, schema=schema)
        # make sure table was added to log
        sql.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=sql.user, t=test_clean_up_new_table
        ))
        assert sql.data
        # drop table
        sql.drop_table(table=test_clean_up_new_table, schema=schema)
        # check table is no longer in the log
        sql.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=sql.user, t=test_clean_up_new_table
        ))
        assert not sql.data

    def test_clean_up_new_tables_rename(self):
        schema=sql.default_schema
        # make sure table doesnt exists
        sql.drop_table(table=test_clean_up_new_table, schema=schema)
        # create table
        sql.query("""
            CREATE TABLE {} (
                id int,
                column2 text,
                column3 datetime
            );
        """.format(test_clean_up_new_table))
        # make sure table was created
        assert sql.table_exists(test_clean_up_new_table, schema=schema)
        # make sure table was added to log
        sql.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=sql.user, t=test_clean_up_new_table
        ))
        assert sql.data
        # rename table
        sql.drop_table(schema, test_clean_up_new_table2)
        sql.query("exec sp_rename '{s}.{t1}', '{t2}'".format(s=schema, t1=test_clean_up_new_table,
                                                             t2=test_clean_up_new_table2))

        # make sure log was updated
        sql.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=sql.user, t=test_clean_up_new_table
        ))
        assert not sql.data

        sql.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=sql.user, t=test_clean_up_new_table2
        ))
        assert sql.data

        # drop table
        sql.drop_table(table=test_clean_up_new_table2, schema=schema)
        # check table is no longer in the log
        sql.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=sql.user, t=test_clean_up_new_table2
        ))
        assert not sql.data

    def test_clean_up_new_tables_temp(self):
        schema=sql.default_schema
        table_name = 'test_new_table_92820_testing'
        sql.query("""
            CREATE TABLE #{} (
                id int,
                column2 text,
                column3 datetime
            );
        """.format(table_name))
        sql.query(""" INSERT INTO #{}
            VALUES (1, 'test', current_timestamp)
            """.format(table_name))

        # make sure table was created - table exists wont work on temp tables
        sql.query("select * from #%s" % table_name)
        assert sql.data
        # make sure table was not added to log
        sql.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=sql.user, t=table_name
        ))
        assert not sql.data

    def test_clean_up_new_tables_schema_single_statement(self):
        schema = sql.default_schema
        # create initial table
        sql.query("""
                   CREATE TABLE {s}.{t} (
                       id int
                   );""".format(s=schema, t=test_clean_up_new_table))
        # create table-drop-create
        sql.query("""
            drop table {s}.{t};
            CREATE TABLE {s}.{t} (
                id int,
                column2 text,
                column3 datetime
            );
            drop table {s}.{t};
        """.format(s=schema, t=test_clean_up_new_table))
        # make sure table was dropped
        assert not sql.table_exists(test_clean_up_new_table, schema=schema)
        # check table is no longer in the log
        sql.query("select * from {schema}.__temp_log_table_{uname}__ where table_name = '{t}'".format(
            schema=schema, uname=sql.user, t=test_clean_up_new_table
        ))
        print(sql.data)
        assert not sql.data
