import os

import configparser

from .. import pysqldb3 as pysqldb

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

pg_dbconn = pysqldb.DbConnect(db_type=config.get('PG_DB', 'TYPE'),
                              host=config.get('PG_DB', 'SERVER'),
                              db_name=config.get('PG_DB', 'DB_NAME'),
                              username=config.get('PG_DB', 'DB_USER'),
                              password=config.get('PG_DB', 'DB_PASSWORD'),
                              allow_temp_tables=True)

ris_dbconn = pysqldb.DbConnect(db_type=config.get('SECOND_PG_DB', 'TYPE'),
                               host=config.get('SECOND_PG_DB', 'SERVER'),
                               db_name=config.get('SECOND_PG_DB', 'DB_NAME'),
                               username=config.get('SECOND_PG_DB', 'DB_USER'),
                               password=config.get('SECOND_PG_DB', 'DB_PASSWORD'),
                               allow_temp_tables=True)

ms_dbconn = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                              host=config.get('SQL_DB', 'SERVER'),
                              db_name=config.get('SQL_DB', 'DB_NAME'),
                              username=config.get('SQL_DB', 'DB_USER'),
                              password=config.get('SQL_DB', 'DB_PASSWORD'),
                              allow_temp_tables=True)

test_table = 'pytest_{user}'.format(user=pg_dbconn.username)


class TestTableExistsPG():

    def test_table_exists_pg_public_create(self):
        pg_dbconn.query("""
            drop table if exists {table};
            create table {table} (id integer)
        """.format(table=test_table))

        assert pg_dbconn.table_exists(test_table)

        pg_dbconn.query("""
            drop table if exists {table}
        """.format(table=test_table))

    def test_table_exists_pg_public_drop(self):
        pg_dbconn.query("""
             drop table if exists {table};
             create table {table} (id integer)
         """.format(table=test_table))

        assert pg_dbconn.table_exists(test_table)

        pg_dbconn.query("""
             drop table if exists {table}
         """.format(table=test_table))

        assert not pg_dbconn.table_exists(test_table)

    def test_table_exists_pg_working_create(self):
        ris_dbconn.query("""
            drop table if exists working.{table};
            create table working.{table} (id integer)
        """.format(table=test_table))

        assert ris_dbconn.table_exists(schema='working', table_name=test_table)

        ris_dbconn.drop_table(schema_name='working', table_name=test_table)

    def test_table_exists_pg_rename(self):
        renamed_test_table = test_table + "_renamed"

        ris_dbconn.query("""
        drop table if exists working.{renamed};
        drop table if exists working.{table};
        create table working.{table} (id integer);
        """.format(renamed=renamed_test_table, table=test_table))

        assert ris_dbconn.table_exists(schema='working', table_name=test_table)

        ris_dbconn.query("""
            alter table working.{table} rename to {renamed}
        """.format(table=test_table, renamed=renamed_test_table))

        assert not ris_dbconn.table_exists(schema='working', table_name=test_table)
        assert ris_dbconn.table_exists(schema='working', table_name=renamed_test_table)

        ris_dbconn.drop_table(schema_name='working', table_name=renamed_test_table)

    def test_table_exists_pg_working_drop(self):
        renamed_test_table = test_table + "_renamed"

        ris_dbconn.query("""
        drop table if exists working.{renamed};
        drop table if exists working.{table};
        
        create table working.{renamed} (id integer);

        alter table working.{renamed} rename to {table};
        """.format(renamed=renamed_test_table, table=test_table))

        assert ris_dbconn.table_exists(schema='working', table_name=test_table)
        assert not ris_dbconn.table_exists(schema='working', table_name=renamed_test_table)

        ris_dbconn.query("""
        drop table working.{table};
        drop table if exists working.{renamed}
        """.format(table=test_table, renamed=renamed_test_table))

        assert not ris_dbconn.table_exists(schema='working', table_name=test_table)

    def test_table_exists_pg_temp_create(self):
        ris_dbconn.query("""create temp table bf_pytest (id integer); """)
        assert ris_dbconn.table_exists(table_name='bf_pytest') == False

    def test_table_exists_pg_temp_drop(self):
        ris_dbconn.query("""drop table bf_pytest""")
        assert ris_dbconn.table_exists(table_name='bf_pytest') == False


class TestTableExistsMS():

    def test_table_exists_ms_create(self):
        ms_dbconn.drop_table(schema_name=ms_dbconn.default_schema, table_name=test_table)

        ms_dbconn.query("""
            create table {table} (id integer) 
        """.format(table=test_table))

        assert ms_dbconn.table_exists(table_name=test_table)

        ms_dbconn.drop_table(schema_name=ms_dbconn.default_schema, table_name=test_table)

    def test_table_exists_ms_rename(self):
        renamed_test_table = test_table + "_renamed"

        ms_dbconn.drop_table(schema_name=ms_dbconn.default_schema, table_name=renamed_test_table)
        ms_dbconn.drop_table(schema_name=ms_dbconn.default_schema, table_name=test_table)

        ms_dbconn.query("""
            create table {table} (id integer) 
        """.format(table=test_table))

        assert ms_dbconn.table_exists(table_name=test_table)

        ms_dbconn.query("""
            sp_rename {table}, {renamed}
        """.format(table=test_table, renamed=renamed_test_table))

        assert not ms_dbconn.table_exists(table_name=test_table)
        assert ms_dbconn.table_exists(table_name=renamed_test_table)

        # Cleanup
        ms_dbconn.drop_table(schema_name=ms_dbconn.default_schema, table_name=renamed_test_table)

    def test_table_exists_ms_drop(self):
        renamed_test_table = test_table + "_renamed"

        ms_dbconn.drop_table(schema_name=ms_dbconn.default_schema, table_name=test_table)
        ms_dbconn.drop_table(schema_name=ms_dbconn.default_schema, table_name=renamed_test_table)

        ms_dbconn.query("""
            create table {table} (id integer) 
        """.format(table=renamed_test_table))

        assert not ms_dbconn.table_exists(test_table)
        assert ms_dbconn.table_exists(renamed_test_table)

        ms_dbconn.query("""
            sp_rename {renamed}, {table};
            drop table {table};
        """.format(renamed=renamed_test_table, table=test_table))

        assert not ms_dbconn.table_exists(table_name=test_table)

    def test_table_exists_ms_temp_create(self):
        ms_dbconn.query("""create table #pytest (id integer)""")
        assert not ms_dbconn.table_exists('#pytest')

    def test_table_exists_ms_temp_drop(self):
        ms_dbconn.query("""drop table #pytest""")
        assert not ms_dbconn.table_exists('#pytest')
