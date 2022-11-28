from .. import query

import os

import configparser

from .. import pysqldb3 as pysqldb

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

pg_dbconn = pysqldb.DbConnect(db_type=config.get('PG_DB', 'TYPE'),
                              host=config.get('PG_DB', 'SERVER'),
                              db_name=config.get('PG_DB', 'DB_NAME'),
                              username=config.get('PG_DB', 'DB_USER'),
                              password=config.get('PG_DB', 'DB_PASSWORD'))

ms_dbconn = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                              host=config.get('SQL_DB', 'SERVER'),
                              db_name=config.get('SQL_DB', 'DB_NAME'),
                              username=config.get('SQL_DB', 'DB_USER'),
                              password=config.get('SQL_DB', 'DB_PASSWORD'))


class TestQueryCreatesTablesSql():
    def test_query_renames_table_from_qry(self):
        query_string = """
            EXEC sp_rename 'RISCRASHDATA.dbo.test', 'node'
        """
        assert query.Query.query_renames_table(query_string, 'dbo') == {'dbo.node': 'test'}

    def test_query_renames_table_from_qry_no_db(self):
        query_string = """
             EXEC sp_rename 'dbo.test', 'node'
         """
        assert query.Query.query_renames_table(query_string, 'dbo') == {'dbo.node': 'test'}

    def test_query_renames_table_from_qry_just_table(self):
        query_string = """
             EXEC sp_rename 'test', 'node'
         """
        assert query.Query.query_renames_table(query_string, 'dbo') == {'dbo.node': 'test'}

    def test_query_renames_table_from_qry_ssever(self):
        query_string = """
             EXEC sp_rename 'dotserver.RISCRASHDATA.dbo.test', 'node'
         """
        assert query.Query.query_renames_table(query_string, 'dbo') == {'dbo.node': 'test'}

    def test_query_renames_table_from_qry_brackets(self):
        query_string = """
               EXEC sp_rename '[RISCRASHDATA].[dbo].[test]', '[node]'
           """
        assert query.Query.query_renames_table(query_string, 'dbo') == {'[dbo].[node]': '[test]'}

    def test_query_renames_table_from_qry_multiple(self):
        query_string = """
            EXEC sp_rename 'RISCRASHDATA.dbo.test3', 'node0'

            select *
             into RISCRASHDATA.dbo.test
            from RISCRASHDATA.dbo.test0;

            EXEC sp_rename 'RISCRASHDATA.dbo.test', 'node'
        """
        assert query.Query.query_renames_table(query_string, 'dbo') == {'dbo.node': 'test', 'dbo.node0': 'test3'}

    def test_query_renames_table_from_qry_w_comments(self):
        query_string = """
        -- EXEC sp_rename 'RISCRASHDATA.dbo.old1', 'new1'
        /*  EXEC sp_rename 'RISCRASHDATA.dbo.old2', 'new2' */
        /*
             EXEC sp_rename 'RISCRASHDATA.dbo.old3', 'new3'
        */
            EXEC sp_rename 'RISCRASHDATA.dbo.test', 'node'
        """
        assert query.Query.query_renames_table(query_string, 'dbo') == {'dbo.node': 'test'}

    def test_query_renames_table_logging_not_temp(self):
        ms_dbconn.drop_table('dbo', '___test___test___')
        assert not pg_dbconn.table_exists('___test___test___', schema_name='dbo')
        ms_dbconn.query("create table dbo.___test___test___ (id int);", temp=False)
        assert ms_dbconn.table_exists('___test___test___', schema_name='dbo')
        assert not ms_dbconn.check_table_in_log('___test___test___', schema_name='dbo')

        ms_dbconn.drop_table('dbo', '___test___test___2')
        ms_dbconn.query("EXEC sp_rename 'dbo.___test___test___', '___test___test___2'")
        assert ms_dbconn.table_exists('___test___test___2', schema_name='dbo')
        assert not ms_dbconn.check_table_in_log('___test___test___2', schema_name='dbo')
        ms_dbconn.drop_table('dbo', '___test___test___2')

    def test_query_renames_table_logging_temp(self):
        ms_dbconn.drop_table('dbo', '___test___test___')
        assert not pg_dbconn.table_exists('___test___test___', schema_name='dbo')
        ms_dbconn.query("create table dbo.___test___test___ (id int);", temp=True)
        assert ms_dbconn.table_exists('___test___test___', schema_name='dbo')
        assert ms_dbconn.check_table_in_log('___test___test___', schema_name='dbo')

        ms_dbconn.drop_table('dbo', '___test___test___2')
        ms_dbconn.query("EXEC sp_rename 'dbo.___test___test___', '___test___test___2'")
        assert ms_dbconn.table_exists('___test___test___2', schema_name='dbo')
        assert ms_dbconn.check_table_in_log('___test___test___2', schema_name='dbo')
        ms_dbconn.drop_table('dbo', '___test___test___2')


class TestQueryCreatesTablesPgSql():
    def test_query_renames_table_from_qry(self):
        query_string = """
            ALTER TABLE working.test
            RENAME TO node
        """
        assert query.Query.query_renames_table(query_string, 'public') == {'working.node': 'test'}

    def test_query_renames_table_from_qry_quotes(self):
        query_string = """
            ALTER TABLE "working"."test"
            RENAME TO "node"
        """
        assert query.Query.query_renames_table(query_string, 'public') == {'"working"."node"': '"test"'}

    def test_query_renames_table_from_qry_mulitple(self):
        query_string = """
            ALTER TABLE "working"."test"
            RENAME TO "node";

            create table working.test_table_error as
            select * from node;

            ALTER TABLE working.test2
            RENAME TO node2;
        """
        assert query.Query.query_renames_table(query_string, 'public') == {'"working"."node"': '"test"',
                                                                               'working.node2': 'test2'}

    def test_query_renames_table_from_qry_w_comments(self):
        query_string = """
        -- ALTER TABLE working.old1 rename to new1
        /*  ALTER TABLE old2 rename to new2 */
        /*
             ALTER TABLE working.old3 rename to new3
        */
        ALTER TABLE working.test
        RENAME TO node
        """
        assert query.Query.query_renames_table(query_string, 'public') == {'working.node': 'test'}


    def test_query_renames_table_logging_not_temp(self):
        pg_dbconn.drop_table('working', '___test___test___')
        assert not pg_dbconn.table_exists('___test___test___', schema_name='working')
        pg_dbconn.query("create table working.___test___test___ (id int);", temp=False)
        assert pg_dbconn.table_exists('___test___test___', schema_name='working')
        assert not pg_dbconn.check_table_in_log('___test___test___', schema_name='working')

        pg_dbconn.drop_table('working', '___test___test___2')
        pg_dbconn.query("alter table working.___test___test___ rename to ___test___test___2 ")
        assert pg_dbconn.table_exists('___test___test___2', schema_name='working')
        assert not pg_dbconn.check_table_in_log('___test___test___2', schema_name='working')
        pg_dbconn.drop_table('working', '___test___test___2')

    def test_query_renames_table_logging_temp(self):
        pg_dbconn.drop_table('working', '___test___test___')
        assert not pg_dbconn.table_exists('___test___test___', schema_name='working')
        pg_dbconn.query("create table working.___test___test___ (id int);", temp=True)
        assert pg_dbconn.table_exists('___test___test___', schema_name='working')
        assert pg_dbconn.check_table_in_log('___test___test___', schema_name='working')

        pg_dbconn.drop_table('working', '___test___test___2')
        pg_dbconn.query("alter table working.___test___test___ rename to ___test___test___2 ")
        assert pg_dbconn.table_exists('___test___test___2', schema_name='working')
        assert pg_dbconn.check_table_in_log('___test___test___2', schema_name='working')
        pg_dbconn.drop_table('working', '___test___test___2')

