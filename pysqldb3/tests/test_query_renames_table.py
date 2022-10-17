from .. import query

import os

import configparser

from .. import pysqldb3 as pysqldb

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                       server=config.get('PG_DB', 'SERVER'),
                       db_name=config.get('PG_DB', 'DB_NAME'),
                       user=config.get('PG_DB', 'DB_USER'),
                       password=config.get('PG_DB', 'DB_PASSWORD'))

sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                        server=config.get('SQL_DB', 'SERVER'),
                        db_name=config.get('SQL_DB', 'DB_NAME'),
                        user=config.get('SQL_DB', 'DB_USER'),
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
        sql.drop_table('dbo', '___test___test___')
        assert not db.table_exists('___test___test___', schema='dbo')
        sql.query("create table dbo.___test___test___ (id int);", temp=False)
        assert sql.table_exists('___test___test___', schema='dbo')
        assert not sql.check_table_in_log('___test___test___', schema='dbo')

        sql.drop_table('dbo', '___test___test___2')
        sql.query("EXEC sp_rename 'dbo.___test___test___', '___test___test___2'")
        assert sql.table_exists('___test___test___2', schema='dbo')
        assert not sql.check_table_in_log('___test___test___2', schema='dbo')
        sql.drop_table('dbo', '___test___test___2')

    def test_query_renames_table_logging_temp(self):
        sql.drop_table('dbo', '___test___test___')
        assert not db.table_exists('___test___test___', schema='dbo')
        sql.query("create table dbo.___test___test___ (id int);", temp=True)
        assert sql.table_exists('___test___test___', schema='dbo')
        assert sql.check_table_in_log('___test___test___', schema='dbo')

        sql.drop_table('dbo', '___test___test___2')
        sql.query("EXEC sp_rename 'dbo.___test___test___', '___test___test___2'")
        assert sql.table_exists('___test___test___2', schema='dbo')
        assert sql.check_table_in_log('___test___test___2', schema='dbo')
        sql.drop_table('dbo', '___test___test___2')


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
        db.drop_table('working', '___test___test___')
        assert not db.table_exists('___test___test___', schema='working')
        db.query("create table working.___test___test___ (id int);", temp=False)
        assert db.table_exists('___test___test___', schema='working')
        assert not db.check_table_in_log('___test___test___', schema='working')

        db.drop_table('working', '___test___test___2')
        db.query("alter table working.___test___test___ rename to ___test___test___2 ")
        assert db.table_exists('___test___test___2', schema='working')
        assert not db.check_table_in_log('___test___test___2', schema='working')
        db.drop_table('working', '___test___test___2')

    def test_query_renames_table_logging_temp(self):
        db.drop_table('working', '___test___test___')
        assert not db.table_exists('___test___test___', schema='working')
        db.query("create table working.___test___test___ (id int);", temp=True)
        assert db.table_exists('___test___test___', schema='working')
        assert db.check_table_in_log('___test___test___', schema='working')

        db.drop_table('working', '___test___test___2')
        db.query("alter table working.___test___test___ rename to ___test___test___2 ")
        assert db.table_exists('___test___test___2', schema='working')
        assert db.check_table_in_log('___test___test___2', schema='working')
        db.drop_table('working', '___test___test___2')

