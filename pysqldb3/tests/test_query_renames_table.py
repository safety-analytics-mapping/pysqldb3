from .. import query

import os

import configparser

from .. import pysqldb3 as pysqldb

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


class TestQueryCreatesTablesSql():
    def test_query_renames_table_from_qry(self, schema_name='dbo'):
        query_string = f"""
            EXEC sp_rename 'RISCRASHDATA.{schema_name}.test', 'node'
        """
        assert query.Query.query_renames_table(query_string, schema_name) == {f'{schema_name}.node': 'test'}

    def test_query_renames_table_from_qry_no_db(self, schema_name='dbo'):
        query_string = f"""
             EXEC sp_rename '{schema_name}.test', 'node'
         """
        assert query.Query.query_renames_table(query_string, schema_name) == {f'{schema_name}.node': 'test'}

    def test_query_renames_table_from_qry_just_table(self, schema_name='dbo'):
        query_string = """
             EXEC sp_rename 'test', 'node'
         """
        assert query.Query.query_renames_table(query_string, schema_name) == {f'{schema_name}.node': 'test'}

    def test_query_renames_table_from_qry_ssever(self, schema_name='dbo'):
        query_string = f"""
             EXEC sp_rename 'dotserver.RISCRASHDATA.{schema_name}.test', 'node'
         """
        assert query.Query.query_renames_table(query_string, schema_name) == {f'{schema_name}.node': 'test'}

    def test_query_renames_table_from_qry_brackets(self, schema_name='dbo'):
        query_string = f"""
               EXEC sp_rename '[RISCRASHDATA].[{schema_name}].[test]', '[node]'
           """
        assert query.Query.query_renames_table(query_string, schema_name) == {f'[{schema_name}].[node]': '[test]'}

    def test_query_renames_table_from_qry_multiple(self, schema_name='dbo'):
        query_string = f"""
            EXEC sp_rename 'RISCRASHDATA.{schema_name}.test3', 'node0'

            select *
             into RISCRASHDATA.{schema_name}.test
            from RISCRASHDATA.{schema_name}.test0;

            EXEC sp_rename 'RISCRASHDATA.{schema_name}.test', 'node'
        """
        assert query.Query.query_renames_table(query_string, schema_name=schema_name) == {f'{schema_name}.node': 'test', f'{schema_name}.node0': 'test3'}

    def test_query_renames_table_from_qry_w_comments(self, schema_name='dbo'):
        query_string = f"""
        -- EXEC sp_rename 'RISCRASHDATA.{schema_name}.old1', 'new1'
        /*  EXEC sp_rename 'RISCRASHDATA.{schema_name}.old2', 'new2' */
        /*
            EXEC sp_rename 'RISCRASHDATA.{schema_name}.old3', 'new3'
        */
            EXEC sp_rename 'RISCRASHDATA.{schema_name}.test', 'node'
        """
        assert query.Query.query_renames_table(query_string, schema_name) == {f'{schema_name}.node': 'test'}

    def test_query_renames_table_logging_not_temp(self, schema_name='dbo'):
        sql.drop_table(schema_name,f'___test___test___')
        assert not db.table_exists('___test___test___', schema=schema_name)
        sql.query(f"create table {schema_name}.___test___test___ (id int);", temp=False)
        assert sql.table_exists('___test___test___', schema=schema_name)
        assert not sql.check_table_in_log('___test___test___', schema=schema_name)

        sql.drop_table(schema_name, '___test___test___2')
        sql.query(f"EXEC sp_rename '{schema_name}.___test___test___', '___test___test___2'")
        assert sql.table_exists('___test___test___2', schema=schema_name)
        assert not sql.check_table_in_log('___test___test___2', schema=schema_name)
        sql.drop_table(schema_name, '___test___test___2')

    def test_query_renames_table_logging_temp(self, schema_name='dbo'):
        sql.drop_table(schema_name, '___test___test___')
        assert not db.table_exists('___test___test___', schema=schema_name)
        sql.query(f"create table {schema_name}.___test___test___ (id int);", temp=True)
        assert sql.table_exists('___test___test___', schema=schema_name)
        assert sql.check_table_in_log('___test___test___', schema=schema_name)

        sql.drop_table(schema_name, '___test___test___2')
        sql.query(f"EXEC sp_rename '{schema_name}.___test___test___', '___test___test___2'")
        assert sql.table_exists('___test___test___2', schema=schema_name)
        assert sql.check_table_in_log('___test___test___2', schema=schema_name)
        sql.drop_table(schema_name, '___test___test___2')


class TestQueryCreatesTablesPgSql():
    def test_query_renames_table_from_qry(self, schema_name='working'):
        query_string = f"""
            ALTER TABLE {schema_name}.test
            RENAME TO node
        """
        assert query.Query.query_renames_table(query_string, 'public') == {f'{schema_name}.node': 'test'}

    def test_query_renames_table_from_qry_quotes(self, schema_name='working'):
        query_string = f"""
            ALTER TABLE "{schema_name}"."test"
            RENAME TO "node"
        """
        assert query.Query.query_renames_table(query_string, 'public') == {f'"{schema_name}"."node"': '"test"'}

    def test_query_renames_table_from_qry_mulitple(self, schema_name='working'):
        query_string = f"""
            ALTER TABLE "{schema_name}"."test"
            RENAME TO "node";

            create table {schema_name}.test_table_error as
            select * from node;

            ALTER TABLE {schema_name}.test2
            RENAME TO node2;
        """
        assert query.Query.query_renames_table(query_string, 'public') == {f'"{schema_name}"."node"': '"test"',
                                                                               f'{schema_name}.node2': 'test2'}

    def test_query_renames_table_from_qry_w_comments(self, schema_name='working'):
        query_string = f"""
        -- ALTER TABLE {schema_name}.old1 rename to new1
        /*  ALTER TABLE old2 rename to new2 */
        /*
             ALTER TABLE {schema_name}.old3 rename to new3
        */
        ALTER TABLE {schema_name}.test
        RENAME TO node
        """
        assert query.Query.query_renames_table(query_string, 'public') == {f'{schema_name}.node': 'test'}


    def test_query_renames_table_logging_not_temp(self, schema_name='working'):
        db.drop_table(schema_name, '___test___test___')
        assert not db.table_exists('___test___test___', schema=schema_name)
        db.query(f"create table {schema_name}.___test___test___ (id int);", temp=False)
        assert db.table_exists('___test___test___', schema=schema_name)
        assert not db.check_table_in_log('___test___test___', schema=schema_name)

        db.drop_table(schema_name, '___test___test___2')
        db.query(f"alter table {schema_name}.___test___test___ rename to ___test___test___2 ")
        assert db.table_exists('___test___test___2', schema=schema_name)
        assert not db.check_table_in_log('___test___test___2', schema=schema_name)
        db.drop_table(schema_name, '___test___test___2')

    def test_query_renames_table_logging_temp(self, schema_name='working'):
        db.drop_table(schema_name, '___test___test___')
        assert not db.table_exists('___test___test___', schema=schema_name)
        db.query(f"create table {schema_name}.___test___test___ (id int);", temp=True)
        assert db.table_exists('___test___test___', schema=schema_name)
        assert db.check_table_in_log('___test___test___', schema=schema_name)

        db.drop_table(schema_name, '___test___test___2')
        db.query(f"alter table {schema_name}.___test___test___ rename to ___test___test___2 ")
        assert db.table_exists('___test___test___2', schema=schema_name)
        assert db.check_table_in_log('___test___test___2', schema=schema_name)
        db.drop_table(schema_name, '___test___test___2')

