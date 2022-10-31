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
                        password=config.get('SQL_DB', 'DB_PASSWORD'), ldap=True)

sql_test_schema = 'testing'
pg_test_schema = 'working'

class TestQueryCreatesTablesSql():
    def test_query_renames_table_from_qry(self, schema_name=sql_test_schema):
        query_string = f"""
            EXEC sp_rename 'RISCRASHDATA.{schema_name}.test_{sql.user}', 'node_{sql.user}'
        """
        assert query.Query.query_renames_table(query_string, schema_name) == {f'{schema_name}.node_{sql.user}': f'test_{sql.user}'}

    def test_query_renames_table_from_qry_no_db(self, schema_name=sql_test_schema):
        query_string = f"""
             EXEC sp_rename '{schema_name}.test_{sql.user}', 'node_{sql.user}'
         """
        assert query.Query.query_renames_table(query_string, schema_name) == {f'{schema_name}.node_{sql.user}': f'test_{sql.user}'}

    def test_query_renames_table_from_qry_just_table(self, schema_name=sql_test_schema):
        query_string = f"""
             EXEC sp_rename 'test_{sql.user}', 'node_{sql.user}'
         """
        assert query.Query.query_renames_table(query_string, schema_name) == {f'{schema_name}.node_{sql.user}': f'test_{sql.user}'}

    def test_query_renames_table_from_qry_server(self, schema_name=sql_test_schema):
        query_string = f"""
             EXEC sp_rename 'dotserver.RISCRASHDATA.{schema_name}.test_{sql.user}', 'node_{sql.user}'
         """
        assert query.Query.query_renames_table(query_string, schema_name) == {f'{schema_name}.node_{sql.user}': f'test_{sql.user}'}

    def test_query_renames_table_from_qry_brackets(self, schema_name=sql_test_schema):
        query_string = f"""
               EXEC sp_rename '[RISCRASHDATA].[{schema_name}].[test_{sql.user}]', '[node_{sql.user}]'
           """
        assert query.Query.query_renames_table(query_string, schema_name) == {f'[{schema_name}].[node_{sql.user}]': f'[test_{sql.user}]'}

    def test_query_renames_table_from_qry_multiple(self, schema_name=sql_test_schema):

        query_string = f"""
            EXEC sp_rename 'RISCRASHDATA.{schema_name}.test3_{sql.user}', 'node0_{sql.user}'

            select *
             into RISCRASHDATA.{schema_name}.test_{sql.user}
            from RISCRASHDATA.{schema_name}.test0_{sql.user};

            EXEC sp_rename 'RISCRASHDATA.{schema_name}.test_{sql.user}', 'node_{sql.user}'
        """

        assert query.Query.query_renames_table(query_string, default_schema=schema_name) == \
        {f"{schema_name}.node_{sql.user}": f"test_{sql.user}", f"{schema_name}.node0_{sql.user}": f"test3_{sql.user}"}

    def test_query_renames_table_from_qry_w_comments(self, schema_name=sql_test_schema):
        query_string = f"""
        -- EXEC sp_rename 'RISCRASHDATA.{schema_name}.old1_{sql.user}', 'new1_{sql.user}'
        /*  EXEC sp_rename 'RISCRASHDATA.{schema_name}.old2_{sql.user}', 'new2_{sql.user}' */
        /*
            EXEC sp_rename 'RISCRASHDATA.{schema_name}.old3_{sql.user}', 'new3_{sql.user}'
        */
            EXEC sp_rename 'RISCRASHDATA.{schema_name}.test_{sql.user}', 'node_{sql.user}'
        """
        assert query.Query.query_renames_table(query_string, schema_name) == {f'{schema_name}.node_{sql.user}': f'test_{sql.user}'}

    def test_query_renames_table_logging_not_temp(self, schema_name=sql_test_schema):
        sql.drop_table(schema_name,f'___test___test___{sql.user}___')
        assert not db.table_exists(f'___test___test___{sql.user}___', schema=schema_name)
        sql.query(f"create table {schema_name}.___test___test___{sql.user}___ (id int);", temp=False)
        assert sql.table_exists(f'___test___test___{sql.user}___', schema=schema_name)
        assert not sql.check_table_in_log(f'___test___test___{sql.user}___', schema=schema_name)

        sql.drop_table(schema_name, f'___test___test___2___{sql.user}___')
        sql.query(f"EXEC sp_rename '{schema_name}.___test___test___{sql.user}___', '___test___test___2___{sql.user}___'")
        assert sql.table_exists(f'___test___test___2___{sql.user}___', schema=schema_name)
        assert not sql.check_table_in_log(f'___test___test___2___{sql.user}___', schema=schema_name)
        sql.drop_table(schema_name, f'___test___test___2___{sql.user}___')

    def test_query_renames_table_logging_temp(self, schema_name=sql_test_schema):
        sql.drop_table(schema_name, f'___test___test___{sql.user}___')
        assert not db.table_exists(f'___test___test___{sql.user}___', schema=schema_name)
        sql.query(f"create table {schema_name}.___test___test___{sql.user}___ (id int);", temp=True)
        assert sql.table_exists(f'___test___test___{sql.user}___', schema=schema_name)
        assert sql.check_table_in_log(f'___test___test___{sql.user}___', schema=schema_name)

        sql.drop_table(schema_name, f'___test___test___2___{sql.user}___')
        sql.query(f"EXEC sp_rename '{schema_name}.___test___test___{sql.user}___', '___test___test___2___{sql.user}___'")
        assert sql.table_exists(f'___test___test___2___{sql.user}___', schema=schema_name)
        assert sql.check_table_in_log(f'___test___test___2___{sql.user}___', schema=schema_name)
        sql.drop_table(schema_name, f'___test___test___2___{sql.user}___')


class TestQueryCreatesTablesPgSql():
    def test_query_renames_table_from_qry(self, schema_name=pg_test_schema):
        query_string = f"""
            ALTER TABLE {schema_name}.test_{db.user}
            RENAME TO node_{db.user}
        """
        assert query.Query.query_renames_table(query_string, 'public') == {f'{schema_name}.node_{db.user}': f'test_{db.user}'}

    def test_query_renames_table_from_qry_quotes(self, schema_name=pg_test_schema):
        query_string = f"""
            ALTER TABLE "{schema_name}"."test_{db.user}"
            RENAME TO "node_{db.user}"
        """
        assert query.Query.query_renames_table(query_string, 'public') == {f'"{schema_name}"."node_{db.user}"': f'"test_{db.user}"'}

    def test_query_renames_table_from_qry_multiple(self, schema_name=pg_test_schema):
        query_string = f"""
            ALTER TABLE "{schema_name}"."test_{db.user}"
            RENAME TO "node_{db.user}";

            CREATE TABLE {schema_name}.test_table_error_{db.user} as
            SELECT * FROM node_{db.user};

            ALTER TABLE {schema_name}.test2_{db.user}
            RENAME TO node2_{db.user};
        """
        assert query.Query.query_renames_table(query_string, 'public') == {f'"{schema_name}"."node_{db.user}"': f'"test_{db.user}"',
                                                                               f'{schema_name}.node2_{db.user}': f'test2_{db.user}'}

    def test_query_renames_table_from_qry_w_comments(self, schema_name=pg_test_schema):
        query_string = f"""
        -- ALTER TABLE {schema_name}.old1_{db.user} rename to new1_{db.user}
        /*  ALTER TABLE old2_{db.user} rename to new2_{db.user} */
        /*
            ALTER TABLE {schema_name}.old3_{db.user} rename to new3_{db.user}
        */
        ALTER TABLE {schema_name}.test_{db.user}
        RENAME TO node_{db.user}
        """
        assert query.Query.query_renames_table(query_string, 'public') == {f'{schema_name}.node_{db.user}': f'test_{db.user}'}


    def test_query_renames_table_logging_not_temp(self, schema_name=pg_test_schema):
        db.drop_table(schema_name, f'___test___test___{db.user}')
        assert not db.table_exists(f'___test___test___{db.user}', schema=schema_name)
        db.query(f"create table {schema_name}.___test___test___{db.user} (id int);", temp=False)
        assert db.table_exists(f'___test___test___{db.user}', schema=schema_name)
        assert not db.check_table_in_log(f'___test___test___{db.user}', schema=schema_name)

        db.drop_table(schema_name, f'___test___test___2___{db.user}')
        db.query(f"alter table {schema_name}.___test___test___{db.user} rename to ___test___test___2___{db.user}")
        assert db.table_exists(f'___test___test___2___{db.user}', schema=schema_name)
        assert not db.check_table_in_log(f'___test___test___2___{db.user}', schema=schema_name)
        db.drop_table(schema_name, f'___test___test___2___{db.user}')

    def test_query_renames_table_logging_temp(self, schema_name=pg_test_schema):
        db.drop_table(schema_name, f'___test___test___{db.user}')
        assert not db.table_exists(f'___test___test___{db.user}', schema=schema_name)
        db.query(f"create table {schema_name}.___test___test___{db.user} (id int);", temp=True)
        assert db.table_exists(f'___test___test___{db.user}', schema=schema_name)
        assert db.check_table_in_log(f'___test___test___{db.user}', schema=schema_name)

        db.drop_table(schema_name, f'___test___test___2___{db.user}')
        db.query(f"alter table {schema_name}.___test___test___{db.user} rename to ___test___test___2___{db.user} ")
        assert db.table_exists(f'___test___test___2___{db.user}', schema=schema_name)
        assert db.check_table_in_log(f'___test___test___2___{db.user}', schema=schema_name)
        db.drop_table(schema_name, f'___test___test___2___{db.user}')

