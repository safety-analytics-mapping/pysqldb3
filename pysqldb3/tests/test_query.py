"""
Joint testing script for DbConnect and Query classes
"""
import os
from collections.abc import Iterable

import configparser
import pandas as pd

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

test_query_table = 'test_query_table_{user}'.format(user=pg_dbconn.username)


class TestQuery:
    def test_query_returns_correct_pg(self):
        pg_dbconn.drop_table(table_name=test_query_table, schema_name='working')
        assert not pg_dbconn.table_exists(table_name=test_query_table, schema_name='working')

        pg_dbconn.query("""
            create table working.{table} (col1 varchar, col2 varchar, col3 varchar);
            
            insert into working.{table} values ('a', 'b', 'c');
        """.format(table=test_query_table))

        # Assert query successfully executed create table
        assert pg_dbconn.table_exists(table_name=test_query_table, schema_name='working')

        # Assert correctly executed insert
        pg_dbconn.query("""
            select * 
            from working.{table}
        """.format(table=test_query_table))

        last_query = pg_dbconn.queries[-1]

        # Assert values are correct for input
        assert last_query.data[0] == ('a', 'b', 'c')
        assert len(last_query.data) == 1

        assert set(last_query.data_columns) == {'col1', 'col2', 'col3'}
        assert len(last_query.data_columns) == 3

        # Cleanup
        pg_dbconn.drop_table(table_name=test_query_table, schema_name='working')

    def test_query_returns_correct_ms(self):
        ms_dbconn.drop_table(table_name=test_query_table, schema_name='dbo')
        assert not ms_dbconn.table_exists(table_name=test_query_table, schema_name='dbo')

        ms_dbconn.query("""
            create table dbo.{table} (col1 varchar, col2 varchar, col3 varchar);
            insert into dbo.{table} values ('a', 'b', 'c');
        """.format(table=test_query_table))

        # Assert query successfully executed create table
        assert ms_dbconn.table_exists(table_name=test_query_table, schema='dbo')

        # Assert correctly executed insert
        ms_dbconn.query("""
            select * 
            from dbo.{table}
        """.format(table=test_query_table))

        last_query = ms_dbconn.queries[-1]

        # Assert values are correct for input (unicode raw output)
        a, b, c = last_query.data[0]
        assert a == u'a'
        assert b == u'b'
        assert c == u'c'
        assert len(last_query.data) == 1

        assert set(last_query.data_columns) == {u'col1', u'col2', u'col3'}
        assert len(last_query.data_columns) == 3

        # Cleanup
        ms_dbconn.drop_table(table_name=test_query_table, schema_name='dbo')

    def test_successful_query_pg(self):
        """
        Above, we confirm by results that the query was successfully processed through psycopg2 or pyodbc
        by virtue of the target not originally existing and existing only after expected behavior on behalf of the
        query function.

        Here, we take a different approach, confirming that PostgreSql has received the query as intended
        through the built in pg_stat_activity.
        """
        pg_dbconn.drop_table(table_name=test_query_table, schema_name='working')
        assert not pg_dbconn.table_exists(table_name=test_query_table, schema_name='working')

        create_insert_table_string = """
            create table working.{table} (col1 varchar, col2 varchar, col3 varchar);

            insert into working.{table} values ('a', 'b', 'c');
            
            select query 
            from pg_stat_activity
            where usename = '{table}' and query is not null and query != ''
            order by query_start desc;""".format(table=test_query_table)
        pg_dbconn.query(create_insert_table_string)

        # Assert query successfully executed above--thereby also returning its own query string
        # Queries are wrapped in "" and () and contain literal newlines
        all_query_data = [q.data for q in pg_dbconn.queries if q]
        legitimate_query_results = [d for d in all_query_data if d and isinstance(d, Iterable)]
        possibly_relevant_queries = [str(q)[2:-3].replace('\\n', '\n').replace("\\'", "\'") for r in
                                     legitimate_query_results for q in r]

        assert create_insert_table_string in possibly_relevant_queries
        assert pg_dbconn.table_exists(table_name=test_query_table, schema_name='working')
        pg_dbconn.drop_table(table_name=test_query_table, schema_name='working')

    def test_successful_query_ms(self):
        # Unclear of logic right now
        return

    def test_dbconnect_state_create_pg(self):
        pg_dbconn.drop_table(table_name=test_query_table, schema_name='working')
        assert not pg_dbconn.table_exists(table_name=test_query_table, schema_name='working')

        # Reset
        pg_dbconn.tables_created = []

        # Create
        create_table_string = """
            create table working.{table} (col1 varchar, col2 varchar, col3 varchar);
            insert into working.{table} values ('a', 'b', 'c');
        """.format(table=test_query_table)
        pg_dbconn.query(create_table_string)

        # Assert state is in proper shape
        assert pg_dbconn.table_exists(table_name=test_query_table, schema_name='working')
        assert pg_dbconn.tables_created[0] == 'working.' + test_query_table
        assert len(pg_dbconn.tables_created) == 1

        # Cleanup
        pg_dbconn.drop_table(table_name=test_query_table, schema_name='working')

    def test_dbconnect_state_create_ms(self):
        ms_dbconn.drop_table(table_name=test_query_table, schema_name='dbo')
        assert not ms_dbconn.table_exists(table_name=test_query_table, schema_name='dbo')

        # Reset
        ms_dbconn.tables_created = []

        # Create
        create_table_string = """
            create table dbo.{table} (col1 varchar, col2 varchar, col3 varchar);
            insert into dbo.{table} values ('a', 'b', 'c');
        """.format(table=test_query_table)
        ms_dbconn.query(create_table_string)

        # Confirm state has been updated
        assert ms_dbconn.table_exists(table_name=test_query_table, schema_name='dbo')
        assert ms_dbconn.tables_created[0] == f'[dbo].[{test_query_table}]'
        assert len(ms_dbconn.tables_created) == 1

        # Cleanup
        ms_dbconn.drop_table(table_name=test_query_table, schema_name='dbo')

    def test_dbconnect_state_remove_pg(self):
        assert not pg_dbconn.table_exists(table_name=test_query_table, schema_name='working')

        pg_dbconn.tables_dropped = []

        create_table_string = """
            create table working.{table} (col1 varchar, col2 varchar, col3 varchar);
            insert into working.{table} values ('a', 'b', 'c');
        """.format(table=test_query_table)
        pg_dbconn.query(create_table_string)

        assert pg_dbconn.table_exists(table_name=test_query_table, schema_name='working')

        drop_table_string = """
            drop table if exists working.{table};
        """.format(table=test_query_table)
        pg_dbconn.query(drop_table_string)

        assert pg_dbconn.tables_dropped[0] == f'working.{test_query_table}'
        assert len(pg_dbconn.tables_dropped) == 1

    def test_dbconnect_state_remove_ms(self):
        ms_dbconn.drop_table(table_name=test_query_table, schema_name='dbo')

        ms_dbconn.tables_dropped = []
        create_table_string = """
                    create table dbo.{table} (col1 varchar, col2 varchar, col3 varchar);
                    insert into dbo.{table} values ('a', 'b', 'c');
                """.format(table=test_query_table)

        ms_dbconn.query(create_table_string)
        assert ms_dbconn.table_exists(table_name=test_query_table, schema='dbo')

        drop_table_string = """
                    drop table dbo.{table};
                """.format(table=test_query_table)
        ms_dbconn.query(drop_table_string)

        assert ms_dbconn.tables_dropped[0] == 'dbo.' + test_query_table
        assert len(ms_dbconn.tables_dropped) == 1

    def test_dbconnect_state_pg(self):
        # queries
        # data
        return

    def test_dbconnect_state_ms(self):
        # queries
        # data
        return

    def test_query_strict_pg(self):
        # Intentional failing query with strict
        try:
            pg_dbconn.query('select * from nonexistenttable', strict=True)
        except SystemExit:
            # Should result in SystemExit; if so, pass test
            assert True
            return

        # Otherwise, fail test
        assert False

    def test_query_strict_pass_pg(self):
        # Intentional failing query with strict
        pg_dbconn.query('select * from nonexistenttable', strict=False)
        # Should not SysExit

    def test_query_strict_ms(self):
        # Intentional failing query with strict
        try:
            ms_dbconn.query('select * from nonexistenttable', strict=True)
        except SystemExit:
            # Should result in SystemExit; if so, pass test
            assert True
            return

        # Otherwise, fail test
        assert False

    def test_query_strict_pass_ms(self):
        # Intentional failing query with strict
        ms_dbconn.query('select * from nonexistenttable', strict=False)
        # Should not SysExit

    def test_query_permission_pg(self):
        # TODO: permissions overhaul re: PG default
        return

    def test_query_permission_ms(self):
        # TODO: permissions overhaul re: MS default
        return

    def test_query_comment_pg(self):
        pg_dbconn.drop_table(table_name=test_query_table, schema_name='working')

        # Add custom comment
        pg_dbconn.query(query='create table working.{table} (a varchar, b varchar, c varchar)'.format(table=test_query_table),
                        comment='test comment')

        comment_df = pg_dbconn.dfquery("""
        SELECT obj_description(oid) as comment
        FROM pg_class
        WHERE relkind = 'r' and relname='{table}'
        ;
        """.format(table=test_query_table))

        # Assert comment is on table
        assert 'test comment' in comment_df['comment'].iloc[0]

        # Cleanup
        pg_dbconn.drop_table(table_name=test_query_table, schema_name='working')

    def test_query_comment_ms(self):
        ms_dbconn.drop_table(table_name=test_query_table, schema_name='dbo')

        # Add custom comment
        ms_dbconn.query(query='create table dbo.{table} (a varchar, b varchar, c varchar)'.format(table=test_query_table),
                        comment='test comment')

        # Cleanup
        ms_dbconn.drop_table(table_name=test_query_table, schema_name='dbo')

    def test_query_lock_table_pg(self):
        # TODO: block testing
        return

    def test_query_lock_table_ms(self):
        # TODO: block testing
        return

    """
    Notes:
    -Connection state tested in the allow_temp_tables tests
    -Temp table testing in db_connect logging testing  
    -Permission re-work needed for tests
    """


class TestDfQuery:
    def test_output_df_query_pg(self):
        pg_dbconn.drop_table(table_name=test_query_table, schema_name='working')

        # Create table
        pg_dbconn.query(query='create table working.{table} (a varchar, b varchar, c varchar)'.format(table=test_query_table))
        pg_dbconn.query(query="insert into working.{table} values('1', '2', '3')".format(table=test_query_table))

        # Select from new table
        test_df = pg_dbconn.dfquery("""select * from working.{table}""".format(table=test_query_table))
        df_from_data = pd.DataFrame(pg_dbconn.queries[-1].data, columns=pg_dbconn.queries[-1].data_columns)

        # Assert equality
        pd.testing.assert_frame_equal(test_df, df_from_data)

        # Cleanup
        pg_dbconn.drop_table(table_name=test_query_table, schema_name='working')

    def test_output_df_query_ms(self):
        ms_dbconn.drop_table(table_name=test_query_table, schema_name='dbo')

        # Create table
        ms_dbconn.query(query='create table dbo.{table} (a varchar, b varchar, c varchar)'.format(table=test_query_table))
        ms_dbconn.query(query="insert into dbo.{table} values('1', '2', '3')".format(table=test_query_table))

        # Select from new table
        test_df = ms_dbconn.dfquery("""select * from dbo.{table}""".format(table=test_query_table))
        df_from_data = pd.DataFrame(ms_dbconn.queries[-1].data, columns=ms_dbconn.queries[-1].data_columns)

        # Assert equality
        pd.testing.assert_frame_equal(test_df, df_from_data)

        # Cleanup
        ms_dbconn.drop_table(table_name=test_query_table, schema_name='dbo')

    def test_output_df_query_types_pg(self):
        pg_dbconn.drop_table(table_name=test_query_table, schema_name='working')

        # Create table
        pg_dbconn.query(query='create table working.{table} (a int, b int, c int)'.format(table=test_query_table))
        pg_dbconn.query(query="insert into working.{table} values(1, 2, 3)".format(table=test_query_table))

        # Select from new table
        test_df = pg_dbconn.dfquery("""select * from working.{table}""".format(table=test_query_table))
        df_from_data = pd.DataFrame(pg_dbconn.queries[-1].data, columns=pg_dbconn.queries[-1].data_columns)

        # Assert equality
        pd.testing.assert_frame_equal(test_df, df_from_data)

        # Cleanup
        pg_dbconn.drop_table(table_name=test_query_table, schema_name='working')

        ###################

        # Create table (float)
        pg_dbconn.query(query='create table working.{table} (a float, b float, c float)'.format(table=test_query_table))
        pg_dbconn.query(query="insert into working.{table} values(1.0, 2.0, 3.0)".format(table=test_query_table))

        # Select from new table
        test_df = pg_dbconn.dfquery("""select * from working.{table}""".format(table=test_query_table))
        df_from_data = pd.DataFrame(pg_dbconn.queries[-1].data, columns=pg_dbconn.queries[-1].data_columns)

        # Assert equality
        pd.testing.assert_frame_equal(test_df, df_from_data)

        # Cleanup
        pg_dbconn.drop_table(table_name=test_query_table, schema_name='working')

    def test_output_df_query_types_ms(self):
        ms_dbconn.drop_table(table_name=test_query_table, schema_name='dbo')

        # Create table (integer)
        ms_dbconn.query(query='create table dbo.{table} (a int, b int, c int)'.format(table=test_query_table))
        ms_dbconn.query(query="insert into dbo.{table} values(1, 2, 3)".format(table=test_query_table))

        # Select from new table
        test_df = ms_dbconn.dfquery("""select * from dbo.{table}""".format(table=test_query_table))
        df_from_data = pd.DataFrame(ms_dbconn.queries[-1].data, columns=ms_dbconn.queries[-1].data_columns)

        # Assert equality
        pd.testing.assert_frame_equal(test_df, df_from_data)

        # Cleanup
        ms_dbconn.drop_table(table_name=test_query_table, schema_name='dbo')

        ###################

        # Create table (float)
        ms_dbconn.query(query='create table dbo.{table} (a float, b float, c float)'.format(table=test_query_table))
        ms_dbconn.query(query="insert into dbo.{table} values(1.0, 2.0, 3.0)".format(table=test_query_table))

        # Select from new table
        test_df = ms_dbconn.dfquery("""select * from dbo.{table}""".format(table=test_query_table))
        df_from_data = pd.DataFrame(ms_dbconn.queries[-1].data, columns=ms_dbconn.queries[-1].data_columns)

        # Assert equality
        pd.testing.assert_frame_equal(test_df, df_from_data)

        # Cleanup
        ms_dbconn.drop_table(table_name=test_query_table, schema_name='dbo')

    def test_output_df_special_char_pg(self):
        # TODO: fill out with special char fix
        assert True

    def test_output_df_special_char_ms(self):
        # TODO: fill out with special char fix
        assert True
