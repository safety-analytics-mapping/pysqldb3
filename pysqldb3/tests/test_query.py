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

test_query_table = f'test_query_table_{db.user}'


class TestQuery:
    def test_query_returns_correct_pg(self):
        db.drop_table(table=test_query_table, schema='working')
        assert not db.table_exists(table=test_query_table, schema='working')

        db.query(f"""
            create table working.{test_query_table} (col1 varchar, col2 varchar, col3 varchar);
            insert into working.{test_query_table} values ('a', 'b', 'c');
        """)

        # Assert query successfully executed create table
        assert db.table_exists(table=test_query_table, schema='working')

        # Assert correctly executed insert
        db.query(f"""
            select * 
            from working.{test_query_table}
        """)

        last_query = db.queries[-1]

        # Assert values are correct for input
        assert last_query.data[0] == ('a', 'b', 'c')
        assert len(last_query.data) == 1

        assert set(last_query.data_columns) == {'col1', 'col2', 'col3'}
        assert len(last_query.data_columns) == 3

        # Cleanup
        db.drop_table(table=test_query_table, schema='working')

    def test_query_returns_correct_ms(self):
        sql.drop_table(table=test_query_table, schema='dbo')
        assert not sql.table_exists(table=test_query_table, schema='dbo')

        sql.query(f"""
            create table dbo.{test_query_table} (col1 varchar, col2 varchar, col3 varchar);
            insert into dbo.{test_query_table} values ('a', 'b', 'c');
        """)

        # Assert query successfully executed create table
        assert sql.table_exists(table=test_query_table, schema='dbo')

        # Assert correctly executed insert
        sql.query(f"select * from dbo.{test_query_table}")

        last_query = sql.queries[-1]

        # Assert values are correct for input (unicode raw output)
        a, b, c = last_query.data[0]
        assert a == u'a'
        assert b == u'b'
        assert c == u'c'
        assert len(last_query.data) == 1

        assert set(last_query.data_columns) == {u'col1', u'col2', u'col3'}
        assert len(last_query.data_columns) == 3

        # Cleanup
        sql.drop_table(table=test_query_table, schema='dbo')

    def test_successful_query_pg(self):
        """
        Above, we confirm by results that the query was successfully processed through psycopg2 or pyodbc
        by virtue of the target not originally existing and existing only after expected behavior on behalf of the
        query function.

        Here, we take a different approach, confirming that PostgreSql has received the query as intended
        through the built in pg_stat_activity.
        """
        db.drop_table(table=test_query_table, schema='working')
        assert not db.table_exists(table=test_query_table, schema='working')

        create_insert_table_string = f"""
            create table working.{test_query_table} (col1 varchar, col2 varchar, col3 varchar);
            insert into working.{test_query_table} values ('a', 'b', 'c');
            select query 
                from pg_stat_activity
                where usename = '{db.user}' and query is not null and query != ''
            order by query_start desc;"""
        db.query(create_insert_table_string)

        # Assert query successfully executed above--thereby also returning its own query string
        # Queries are wrapped in "" and () and contain literal newlines
        all_query_data = [q.data for q in db.queries if q]
        legitimate_query_results = [d for d in all_query_data if d and isinstance(d, Iterable)]
        possibly_relevant_queries = [str(q)[2:-3].replace('\\n', '\n').replace("\\'", "\'") for r in
                                     legitimate_query_results for q in r]

        assert create_insert_table_string in possibly_relevant_queries
        assert db.table_exists(table=test_query_table, schema='working')

        db.drop_table(table=test_query_table, schema='working')

    def test_successful_query_ms(self):
        # Unclear of logic right now
        return

    def test_dbconnect_state_create_pg(self):
        db.drop_table(table=test_query_table, schema='working')
        assert not db.table_exists(table=test_query_table, schema='working')

        # Reset
        db.tables_created = []

        # Create
        db.query(f"""
            create table working.{test_query_table} (col1 varchar, col2 varchar, col3 varchar);
            insert into working.{test_query_table} values ('a', 'b', 'c');
        """)
        

        # Assert state is in proper shape
        assert db.table_exists(table=test_query_table, schema='working')
        assert db.tables_created[0] == f'working.{test_query_table}'
        assert len(db.tables_created) == 1

        # Cleanup
        db.drop_table(table=test_query_table, schema='working')

    def test_dbconnect_state_create_ms(self):
        sql.drop_table(table=test_query_table, schema='dbo')
        assert not sql.table_exists(table=test_query_table, schema='dbo')

        # Reset
        sql.tables_created = []

        # Create
        sql.query(f"""
            create table dbo.{test_query_table} (col1 varchar, col2 varchar, col3 varchar);
            insert into dbo.{test_query_table} values ('a', 'b', 'c');
        """)

        # Confirm state has been updated
        assert sql.table_exists(table=test_query_table, schema='dbo')
        assert sql.tables_created[0] == f'[dbo].[{test_query_table}]'
        assert len(sql.tables_created) == 1

        # Cleanup
        sql.drop_table(table=test_query_table, schema='dbo')

    def test_dbconnect_state_remove_pg(self):
        assert not db.table_exists(table=test_query_table, schema='working')

        db.tables_dropped = []

        db.query(f"""
            create table working.{test_query_table} (col1 varchar, col2 varchar, col3 varchar);
            insert into working.{test_query_table} values ('a', 'b', 'c');
        """)

        assert db.table_exists(table=test_query_table, schema='working')

        db.query(f"drop table if exists working.{test_query_table};")

        assert db.tables_dropped[0] == f'working.{test_query_table}'
        assert len(db.tables_dropped) == 1

    def test_dbconnect_state_remove_ms(self):
        sql.drop_table(table=test_query_table, schema='dbo')

        sql.tables_dropped = []
        sql.query(f"""
                    create table dbo.{test_query_table} (col1 varchar, col2 varchar, col3 varchar);
                    insert into dbo.{test_query_table} values ('a', 'b', 'c');
                """)
        assert sql.table_exists(table=test_query_table, schema='dbo')

        drop_table_string = f"drop table dbo.{test_query_table};"
        sql.query(drop_table_string)

        assert sql.tables_dropped[0] == f'dbo.{test_query_table}'
        assert len(sql.tables_dropped) == 1

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
            db.query('select * from nonexistenttable', strict=True)
        except SystemExit:
            # Should result in SystemExit; if so, pass test
            assert True
            return

        # Otherwise, fail test
        assert False

    def test_query_strict_pass_pg(self):
        # Intentional failing query with strict
        db.query('select * from nonexistenttable', strict=False)
        # Should not SysExit

    def test_query_strict_ms(self):
        # Intentional failing query with strict
        try:
            sql.query('select * from nonexistenttable', strict=True)
        except SystemExit:
            # Should result in SystemExit; if so, pass test
            assert True
            return

        # Otherwise, fail test
        assert False

    def test_query_strict_pass_ms(self):
        # Intentional failing query with strict
        sql.query('select * from nonexistenttable', strict=False)
        # Should not SysExit

    def test_query_permission_pg(self):
        # TODO: permissions overhaul re: PG default
        return

    def test_query_permission_ms(self):
        # TODO: permissions overhaul re: MS default
        return

    def test_query_comment_pg(self):
        db.drop_table(table=test_query_table, schema='working')

        # Add custom comment
        db.query(query=f'create table working.{test_query_table} (a varchar, b varchar, c varchar)', comment='test comment')

        comment_df = db.dfquery(f"""
        SELECT obj_description(oid) as comment
            FROM pg_class
            WHERE relkind = 'r' and relname='{test_query_table}'
        ;""")

        # Assert comment is on table
        assert 'test comment' in comment_df['comment'].iloc[0]

        # Cleanup
        db.drop_table(table=test_query_table, schema='working')

    def test_query_comment_ms(self):
        sql.drop_table(table=test_query_table, schema='dbo')

        # Add custom comment
        sql.query(query=f'create table dbo.{test_query_table} (a varchar, b varchar, c varchar)', comment='test comment')

        # Cleanup
        sql.drop_table(table=test_query_table, schema='dbo')

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
        db.drop_table(table=test_query_table, schema='working')

        # Create table
        db.query(query=f'create table working.{test_query_table} (a varchar, b varchar, c varchar)')
        db.query(query=f"insert into working.{test_query_table} values('1', '2', '3')")

        # Select from new table
        test_df = db.dfquery(f"""select * from working.{test_query_table}""")
        df_from_data = pd.DataFrame(db.queries[-1].data, columns=db.queries[-1].data_columns)

        # Assert equality
        pd.testing.assert_frame_equal(test_df, df_from_data)

        # Cleanup
        db.drop_table(table=test_query_table, schema='working')

    def test_output_df_query_ms(self):
        sql.drop_table(table=test_query_table, schema='dbo')

        # Create table
        sql.query(query=f'create table dbo.{test_query_table} (a varchar, b varchar, c varchar)')
        sql.query(query=f"insert into dbo.{test_query_table} values('1', '2', '3')")

        # Select from new table
        test_df = sql.dfquery(f"""select * from dbo.{test_query_table}""")
        df_from_data = pd.DataFrame(sql.queries[-1].data, columns=sql.queries[-1].data_columns)

        # Assert equality
        pd.testing.assert_frame_equal(test_df, df_from_data)

        # Cleanup
        sql.drop_table(table=test_query_table, schema='dbo')

    def test_output_df_query_types_pg(self):
        db.drop_table(table=test_query_table, schema='working')

        # Create table
        db.query(query=f'create table working.{test_query_table} (a int, b int, c int)')
        db.query(query=f"insert into working.{test_query_table} values (1, 2, 3)")

        # Select from new table
        test_df = db.dfquery(f"""select * from working.{test_query_table}""")
        df_from_data = pd.DataFrame(db.queries[-1].data, columns=db.queries[-1].data_columns)

        # Assert equality
        pd.testing.assert_frame_equal(test_df, df_from_data)

        # Cleanup
        db.drop_table(table=test_query_table, schema='working')

        ###################

        # Create table (float)
        db.query(query=f'create table working.{test_query_table} (a float, b float, c float)')
        db.query(query=f"insert into working.{test_query_table} values (1.0, 2.0, 3.0)")

        # Select from new table
        test_df = db.dfquery(f"""select * from working.{test_query_table}""")
        df_from_data = pd.DataFrame(db.queries[-1].data, columns=db.queries[-1].data_columns)

        # Assert equality
        pd.testing.assert_frame_equal(test_df, df_from_data)

        # Cleanup
        db.drop_table(table=test_query_table, schema='working')

    def test_output_df_query_types_ms(self):
        sql.drop_table(table=test_query_table, schema='dbo')

        # Create table (integer)
        sql.query(query=f'create table dbo.{test_query_table} (a int, b int, c int)')
        sql.query(query=f"insert into dbo.{test_query_table} values (1, 2, 3)")

        # Select from new table
        test_df = sql.dfquery(f"""select * from dbo.{test_query_table}""")
        df_from_data = pd.DataFrame(sql.queries[-1].data, columns=sql.queries[-1].data_columns)

        # Assert equality
        pd.testing.assert_frame_equal(test_df, df_from_data)

        # Cleanup
        sql.drop_table(table=test_query_table, schema='dbo')

        ###################

        # Create table (float)
        sql.query(query=f'create table dbo.{test_query_table} (a float, b float, c float)')
        sql.query(query=f"insert into dbo.{test_query_table} values (1.0, 2.0, 3.0)")

        # Select from new table
        test_df = sql.dfquery(f"select * from dbo.{test_query_table}")
        df_from_data = pd.DataFrame(sql.queries[-1].data, columns=sql.queries[-1].data_columns)

        # Assert equality
        pd.testing.assert_frame_equal(test_df, df_from_data)

        # Cleanup
        sql.drop_table(table=test_query_table, schema='dbo')

    def test_output_df_special_char_pg(self):
        # TODO: fill out with special char fix
        assert True

    def test_output_df_special_char_ms(self):
        # TODO: fill out with special char fix
        assert True
