import os
import random

import configparser
import pandas as pd
import datetime

from .. import pysqldb3 as pysqldb, data_io
from . import helpers

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                       server=config.get('PG_DB', 'SERVER'),
                       database=config.get('PG_DB', 'DB_NAME'),
                       user=config.get('PG_DB', 'DB_USER'),
                       password=config.get('PG_DB', 'DB_PASSWORD'),
                       allow_temp_tables=True)

# should be RIS DB as the second database in db_config
ris = pysqldb.DbConnect(type=config.get('SECOND_PG_DB', 'TYPE'),
                            server=config.get('SECOND_PG_DB', 'SERVER'),
                            database=config.get('SECOND_PG_DB', 'DB_NAME'),
                            user=config.get('SECOND_PG_DB', 'DB_USER'),
                            password=config.get('SECOND_PG_DB', 'DB_PASSWORD'))

sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                        server=config.get('SQL_DB', 'SERVER'),
                        database=config.get('SQL_DB', 'DB_NAME'),
                        user=config.get('SQL_DB', 'DB_USER'),
                        password=config.get('SQL_DB', 'DB_PASSWORD'),
                        allow_temp_tables=True)

sql2 = pysqldb.DbConnect(type=config.get('SECOND_SQL_DB', 'TYPE'),
                        server=config.get('SECOND_SQL_DB', 'SERVER'),
                        database=config.get('SECOND_SQL_DB', 'DB_NAME'),
                        user=config.get('SECOND_SQL_DB', 'DB_USER'),
                        password=config.get('SECOND_SQL_DB', 'DB_PASSWORD'),
                        allow_temp_tables=True)

pg_table_name = f'pg_test_table_{db.user}'
test_pg_to_sql_table = f'tst_pg_to_sql_tbl_{db.user}'
test_pg_to_sql_qry_table = f'tst_pg_to_sql_qry_table_{db.user}'
test_pg_to_sql_qry_spatial_table = f'tst_pg_to_sql_qry_spatial_table_{db.user}'
test_sql_to_pg_qry_table = f'tst_sql_to_pg_qry_table_{db.user}'
test_sql_to_pg_qry_spatial_table = f'tst_sql_to_pg_qry_spatial_table_{db.user}'
test_sql_to_pg_table = f'tst_sql_to_pg_table_{db.user}'
test_pg_to_pg_tbl = f'tst_pg_to_pg_tbl_{db.user}'
test_pg_to_pg_qry_table = f'tst_pg_to_pg_qry_table_{db.user}'

pg_schema = 'working'
sql_schema = 'dbo'

test_org_schema = 'risadmin'
test_dest_schema = 'dbo'
test_sql_to_sql_tbl_to = f'tst_sql_to_sql_to_tbl_{db.user}'
test_sql_to_sql_tbl_from = f'tst_sql_to_sql_from_tbl_{db.user}'


class TestPgToSql:
    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_pg(db)

    def test_pg_to_sql_basic(self):

        """
        Copy an existing Postgres table to MS SQL Server, maintaining the name of the original table
        """

        # assert that output tables are droped
        sql.drop_table(schema=sql.default_schema, table=test_pg_to_sql_table)
        db.drop_table(schema=pg_schema, table=test_pg_to_sql_table)
        assert not sql.table_exists(schema=sql.default_schema, table=test_pg_to_sql_table)
        assert not db.table_exists(schema=pg_schema, table=test_pg_to_sql_table)

        # create table in pg
        db.query(f"""
        create table {pg_schema}.{test_pg_to_sql_table} as

        select *
        from {pg_schema}.{pg_table_name}
        limit 10
        """)

        # Assert table created correctly
        assert db.table_exists(table=test_pg_to_sql_table, schema=pg_schema)

        # Run pg_to_sql
        data_io.pg_to_sql(db, sql, org_table=test_pg_to_sql_table, org_schema=pg_schema, print_cmd=True)

        # Assert exists in sql
        assert sql.table_exists(table=test_pg_to_sql_table)

        # Assert df equality -- some types need to be coerced from the Pandas df for the equality assertion to hold

        pg_df = db.dfquery(f"""
        select *
        from {pg_schema}.{test_pg_to_sql_table}
        order by id
        """).infer_objects()

        sql_df = sql.dfquery(f"""
        select *
        from {test_pg_to_sql_table}
        order by id
        """).infer_objects()

        # Assert that data columns are equal
        shared_non_geom_cols = list(set(pg_df.columns).intersection(set(sql_df.columns)) - {'geom'})
        
        pd.testing.assert_frame_equal(pg_df[shared_non_geom_cols], sql_df[shared_non_geom_cols],
                                      check_dtype=False,
                                      check_exact=False,
                                      check_datetimelike_compat=True)

        # Clean up
        sql.drop_table(schema=sql.default_schema, table=test_pg_to_sql_table)
        db.drop_table(table=test_pg_to_sql_table, schema=pg_schema)


    def test_pg_to_sql_naming(self):
        
        """
        Copy an existing Postgres table to MS SQL Server, and change the name of the copied table.
        """

        # assert that output tables are not created
        dest_name = f'another_tst_name_{db.user}'
        db.drop_table(schema=pg_schema, table=test_pg_to_sql_table)
        sql.drop_table(table=dest_name, schema = sql.default_schema)
        assert not db.table_exists(schema=pg_schema, table=test_pg_to_sql_table)
        assert not sql.table_exists(table=dest_name)

        # create table in pg
        db.query(f"""
           drop table if exists {pg_schema}.{test_pg_to_sql_table};
           create table {pg_schema}.{test_pg_to_sql_table} as
            select *
            from {pg_schema}.{pg_table_name}
            limit 10
        """)

        # Assert table created correctly
        assert db.table_exists(table=test_pg_to_sql_table, schema=pg_schema)

        # Run pg_to_sql
        data_io.pg_to_sql(db, sql,
                          org_schema=pg_schema, org_table=test_pg_to_sql_table,
                          dest_table=dest_name,
                          print_cmd=True)

        # Assert created properly in sql with names
        assert sql.table_exists(table=dest_name, schema=sql.default_schema)

        # Assert df equality -- some types need to be coerced from the Pandas df for the equality assertion to hold
        pg_df = db.dfquery(f"""
                select *
                from {pg_schema}.{test_pg_to_sql_table}
                order by id
        """).infer_objects()

        sql_df = sql.dfquery(f"""
                select *
                from {dest_name}
                order by id
        """).infer_objects()

        # Assert that tables are the same
        shared_non_geom_cols = list(set(pg_df.columns).intersection(set(sql_df.columns)) - {'geom'})
        pd.testing.assert_frame_equal(pg_df[shared_non_geom_cols], sql_df[shared_non_geom_cols],
                                      check_dtype=False,
                                      check_exact=False,
                                      check_datetimelike_compat=True)

        # Clean up
        sql.drop_table(schema=sql.default_schema, table=test_pg_to_sql_table)
        db.drop_table(table=test_pg_to_sql_table, schema=pg_schema)

    def test_pg_to_sql_spatial_table(self):
        """
        Copy a table with spatial data in Postgres to MS SQL Server
        """

        # assert that output tables dropped
        sql.drop_table(schema=sql.default_schema, table=test_pg_to_sql_table)
        db.drop_table(table=test_pg_to_sql_table, schema=pg_schema)
        assert not sql.table_exists(schema=sql.default_schema, table=test_pg_to_sql_table)
        assert not db.table_exists(table=test_pg_to_sql_table, schema=pg_schema)

        # create table in pg
        db.query(f"""
           drop table if exists {pg_schema}.{test_pg_to_sql_table};
           create table {pg_schema}.{test_pg_to_sql_table} as

           select 'hello' as c, st_transform(geom, 4326) as geom
           from {pg_schema}.{pg_table_name}
           limit 10
        """)

        # Assert table created correctly
        assert db.table_exists(table=test_pg_to_sql_table, schema=pg_schema)

        # Assert neither table in SQL Server yet
        assert not sql.table_exists(table=test_pg_to_sql_table)

        # run pg_to_sql, with different spatial flags
        data_io.pg_to_sql(db, sql, org_table=test_pg_to_sql_table, org_schema=pg_schema, dest_table=test_pg_to_sql_table, spatial=True,
                          print_cmd=True)

        # Assert move worked and output tables were created
        assert sql.table_exists(table=test_pg_to_sql_table)

        # assert that the SQL dataframes are the same
        spatial_df = sql.dfquery(f"""
        select c, geom.STX test_lat, geom.STY test_long
        from {test_pg_to_sql_table}
        """).infer_objects()
        
        # Assert df equality -- some types need to be coerced from the Pandas df for the equality assertion to hold
        pg_df = db.dfquery(f"""
        select c, ST_X(geom) test_lat, ST_Y(geom) test_long
        from {pg_schema}.{test_pg_to_sql_table}
        """).infer_objects()

        pd.testing.assert_frame_equal(pg_df, spatial_df,
                                      check_dtype=False,
                                      check_exact=False,
                                      check_datetimelike_compat=True)

        # Clean up
        sql.drop_table(schema=sql.default_schema, table=test_pg_to_sql_table)
        db.drop_table(table=test_pg_to_sql_table, schema=pg_schema)

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(db)
        helpers.clean_up_test_table_sql(sql)


class TestPgtoSqlQry:

    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_pg(db)

    def test_pg_to_sql_qry_basic_table(self):

        """
        Copy a query from Postgres to an output table in SQL
        """

        # drop output tables if they exist
        db.drop_table(schema=pg_schema, table=test_pg_to_sql_qry_table)
        assert not db.table_exists(schema = pg_schema, table = test_pg_to_sql_qry_table)
        sql.drop_table(schema = sql_schema, table = test_pg_to_sql_qry_table)
        assert not sql.table_exists(schema = sql_schema, table = test_pg_to_sql_qry_table)

        # create pg table
        db.query(f"""
                    create table {pg_schema}.{test_pg_to_sql_qry_table} (test_col1 int, test_col2 int);
                    insert into {pg_schema}.{test_pg_to_sql_qry_table} VALUES(1, 2);
                    insert into {pg_schema}.{test_pg_to_sql_qry_table} VALUES(3, 4);
        """)

        # run pg_to_sql_qry
        data_io.pg_to_sql_qry(db, sql, query=
                             f"""
                             select test_col1, test_col2 from {pg_schema}.{test_pg_to_sql_qry_table}
                             """,
                             dest_table=test_pg_to_sql_qry_table,
                             dest_schema = sql_schema,
                             print_cmd=True,
                             spatial=False)

        # Assert pg to sql query was successful (table exists)
        assert sql.table_exists(table=test_pg_to_sql_qry_table, schema = sql_schema)

        # Assert df equality
        pg_df = db.dfquery(f"""
        select test_col1, test_col2 from {pg_schema}.{test_pg_to_sql_qry_table}
        order by test_col1
        """).infer_objects().replace(r'\s+', '', regex=True)

        sql_df = sql.dfquery(f"""
        select test_col1, test_col2 from {sql_schema}.{test_pg_to_sql_qry_table}
        order by test_col1
        """).infer_objects().replace(r'\s+', '', regex=True)

        # Assert
        pd.testing.assert_frame_equal(pg_df, sql_df,
                                      check_dtype=False,
                                      check_column_type=False)

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_pg_to_sql_qry_table)
        sql.drop_table(schema=sql_schema, table=test_pg_to_sql_qry_table)

    def test_pg_to_sql_qry_basic_with_comments_table(self):

        """
        Copy a query full of text comments from Postgres to an output table in SQL.
        """

        # assert that output table dropped
        sql.drop_table(schema=sql_schema, table=test_pg_to_sql_qry_table)
        assert not sql.table_exists(table=test_pg_to_sql_qry_table)

        # run pg_to_sql_qry
        data_io.pg_to_sql_qry(db, sql, query=
                                f"""
                                -- testing out comments
                                select id, test_col1, test_col2 from /* what if there are comments here too */
                                {pg_schema}.{pg_table_name} -- table name
                                order by test_col1
                                -- another comment
                                limit 10; -- limit to 10 rows
                                """,
                              dest_table=test_pg_to_sql_qry_table,
                              dest_schema = sql_schema,
                              print_cmd=True)

        # Assert sql to pg query was successful (table exists)
        assert sql.table_exists(table=test_pg_to_sql_qry_table, schema = sql_schema)

        # Assert df equality
        pg_df = db.dfquery(f"""
        select id, test_col1, test_col2 from {pg_schema}.{pg_table_name}
        order by test_col1
        limit 10
        """).infer_objects().replace(r'\s+', '', regex=True)

        # hardcoded the columns because they go in a different order when uploaded
        sql_df = sql.dfquery(f"""
        select id, test_col1, test_col2 from {sql_schema}.{test_pg_to_sql_qry_table}
        order by test_col1
        """).infer_objects().replace(r'\s+', '', regex=True)

        # Assert that dataframes are equal
        pd.testing.assert_frame_equal(pg_df, sql_df,
                                    check_dtype=False,
                                      check_column_type=False)

        # Cleanup
        sql.drop_table(schema=sql_schema, table=test_pg_to_sql_qry_table)

    def test_pg_to_sql_qry_spatial(self):

        """
        Copy a spatial query from Postgres to SQL
        """

        # confirm that output tables are dropped
        sql.drop_table(schema = sql_schema, table = test_pg_to_sql_qry_spatial_table)
        db.drop_table(schema = pg_schema, table = test_pg_to_sql_qry_spatial_table)
        assert not db.table_exists(table=test_pg_to_sql_qry_spatial_table, schema = pg_schema)
        assert not sql.table_exists(table=test_pg_to_sql_qry_spatial_table, schema = sql_schema)

        # create spatial table
        db.query(f"""
            create table {pg_schema}.{test_pg_to_sql_qry_spatial_table}
                (test_col1 int, test_col2 int, test_geom geometry);
            insert into {pg_schema}.{test_pg_to_sql_qry_spatial_table} (test_col1, test_col2, test_geom)
                 VALUES (1, 2, ST_SetSRID(ST_MAKEPOINT(-71.10434, 42.31506), 2236));
            insert into {pg_schema}.{test_pg_to_sql_qry_spatial_table} (test_col1, test_col2, test_geom)
                VALUES (3, 4, ST_SetSRID(ST_MAKEPOINT(91.2763, 11.9434), 2236));
        """)

        # make sure data is in source
        assert len(db.dfquery(
            f'select test_col1, test_col2, test_geom from {pg_schema}.{test_pg_to_sql_qry_spatial_table}')) == 2

        # run pg_to_sql_qry
        data_io.pg_to_sql_qry(db, sql, query=f"""
                                               SELECT test_col1, test_col2, test_geom --comments within the query
                                                FROM {pg_schema}.{test_pg_to_sql_qry_spatial_table} -- geom here
                                                -- end here""",
                              dest_table= test_pg_to_sql_qry_spatial_table,
                              dest_schema = sql_schema,                      
                              spatial = True
                              )
        
        # check data exists in both dbs
        assert db.table_exists(table=test_pg_to_sql_qry_spatial_table, schema = pg_schema)
        assert sql.table_exists(table=test_pg_to_sql_qry_spatial_table, schema=sql_schema)


        # doing it by long / lat was the only way the data frames would be equivalent
        pg_df = db.dfquery(f"""
        select test_col1, test_col2, ST_X(test_geom) test_lat, ST_Y(test_geom) test_long
        from {pg_schema}.{test_pg_to_sql_qry_spatial_table}
        order by test_col1
        """)

        sql_df = sql.dfquery(f"""
                select test_col1, test_col2, test_geom.STX test_lat, test_geom.STY test_long
                from {sql_schema}.{test_pg_to_sql_qry_spatial_table}
                order by test_col1
        """)

        # check the first 2 columns using assert_frame_equal
        pd.testing.assert_frame_equal(pg_df, sql_df,
                                      check_dtype=False,
                                      check_column_type=False)
        
        # clean up tables
        db.drop_table(schema=pg_schema, table=test_pg_to_sql_qry_spatial_table)
        sql.drop_table(schema=sql_schema, table=test_pg_to_sql_qry_spatial_table)

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(db)
        helpers.clean_up_test_table_sql(sql)

class TestSqlToPgQry:


    def test_sql_to_pg_qry_basic_table(self):
        """
        Copy a spatial query from SQL to Postgres
        """

        # Assert pg table doesn't exist
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)
        assert not db.table_exists(table=test_sql_to_pg_qry_table, schema = pg_schema)

        # create temp table in sql
        sql.query(f"""
            drop table if exists ##{test_sql_to_pg_qry_spatial_table};
            create table ##{test_sql_to_pg_qry_table} (test_col1 int, test_col2 int);
            insert into ##{test_sql_to_pg_qry_table} (test_col1, test_col2) VALUES (1, 2);
            insert into ##{test_sql_to_pg_qry_table} (test_col1, test_col2) VALUES (3, 4);
        """)

        # run sql_to_pg_qry
        data_io.sql_to_pg_qry(sql, db, query=f"select * from ##{test_sql_to_pg_qry_table};",
                              dest_table=test_sql_to_pg_qry_table,
                              dest_schema = pg_schema, print_cmd=True)

        # Assert sql to pg query was successful (table exists)
        assert db.table_exists(table=test_sql_to_pg_qry_table, schema = pg_schema)

        # Assert df equality
        sql_df = sql.dfquery(f"""
        select * from ##{test_sql_to_pg_qry_table}
         order by test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        pg_df = db.dfquery(f"""
        select * from {pg_schema}.{test_sql_to_pg_qry_table}
        order by test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        # Assert that data frames are equal
        pd.testing.assert_frame_equal(sql_df, pg_df.drop(['geom', 'ogc_fid'], axis = 1),
                                    check_dtype=False,
                                      check_column_type=False)

        # assert that permissions are changed to public
        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_sql_to_pg_qry_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

        # assert added to tables created in dest dbo
        assert  db.tables_created[-1] == ('', '', f'{pg_schema}', f'{test_sql_to_pg_qry_table}')

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)
        # don't remove sql temp table as it is used in the subsequent test

    def test_sql_to_pg_qry_basic_with_comments_table(self):
        
        """
        Copy a SQL query full of comments to a Postgres output table
        """
        
        # Assert pg table doesn't exist
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)
        assert not db.table_exists(table=test_sql_to_pg_qry_table)

        # run sql_to_pg_qry
        data_io.sql_to_pg_qry(sql, db, query=f"""
                                            -- comments within the query
                                            select *
                                            -- including here
                                            from ##{test_sql_to_pg_qry_table};
                                                -- end here
                                                /* hello */""",
                              dest_table=test_sql_to_pg_qry_table,
                              dest_schema = pg_schema,
                              print_cmd=True)

        # Assert sql to pg query was successful (table exists)
        assert db.table_exists(table=test_sql_to_pg_qry_table, schema = pg_schema)

        # Assert df equality
        sql_df = sql.dfquery(f"""
        select * from ##{test_sql_to_pg_qry_table}
         order by test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        pg_df = db.dfquery(f"""
        select * from {pg_schema}.{test_sql_to_pg_qry_table}
        order by test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        # Assert the dataframes are equal
        pd.testing.assert_frame_equal(sql_df, pg_df.drop(['geom', 'ogc_fid'], axis = 1),
                                    check_dtype=False,
                                      check_column_type=False)

        # assert that permissions are set to PUBLIC
        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_sql_to_pg_qry_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)

    def test_sql_to_pg_qry_spatial(self):
        """
        Copy a SQL query with spatial data to Postgres
        """

        # assert that output tables are dropped
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_spatial_table)
        assert not db.table_exists(table=test_sql_to_pg_qry_spatial_table, schema = pg_schema)

        # create temp table
        sql.query(f"""
             drop table if exists ##{test_sql_to_pg_qry_spatial_table};
            create table ##{test_sql_to_pg_qry_spatial_table}
                (test_col1 int, test_col2 int, test_geom geometry);
            insert into ##{test_sql_to_pg_qry_spatial_table} (test_col1, test_col2, test_geom)
                VALUES (1, 2, CAST(geometry::Point(70.0890, 12.3456, 2236) AS VARCHAR));
            insert into ##{test_sql_to_pg_qry_spatial_table} (test_col1, test_col2, test_geom)
                VALUES (3, 4, CAST(geometry::Point(91.2763, 11.9434, 2236) AS VARCHAR));
        """)

        # run sql_to_pg_qry
        data_io.sql_to_pg_qry(sql, db, query=f"""
                                                SELECT test_col1, test_col2, CAST(test_geom.STAsText() AS VARCHAR) test_geom --comments within the query
                                                FROM ##{test_sql_to_pg_qry_spatial_table} -- geom here
                                                -- end here""",
                              dest_table= test_sql_to_pg_qry_spatial_table,
                              dest_schema = pg_schema,
                              spatial = False)

        # assert that output table was created
        assert db.table_exists(table=test_sql_to_pg_qry_spatial_table, schema = pg_schema)
        assert len(db.dfquery(f'select test_col1, test_col2, test_geom from {pg_schema}.{test_sql_to_pg_qry_spatial_table}')) == 2

        # assert that the SQL and PG dataframes are equal
        pg_df = db.dfquery(f"""select test_col1::int test_col1, test_col2::int test_col2, test_geom
                                from {pg_schema}.{test_sql_to_pg_qry_spatial_table}""")

        sql_df = sql.dfquery(f""" select test_col1, test_col2, CAST(test_geom.STAsText() AS VARCHAR) test_geom from ##{test_sql_to_pg_qry_spatial_table}""")

        pd.testing.assert_frame_equal(pg_df, sql_df,
                                      check_dtype=False,
                                      check_column_type=False)
        
        # assert that permissions are set to PUBLIC
        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_sql_to_pg_qry_spatial_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

        # clean up
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_spatial_table)
        sql.query(f"""DROP TABLE IF EXISTS ##{test_sql_to_pg_qry_spatial_table}""")


    def test_sql_to_pg_qry_dest_schema(self):
        
        """
        Copy a SQL query to Postgres, and define the destination schema
        """

        # Assert that output tables are dropped
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)
        assert not db.table_exists(schema=pg_schema, table=test_sql_to_pg_qry_table)

        # run sql_to_pg_qry
        data_io.sql_to_pg_qry(sql, db, query=f"""
                                                -- comments within the query
                                                -- middle comment
                                                select * from ##{test_sql_to_pg_qry_table}; /* hi
                                                */
                                                -- end here
                              """,
                              dest_table=test_sql_to_pg_qry_table,
                              dest_schema=pg_schema, print_cmd=True)

        # Assert sql_to_pg_qry successful and correct length
        assert db.table_exists(schema=pg_schema, table=test_sql_to_pg_qry_table)
        assert len(db.dfquery(f'select * from {pg_schema}.{test_sql_to_pg_qry_table}')) == 2

        # Assert df equality
        sql_df = sql.dfquery(f"""
            select * from ##{test_sql_to_pg_qry_table}
            order by test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        pg_df = db.dfquery(f"""
        select * from {pg_schema}.{test_sql_to_pg_qry_table}
             order by test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        # Assert that hte data frames are equal
        pd.testing.assert_frame_equal(sql_df, pg_df.drop(['ogc_fid', 'geom'], axis = 1), check_column_type=False,
                                      check_dtype=False)

        # assert that permissions are set to PUBLIC
        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_sql_to_pg_qry_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

        # Clean up
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)

    def test_sql_to_pg_qry_no_dest_table_input(self):

        """
        Copy a SQL query to a Postgres table where the destination table name has not been defined.
        The name should default to '_{user}_{date}'
        """
        
        # Assert output tables dropped
        db.drop_table(schema=pg_schema, table=f"_{db.user}_{datetime.datetime.now().strftime('%Y%m%d%H%M')}")
        assert not db.table_exists(schema=pg_schema, table = f"_{db.user}_{datetime.datetime.now().strftime('%Y%m%d%H%M')}")

        # sql_to_pg_qry
        data_io.sql_to_pg_qry(sql, db, query=f"""
                              select * from ##{test_sql_to_pg_qry_table}; /* hi */""",
                              # no dest_table input
                              dest_schema=pg_schema,
                              print_cmd=True,
                              spatial = False)
        
        # assert that the table exists
        assert db.table_exists(schema = pg_schema,
                               table = f"_{db.user}_{datetime.datetime.now().strftime('%Y%m%d%H%M')}")

        # assert that sql and pg tables are equal
        sql_df = sql.dfquery(f"""
            select * from ##{test_sql_to_pg_qry_table}
            order by test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        pg_df = db.dfquery(f"""
        select * from {pg_schema}._{db.user}_{datetime.datetime.now().strftime('%Y%m%d%H%M')}
             order by test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        print(sql_df)
        print(pg_df)

        pd.testing.assert_frame_equal(sql_df, pg_df.drop(['ogc_fid'], axis = 1), check_column_type=False,
                                      check_dtype=False)

        # drop tables
        db.drop_table(schema = pg_schema, table =  f"_{db.user}_{datetime.datetime.now().strftime('%Y%m%d%H%M')}")

    def test_sql_to_pg_qry_empty_query_error(self):
        """
        Copy an empty SQL query to Postgres, which should lead to an error.
        """

        # Assert output tables dropped
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)
        assert not db.table_exists(schema=pg_schema, table=test_sql_to_pg_qry_table)

        # sql_to_pg_qry
        try:
            data_io.sql_to_pg_qry(sql, db, query=f"",
                              dest_table = f'{test_sql_to_pg_qry_table}',
                              dest_schema=sql_schema, print_cmd=True)
        
        except:
            Failed = True
        
        assert Failed == True

        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)

    def test_sql_to_pg_qry_empty_wrong_layer_error(self):
        return

    def test_sql_to_pg_qry_empty_overwrite_error(self):
        """
        Return an error if a SQL query creates a PG table with the same name as an existing table.
        """

        # Assert output tables dropped
        db.drop_table(table=test_sql_to_pg_qry_table, schema = pg_schema)
        assert not db.table_exists(table=test_sql_to_pg_qry_table, schema = pg_schema)

        # create an existing pg table
        db.query(f"""
                    create table {pg_schema}.{test_sql_to_pg_qry_table} (test_col1 int, test_col2 int);
                    insert into {pg_schema}.{test_sql_to_pg_qry_table} VALUES(1, 2);
                    insert into {pg_schema}.{test_sql_to_pg_qry_table} VALUES(3, 4);
        """)

        # copy the temp table from SQL query over
        try:
            data_io.sql_to_pg_qry(sql, db, query=f"select * from ##{test_sql_to_pg_qry_table}",
                              dest_table = f'{test_sql_to_pg_qry_table}',
                              dest_schema=sql_schema, print_cmd=True)
        
        except:
            Failed = True
        
        # assert that an error occurred
        assert Failed == True

        # drop tables
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)
        sql.query(f"""drop table if exists ##{test_sql_to_pg_qry_table}""") # run query for global temp table

    
    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(sql)
        helpers.clean_up_test_table_pg(db)


class TestSqlToPg:
    def test_sql_to_pg_basic_table(self):
        """
        Copy a SQL table to Postgres
        """

        # Assert output tables dropped
        db.drop_table(pg_schema, test_sql_to_pg_table)
        assert not db.table_exists(table=test_sql_to_pg_table, schema = pg_schema)

        # Add test_table
        sql.drop_table(schema=sql_schema, table=test_sql_to_pg_table)
        sql.query(f"""
         create table {sql_schema}.{test_sql_to_pg_table} (test_col1 int, test_col2 int);
         insert into {sql_schema}.{test_sql_to_pg_table} VALUES(1, 2);
         insert into {sql_schema}.{test_sql_to_pg_table} VALUES(3, 4);
         """)

        # Run Sql_to_pg
        data_io.sql_to_pg(sql, db, org_table=test_sql_to_pg_table, org_schema=sql_schema, dest_table=test_sql_to_pg_table,
                          dest_schema = pg_schema, print_cmd=True)

        # Assert sql_to_pg was successful; table exists in pg
        assert db.table_exists(table=test_sql_to_pg_table, schema=pg_schema)

        # Assert df equality
        sql_df = sql.dfquery(f"""
        select * from {sql_schema}.{test_sql_to_pg_table} order by test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        pg_df = db.dfquery(f"""
        select * from {pg_schema}.{test_sql_to_pg_table} order by test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        # Assert that the dataframes are equal
        pd.testing.assert_frame_equal(sql_df, pg_df.drop(['geom', 'ogc_fid'], axis=1),
                                      check_dtype=False, check_column_type=False)

        # assert that permissions were set to PUBLIC
        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_sql_to_pg_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

        # assert added to tables created in dest dbo
        assert db.tables_created[-1] == ('', '', f'{pg_schema}', f'{test_sql_to_pg_table}')

        # Clean up
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_table)
        sql.drop_table(schema=sql_schema, table=test_sql_to_pg_table)

    def test_sql_to_pg_dest_schema_name(self):
        """
        Copy a SQL table to PG and define the destination schema
        """

        # Assert table doesn't exist in pg
        db.drop_table(pg_schema, test_sql_to_pg_table)
        assert not db.table_exists(schema=pg_schema, table=test_sql_to_pg_table)

        # Add test_table in SQL
        sql.drop_table(schema=sql_schema, table=test_sql_to_pg_table)
        sql.query(f"""
            create table {sql_schema}.{test_sql_to_pg_table} (test_col1 int, test_col2 int);
         insert into {sql_schema}.{test_sql_to_pg_table} VALUES(1, 2);
         insert into {sql_schema}.{test_sql_to_pg_table} VALUES(3, 4);
         """)

        # Sql_to_pg
        data_io.sql_to_pg(sql, db, org_table=test_sql_to_pg_table, org_schema=sql_schema, dest_table=test_sql_to_pg_table,
                          dest_schema=pg_schema, print_cmd=True)

        # Assert sql_to_pg was successful; table exists in pg
        assert db.table_exists(schema=pg_schema, table=test_sql_to_pg_table)

        # Assert df equality
        sql_df = sql.dfquery(f"""
        select * from {sql_schema}.{test_sql_to_pg_table} order by test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        pg_df = db.dfquery(f"""
        select * from {pg_schema}.{test_sql_to_pg_table} order by test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        # Assert that the 2 dataframes are equal
        pd.testing.assert_frame_equal(sql_df, pg_df.drop(['geom', 'ogc_fid'], axis=1),
                                      check_dtype=False,
                                      check_column_type=False)

        # asser that permissions were granted to PUBLIC
        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_sql_to_pg_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

        # Clean up output tables
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_table)
        sql.drop_table(schema=sql_schema, table=test_sql_to_pg_table)

    def test_sql_to_pg_org_schema_name(self):
        
        """
        Copy a non-dbo SQL table to Postgres
        """
        
        # drop output table if it exists
        sql.drop_table(schema=test_org_schema, table=test_sql_to_pg_table)
        assert not db.table_exists(schema=test_org_schema, table=test_sql_to_pg_table)

        # create table in SQL non-dbo schema
        sql.query(f"""
            create table {test_org_schema}.{test_sql_to_pg_table} (test_col1 int, test_col2 int);
         insert into {test_org_schema}.{test_sql_to_pg_table} VALUES(1, 2);
         insert into {test_org_schema}.{test_sql_to_pg_table} VALUES(3, 4);
         """)
        
        # copy over table using sql_to_pg
        data_io.sql_to_pg(sql, db, org_table=test_sql_to_pg_table, org_schema= test_org_schema, dest_table=test_sql_to_pg_table,
                          dest_schema=pg_schema, print_cmd=True)

        # assert that the resulting table exists
        assert db.table_exists(schema = pg_schema, table = test_sql_to_pg_table)

        # clean tables
        db.drop_table(schema = pg_schema, table = test_sql_to_pg_table)
        sql.drop_table(schema = test_org_schema, table = test_sql_to_pg_table)

    def test_sql_to_pg_spatial(self):
        # TODO: when adding spatial features like SRID via a_srs, test spatial
        return

    def test_sql_to_pg_wrong_layer_error(self):
        return

    def test_sql_to_pg_error(self):
        return
    
    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(sql)
        helpers.clean_up_test_table_pg(db)


class TestPgToPg:
    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_pg(db)

    def test_pg_to_pg_basic_table(self):
        """
        Copy a PG table to another Postgres database, maintaining the same table name
        """

        # drop output tables if they exist
        db.drop_table(schema=pg_schema, table=test_pg_to_pg_tbl)
        ris.drop_table(schema=pg_schema, table=test_pg_to_pg_tbl)
        assert not db.table_exists(schema=pg_schema, table=test_pg_to_pg_tbl)
        assert not ris.table_exists(schema=pg_schema, table=test_pg_to_pg_tbl)

        # Create table
        db.query(f"""
            create table {pg_schema}.{test_pg_to_pg_tbl} as
            select *
            from {pg_schema}.{pg_table_name}
            limit 10
        """)

        # Assert tables don't already exist in destination
        assert db.table_exists(schema=pg_schema, table=test_pg_to_pg_tbl)
        assert not ris.table_exists(schema=pg_schema, table=test_pg_to_pg_tbl)

        # pg_to_pg
        data_io.pg_to_pg(db, ris, org_schema=pg_schema, org_table=test_pg_to_pg_tbl, dest_schema=pg_schema,
                         print_cmd=True)

        # Assert pg_to_pg successful
        assert db.table_exists(schema=pg_schema, table=test_pg_to_pg_tbl)
        assert ris.table_exists(schema=pg_schema, table=test_pg_to_pg_tbl)

        # Assert db equality
        risdf = ris.dfquery(f"""
              select *
              from {pg_schema}.{test_pg_to_pg_tbl}
          """).infer_objects()

        dbdf = db.dfquery(f"""
              select *
              from {pg_schema}.{test_pg_to_pg_tbl}
          """).infer_objects()

        # Assert
        pd.testing.assert_frame_equal(risdf.drop(['geom', 'ogc_fid'], axis=1), dbdf.drop(['geom'], axis=1),
                                      check_exact=False
                                      )

        # Assert that the permissions is in PUBLIC
        assert ris.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_pg_to_pg_tbl}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

        # assert added to tables created in dest dbo
        assert ris.tables_created == [('', '', f'{pg_schema}', f'{test_pg_to_pg_tbl}')]

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_pg_to_pg_tbl)
        ris.drop_table(schema=pg_schema, table=test_pg_to_pg_tbl)

    def test_pg_to_pg_basic_name_table(self):
        """
        Copy a PG table to another Postgres database, changing the destination table name
        """

        # drop table 
        test_pg_to_pg_tbl_other = test_pg_to_pg_tbl + '_another_name'
        db.drop_table(schema=pg_schema, table=test_pg_to_pg_tbl)
        ris.drop_table(schema=pg_schema, table=test_pg_to_pg_tbl_other)
        assert not db.table_exists(schema=pg_schema, table=test_pg_to_pg_tbl)
        assert not ris.table_exists(schema=pg_schema, table=test_pg_to_pg_tbl_other)


        # Create table for testing
        db.query(f"""
            create table {pg_schema}.{test_pg_to_pg_tbl} as
            select *
            from {pg_schema}.{pg_table_name}
            limit 10
        """)

        # Assert final table doesn't already exist
        assert not ris.table_exists(schema=pg_schema, table=test_pg_to_pg_tbl_other)
        assert db.table_exists(schema=pg_schema, table=test_pg_to_pg_tbl)

        # pg_to_pg
        data_io.pg_to_pg(db, ris, org_schema=pg_schema, org_table=test_pg_to_pg_tbl,
                         dest_schema=pg_schema, dest_table=test_pg_to_pg_tbl_other, print_cmd=True)

        # Assert pg_to_pg was successful
        assert db.table_exists(schema=pg_schema, table=test_pg_to_pg_tbl)
        assert ris.table_exists(schema=pg_schema, table=test_pg_to_pg_tbl_other)

        # Assert db equality by first creating pandas objects of the tables
        risdf = ris.dfquery(f"""
            select *
            from {pg_schema}.{test_pg_to_pg_tbl_other}
        """).infer_objects()

        dbdf = db.dfquery(f"""
            select *
            from {pg_schema}.{test_pg_to_pg_tbl}
        """).infer_objects()

        # Assert that the two tables are the same
        pd.testing.assert_frame_equal(risdf.drop(['geom', 'ogc_fid'], axis=1), dbdf.drop(['geom'], axis=1),
                                      check_exact=False)

        # Assert that the permissions are in PUBLIC
        assert ris.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_pg_to_pg_tbl_other}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"
        
        # Clean up output tables
        ris.drop_table(schema=pg_schema, table=test_pg_to_pg_tbl_other)
        db.drop_table(schema=pg_schema, table=test_pg_to_pg_tbl)

    def test_pg_to_pg_src_table_nonexist(self):

        """
        Copy a table whose output name already exists in the destination database. It should overwrite it.
        """

        # make sure output tables do not exist
        db.drop_table(schema = pg_schema, table = test_pg_to_pg_tbl + '_2')
        ris.drop_table(schema = pg_schema, table = test_pg_to_pg_tbl)
        assert not db.table_exists(schema=pg_schema, table=test_pg_to_pg_tbl + '_2')
        assert not ris.table_exists(schema=pg_schema, table=test_pg_to_pg_tbl)

        # try to copy a table that doesn't exist
        try:
            data_io.pg_to_pg(from_pg=db,
                        to_pg = ris,
                        org_schema = pg_schema,
                        org_table = test_pg_to_pg_tbl + '_2',
                        dest_schema = pg_schema,
                        dest_table = test_pg_to_pg_tbl,
                        print_cmd = True)

        except:
            Failed = True

        # assert that a non-existent table cannot be copied
        assert Failed == True

        # clean tables
        ris.drop_table(schema = pg_schema, table = test_pg_to_pg_tbl)


    def test_pg_to_pg_src_schema_nonexist(self):

        """
        Copy a table from a schema that doesn't exist to yield an error.
        """

        # remove output table if it exists
        db.drop_table(schema = pg_schema, table = test_pg_to_pg_tbl)
        assert not db.table_exists(schema = pg_schema, table = test_pg_to_pg_tbl)

        # create a table in the origin schema
        db.query(f"""

                                create table {pg_schema}.{test_pg_to_pg_tbl} (test_col1 int, test_col2 int);
                                insert into {pg_schema}.{test_pg_to_pg_tbl} (test_col1, test_col2) VALUES (1, 2);
                                insert into {pg_schema}.{test_pg_to_pg_tbl} (test_col1, test_col2) VALUES (3, 4);

                                select * from {pg_schema}.{test_pg_to_pg_tbl};
                                """)

        # try to copy a table with the same name but under a different schema that doesn't exist
        try:
            data_io.pg_to_pg(from_pg = db,
                        to_pg = ris,
                        org_schema = pg_schema + '_2',
                        org_table = test_pg_to_pg_tbl,
                        dest_schema = pg_schema,
                        dest_table = test_pg_to_pg_tbl,
                        print_cmd = True)

        except:
            Failed = True

        # assert that the new table cannot be created in a non-existent schema
        assert Failed == True

        # drop tables
        db.drop_table(schema = pg_schema, table = test_pg_to_pg_tbl) # in case it somehow got created
        ris.drop_table(schema = pg_schema, table = test_pg_to_pg_tbl)

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(db)
        db.cleanup_new_tables()


class TestPgToPgQry:

    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_pg(db)
    
    def test_pg_to_pg_qry_basic_table(self):
        """
        Run a PG query in one database and copy the output table to another PG database
        """

        # Assert pg table doesn't exist
        db.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)
        assert not db.table_exists(table=test_pg_to_pg_qry_table, schema = pg_schema)

        # Add test_table
        ris.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)
        ris.query(f"""
                        create table {pg_schema}.{test_pg_to_pg_qry_table} (test_col1 int, test_col2 int);
                        insert into {pg_schema}.{test_pg_to_pg_qry_table} VALUES(1, 2);
                        insert into {pg_schema}.{test_pg_to_pg_qry_table} VALUES(3, 4);
        """)

        # sql_to_pg_qry
        data_io.pg_to_pg_qry(ris, db, query=
                             f"""
                             select * from {pg_schema}.{test_pg_to_pg_qry_table}
                             """,
                             dest_table=test_pg_to_pg_qry_table,
                             dest_schema = pg_schema, print_cmd=True, spatial=False)

        # Assert sql to pg query was successful (table exists)
        assert db.table_exists(table=test_pg_to_pg_qry_table, schema = pg_schema)

        # Assert df equality
        org_pg_df = ris.dfquery(f"""
        select * from {pg_schema}.{test_pg_to_pg_qry_table}
        order by test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        pg_df = db.dfquery(f"""
        select * from {pg_schema}.{test_pg_to_pg_qry_table}
        order by test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        org_pg_df.columns = [c.lower() for c in list(org_pg_df.columns)]

        # Assert
        pd.testing.assert_frame_equal(org_pg_df, pg_df.drop(['ogc_fid'], axis=1), check_dtype=False,
                                      check_column_type=False)

        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_pg_to_pg_qry_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

        # assert added to tables created in dest dbo
        assert db.tables_created[-1] == ('', '', f'{pg_schema}', f'{test_pg_to_pg_qry_table}')

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)
        ris.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)

    def test_pg_to_pg_qry_basic_with_comments_table(self):
        """
        Run a PG query in one database and copy the output table to another PG database.
        Include several comments within the query.
        """

        # Assert pg table doesn't exist
        db.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)
        assert not db.table_exists(table=test_pg_to_pg_qry_table, schema = pg_schema)

        # Add test_table
        ris.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)
        ris.query(f"""
        create table {pg_schema}.{test_pg_to_pg_qry_table} (test_col1 int, test_col2 int);
        insert into {pg_schema}.{test_pg_to_pg_qry_table} VALUES(1, 2);
        insert into {pg_schema}.{test_pg_to_pg_qry_table} VALUES(3, 4);
        """)

        # sql_to_pg_qry
        data_io.pg_to_pg_qry(ris, db, query=f"""
                             -- beginning of query
                             select *
                             -- middle of query
                             from {pg_schema}.{test_pg_to_pg_qry_table}
                             -- end of query
                             """,
                             dest_table=test_pg_to_pg_qry_table,
                             dest_schema = pg_schema, print_cmd=True, spatial=False)

        # Assert sql to pg query was successful (table exists)
        assert db.table_exists(table=test_pg_to_pg_qry_table, schema = pg_schema)

        # Assert df equality
        org_pg_df = ris.dfquery(f"""
            select * from {pg_schema}.{test_pg_to_pg_qry_table}
            order by test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        pg_df = db.dfquery(f"""
            select * from {pg_schema}.{test_pg_to_pg_qry_table}
            order by test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        org_pg_df.columns = [c.lower() for c in list(org_pg_df.columns)]

        # Assert
        pd.testing.assert_frame_equal(org_pg_df, pg_df.drop(['ogc_fid'], axis=1), check_dtype=False,
                                      check_column_type=False)

        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_pg_to_pg_qry_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)
        ris.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)

    def test_pg_to_pg_qry_dest_schema(self):
        """
        Copy a PG table to another PG database, changing the destination schema
        """

        # Assert doesn't exist already
        db.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)
        assert not db.table_exists(schema=pg_schema, table=test_pg_to_pg_qry_table)

        # Add test_table
        ris.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)
        ris.query(f"""
        create table {pg_schema}.{test_pg_to_pg_qry_table} (test_col1 int, test_col2 int);
        insert into {pg_schema}.{test_pg_to_pg_qry_table} VALUES(1, 2);
        insert into {pg_schema}.{test_pg_to_pg_qry_table} VALUES(3, 4);
        """)

        # sql_to_pg_qry
        data_io.pg_to_pg_qry(ris, db, query=f"""select *
                                                from {pg_schema}.{test_pg_to_pg_qry_table}""",
                             dest_table=test_pg_to_pg_qry_table, dest_schema=pg_schema, print_cmd=True)

        # Assert sql_to_pg_qry successful and correct length
        assert db.table_exists(schema=pg_schema, table=test_pg_to_pg_qry_table)
        assert len(db.dfquery(f'select * from {pg_schema}.{test_pg_to_pg_qry_table}')) == 2

        # Assert df equality
        org_pg_df = ris.dfquery(f"""
        select * from {pg_schema}.{test_pg_to_pg_qry_table}
        order by test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        pg_df = db.dfquery(f"""
        select * from {pg_schema}.{test_pg_to_pg_qry_table}
        order by test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        org_pg_df.columns = [c.lower() for c in list(org_pg_df.columns)]

        # Assert
        pd.testing.assert_frame_equal(org_pg_df, pg_df.drop(['ogc_fid'], axis=1), check_column_type=False,
                                      check_dtype=False)

        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_pg_to_pg_qry_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)
        ris.drop_table(schema = pg_schema, table = test_pg_to_pg_qry_table)

    def test_pg_to_pg_qry_no_dest_table_input(self):
        """
        Run a SQL query in one database and copy the output table to PG database
        with no defined destination table name.
        """
        # drop output tables if they already exist
        db.drop_table(schema = pg_schema, table =  f"_{db.user}_{datetime.datetime.now().strftime('%Y%m%d%H%M')}")
        ris.drop_table(schema = pg_schema, table = test_pg_to_pg_qry_table)
        assert not db.table_exists(schema = pg_schema, table =  f"_{db.user}_{datetime.datetime.now().strftime('%Y%m%d%H%M')}")
        assert not ris.table_exists(schema = pg_schema, table = test_pg_to_pg_qry_table)
        
        # create postgres table query to be copied and run pg_to_pg_qry
        data_io.pg_to_pg_qry(ris, db, query=f"""
                create table {pg_schema}.{test_pg_to_pg_qry_table} (test_col1 int, test_col2 int);
                insert into {pg_schema}.{test_pg_to_pg_qry_table} VALUES(1, 2);
                insert into {pg_schema}.{test_pg_to_pg_qry_table} VALUES(3, 4);
                
                select * from {pg_schema}.{test_pg_to_pg_qry_table};""",
                dest_schema=pg_schema, print_cmd=True)

        # assert that the output table exists
        assert db.table_exists(schema = pg_schema, table = f"_{db.user}_{datetime.datetime.now().strftime('%Y%m%d%H%M')}")

        # drop output tables
        db.drop_table(schema = pg_schema, table =  f"_{db.user}_{datetime.datetime.now().strftime('%Y%m%d%H%M')}")
        ris.drop_table(schema = pg_schema, table = test_pg_to_pg_qry_table)


    def test_pg_to_pg_qry_empty_query_error(self):
        """
        Run an empty SQL query and copy the output to Postgres.
        Should yield an error.
        """

        # drop output tables tables 
        db.drop_table(schema = pg_schema, table = test_pg_to_pg_qry_table)
        ris.drop_table(schema = pg_schema, table = test_pg_to_pg_qry_table)
        assert not db.table_exists(schema = pg_schema, table = test_pg_to_pg_qry_table)
        assert not ris.table_exists(schema = pg_schema, table = test_pg_to_pg_qry_table)
        
        # create empty postgres table query to be copied
        try:
            data_io.pg_to_pg_qry(ris, db, query=f"",
                dest_schema=pg_schema, dest_table =  test_pg_to_pg_qry_table, print_cmd=True)
        except:
            Failed = True

        # assert that pg_to_pg_qry failed
        assert Failed == True
        # assert that no table was created
        assert db.table_exists(schema = pg_schema, table = test_pg_to_pg_qry_table) == False

        # drop tables if they somehow got written
        db.drop_table(schema = pg_schema, table = test_pg_to_pg_qry_table)
        ris.drop_table(schema = pg_schema, table = test_pg_to_pg_qry_table)
        

    def test_pg_to_pg_qry_empty_wrong_layer_error(self):
        return

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(db)

class TestSqlToSqlQry:

    def test_sql_to_sql_basic_table(self):
        """
        Copy a SQL table to another SQL database
        """

        # drop output tables if they exist
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)
        sql.drop_table(schema = test_org_schema, table = test_sql_to_sql_tbl_from)
        assert not sql2.table_exists(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)
        assert not sql.table_exists(schema = test_org_schema, table = test_sql_to_sql_tbl_from)

        # create table
        sql2.query(f"""
                    create table {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1 int, test_col2 int, test_col3 varchar(4));
                    insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3) VALUES (1, 2, 'ABCD');
                    insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3) VALUES (3, 4, 'DE*G');
                    insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3) VALUES (5, 60, 'HIj_');
                    insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3) VALUES (-3, 24271, 'zhyw');
                    """)

        # run sql_to_sl
        data_io.sql_to_sql(from_sql = sql,
                        to_sql = sql2,
                        org_schema = test_org_schema,
                        org_table = test_sql_to_sql_tbl_from,
                        dest_schema = test_dest_schema,
                        dest_table = test_sql_to_sql_tbl_to,
                        spatial = False,
                        print_cmd = True
                        )

        # check that the output table exists
        assert sql2.table_exists(table = test_sql_to_sql_tbl_to, schema = test_dest_schema)

        # check that the tables are the same.
        # list out columns to avoid the ogr_fid and null fields in the output table
        first_table = sql2.dfquery(f"select * from {test_dest_schema}.{test_sql_to_sql_tbl_to};").drop('ogr_fid', axis=1)

        second_table = sql.dfquery(f"select * from {test_org_schema}.{test_sql_to_sql_tbl_from}")

        # check for the same dimensions and columns
        assert first_table.shape == second_table.shape
        assert list(first_table.columns) == list(second_table.columns)
        assert first_table.equals(second_table) # this fails because there are dtype issues

        # assert added to tables created in dest dbo
        assert sql2.tables_created[-1] == (sql2.server, sql2.database, test_dest_schema, test_sql_to_sql_tbl_to)

        # drop ouptut table
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)

    def test_sql_to_sql_qry(self):

        """
        Run a SQL query and copy the output to a different SQL database.
        """
        # drop output table if created
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)
        sql.drop_table(schema = test_org_schema, table = test_sql_to_sql_tbl_from)
        assert not sql2.table_exists(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)
        assert not sql.table_exists(schema = test_org_schema, table = test_sql_to_sql_tbl_from)

        # create original sql table
        sql2.query(f"""
                    create table {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1 int, test_col2 int, test_col3 varchar(4));
                    insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3) VALUES (1, 2, 'ABCD');
                    insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3) VALUES (3, 4, 'DE*G');
                    insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3) VALUES (5, 60, 'HIj_');
                    insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3) VALUES (-3, 24271, 'zhyw');
                    """)

        # run sql_to_sql_qry
        data_io.sql_to_sql_qry(from_sql = sql,
                                to_sql = sql2,
                                org_schema = test_org_schema,
                                qry = f"select top (3) * from {test_org_schema}.{test_sql_to_sql_tbl_from}",
                        dest_schema = test_dest_schema,
                        dest_table = test_sql_to_sql_tbl_to,
                        spatial = False,
                        print_cmd = True)

        # check that output table was created
        assert sql2.table_exists(table = test_sql_to_sql_tbl_to, schema = test_dest_schema)

        # check that columns between the 2 tables 
        test_dest_df = sql2.dfquery(f"select * from {test_dest_schema}.{test_sql_to_sql_tbl_to}").drop('ogr_fid', axis=1)
        test_org_df = sql.dfquery(f"select top (3) * from {test_org_schema}.{test_sql_to_sql_tbl_from}")
        assert list(test_dest_df.columns) == list(test_org_df.columns)

        # check the dimensions between the tables
        assert test_dest_df.shape == test_org_df.shape

        # drop table
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)
        sql.drop_table(schema = test_org_schema, table = test_sql_to_sql_tbl_from)

    def test_sql_to_sql_tbl_exists(self):

        """
        Copy a table whose name already exists in the destination database. It will overwrite the table.
        """

        # drop any output tables
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)
        assert not sql2.table_exists(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)

        # create a fake table that we will try to overwrite
        reference_table = sql2.dfquery(f"""

                                create table {test_dest_schema}.{test_sql_to_sql_tbl_to} (test_col1 int, test_col2 int);
                                insert into {test_dest_schema}.{test_sql_to_sql_tbl_to} (test_col1, test_col2) VALUES (1, 2);
                                insert into {test_dest_schema}.{test_sql_to_sql_tbl_to} (test_col1, test_col2) VALUES (3, 4);

                                select * from {test_dest_schema}.{test_sql_to_sql_tbl_to};
                                """)

        # try to copy a table with the same name
        try:
            data_io.sql_to_sql(from_sql = sql,
                        to_sql = sql2,
                        org_schema = test_org_schema,
                        org_table = test_sql_to_sql_tbl_from,
                        dest_schema = test_dest_schema,
                        dest_table = test_sql_to_sql_tbl_to,
                        print_cmd = True)

        except:
            Failed = True

        # assert that sql_to_sql fails
        assert Failed == True
        
        # create a table object based on the new table that was created
        new_table = sql2.dfquery(f"select * from {test_dest_schema}.{test_sql_to_sql_tbl_to}")

        # assert that the output table remains the same as the originaal
        assert new_table.equals(reference_table)

        # drop table
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)

    def test_sql_to_sql_src_table_nonexist(self):

        """
        Copy a table whose output name already exists in the destination database. It should overwrite it.
        """
        
        # drop output table if it exists
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)
        assert not sql2.table_exists(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)
        
        # try to copy a table with the same name
        try:
            data_io.sql_to_sql(from_sql = sql,
                        to_sql = sql2,
                        org_schema = test_org_schema,
                        org_table = test_sql_to_sql_tbl_from + '_2',
                        dest_schema = test_dest_schema,
                        dest_table = test_sql_to_sql_tbl_to,
                        print_cmd = True)

        except:
            Failed = True

        # assert that a non-existent table cannot be copied
        assert Failed == True

        # clean tables
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)


    def test_sql_to_sql_src_schema_nonexist(self):

        """
        Copy a table from a schema that doesn't exist to yield an error.
        """

        # remove output table if it exists
        sql.drop_table(schema = test_org_schema, table = test_sql_to_sql_tbl_from)
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)
        assert not sql.table_exists(schema = test_org_schema, table = test_sql_to_sql_tbl_from)
        assert not sql2.table_exists(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)

        # create a table in the origin schema
        sql.query(f"""

                                create table {test_dest_schema}.{test_sql_to_sql_tbl_to} (test_col1 int, test_col2 int);
                                insert into {test_dest_schema}.{test_sql_to_sql_tbl_to} (test_col1, test_col2) VALUES (1, 2);
                                insert into {test_dest_schema}.{test_sql_to_sql_tbl_to} (test_col1, test_col2) VALUES (3, 4);

                                select * from {test_dest_schema}.{test_sql_to_sql_tbl_to};
                                """)

        # try to copy a table with the same name but under a different schema that doesn't exist
        try:
            data_io.sql_to_sql(from_sql = sql,
                        to_sql = sql2,
                        org_schema = test_org_schema + '_2',
                        org_table = test_sql_to_sql_tbl_from,
                        dest_schema = test_dest_schema,
                        dest_table = test_sql_to_sql_tbl_to,
                        print_cmd = True)

        except:
            Failed = True

        # assert that the new table cannot be created in a non-existent schema
        assert Failed == True

        # drop tables
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to) # in case it somehow got created
        sql.drop_table(schema = test_org_schema, table = test_sql_to_sql_tbl_from)

    def test_sql_to_sql_query_geom(self):

        """
        Copy a table whose name already exists in the destination database. It should overwrite it.
        """

        # remove org table and replace with a table with a geometry column
        sql.drop_table(schema = test_org_schema, table = test_sql_to_sql_tbl_from)
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)
        assert not sql.table_exists(schema = test_org_schema, table = test_sql_to_sql_tbl_from)
        assert not sql2.table_exists(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)

        geometry_table = sql.dfquery(f"""

                create table {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1 int, test_col2 int, test_col3 varchar(4), geom geometry);
                insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3, geom) VALUES (1, 2, 'ABCD', geometry::Point(1015329.1, 213793.1, 2263));
                insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3, geom) VALUES (3, 4, 'DE*G', geometry::Point(1015329.1, 213793.1, 2263));
                insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3, geom) VALUES (5, 60, 'HIj_', geometry::Point(1015329.1, 213793.1, 2263));
                insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3, geom) VALUES (-3, 24271, 'zhyw', geometry::Point(1015329.1, 213793.1, 2263));

                select * from {test_org_schema}.{test_sql_to_sql_tbl_from};
                """)
        
        # assert that the new table is created
        assert sql.table_exists(table = test_sql_to_sql_tbl_from, schema=test_org_schema)

        # run sql_to_sql function
        data_io.sql_to_sql(from_sql = sql,
                        to_sql = sql2,
                        org_schema = test_org_schema,
                        org_table = test_sql_to_sql_tbl_from,
                        dest_schema = test_dest_schema,
                        dest_table = test_sql_to_sql_tbl_to,
                        print_cmd = True)

        # assert that resulting sql table was created
        assert sql2.table_exists(table = test_sql_to_sql_tbl_to, schema=test_dest_schema)

        # geom field moved to 1st column by gdal or sql default behavior so set column order
        output_table = sql2.dfquery(f"select test_col1, test_col2, test_col3, geom from {test_dest_schema}.{test_sql_to_sql_tbl_to};")

        # assert that the input and output tables are the same
        assert set(geometry_table.columns) == set(output_table.columns)
        assert geometry_table.shape == output_table.shape
        assert geometry_table.equals(output_table)

        # drop table
        sql.drop_table(schema = test_org_schema, table = test_sql_to_sql_tbl_from)
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)

    def test_sql_to_sql_funky_field_names(self):

        # remove org table and replace with funky field names table
        sql.drop_table(schema = test_org_schema, table = test_sql_to_sql_tbl_from)
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)
        assert not sql.table_exists(schema = test_org_schema, table = test_sql_to_sql_tbl_from)
        assert not sql2.table_exists(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)

        # create table
        reference_table = sql.dfquery(f"""
            CREATE TABLE {test_org_schema}.{test_sql_to_sql_tbl_from} (id int, [t.txt] text, [1t txt] text, [t_txt] text, dte datetime, geom geometry);

            INSERT INTO {test_org_schema}.{test_sql_to_sql_tbl_from}
            (id, [t.txt], [1t txt], [t_txt], dte, geom)
            VALUES (1, 'test text','test text','test text', CURRENT_TIMESTAMP,
            geometry::Point(1015329.1, 213793.1, 2263 ));

            select * from {test_org_schema}.{test_sql_to_sql_tbl_from};
        """)

        # confirm that input table exists
        assert sql.table_exists(table = test_sql_to_sql_tbl_from, schema=test_org_schema)

        # run sql_to_sql function
        data_io.sql_to_sql(from_sql = sql,
                        to_sql = sql2,
                        org_schema = test_org_schema,
                        org_table = test_sql_to_sql_tbl_from,
                        dest_schema = test_dest_schema,
                        dest_table = test_sql_to_sql_tbl_to,
                        print_cmd = True)
        
        # assert that the output table is successfully created and exists
        assert sql2.table_exists(table = test_sql_to_sql_tbl_to, schema = test_dest_schema)

        # create python object called output_table
        output_table = sql2.dfquery(f"select id, [t.txt], [1t txt], [t_txt], dte, geom from {test_dest_schema}.{test_sql_to_sql_tbl_to}") 

        # assert that the output and input tables are the same based on shape, column names, and data overall
        assert reference_table.shape == output_table.shape
        assert set(reference_table.columns) == set(output_table.columns) # order doesnt matter
        assert reference_table.equals(output_table)

        # clean up
        sql.drop_table(test_org_schema, test_sql_to_sql_tbl_from)
        sql2.drop_table(test_dest_schema, test_sql_to_sql_tbl_to)

    def test_sql_to_sql_basic_long_names(self):
        
        """
        Test copying a SQL table with long column names
        """

        # drop any output tables
        sql2.drop_table(schema= test_dest_schema, table = test_sql_to_sql_tbl_from)
        sql.drop_table(schema= test_org_schema, table = test_sql_to_sql_tbl_from)
        assert not sql2.table_exists(schema= test_dest_schema, table = test_sql_to_sql_tbl_from)
        assert not sql.table_exists(schema= test_org_schema, table = test_sql_to_sql_tbl_from)

        # create table to be copied (has geoms)
        reference_table = sql.dfquery(f"""
            CREATE TABLE {test_org_schema}.{test_sql_to_sql_tbl_from} (id_name_one int,
            [123text name one] text,
            [text@name-two~three four five six seven] text,
            current_date_time datetime,
            [x-coord] float,
            geom geometry);

            INSERT INTO {test_org_schema}.{test_sql_to_sql_tbl_from}
            VALUES (1, 'test text', 'test text', CURRENT_TIMESTAMP,
            123.456, geometry::Point(1015329.1, 213793.1, 2263 ));

            SELECT * FROM {test_org_schema}.{test_sql_to_sql_tbl_from};
        """)

        # confirm that the table is correctly created
        assert sql.table_exists(schema=test_org_schema, table = test_sql_to_sql_tbl_from)

        # run sql_to_sql function
        data_io.sql_to_sql(
                        from_sql = sql,
                        to_sql = sql2,
                        org_schema = test_org_schema,
                        org_table = test_sql_to_sql_tbl_from,
                        dest_schema = test_dest_schema,
                        dest_table = test_sql_to_sql_tbl_to,
                        print_cmd = True)

        # assert that table is created
        assert sql2.table_exists(schema=test_dest_schema, table = test_sql_to_sql_tbl_to)

        # call the table but remove the ogr_fid field that gets created
        output_table = sql2.dfquery(f"""
            select id_name_one, [123text name one], [text@name-two~three four five six seven], 
                current_date_time, [x-coord], geom 
                from {test_dest_schema}.{test_sql_to_sql_tbl_to};""")

        # assert that the tables are the same based on the column names, the shape, and values overall
        assert set(reference_table.columns) == set(output_table.columns)
        assert reference_table.shape == output_table.shape
        assert reference_table.equals(output_table)

        # remove output tables
        sql2.drop_table(test_dest_schema, test_sql_to_sql_tbl_to)
        sql.drop_table(test_org_schema, test_sql_to_sql_tbl_from)

    def test_sql_to_sql_qry_empty_table(self):

        """
        Test that an empty table is created if that is what the query outlines
        """

        # drop tables
        sql.drop_table(schema= test_org_schema, table= test_sql_to_sql_tbl_from)
        sql2.drop_table(schema= test_dest_schema, table= test_sql_to_sql_tbl_to)
        assert not sql.table_exists(table=test_org_schema, schema=test_sql_to_sql_tbl_from)
        assert not sql2.table_exists(table=test_dest_schema, schema=test_sql_to_sql_tbl_from)

        # create table
        sql.query(f"""
            CREATE TABLE {test_org_schema}.{test_sql_to_sql_tbl_from} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {test_org_schema}.{test_sql_to_sql_tbl_from}
                 VALUES (1, 'test text', cast(CURRENT_TIMESTAMP as datetime), geometry::Point(1015329.1, 213793.1, 2263 ));
        """)

        # assert newly created table exists
        assert sql.table_exists(table = test_sql_to_sql_tbl_from, schema= test_org_schema)

        # run sql_to_sql_qry
        data_io.sql_to_sql_qry( from_sql = sql,
                                to_sql = sql2,
                                qry = f"select top (0) * from {test_org_schema}.{test_sql_to_sql_tbl_from}",
                                org_schema = test_org_schema,
                                dest_schema = test_dest_schema,
                                dest_table = test_sql_to_sql_tbl_to,
                                print_cmd=True)

        # assert that sql_to_sql_qry output created
        assert sql2.table_exists(table = test_sql_to_sql_tbl_to, schema= test_dest_schema)

        ref_table = sql.dfquery(f"select top (0) * from {test_org_schema}.{test_sql_to_sql_tbl_from}")
        output_table = sql2.dfquery(f"select * from {test_dest_schema}.{test_sql_to_sql_tbl_to}").drop(['ogr_fid'], axis = 1)

        # check that the output table returns as expected (empty table)
        assert set(ref_table.columns) == set(output_table.columns)
        assert ref_table.shape == output_table.shape

        # clean up
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)
        sql.drop_table(test_org_schema, test_sql_to_sql_tbl_from)
        
    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(sql)