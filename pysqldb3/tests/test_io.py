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

org_pg = pysqldb.DbConnect(type=config.get('SECOND_PG_DB', 'TYPE'),
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

        sql.drop_table(schema=sql.default_schema, table=test_pg_to_sql_table)
        db.drop_table(schema=pg_schema, table=test_pg_to_sql_table)

        db.query(f"""
        create table {pg_schema}.{test_pg_to_sql_table} as

        select *
        from {pg_schema}.{pg_table_name}
        limit 10
        """)

        # Assert created correctly
        assert db.table_exists(table=test_pg_to_sql_table, schema=pg_schema)
        assert not sql.table_exists(table=test_pg_to_sql_table)

        # Assert not in sql yet
        org_schema = pg_schema
        org_table = test_pg_to_sql_table

        # Move to sql
        data_io.pg_to_sql(db, sql, org_table=org_table, org_schema=org_schema, print_cmd=True)

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

        # Assert
        shared_non_geom_cols = list(set(pg_df.columns).intersection(set(sql_df.columns)) - {'geom'})
        
        pd.testing.assert_frame_equal(pg_df[shared_non_geom_cols], sql_df[shared_non_geom_cols],
                                      check_dtype=False,
                                      check_exact=False,
                                      check_datetimelike_compat=True)

        # Cleanup
        sql.drop_table(schema=sql.default_schema, table=test_pg_to_sql_table)
        db.drop_table(table=test_pg_to_sql_table, schema=pg_schema)


    def test_pg_to_sql_naming(self):
        
        """
        Copy an existing Postgres table to MS SQL Server, and change the name of the copied table.
        """

        dest_name = f'another_tst_name_{db.user}'
        sql.drop_table(schema=sql.default_schema, table=test_pg_to_sql_table)
        sql.drop_table(schema=sql.default_schema, table=dest_name)

        db.query(f"""
           drop table if exists {pg_schema}.{test_pg_to_sql_table};
           create table {pg_schema}.{test_pg_to_sql_table} as
            select *
            from {pg_schema}.{pg_table_name}
            limit 10
        """)

        # Assert table created correctly
        assert db.table_exists(table=test_pg_to_sql_table, schema=pg_schema)

        # Assert not in sql yet
        assert not sql.table_exists(table=dest_name)

        # Move to sql from pg
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

        # Assert
        shared_non_geom_cols = list(set(pg_df.columns).intersection(set(sql_df.columns)) - {'geom'})
        pd.testing.assert_frame_equal(pg_df[shared_non_geom_cols], sql_df[shared_non_geom_cols],
                                      check_dtype=False,
                                      check_exact=False,
                                      check_datetimelike_compat=True)

        # Cleanup
        sql.drop_table(schema=sql.default_schema, table=dest_name)
        db.drop_table(table=test_pg_to_sql_table, schema=pg_schema)

    def test_pg_to_sql_spatial_table(self):
        """
        Copy a table with spatial data in Postgres to MS SQL Server
        """

        org_schema = pg_schema
        org_table = test_pg_to_sql_table

        dest_name = test_pg_to_sql_table
        dest_name_not_spatial = test_pg_to_sql_table + '_not_spatial'

        sql.drop_table(schema=sql.default_schema, table=dest_name)
        sql.drop_table(schema=sql.default_schema, table=dest_name_not_spatial)

        db.query(f"""
           drop table if exists {pg_schema}.{test_pg_to_sql_table};
           create table {pg_schema}.{test_pg_to_sql_table} as

           select 'hello' as c, st_transform(geom, 4326) as geom
           from {pg_schema}.{pg_table_name}
           limit 10
        """)

        # Assert table made correctly
        assert db.table_exists(table=test_pg_to_sql_table, schema=pg_schema)

        # Assert neither table in SQL Server yet
        assert not sql.table_exists(table=dest_name)
        assert not sql.table_exists(table=dest_name_not_spatial)

        # Move from pg to sql, with different spatial flags
        data_io.pg_to_sql(db, sql, org_table=org_table, org_schema=org_schema, dest_table=dest_name, spatial=True,
                          print_cmd=True)
        data_io.pg_to_sql(db, sql, org_table=org_table, org_schema=org_schema, dest_table=dest_name_not_spatial,
                          spatial=False, print_cmd=True)

        # Assert move worked
        assert sql.table_exists(table=dest_name)
        assert sql.table_exists(table=dest_name_not_spatial)

        spatial_df = sql.dfquery(f"""
        select *
        from {dest_name}
        """).infer_objects()

        not_spatial_df = sql.dfquery(f"""
        select *
        from {dest_name_not_spatial}
        """).infer_objects()
        
        # work around for older GDALs where non-spatial means you dont have a geom column
        if 'geom' not in not_spatial_df.columns and 'ogr_geometry' not in not_spatial_df.columns:
            # worked correctly
            assert True
        else:
            joined_df = spatial_df.join(not_spatial_df, on='ogr_fid', rsuffix='_ns')

            # Assert spatial functionality worked
            # This is shown by geom being different; as when spatial is true, the a_srs flag works successfully and
            # converts the srid
            assert len(spatial_df) == len(not_spatial_df)
            if 'geom_ns' in joined_df.columns:
                assert len(joined_df) == len(joined_df[joined_df['geom'] != joined_df['geom_ns']])
            elif 'ogr_geometry' in joined_df.columns:
                assert len(joined_df) == len(joined_df[joined_df['ogr_geometry'] != joined_df['ogr_geometry_ns']])

        # Assert df equality -- some types need to be coerced from the Pandas df for the equality assertion to hold
        pg_df = db.dfquery(f"""
        select *
        from {pg_schema}.{test_pg_to_sql_table}
        """).infer_objects()

        # Assert
        shared_non_geom_cols = list(set(pg_df.columns).intersection(set(spatial_df.columns)) - {'geom'})
        pd.testing.assert_frame_equal(pg_df[shared_non_geom_cols], spatial_df[shared_non_geom_cols],
                                      check_dtype=False,
                                      check_exact=False,
                                      check_datetimelike_compat=True)

        shared_non_geom_cols = list(set(pg_df.columns).intersection(set(spatial_df.columns)) - {'geom'})
        pd.testing.assert_frame_equal(pg_df[shared_non_geom_cols], not_spatial_df[shared_non_geom_cols],
                                      check_dtype=False,
                                      check_exact=False,
                                      check_datetimelike_compat=True)

        # Cleanup
        sql.drop_table(schema=sql.default_schema, table=dest_name_not_spatial)
        sql.drop_table(schema=sql.default_schema, table=dest_name)
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

        # pg_to_sql_qry
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

        sql.drop_table(schema=sql_schema, table=test_pg_to_sql_qry_table)
        assert not sql.table_exists(table=test_pg_to_sql_qry_table)

        # pg_to_sql_qry
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

        # Assert
        pd.testing.assert_frame_equal(pg_df, sql_df,
                                    check_dtype=False,
                                      check_column_type=False)

        # Cleanup
        sql.drop_table(schema=sql_schema, table=test_pg_to_sql_qry_table)

    def test_pg_to_sql_qry_spatial(self):

        """
        Copy a spatial query from Postgres to SQL
        """

        sql.drop_table(schema = sql_schema, table = test_pg_to_sql_qry_spatial_table)
        db.drop_table(schema = pg_schema, table = test_pg_to_sql_qry_spatial_table)

        assert not db.table_exists(table=test_pg_to_sql_qry_spatial_table, schema = pg_schema)
        assert not sql.table_exists(table=test_pg_to_sql_qry_spatial_table, schema = sql_schema)

        # add new spatial table
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

        # chekc the first 2 columns using assert_frame_equal
        pd.testing.assert_frame_equal(pg_df, sql_df,
                                      check_dtype=False,
                                      check_column_type=False)
        
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

        sql.query(f"""
            drop table if exists ##{test_sql_to_pg_qry_spatial_table};
            create table ##{test_sql_to_pg_qry_table} (test_col1 int, test_col2 int);
            insert into ##{test_sql_to_pg_qry_table} (test_col1, test_col2) VALUES (1, 2);
            insert into ##{test_sql_to_pg_qry_table} (test_col1, test_col2) VALUES (3, 4);
        """)

        # sql_to_pg_qry
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

        # Assert
        pd.testing.assert_frame_equal(sql_df, pg_df.drop(['geom', 'ogc_fid'], axis = 1),
                                    check_dtype=False,
                                      check_column_type=False)

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

        # sql_to_pg_qry
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

        # Assert
        pd.testing.assert_frame_equal(sql_df, pg_df.drop(['geom', 'ogc_fid'], axis = 1),
                                    check_dtype=False,
                                      check_column_type=False)

        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_sql_to_pg_qry_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)

    def test_sql_to_pg_qry_spatial(self):
        """
        Copy a SQL query with spatial data to Postgres
        """

        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_spatial_table)

        assert not db.table_exists(table=test_sql_to_pg_qry_spatial_table, schema = pg_schema)

        sql.query(f"""
             drop table if exists ##{test_sql_to_pg_qry_spatial_table};
            create table ##{test_sql_to_pg_qry_spatial_table}
                (test_col1 int, test_col2 int, test_geom geometry);
            insert into ##{test_sql_to_pg_qry_spatial_table} (test_col1, test_col2, test_geom)
                VALUES (1, 2, CAST(geometry::Point(70.0890, 12.3456, 2236) AS VARCHAR));
            insert into ##{test_sql_to_pg_qry_spatial_table} (test_col1, test_col2, test_geom)
                VALUES (3, 4, CAST(geometry::Point(91.2763, 11.9434, 2236) AS VARCHAR));
        """)

        data_io.sql_to_pg_qry(sql, db, query=f"""
                                                SELECT test_col1, test_col2, CAST(test_geom.STAsText() AS VARCHAR) test_geom --comments within the query
                                                FROM ##{test_sql_to_pg_qry_spatial_table} -- geom here
                                                -- end here""",
                              dest_table= test_sql_to_pg_qry_spatial_table,
                              dest_schema = pg_schema,
                              spatial = False)

        assert db.table_exists(table=test_sql_to_pg_qry_spatial_table, schema = pg_schema)
        assert len(db.dfquery(f'select test_col1, test_col2, test_geom from {pg_schema}.{test_sql_to_pg_qry_spatial_table}')) == 2

        pg_df = db.dfquery(f"""select test_col1::int test_col1, test_col2::int test_col2, test_geom
                                from {pg_schema}.{test_sql_to_pg_qry_spatial_table}""")

        sql_df = sql.dfquery(f""" select test_col1, test_col2, CAST(test_geom.STAsText() AS VARCHAR) test_geom from ##{test_sql_to_pg_qry_spatial_table}""")

        pd.testing.assert_frame_equal(pg_df, sql_df,
                                      check_dtype=False,
                                      check_column_type=False)
        
        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_sql_to_pg_qry_spatial_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_spatial_table)
        sql.query(f"""DROP TABLE IF EXISTS ##{test_sql_to_pg_qry_spatial_table}""")

    def test_sql_to_pg_qry_dest_schema(self):
        
        """
        Copy a SQL query to Postgres, and define the destination schema
        """

        # Assert doesn't exist already
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)
        assert not db.table_exists(schema=pg_schema, table=test_sql_to_pg_qry_table)

        # sql_to_pg_qry
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

        sql_df.columns = [c.lower() for c in list(sql_df.columns)]

        # Assert
        pd.testing.assert_frame_equal(sql_df, pg_df.drop(['ogc_fid', 'geom'], axis = 1), check_column_type=False,
                                      check_dtype=False)

        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_sql_to_pg_qry_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)

    def test_sql_to_pg_qry_no_dest_table_input(self):

        """
        Copy a SQL query to a Postgres table where the destination table name has not been defined.
        The name should default to '_{user}_{date}'
        """
        
        # Assert doesn't exist already
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)
        assert not db.table_exists(schema=pg_schema, table=test_sql_to_pg_qry_table)

        # sql_to_pg_qry
        data_io.sql_to_pg_qry(sql, db, query=f"""
                            
                              select * from ##{test_sql_to_pg_qry_table}; /* hi */""",
                              # no dest_table input
                              dest_schema=pg_schema, print_cmd=True)
        
        assert db.table_exists(schema = pg_schema, table = f"_{db.user}_{datetime.datetime.now().strftime('%Y%m%d%H%M')}")

        # drop tables
        db.drop_table(schema = pg_schema, table =  f"_{db.user}_{datetime.datetime.now().strftime('%Y%m%d%H%M')}")

    def test_sql_to_pg_qry_empty_query_error(self):
        """
        Copy an empty SQL query to Postgres, which should lead to an error.
        """

          # Assert doesn't exist already
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

        db.drop_table(pg_schema, test_sql_to_pg_table)
        # Assert table doesn't exist in pg
        assert not db.table_exists(table=test_sql_to_pg_table, schema = pg_schema)

        # Add test_table
        sql.drop_table(schema=sql_schema, table=test_sql_to_pg_table)
        sql.query(f"""
         create table {sql_schema}.{test_sql_to_pg_table} (test_col1 int, test_col2 int);
         insert into {sql_schema}.{test_sql_to_pg_table} VALUES(1, 2);
         insert into {sql_schema}.{test_sql_to_pg_table} VALUES(3, 4);
         """)

        # Sql_to_pg
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

        sql_df.columns = [c.lower() for c in list(sql_df.columns)]

        # Assert
        pd.testing.assert_frame_equal(sql_df, pg_df.drop(['geom', 'ogc_fid'], axis=1),
                                      check_dtype=False, check_column_type=False)

        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_sql_to_pg_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

        # assert added to tables created in dest dbo
        assert db.tables_created[-1] == ('', '', f'{pg_schema}', f'{test_sql_to_pg_table}')

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_table)
        sql.drop_table(schema=sql_schema, table=test_sql_to_pg_table)

    def test_sql_to_pg_dest_schema_name(self):
        """
        Copy a SQL table to PG and define the destination schema
        """
        # Assert table doesn't exist in pg
        db.drop_table(pg_schema, test_sql_to_pg_table)
        assert not db.table_exists(schema=pg_schema, table=test_sql_to_pg_table)

        # Add test_table
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

        sql_df.columns = [c.lower() for c in list(sql_df.columns)]

        # Assert
        pd.testing.assert_frame_equal(sql_df, pg_df.drop(['geom', 'ogc_fid'], axis=1),
                                      check_dtype=False,
                                      check_column_type=False)

        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_sql_to_pg_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_table)
        sql.drop_table(schema=sql_schema, table=test_sql_to_pg_table)

    def test_sql_to_pg_org_schema_name(self):
        
        """
        Copy a non-dbo SQL table to Postgres
        """
        # create table in SQL non-dbo schema
        sql.drop_table(schema=test_org_schema, table=test_sql_to_pg_table)

        sql.query(f"""
            create table {test_org_schema}.{test_sql_to_pg_table} (test_col1 int, test_col2 int);
         insert into {test_org_schema}.{test_sql_to_pg_table} VALUES(1, 2);
         insert into {test_org_schema}.{test_sql_to_pg_table} VALUES(3, 4);
         """)
        
        # copy over table
        data_io.sql_to_pg(sql, db, org_table=test_sql_to_pg_table, org_schema= test_org_schema, dest_table=test_sql_to_pg_table,
                          dest_schema=pg_schema, print_cmd=True)

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
        # Must have RIS DB info in db_config.cfg [SECOND_PG_DB] section
        ris = pysqldb.DbConnect(type=config.get('SECOND_PG_DB', 'TYPE'),
                                server=config.get('SECOND_PG_DB', 'SERVER'),
                                database=config.get('SECOND_PG_DB', 'DB_NAME'),
                                user=config.get('SECOND_PG_DB', 'DB_USER'),
                                password=config.get('SECOND_PG_DB', 'DB_PASSWORD'))

        db.drop_table(schema=pg_schema, table=test_pg_to_pg_tbl)
        ris.drop_table(schema=pg_schema, table=test_pg_to_pg_tbl)

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
        # Must have RIS DB info in db_config.cfg [SECOND_PG_DB] section
        ris = pysqldb.DbConnect(type=config.get('SECOND_PG_DB', 'TYPE'),
                                server=config.get('SECOND_PG_DB', 'SERVER'),
                                database=config.get('SECOND_PG_DB', 'DB_NAME'),
                                user=config.get('SECOND_PG_DB', 'DB_USER'),
                                password=config.get('SECOND_PG_DB', 'DB_PASSWORD'))

        db.drop_table(schema=pg_schema, table=test_pg_to_pg_tbl)

        test_pg_to_pg_tbl_other = test_pg_to_pg_tbl + '_another_name'
        ris.drop_table(schema=pg_schema, table=test_pg_to_pg_tbl_other)

        # Create table for testing in ris
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

        # Assert db equality
        risdf = ris.dfquery(f"""
            select *
            from {pg_schema}.{test_pg_to_pg_tbl_other}
        """).infer_objects()

        dbdf = db.dfquery(f"""
            select *
            from {pg_schema}.{test_pg_to_pg_tbl}
        """).infer_objects()

        # Assert
        pd.testing.assert_frame_equal(risdf.drop(['geom', 'ogc_fid'], axis=1), dbdf.drop(['geom'], axis=1),
                                      check_exact=False)

        # Assert that the permissions is in PUBLIC
        assert ris.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_pg_to_pg_tbl_other}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"
        # Cleanup
        ris.drop_table(schema=pg_schema, table=test_pg_to_pg_tbl_other)
        db.drop_table(schema=pg_schema, table=test_pg_to_pg_tbl)

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
        org_pg.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)
        org_pg.query(f"""
                        create table {pg_schema}.{test_pg_to_pg_qry_table} (test_col1 int, test_col2 int);
                        insert into {pg_schema}.{test_pg_to_pg_qry_table} VALUES(1, 2);
                        insert into {pg_schema}.{test_pg_to_pg_qry_table} VALUES(3, 4);
        """)

        # sql_to_pg_qry
        data_io.pg_to_pg_qry(org_pg, db, query=
                             f"""
                             select * from {pg_schema}.{test_pg_to_pg_qry_table}
                             """,
                             dest_table=test_pg_to_pg_qry_table,
                             dest_schema = pg_schema, print_cmd=True, spatial=False)

        # Assert sql to pg query was successful (table exists)
        assert db.table_exists(table=test_pg_to_pg_qry_table, schema = pg_schema)

        # Assert df equality
        org_pg_df = org_pg.dfquery(f"""
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
        org_pg.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)

    def test_pg_to_pg_qry_basic_with_comments_table(self):
        """
        Run a PG query in one database and copy the output table to another PG database.
        Include several comments within the query.
        """

        # Assert pg table doesn't exist
        db.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)
        assert not db.table_exists(table=test_pg_to_pg_qry_table, schema = pg_schema)

        # Add test_table
        org_pg.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)
        org_pg.query(f"""
        create table {pg_schema}.{test_pg_to_pg_qry_table} (test_col1 int, test_col2 int);
        insert into {pg_schema}.{test_pg_to_pg_qry_table} VALUES(1, 2);
        insert into {pg_schema}.{test_pg_to_pg_qry_table} VALUES(3, 4);
        """)

        # sql_to_pg_qry
        data_io.pg_to_pg_qry(org_pg, db, query=f"""
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
        org_pg_df = org_pg.dfquery(f"""
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
        org_pg.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)

    def test_pg_to_pg_qry_dest_schema(self):
        """
        Copy a PG table to another PG database, changing the destination schema
        """

        # Assert doesn't exist already
        db.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)
        assert not db.table_exists(schema=pg_schema, table=test_pg_to_pg_qry_table)

        # Add test_table
        org_pg.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)
        org_pg.query(f"""
        create table {pg_schema}.{test_pg_to_pg_qry_table} (test_col1 int, test_col2 int);
        insert into {pg_schema}.{test_pg_to_pg_qry_table} VALUES(1, 2);
        insert into {pg_schema}.{test_pg_to_pg_qry_table} VALUES(3, 4);
        """)

        # sql_to_pg_qry
        data_io.pg_to_pg_qry(org_pg, db, query=f"""select *
                                                from {pg_schema}.{test_pg_to_pg_qry_table}""",
                             dest_table=test_pg_to_pg_qry_table, dest_schema=pg_schema, print_cmd=True)

        # Assert sql_to_pg_qry successful and correct length
        assert db.table_exists(schema=pg_schema, table=test_pg_to_pg_qry_table)
        assert len(db.dfquery(f'select * from {pg_schema}.{test_pg_to_pg_qry_table}')) == 2

        # Assert df equality
        org_pg_df = org_pg.dfquery(f"""
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
        org_pg.drop_table(schema = pg_schema, table = test_pg_to_pg_qry_table)

    def test_pg_to_pg_qry_no_dest_table_input(self):
        """
        Run a SQL query in one database and copy the output table to PG database
        with no defined destination table name.
        """
        # drop resulting table if it already exists
        db.drop_table(schema = pg_schema, table =  f"_{db.user}_{datetime.datetime.now().strftime('%Y%m%d%H%M')}")
        org_pg.drop_table(schema = pg_schema, table = test_pg_to_pg_qry_table)
        
        # create postgres table query to be copied
        data_io.pg_to_pg_qry(org_pg, db, query=f"""
                create table {pg_schema}.{test_pg_to_pg_qry_table} (test_col1 int, test_col2 int);
                insert into {pg_schema}.{test_pg_to_pg_qry_table} VALUES(1, 2);
                insert into {pg_schema}.{test_pg_to_pg_qry_table} VALUES(3, 4);
                
                select * from {pg_schema}.{test_pg_to_pg_qry_table};""",
                dest_schema=pg_schema, print_cmd=True)

        assert db.table_exists(schema = pg_schema, table = f"_{db.user}_{datetime.datetime.now().strftime('%Y%m%d%H%M')}")

        # drop tables
        db.drop_table(schema = pg_schema, table =  f"_{db.user}_{datetime.datetime.now().strftime('%Y%m%d%H%M')}")
        org_pg.drop_table(schema = pg_schema, table = test_pg_to_pg_qry_table)


    def test_pg_to_pg_qry_empty_query_error(self):
        """
        Run an empty SQL query and copy the output to Postgres.
        Should yield an error.
        """

        # drop tables 
        db.drop_table(schema = pg_schema, table = test_pg_to_pg_qry_table)
        org_pg.drop_table(schema = pg_schema, table = test_pg_to_pg_qry_table)
        
        # create empty postgres table query to be copied
        try:
            data_io.pg_to_pg_qry(org_pg, db, query=f"",
                dest_schema=pg_schema, dest_table =  test_pg_to_pg_qry_table, print_cmd=True)
        except:
            Failed = True

        assert Failed == True # assert that pg_to_pg_qry failed
        assert db.table_exists(schema = pg_schema, table = test_pg_to_pg_qry_table) == False # asser that no table was created

        # drop tables if they somehow got written
        db.drop_table(schema = pg_schema, table = test_pg_to_pg_qry_table)
        org_pg.drop_table(schema = pg_schema, table = test_pg_to_pg_qry_table)
        

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

        # copy over an existing table
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)
        sql.drop_table(schema = test_org_schema, table = test_sql_to_sql_tbl_from)

        sql2.query(f"""
                    create table {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1 int, test_col2 int, test_col3 varchar(4));
                    insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3) VALUES (1, 2, 'ABCD');
                    insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3) VALUES (3, 4, 'DE*G');
                    insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3) VALUES (5, 60, 'HIj_');
                    insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3) VALUES (-3, 24271, 'zhyw');
                    """)

        data_io.sql_to_sql(from_sql = sql,
                        to_sql = sql2,
                        org_schema = test_org_schema,
                        org_table = test_sql_to_sql_tbl_from,
                        dest_schema = test_dest_schema,
                        dest_table = test_sql_to_sql_tbl_to,
                        spatial = False,
                        print_cmd = True
                        )

        # check that hte table exists
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

        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)

        data_io.sql_to_sql_qry(from_sql = sql,
                                to_sql = sql2,
                                org_schema = test_org_schema,
                                qry = f"select top (3) * from {test_org_schema}.{test_sql_to_sql_tbl_from}",
                        dest_schema = test_dest_schema,
                        dest_table = test_sql_to_sql_tbl_to,
                        spatial = False,
                        print_cmd = True)

        # check that the table exists
        assert sql2.table_exists(table = test_sql_to_sql_tbl_to, schema = test_dest_schema)

        # check that we are expecting the same values
        test_dest_df = sql2.dfquery(f"select * from {test_dest_schema}.{test_sql_to_sql_tbl_to}").drop('ogr_fid', axis=1)
        test_org_df = sql.dfquery(f"select top (3) * from {test_org_schema}.{test_sql_to_sql_tbl_from}")

        assert list(test_dest_df.columns) == list(test_org_df.columns)

        # check the dimensions
        assert test_dest_df.shape == test_org_df.shape

        # drop table
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)

    def test_sql_to_sql_tbl_exists(self):

        """
        Copy a table whose name already exists in the destination database. It will overwrite the table.
        """

        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)

        # create a fake table in RISCRASHDATA that we will try to overwrite
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

        assert Failed == True
        
        new_table = sql2.dfquery(f"select * from {test_dest_schema}.{test_sql_to_sql_tbl_to}")

        assert new_table.equals(reference_table) # assert that the output table remains the same as the originaal

        # clean data
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)

    def test_sql_to_sql_src_table_nonexist(self):

        """
        Copy a table whose ou`tput name already exists in the destination database. It should overwrite it.
        """

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

        assert Failed == True

        # clean data
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)


    def test_sql_to_sql_src_schema_nonexist(self):

        """
        Copy a table from a schema that doesn't exist to yield an error.
        """

        sql.drop_table(schema = test_org_schema, table = test_sql_to_sql_tbl_from)

        # create a table
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

        assert Failed == True

        # drop table
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to) # in case it somehow got created
        sql.drop_table(schema = test_org_schema, table = test_sql_to_sql_tbl_from)

    def test_sql_to_sql_query_geom(self):

        """
        Copy a table whose name already exists in the destination database. It should overwrite it.
        """

        # remove org table and replace with a table with a geometry column
        sql.drop_table(schema = test_org_schema, table = test_sql_to_sql_tbl_from)

        geometry_table = sql.dfquery(f"""

                                create table {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1 int, test_col2 int, test_col3 varchar(4), geom geometry);
                                insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3, geom) VALUES (1, 2, 'ABCD', geometry::Point(1015329.1, 213793.1, 2263));
                                insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3, geom) VALUES (3, 4, 'DE*G', geometry::Point(1015329.1, 213793.1, 2263));
                                insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3, geom) VALUES (5, 60, 'HIj_', geometry::Point(1015329.1, 213793.1, 2263));
                                insert into {test_org_schema}.{test_sql_to_sql_tbl_from} (test_col1, test_col2, test_col3, geom) VALUES (-3, 24271, 'zhyw', geometry::Point(1015329.1, 213793.1, 2263));

                                select * from {test_org_schema}.{test_sql_to_sql_tbl_from};
                                """)

        assert sql.table_exists(table = test_sql_to_sql_tbl_from, schema=test_org_schema)

        # run sql_to_sql function
        data_io.sql_to_sql(from_sql = sql,
                        to_sql = sql2,
                        org_schema = test_org_schema,
                        org_table = test_sql_to_sql_tbl_from,
                        dest_schema = test_dest_schema,
                        dest_table = test_sql_to_sql_tbl_to,
                        print_cmd = True)

        # geom field moved to 1st column by gdal or sql default behavior so set column order
        output_table = sql2.dfquery(f"select test_col1, test_col2, test_col3, geom from {test_dest_schema}.{test_sql_to_sql_tbl_to};")

        assert sql2.table_exists(table = test_sql_to_sql_tbl_from, schema=test_org_schema)

        # assert that the tables are the same
        assert set(geometry_table.columns) == set(output_table.columns)
        assert geometry_table.shape == output_table.shape
        assert geometry_table.equals(output_table)

        # drop table
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)
        sql.drop_table(schema = test_org_schema, table = test_sql_to_sql_tbl_from)

    def test_sql_to_sql_funky_field_names(self):

        # remove org table and replace with funky field names table
        sql.drop_table(schema = test_org_schema, table = test_sql_to_sql_tbl_from)

        # create table
        reference_table = sql.dfquery(f"""
            CREATE TABLE {test_org_schema}.{test_sql_to_sql_tbl_from} (id int, [t.txt] text, [1t txt] text, [t_txt] text, dte datetime, geom geometry);

            INSERT INTO {test_org_schema}.{test_sql_to_sql_tbl_from}
            (id, [t.txt], [1t txt], [t_txt], dte, geom)
            VALUES (1, 'test text','test text','test text', CURRENT_TIMESTAMP,
            geometry::Point(1015329.1, 213793.1, 2263 ));

            select * from {test_org_schema}.{test_sql_to_sql_tbl_from};
        """)

        assert sql.table_exists(table = test_sql_to_sql_tbl_from, schema=test_org_schema)

        # run sql_to_sql function
        data_io.sql_to_sql(from_sql = sql,
                        to_sql = sql2,
                        org_schema = test_org_schema,
                        org_table = test_sql_to_sql_tbl_from,
                        dest_schema = test_dest_schema,
                        dest_table = test_sql_to_sql_tbl_to,
                        print_cmd = True)

        output_table = sql2.dfquery(f"select id, [t.txt], [1t txt], [t_txt], dte, geom from {test_dest_schema}.{test_sql_to_sql_tbl_to}") #.drop('ogr_fid', axis = 1)

        assert sql2.table_exists(table = test_sql_to_sql_tbl_to, schema = test_dest_schema)

        # assert that the tables are the same
        assert reference_table.shape == output_table.shape
        assert set(reference_table.columns) == set(output_table.columns) # order doesnt matter
        assert reference_table.equals(output_table)

        # clean up
        sql2.drop_table(test_dest_schema, test_sql_to_sql_tbl_to)
        sql.drop_table(test_org_schema, test_sql_to_sql_tbl_from)

    def test_sql_to_sql_basic_long_names(self):
        
        """
        Test copying a SQL table with long column names
        """

        sql2.drop_table(schema= test_dest_schema, table = test_sql_to_sql_tbl_from)
        sql.drop_table(schema= test_org_schema, table = test_sql_to_sql_tbl_from)

        # create table
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

        assert sql.table_exists(schema=test_org_schema, table = test_sql_to_sql_tbl_from)

        # table to shp
        data_io.sql_to_sql(
                        from_sql = sql,
                        to_sql = sql2,
                        org_schema = test_org_schema,
                        org_table = test_sql_to_sql_tbl_from,
                        dest_schema = test_dest_schema,
                        dest_table = test_sql_to_sql_tbl_to,
                        print_cmd = True)

        assert sql2.table_exists(schema=test_org_schema, table = test_sql_to_sql_tbl_from)

        # call the table but remove the ogr_fid field that gets created
        output_table = sql2.dfquery(f"""
            select id_name_one, [123text name one], [text@name-two~three four five six seven], 
                current_date_time, [x-coord], geom 
                from {test_dest_schema}.{test_sql_to_sql_tbl_to};""")

        # assert that the tables are the same
        assert set(reference_table.columns) == set(output_table.columns)
        assert reference_table.shape == output_table.shape
        assert reference_table.equals(output_table)

        # clean up
        sql2.drop_table(test_dest_schema, test_sql_to_sql_tbl_to)
        sql.drop_table(test_org_schema, test_sql_to_sql_tbl_from)

    def test_query_to_shp_basic_no_data(self):

        """
        Test that an empty table is created if that is what the query outlines
        """

        sql.drop_table(schema= test_org_schema, table= test_sql_to_sql_tbl_from)
        assert not sql.table_exists(table=test_org_schema, schema=test_sql_to_sql_tbl_from)

        # create table
        sql.query(f"""
            CREATE TABLE {test_org_schema}.{test_sql_to_sql_tbl_from} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {test_org_schema}.{test_sql_to_sql_tbl_from}
                 VALUES (1, 'test text', cast(CURRENT_TIMESTAMP as datetime), geometry::Point(1015329.1, 213793.1, 2263 ));
        """)

        assert sql.table_exists(table = test_sql_to_sql_tbl_from, schema= test_org_schema)

        # table to shp
        data_io.sql_to_sql_qry( from_sql = sql,
                                to_sql = sql2,
                                qry = f"select top (0) * from {test_org_schema}.{test_sql_to_sql_tbl_from}",
                                org_schema = test_org_schema,
                                dest_schema = test_dest_schema,
                                dest_table = test_sql_to_sql_tbl_to,
                                print_cmd=True)

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