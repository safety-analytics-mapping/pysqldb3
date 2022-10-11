from multiprocessing.sharedctypes import Value
import os
import random
import pytest

import configparser
from sys import orig_argv
import pandas as pd

from .. import pysqldb3 as pysqldb, data_io
from . import helpers

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

pg_table_name = f'pg_test_table_{db.user}'
test_pg_to_sql_table =f'tst_pg_to_sql_tbl_{db.user}'
test_sql_to_pg_qry_table = f'tst_sql_to_pg_qry_table_{db.user}'
test_sql_to_pg_table = f'tst_sql_to_pg_table_{db.user}'
test_pg_to_pg_tbl = f'tst_pg_to_pg_tbl_{db.user}'
test_pg_to_pg_qry_table = f'tst_pg_to_pg_qry_table_{db.user}'


class TestPgToSql:
     @classmethod
     def setup_class(cls):
         helpers.set_up_test_table_pg(db)

     def test_pg_to_sql_basic(self, schema='working', table_name=test_pg_to_sql_table):
         sql.drop_table(schema=sql.default_schema, table=table_name)
         db.query(f"""
         DROP TABLE IF EXISTS {schema}.{table_name};
         CREATE TABLE {schema}.{table_name} AS

         SELECT *
         FROM {schema}.{table_name}
         LIMIT 10
         """)

         # Assert created correctly
         assert db.table_exists(table=table_name, schema=schema)

         # Assert not in sql yet
         org_schema = schema
         org_table = table_name

         assert not sql.table_exists(table=org_table)

         # Move to sql
         data_io.pg_to_sql(db, sql, org_table=table_name, org_schema=schema, print_cmd=True)

         # Assert exists in sql
         assert sql.table_exists(table=table_name)

         # Assert df equality -- some types need to be coerced from the Pandas df for the equality assertion to hold

         pg_df = db.dfquery(f"""
         SELECT *
         FROM {schema}.{table_name}
         ORDER BY id
         """).infer_objects()

         sql_df = sql.dfquery(f"""
         SELECT *
         FROM {table_name}
         ORDER BY id
         """).infer_objects()

         # Assert
         shared_non_geom_cols = list(set(pg_df.columns).intersection(set(sql_df.columns)) - {'geom'})
         print(shared_non_geom_cols)
         pd.testing.assert_frame_equal(pg_df[shared_non_geom_cols], sql_df[shared_non_geom_cols],
                                       check_dtype=False,
                                       check_exact=False,
                                       check_less_precise=True,
                                       check_datetimelike_compat=True)

         # Cleanup
         sql.drop_table(schema=sql.default_schema, table=table_name)
         db.drop_table(schema=schema, table=table_name)

     def test_pg_to_sql_naming(self, schema='working', table_name='test_pg_to_sql_table'):
         dest_name = f'another_tst_name_{db.user}'
         sql.drop_table(schema=sql.default_schema, table=table_name)
         sql.drop_table(schema=sql.default_schema, table=dest_name)

         db.query(f"""
         DROP TABLE IF EXISTS {schema}.{table_name};
         CREATE TABLE {schema}.{table_name} AS

         SELECT *
         FROM {schema}.{table_name}
         LIMIT 10
         """)

         # Assert table created correctly
         assert db.table_exists(schema=schema, table=table_name)

         # Assert not in sql yet
         sql.drop_table(schema=sql.default_schema, table=table_name)
         assert not sql.table_exists(table=table_name)
         assert not sql.table_exists(table=dest_name)

         # Move to sql from pg
         data_io.pg_to_sql(db, sql, org_schema=schema, org_table=table_name, dest_table=dest_name, print_cmd=True)

         # Assert created properly in sql with names
         assert sql.table_exists(schema=sql.default_schema, table=dest_name)
         assert not sql.table_exists(table=table_name)

         # Assert df equality -- some types need to be coerced from the Pandas df for the equality assertion to hold
         pg_df = db.dfquery(f"""
         SELECT *
         FROM {schema}.{table_name}
         ORDER BY id
         """).infer_objects()

         sql_df = sql.dfquery(f"""
         SELECT *
         FROM {schema}.{dest_name}
         ORDER BY id
         """).infer_objects()

         # Assert
         shared_non_geom_cols = list(set(pg_df.columns).intersection(set(sql_df.columns)) - {'geom'})
         pd.testing.assert_frame_equal(pg_df[shared_non_geom_cols], sql_df[shared_non_geom_cols],
                                       check_dtype=False,
                                       check_exact=False,
                                       check_less_precise=True,
                                       check_datetimelike_compat=True)

         # Cleanup
         sql.drop_table(schema=sql.default_schema, table=dest_name)
         db.drop_table(schema=schema, table=table_name)

     def test_pg_to_sql_spatial_table(self, schema='working', table_name='test-pg-to-sql-table'):
         org_table = table_name
         dest_name = table_name
         dest_name_not_spatial = f'{dest_name}_not_spatial'

         sql.drop_table(schema=sql.default_schema, table=dest_name)
         sql.drop_table(schema=sql.default_schema, table=dest_name_not_spatial)

         db.query(f"""
            DROP TABLE IF EXISTS {schema}.{table_name};
            CREATE TABLE {schema}.{table_name} AS

            SELECT 'hello' AS c, st_transform(geom, 4326) AS geom
            FROM {schema}.{table_name}
            LIMIT 10
         """)

         # Assert table made correctly
         assert db.table_exists(schema=schema, table=table_name)

         # Assert neither table in SQL Server yet
         assert not sql.table_exists(table=dest_name)
         assert not sql.table_exists(table=dest_name_not_spatial)

         # Move from pg to sql, with different spatial flags
         data_io.pg_to_sql(db, sql, org_table=org_table, org_schema=schema, dest_table=dest_name, spatial=True, print_cmd=True)
         data_io.pg_to_sql(db, sql, org_table=org_table, org_schema=schema, dest_table=dest_name_not_spatial, spatial=False, print_cmd=True)

         # Assert move worked
         assert sql.table_exists(table=dest_name)
         assert sql.table_exists(table=dest_name_not_spatial)

         spatial_df = sql.dfquery(f"SELECT * FROM {dest_name}").infer_objects()
         not_spatial_df = sql.dfquery(f"SELECT * FROM {dest_name_not_spatial}").infer_objects()
         
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
         pg_df = db.dfquery(f" SELECT * FROM {schema}.{table_name}")

         # Assert
         shared_non_geom_cols = list(set(pg_df.columns).intersection(set(spatial_df.columns)) - {'geom'})
         pd.testing.assert_frame_equal(pg_df[shared_non_geom_cols], spatial_df[shared_non_geom_cols],
                                       check_dtype=False,
                                       check_exact=False,
                                       check_less_precise=True,
                                       check_datetimelike_compat=True)

         shared_non_geom_cols = list(set(pg_df.columns).intersection(set(spatial_df.columns)) - {'geom'})
         pd.testing.assert_frame_equal(pg_df[shared_non_geom_cols], not_spatial_df[shared_non_geom_cols],
                                       check_dtype=False,
                                       check_exact=False,
                                       check_less_precise=True,
                                       check_datetimelike_compat=True)

         # Cleanup
         sql.drop_table(schema=sql.default_schema, table=dest_name_not_spatial)
         sql.drop_table(schema=sql.default_schema, table=dest_name)
         db.drop_table(schema=schema, table=table_name)

     def test_pg_to_sql_error(self):
         return

     # Note: temporary functionality will be tested separately!
     # Still to test: LDAP, print_cmd

     @classmethod
     def teardown_class(cls):
         helpers.clean_up_test_table_pg(db)


class TestSqlToPgQry:
     def test_sql_to_pg_qry_basic_table(self, sql_table_name='test_sql_to_pg_qry_basic_table', pg_table_name='test_sql_to_pg_qry_basic_table'):
         # Assert pg table doesn't exist
         db.drop_table(schema=db.default_schema, table=pg_table_name)
         assert not db.table_exists(table=pg_table_name)

         # Add test_table
         helpers.clean_up_simple_test_table_sql(sql, table_name=sql_table_name)
         helpers.set_up_simple_test_table_sql(sql, table_name=sql_table_name)

         # sql_to_pg_qry
         data_io.sql_to_pg_qry(sql, db, query=f"SELECT * FROM dbo.{sql_table_name}", dest_table=pg_table_name, print_cmd=True)

         # Assert sql to pg query was successful (table exists)
         assert db.table_exists(table=pg_table_name)

         # Assert df equality
         sql_df = sql.dfquery(f"""
         SELECT * FROM dbo.{sql_table_name}
         ORDER BY test_col1
         """).infer_objects().replace('\s+', '', regex=True)

         pg_df = db.dfquery(f"""
         SELECT * FROM {pg_table_name}
         ORDER BY test_col1
         """).infer_objects().replace('\s+', '', regex=True)

         sql_df.columns = [c.lower() for c in list(sql_df.columns)]

         # Assert
         pd.testing.assert_frame_equal(sql_df, pg_df.drop(['ogc_fid', 'geom'], axis=1), check_dtype=False,
                                       check_column_type=False)

         # Cleanup
         db.drop_table(schema=db.default_schema, table=pg_table_name)
         sql.drop_table(schema='dbo', table=sql_table_name)

     def test_sql_to_pg_qry_spatial(self, table_name='tst_sql_to_pg_qry_table', spatial_table_name='tst_sql_to_pg_qry_spatial_table'):

        # First, check to see if table_name and spatial_table_name exist as tables in the db
        db.drop_table(schema=db.default_schema, table=table_name)
        db.drop_table(schema=db.default_schema, table=spatial_table_name)

        # Assert they don't
        assert not db.table_exists(table=table_name)
        assert not db.table_exists(table=spatial_table_name)

        data_io.sql_to_pg_qry(sql, db, query="SELECT TOP 10 CAST(geometry::Point([X_COORD], [Y_COORD], 2236) AS varchar) AS geom FROM WC_ACCIDENT_F_v2",
                            table_name=spatial_table_name)
    
        data_io.sql_to_pg_qry(sql, db, query="SELECT TOP 10 CAST(geometry::Point([X_COORD], [Y_COORD], 2236) AS varchar) AS geom FROM WC_ACCIDENT_F_v2",
                                table_name=table_name, spatial=True)
        
        # Assert table exists
        assert db.table_exists(table=table_name)
        assert len(db.dfquery(f'SELECT * FROM {table_name}')) == 10
        
        # Assert spatial table exists
        assert db.table_exists(table=spatial_table_name)
        assert len(db.dfquery(f'SELECT * FROM {spatial_table_name}')) == 10
        
        spatial_df = db.dfquery(f"""
        SELECT * FROM {spatial_table_name}
            --SELECT column_name, data_type FROM information_schema.columns WHERE
                --table_name = ''
        """)
    
        print(spatial_df)
    
        not_spatial_df = db.dfquery(f"""
                SELECT * FROM {table_name}
                    --SELECT column_name, data_type FROM information_schema.columns WHERE
                        --table_name = ''
        """)
     
        print(not_spatial_df)
     
        joined_df = spatial_df.merge(not_spatial_df, on='ogc_fid')
      
        print(joined_df)
      
        assert len(spatial_df) == len(not_spatial_df) and len(joined_df) == len(joined_df[joined_df['geom_x'] != joined_df['geom_y']])
     
       # cleanup
        db.drop_table(schema=db.default_schema, table=table_name)
        db.drop_table(schema=db.default_schema, table=spatial_table_name)

     def test_sql_to_pg_qry_dest_schema(self, schema='working', table_name='test_sql_to_pg_qry_table'):
         # Assert doesn't exist already
         db.drop_table(schema=schema, table=table_name)
         assert not db.table_exists(schema=schema, table=table_name)

         # Add test_table
         sql.drop_table(schema='dbo', table=table_name)
         sql.query(f"""
         CREATE TABLE dbo.{table_name} (test_col1 int, test_col2 int);
         INSERT INTO dbo.{table_name} VALUES(1, 2);
         INSERT INTO dbo.{table_name} VALUES(3, 4);
         """)

         # sql_to_pg_qry
         data_io.sql_to_pg_qry(sql, db, query=f"SELECT * FROM dbo.{table_name}",
                               dest_table=table_name, dest_schema=schema, print_cmd=True)

         # Assert sql_to_pg_qry successful and correct length
         assert db.table_exists(schema=schema, table=table_name)
         assert len(db.dfquery(f'SELECT * FROM {schema}.{table_name}')) == 2

         # Assert df equality
         sql_df = sql.dfquery(f"""
         SELECT * from dbo.{table_name}
         ORDER BY test_col1
         """).infer_objects().replace('\s+', '', regex=True)

         pg_df = db.dfquery(f"""
         SELECT * FROM {schema}.{table_name}
         ORDER BY test_col1
         """).infer_objects().replace('\s+', '', regex=True)

         sql_df.columns = [c.lower() for c in list(sql_df.columns)]

         # Assert
         pd.testing.assert_frame_equal(sql_df, pg_df.drop(['ogc_fid', 'geom'], axis=1),
                                        check_column_type=False,
                                        check_dtype=False)

         # Cleanup
         db.drop_table(schema=schema, table=table_name)
         sql.drop_table(schema='dbo', table=test_sql_to_pg_qry_table)

     def test_sql_to_pg_qry_no_dest_table_input(self):
         return

     def test_sql_to_pg_qry_empty_query_error(self):
         return

     def test_sql_to_pg_qry_empty_wrong_layer_error(self):
         return

     def test_sql_to_pg_qry_empty_overwrite_error(self):
         return

     # Note: temporary functionality will be tested separately!
     # Still to test: LDAP, print_cmd


class TestSqlToPg:
     def test_sql_to_pg_basic_table(self, table='test_sql_to_pg_table'):
         db.drop_table(db.default_schema, table=table)
         # Assert table doesn't exist in pg
         assert not db.table_exists(table=table)

         # Add test_table
         sql.drop_table(schema='dbo', table=table)
         sql.query(f"""
          CREATE TABLE dbo.{table} (test_col1 int, test_col2 int);
          INSERT INTO dbo.{table} VALUES(1, 2);
          INSERT INTO dbo.{table} VALUES(3, 4);
          """)

         # Sql_to_pg
         data_io.sql_to_pg(sql, db, org_table=table, org_schema='dbo', dest_table=table, print_cmd=True)

         # Assert sql_to_pg was successful; table exists in pg
         assert db.table_exists(table=table)

         # Assert df equality
         sql_df = sql.dfquery(f"""
         SELECT * FROM dbo.{table} ORDER BY test_col1
         """.infer_objects().replace('\s+', '', regex=True))

         pg_df = db.dfquery(f"""
         SELECT * FROM {table} ORDER BY test_col1
         """.infer_objects().replace('\s+', '', regex=True))

         sql_df.columns = [c.lower() for c in list(sql_df.columns)]

         # Assert
         pd.testing.assert_frame_equal(sql_df, pg_df.drop(['geom', 'ogc_fid'], axis=1),
                                       check_dtype=False, check_column_type=False)

         # Cleanup
         db.drop_table(schema=db.default_schema, table=table)
         sql.drop_table(schema='dbo', table=table)

     def test_sql_to_pg_dest_schema_name(self, table='test_sql_to_pg_dest_schema_table', dest_schema_name='working'):

        # SQL default schema is 'dbo'
        sql_schema = 'dbo'

        # Assert table doesn't exist in pg
        db.drop_table(schema=sql_schema, table=table)
        assert not db.table_exists(schema=sql_schema, table=table)

        # Add test_table
        sql.drop_table(schema=sql_schema, table=table)
        sql.query(f"""
        CREATE TABLE {sql_schema}.{table} (test_col1 int, test_col2 int);
        INSERT INTO {sql_schema}.{table} VALUES(1, 2);
        INSERT INTO {sql_schema}.{table} VALUES(3, 4);
        """)

        # Sql_to_pg
        data_io.sql_to_pg(sql, db, org_table=table, org_schema=sql_schema, dest_table=table,
                        dest_schema=dest_schema_name, print_cmd=True)

        # Assert sql_to_pg was successful; table exists in pg
        assert db.table_exists(schema=dest_schema_name, table=table)

        # Assert df equality
        sql_df = sql.dfquery(f"""
        SELECT * FROM {sql_schema}.{table} ORDER BY test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        pg_df = db.dfquery(f"""
        SELECT * FROM {dest_schema_name}.{table} ORDER BY test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        sql_df.columns = [c.lower() for c in list(sql_df.columns)]

        # Assert
        pd.testing.assert_frame_equal(sql_df, pg_df.drop(['geom', 'ogc_fid'], axis=1),
                                    check_dtype=False,
                                    check_column_type=False)

        # Cleanup
        db.drop_table(schema=dest_schema_name, table=table)
        sql.drop_table(schema=sql_schema, table=table)

     def test_sql_to_pg_org_schema_name(self, table='test_sql_to_pg_orig_schema_table', orig_schema_name='dbo'):
        # TODO: test with non-DBO table

        # Assert table doesn't exist in MSSQL
        sql.drop_table(schema=orig_schema_name, table=table)
        assert not db.table_exists(schema=orig_schema_name, table=table)

        return

     def test_sql_to_pg_org_schema_name_non_dbo(self, table='test_sql_to_pg_orig_schema_table_non_dbo', orig_schema_name='dbo'):
        return self.test_sql_to_pg_org_schema_name(table=table, orig_schema_name=orig_schema_name)

     def test_sql_to_pg_spatial(self, table='test_sql_to_pg_spatial_table', orig_schema_name='dbo'):
        # TODO: when adding spatial features like SRID via a_srs, test spatial

        return

     def test_sql_to_pg_wrong_layer_error(self):
         return

     def test_sql_to_pg_error(self):
         return

     # Note: temporary functionality will be tested separately!
     # Still to test: LDAP, print_cmd


class TestPgToPg:
    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_pg(db)

    def test_pg_to_pg_basic_table(self):
        # Must have RIS DB info in db_config.cfg [SECOND_PG_DB] section
        ris = pysqldb.DbConnect(type=config.get('SECOND_PG_DB', 'TYPE'),
                                server=config.get('SECOND_PG_DB', 'SERVER'),
                                database=config.get('SECOND_PG_DB', 'DB_NAME'),
                                user=config.get('SECOND_PG_DB', 'DB_USER'),
                                password=config.get('SECOND_PG_DB', 'DB_PASSWORD'))

        db.drop_table(schema='working', table=test_pg_to_pg_tbl)
        ris.drop_table(schema='working', table=test_pg_to_pg_tbl)

        # Create table
        db.query(f"""
            CREATE TABLE working.{test_pg_to_pg_tbl} AS
            SELECT * 
            FROM working.{pg_table_name} 
            LIMIT 10 
        """)

        # Assert tables don't already exist in destination
        assert db.table_exists(schema='working', table=test_pg_to_pg_tbl)
        assert not ris.table_exists(schema='working', table=test_pg_to_pg_tbl)

        # pg_to_pg
        data_io.pg_to_pg(db, ris, org_schema='working', org_table=test_pg_to_pg_tbl, dest_schema='working',
                         print_cmd=True)

        # Assert pg_to_pg successful
        assert db.table_exists(schema='working', table=test_pg_to_pg_tbl)
        assert ris.table_exists(schema='working', table=test_pg_to_pg_tbl)

        # Assert db equality
        risdf = ris.dfquery(f"""
              SELECT * 
              FROM .{test_pg_to_pg_tbl}
          """).infer_objects()

        dbdf = db.dfquery(f"""
              SELECT 
              FROM working.{test_pg_to_pg_tbl}
          """).infer_objects()

        # Assert
        pd.testing.assert_frame_equal(risdf.drop(['geom', 'ogc_fid'], axis=1), dbdf.drop(['geom'], axis=1),
                                      check_exact=False,
                                      check_less_precise=True)

        # Cleanup
        db.drop_table(schema='working', table=test_pg_to_pg_tbl)
        ris.drop_table(schema='working', table=test_pg_to_pg_tbl)

    def test_pg_to_pg_basic_name_table(self):
        # Must have RIS DB info in db_config.cfg [SECOND_PG_DB] section
        ris = pysqldb.DbConnect(type=config.get('SECOND_PG_DB', 'TYPE'),
                                server=config.get('SECOND_PG_DB', 'SERVER'),
                                database=config.get('SECOND_PG_DB', 'DB_NAME'),
                                user=config.get('SECOND_PG_DB', 'DB_USER'),
                                password=config.get('SECOND_PG_DB', 'DB_PASSWORD'))

        db.drop_table(schema='working', table=test_pg_to_pg_tbl)

        test_pg_to_pg_tbl_other = test_pg_to_pg_tbl + '_another_name'
        ris.drop_table(schema='working', table=test_pg_to_pg_tbl_other)

        # Create table for testing in ris
        db.query(f"""
            CREATE TABLE working.{test_pg_to_pg_tbl} AS 
            SELECT * 
            FROM working.{pg_table_name} 
            LIMIT 10 
        """)

        # Assert final table doesn't already exist
        assert not ris.table_exists(schema='working', table=test_pg_to_pg_tbl_other)
        assert db.table_exists(schema='working', table=test_pg_to_pg_tbl)

        # pg_to_pg
        data_io.pg_to_pg(db, ris, org_schema='working', org_table=test_pg_to_pg_tbl,
                         dest_schema='working', dest_table=test_pg_to_pg_tbl_other, print_cmd=True)

        # Assert pg_to_pg was successful
        assert db.table_exists(schema='working', table=test_pg_to_pg_tbl)
        assert ris.table_exists(schema='working', table=test_pg_to_pg_tbl_other)

        # Assert db equality
        risdf = ris.dfquery(f"""
            SELECT * 
            FROM working.{test_pg_to_pg_tbl_other}
        """).infer_objects()

        dbdf = db.dfquery(f"""
            SELECT * 
            FROM working.{test_pg_to_pg_tbl}
        """).infer_objects()

        # Assert
        pd.testing.assert_frame_equal(risdf.drop(['geom', 'ogc_fid'], axis=1), dbdf.drop(['geom'], axis=1),
                                      check_exact=False,
                                      check_less_precise=True)

        # Cleanup
        ris.drop_table(schema='working', table=test_pg_to_pg_tbl_other)
        db.drop_table(schema='working', table=test_pg_to_pg_tbl)

        # Note: temporary functionality will be tested separately!
        # Still to test: LDAP, print_cmd

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(db)
        db.clean_up_new_tables()
        sql.clean_up_new_tables()


class TestPgToPgQry:
    def test_pg_to_pg_qry_basic_table(self, schema='public'):
        org_pg = pysqldb.DbConnect(type=config.get('SECOND_PG_DB', 'TYPE'),
                                server=config.get('SECOND_PG_DB', 'SERVER'),
                                database=config.get('SECOND_PG_DB', 'DB_NAME'),
                                user=config.get('SECOND_PG_DB', 'DB_USER'),
                                password=config.get('SECOND_PG_DB', 'DB_PASSWORD'))

        # Assert pg table doesn't exist
        db.drop_table(schema=db.default_schema, table=test_pg_to_pg_qry_table)
        assert not db.table_exists(table=test_pg_to_pg_qry_table)

        # Add test_table
        org_pg.drop_table(schema=schema, table=test_pg_to_pg_qry_table)
        org_pg.query(f"""
        CREATE TABLE {schema}.{test_pg_to_pg_qry_table} (test_col1 int, test_col2 int);
        INSERT INTO {schema}.{test_pg_to_pg_qry_table} VALUES(1, 2);
        INSERT INTO {schema}.{test_pg_to_pg_qry_table} VALUES(3, 4);
        """)

        # sql_to_pg_qry
        data_io.pg_to_pg_qry(org_pg, db, query=f"SELECT * FROM {schema}.{test_pg_to_pg_qry_table}",
                              dest_table=test_pg_to_pg_qry_table, print_cmd=True, spatial=False)

        # Assert sql to pg query was successful (table exists)
        assert db.table_exists(table=test_pg_to_pg_qry_table)

        # Assert df equality
        org_pg_df = org_pg.dfquery(f"""
        SELECT * FROM {schema}.{test_pg_to_pg_qry_table}
        ORDER BY test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        pg_df = db.dfquery(f"""
        SELECT * FROM {schema}
        ORDER BY test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        org_pg_df.columns = [c.lower() for c in list(org_pg_df.columns)]

        # Assert
        pd.testing.assert_frame_equal(org_pg_df, pg_df.drop(['ogc_fid'], axis=1), check_dtype=False,
                                      check_column_type=False)

        # Cleanup
        db.drop_table(schema=db.default_schema, table=test_pg_to_pg_qry_table)
        org_pg.drop_table(schema=schema, table=test_pg_to_pg_qry_table)

    def test_pg_to_pg_qry_dest_schema(self, schema=''):
        org_pg = pysqldb.DbConnect(type=config.get('SECOND_PG_DB', 'TYPE'),
                                   server=config.get('SECOND_PG_DB', 'SERVER'),
                                   database=config.get('SECOND_PG_DB', 'DB_NAME'),
                                   user=config.get('SECOND_PG_DB', 'DB_USER'),
                                   password=config.get('SECOND_PG_DB', 'DB_PASSWORD'))

        # Assert doesn't exist already
        db.drop_table(schema='working', table=test_pg_to_pg_qry_table)
        assert not db.table_exists(schema='working', table=test_pg_to_pg_qry_table)

        # Add test_table
        org_pg.drop_table(schema='working', table=test_pg_to_pg_qry_table)
        org_pg.query(f"""
        CREATE TABLE working.{test_pg_to_pg_qry_table} (test_col1 int, test_col2 int);
        INSERT INTO working.{test_pg_to_pg_qry_table} VALUES (1, 2);
        INSERT INTO working.{test_pg_to_pg_qry_table} VALUES (3, 4);
        """)

        # sql_to_pg_qry
        data_io.pg_to_pg_qry(org_pg, db, query=f"SELECT * FROM working.{test_pg_to_pg_qry_table}",
                              dest_table=test_pg_to_pg_qry_table, dest_schema='working', print_cmd=True)

        # Assert sql_to_pg_qry successful and correct length
        assert db.table_exists(schema='working', table=test_pg_to_pg_qry_table)
        assert len(db.dfquery(f'SELECT * FROM working.{test_pg_to_pg_qry_table}')) == 2

        # Assert df equality
        org_pg_df = org_pg.dfquery(f"""
        SELECT * FROM working.{test_pg_to_pg_qry_table}
        ORDER BY test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        pg_df = db.dfquery(f"""
        SELECT * FROM working.{test_pg_to_pg_qry_table}
        ORDER BY test_col1
        """).infer_objects().replace('\s+', '', regex=True)

        org_pg_df.columns = [c.lower() for c in list(org_pg_df.columns)]

        # Assert
        pd.testing.assert_frame_equal(org_pg_df, pg_df.drop(['ogc_fid'], axis=1), check_column_type=False,
                                      check_dtype=False)

        # Cleanup
        db.drop_table(schema='working', table=test_pg_to_pg_qry_table)
        sql.drop_table(schema='dbo', table=test_pg_to_pg_qry_table)

    def test_sql_to_pg_qry_no_dest_table_input(self, sql_schema='dbo'):
        pg = pysqldb.DbConnect(type=config.get('PG_DB','TYPE'),
        server=config.get('PG_DB','SERVER'),
        database=config.get('PG_DB','DB_NAME'),
        user=config.get('PG_DB','DB_USER'),
        password=config.get('PG_DB','DB_PASSWORD'))

        sql = pysqldb.DbConnect(type=config.get('SQL_DB','TYPE'),
        server=config.get('SQL_DB','SERVER'),
        database=config.get('SQL_DB','DB_NAME'),
        user=config.get('SQL_DB','DB_USER'),
        password=config.get('SQL_DB','DB_PASSWORD'))

        # Assert doesn't exist already
        sql.drop_table(schema=sql_schema, table=test_sql_to_pg_qry_table)
        assert not sql.table_exists(test_sql_to_pg_table)

        # Attempt query with no destination table specified
        qry = f"""CREATE TABLE {sql_schema}.{test_sql_to_pg_qry_table}
            INSERT INTO {sql_schema}.{test_sql_to_pg_qry_table} VALUES (1,2,3,4);"""

        # not sure what this is supposed to raise - ValueError?
        with pytest.raises(ValueError):
            data_io.sql_to_pg_qry(sql, pg, query=qry, spatial=False, dest_table='')

    def test_sql_to_pg_qry_empty_query_error(self):
        pg = pysqldb.DbConnect(type=config.get('PG_DB','TYPE'),
        server=config.get('PG_DB','SERVER'),
        database=config.get('PG_DB','DB_NAME'),
        user=config.get('PG_DB','DB_USER'),
        password=config.get('PG_DB','DB_PASSWORD'))

        sql = pysqldb.DbConnect(type=config.get('SQL_DB','TYPE'),
        server=config.get('SQL_DB','SERVER'),
        database=config.get('SQL_DB','DB_NAME'),
        user=config.get('SQL_DB','DB_USER'),
        password=config.get('SQL_DB','DB_PASSWORD'))

        # Assert doesn't exist already
        sql.drop_table(schema='dbo', table=test_sql_to_pg_qry_table)
        assert not sql.table_exists(test_sql_to_pg_qry_table)

        # not sure what this is supposed to raise - AttributeError?
        with pytest.raises(AttributeError):
            data_io.sql_to_pg_qry(sql, pg, query='', spatial=False, dest_table=f'test_sql_to_pg_qry_empty_query_err_{db.user}')

    def test_sql_to_pg_qry_empty_wrong_layer_error(self):
        return

    def test_sql_to_pg_qry_empty_overwrite_error(self):
        return

    # Note: temporary functionality will be tested separately!
    # Still to test: LDAP, print_cmd