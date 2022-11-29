import os
#import random

import configparser
import pandas as pd

from .. import pysqldb3 as pysqldb, data_io
from . import helpers

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

pg_dbc = pysqldb.DbConnect(db_type=config.get('PG_DB', 'TYPE'),
                           host=config.get('PG_DB', 'SERVER'),
                           db_name=config.get('PG_DB', 'DB_NAME'),
                           username=config.get('PG_DB', 'DB_USER'),
                           password=config.get('PG_DB', 'DB_PASSWORD'))

ms_dbconn = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                              host=config.get('SQL_DB', 'SERVER'),
                              db_name=config.get('SQL_DB', 'DB_NAME'),
                              username=config.get('SQL_DB', 'DB_USER'),
                              password=config.get('SQL_DB', 'DB_PASSWORD'))

pg_table_name = 'pg_test_table_{user}'.format(user=pg_dbc.username)
test_pg_to_sql_table ='tst_pg_to_sql_tbl_{user}'.format(user=pg_dbc.username)
test_sql_to_pg_qry_table = 'tst_sql_to_pg_qry_table_{user}'.format(user=pg_dbc.username)
test_sql_to_pg_table = 'tst_sql_to_pg_table_{user}'.format(user=pg_dbc.username)
test_pg_to_pg_tbl = 'tst_pg_to_pg_tbl_{user}'.format(user=pg_dbc.username)
test_pg_to_pg_qry_table = 'tst_pg_to_pg_qry_table_{user}'.format(user=pg_dbc.username)


# class TestPgToSql:
#     @classmethod
#     def setup_class(cls):
#         helpers.set_up_test_table_pg(db)
#
#     def test_pg_to_sql_basic(self):
#         sql.drop_table(schema=sql.default_schema, table=test_pg_to_sql_table)
#         db.query("""
#         drop table if exists working.{};
#         create table working.{} as
#
#         select *
#         from working.{}
#         limit 10
#         """.format(test_pg_to_sql_table, test_pg_to_sql_table, pg_table_name))
#
#         # Assert created correctly
#         assert db.table_exists(table=test_pg_to_sql_table, schema='working')
#
#         # Assert not in sql yet
#         org_schema = 'working'
#         org_table = test_pg_to_sql_table
#
#         assert not sql.table_exists(table=test_pg_to_sql_table)
#
#         # Move to sql
#         data_io.pg_to_sql(db, sql, org_table=org_table, org_schema=org_schema, print_cmd=True)
#
#         # Assert exists in sql
#         assert sql.table_exists(table=test_pg_to_sql_table)
#
#         # Assert df equality -- some types need to be coerced from the Pandas df for the equality assertion to hold
#
#         pg_df = db.dfquery("""
#         select *
#         from working.{}
#         order by id
#         """.format(test_pg_to_sql_table)).infer_objects()
#
#         sql_df = sql.dfquery("""
#         select *
#         from {}
#         order by id
#         """.format(test_pg_to_sql_table)).infer_objects()
#
#         # Assert
#         shared_non_geom_cols = list(set(pg_df.columns).intersection(set(sql_df.columns)) - {'geom'})
#         print(shared_non_geom_cols)
#         pd.testing.assert_frame_equal(pg_df[shared_non_geom_cols], sql_df[shared_non_geom_cols],
#                                       check_dtype=False,
#                                       check_exact=False,
#                                       check_less_precise=True,
#                                       check_datetimelike_compat=True)
#
#         # Cleanup
#         sql.drop_table(schema=sql.default_schema, table=test_pg_to_sql_table)
#         db.drop_table(table=test_pg_to_sql_table, schema='working')
#
#     def test_pg_to_sql_naming(self):
#         dest_name = 'another_tst_name_{}'.format(db.user)
#         sql.drop_table(schema=sql.default_schema, table=test_pg_to_sql_table)
#         sql.drop_table(schema=sql.default_schema, table=dest_name)
#
#         db.query("""
#         drop table if exists working.{};
#         create table working.{} as
#
#         select *
#         from working.{}
#         limit 10
#         """.format(test_pg_to_sql_table, test_pg_to_sql_table, pg_table_name))
#
#         # Assert table created correctly
#         assert db.table_exists(table=test_pg_to_sql_table, schema='working')
#
#         # Assert not in sql yet
#         sql.drop_table(schema=sql.default_schema, table=test_pg_to_sql_table)
#         assert not sql.table_exists(table=test_pg_to_sql_table)
#         assert not sql.table_exists(table=dest_name)
#
#         # Move to sql from pg
#         data_io.pg_to_sql(db, sql, org_schema='working', org_table=test_pg_to_sql_table, dest_table=dest_name,
#                           print_cmd=True)
#
#         # Assert created properly in sql with names
#         assert sql.table_exists(table=dest_name, schema=sql.default_schema)
#         assert not sql.table_exists(table=test_pg_to_sql_table)
#
#         # Assert df equality -- some types need to be coerced from the Pandas df for the equality assertion to hold
#         pg_df = db.dfquery("""
#         select *
#         from working.{}
#         order by id
#         """.format(test_pg_to_sql_table)).infer_objects()
#
#         sql_df = sql.dfquery("""
#         select *
#         from {}
#         order by id
#         """.format(dest_name)).infer_objects()
#
#         # Assert
#         shared_non_geom_cols = list(set(pg_df.columns).intersection(set(sql_df.columns)) - {'geom'})
#         pd.testing.assert_frame_equal(pg_df[shared_non_geom_cols], sql_df[shared_non_geom_cols],
#                                       check_dtype=False,
#                                       check_exact=False,
#                                       check_less_precise=True,
#                                       check_datetimelike_compat=True)
#
#         # Cleanup
#         sql.drop_table(schema=sql.default_schema, table=dest_name)
#         db.drop_table(table=test_pg_to_sql_table, schema='working')
#
#     def test_pg_to_sql_spatial_table(self):
#         org_schema = 'working'
#         org_table = test_pg_to_sql_table
#
#         dest_name = test_pg_to_sql_table
#         dest_name_not_spatial = test_pg_to_sql_table + '_not_spatial'
#
#         sql.drop_table(schema=sql.default_schema, table=dest_name)
#         sql.drop_table(schema=sql.default_schema, table=dest_name_not_spatial)
#
#         db.query("""
#            drop table if exists working.{};
#            create table working.{} as
#
#            select 'hello' as c, st_transform(geom, 4326) as geom
#            from working.{}
#            limit 10
#         """.format(test_pg_to_sql_table, test_pg_to_sql_table, pg_table_name))
#
#         # Assert table made correctly
#         assert db.table_exists(table=test_pg_to_sql_table, schema='working')
#
#         # Assert neither table in SQL Server yet
#         assert not sql.table_exists(table=dest_name)
#         assert not sql.table_exists(table=dest_name_not_spatial)
#
#         # Move from pg to sql, with different spatial flags
#         data_io.pg_to_sql(db, sql, org_table=org_table, org_schema=org_schema, dest_table=dest_name, spatial=True,
#                           print_cmd=True)
#         data_io.pg_to_sql(db, sql, org_table=org_table, org_schema=org_schema, dest_table=dest_name_not_spatial,
#                           spatial=False, print_cmd=True)
#
#         # Assert move worked
#         assert sql.table_exists(table=dest_name)
#         assert sql.table_exists(table=dest_name_not_spatial)
#
#         spatial_df = sql.dfquery("""
#         select *
#         from {}
#         """.format(dest_name)).infer_objects()
#
#         not_spatial_df = sql.dfquery("""
#         select *
#         from {}
#         """.format(dest_name_not_spatial)).infer_objects()
#         # work around for older GDALs where non-spatial means you dont have a geom column
#         if 'geom' not in not_spatial_df.columns and 'ogr_geometry' not in not_spatial_df.columns:
#             # worked correctly
#             assert True
#         else:
#             joined_df = spatial_df.join(not_spatial_df, on='ogr_fid', rsuffix='_ns')
#
#             # Assert spatial functionality worked
#             # This is shown by geom being different; as when spatial is true, the a_srs flag works successfully and
#             # converts the srid
#             assert len(spatial_df) == len(not_spatial_df)
#             if 'geom_ns' in joined_df.columns:
#                 assert len(joined_df) == len(joined_df[joined_df['geom'] != joined_df['geom_ns']])
#             elif 'ogr_geometry' in joined_df.columns:
#                 assert len(joined_df) == len(joined_df[joined_df['ogr_geometry'] != joined_df['ogr_geometry_ns']])
#
#         # Assert df equality -- some types need to be coerced from the Pandas df for the equality assertion to hold
#         pg_df = db.dfquery("""
#         select *
#         from working.{}
#         """.format(test_pg_to_sql_table)).infer_objects()
#
#         # Assert
#         shared_non_geom_cols = list(set(pg_df.columns).intersection(set(spatial_df.columns)) - {'geom'})
#         pd.testing.assert_frame_equal(pg_df[shared_non_geom_cols], spatial_df[shared_non_geom_cols],
#                                       check_dtype=False,
#                                       check_exact=False,
#                                       check_less_precise=True,
#                                       check_datetimelike_compat=True)
#
#         shared_non_geom_cols = list(set(pg_df.columns).intersection(set(spatial_df.columns)) - {'geom'})
#         pd.testing.assert_frame_equal(pg_df[shared_non_geom_cols], not_spatial_df[shared_non_geom_cols],
#                                       check_dtype=False,
#                                       check_exact=False,
#                                       check_less_precise=True,
#                                       check_datetimelike_compat=True)
#
#         # Cleanup
#         sql.drop_table(schema=sql.default_schema, table=dest_name_not_spatial)
#         sql.drop_table(schema=sql.default_schema, table=dest_name)
#         db.drop_table(table=test_pg_to_sql_table, schema='working')
#
#     def test_pg_to_sql_error(self):
#         return
#
#     # Note: temporary functionality will be tested separately!
#     # Still to test: LDAP, print_cmd
#
#     @classmethod
#     def teardown_class(cls):
#         helpers.clean_up_test_table_pg(db)
#
#
# class TestSqlToPgQry:
#     def test_sql_to_pg_qry_basic_table(self):
#         # Assert pg table doesn't exist
#         db.drop_table(schema=db.default_schema, table=test_sql_to_pg_qry_table)
#         assert not db.table_exists(table=test_sql_to_pg_qry_table)
#
#         # Add test_table
#         sql.drop_table(schema='dbo', table=test_sql_to_pg_qry_table)
#         sql.query("""
#         create table dbo.{} (test_col1 int, test_col2 int);
#         insert into dbo.{} VALUES(1, 2);
#         insert into dbo.{} VALUES(3, 4);
#         """.format(test_sql_to_pg_qry_table, test_sql_to_pg_qry_table, test_sql_to_pg_qry_table))
#
#         # sql_to_pg_qry
#         data_io.sql_to_pg_qry(sql, db, query="select * from dbo.{}".format(test_sql_to_pg_qry_table),
#                               dest_table=test_sql_to_pg_qry_table, print_cmd=True)
#
#         # Assert sql to pg query was successful (table exists)
#         assert db.table_exists(table=test_sql_to_pg_qry_table)
#
#         # Assert df equality
#         sql_df = sql.dfquery("""
#         select * from dbo.{}
#         order by test_col1
#         """.format(test_sql_to_pg_qry_table)).infer_objects().replace('\s+', '', regex=True)
#
#         pg_df = db.dfquery("""
#         select * from {}
#         order by test_col1
#         """.format(test_sql_to_pg_qry_table)).infer_objects().replace('\s+', '', regex=True)
#
#         sql_df.columns = [c.lower() for c in list(sql_df.columns)]
#
#         # Assert
#         pd.testing.assert_frame_equal(sql_df, pg_df.drop(['ogc_fid', 'geom'], axis=1), check_dtype=False,
#                                       check_column_type=False)
#
#         # Cleanup
#         db.drop_table(schema=db.default_schema, table=test_sql_to_pg_qry_table)
#         sql.drop_table(schema='dbo', table=test_sql_to_pg_qry_table)
#
#     # def test_sql_to_pg_qry_spatial(self):
#     # db.drop_table(schema=db.default_schema, table='tst_sql_to_pg_qry_table')
#     # db.drop_table(schema=db.default_schema, table='tst_sql_to_pg_qry_spatial_table')
#     #
#     # assert not db.table_exists(table='tst_sql_to_pg_qry_spatial_table')
#     # assert not db.table_exists(table='tst_sql_to_pg_qry_table')
#     #
#     # data_io.sql_to_pg_qry(sql, db, query="select top 10 CAST(geometry::Point([X_COORD], [Y_COORD], 2236) AS VARCHAR) as geom from WC_ACCIDENT_F_v2",
#     #                       table_name='tst_sql_to_pg_qry_spatial_table')
#     #
#     # data_io.sql_to_pg_qry(sql, db, query="select top 10 CAST(geometry::Point([X_COORD], [Y_COORD], 2236) AS VARCHAR) as geom from WC_ACCIDENT_F_v2",
#     #                       table_name='tst_sql_to_pg_qry_table', spatial=True)
#     #
#     # assert db.table_exists(table='tst_sql_to_pg_qry_table')
#     # assert len(db.dfquery('select * from tst_sql_to_pg_qry_table')) == 10
#     #
#     # assert db.table_exists(table='tst_sql_to_pg_qry_spatial_table')
#     # assert len(db.dfquery('select * from tst_sql_to_pg_qry_spatial_table')) == 10
#     #
#     # spatial_df = db.dfquery("""
#     # select *
#     # from tst_sql_to_pg_qry_table
#     #     --SELECT column_name, data_type FROM information_schema.columns WHERE
#     #     --table_name = ''
#     # """)
#     #
#     # print(spatial_df)
#     #
#     # not_spatial_df = db.dfquery("""
#     #         select *
#     #         from tst_sql_to_pg_qry_spatial_table
#     #     --SELECT column_name, data_type FROM information_schema.columns WHERE
#     #     --table_name = ''
#     # """)
#     #
#     # print(not_spatial_df)
#     #
#     # # joined_df = spatial_df.merge(not_spatial_df, on='ogc_fid')
#     # #
#     # # print(joined_df)
#     # #
#     # # assert len(spatial_df) == len(not_spatial_df) and len(joined_df) == len(
#     # #     joined_df[joined_df['geom_x'] != joined_df['geom_y']])
#     #
#     # db.drop_table(schema=db.default_schema, table='tst_sql_to_pg_qry_table')
#     # db.drop_table(schema=db.default_schema, table='tst_sql_to_pg_qry_spatial_table')
#
#     def test_sql_to_pg_qry_dest_schema(self):
#         # Assert doesn't exist already
#         db.drop_table(schema='working', table=test_sql_to_pg_qry_table)
#         assert not db.table_exists(schema='working', table=test_sql_to_pg_qry_table)
#
#         # Add test_table
#         sql.drop_table(schema='dbo', table=test_sql_to_pg_qry_table)
#         sql.query("""
#         create table dbo.{} (test_col1 int, test_col2 int);
#         insert into dbo.{} VALUES(1, 2);
#         insert into dbo.{} VALUES(3, 4);
#         """.format(test_sql_to_pg_qry_table, test_sql_to_pg_qry_table, test_sql_to_pg_qry_table))
#
#         # sql_to_pg_qry
#         data_io.sql_to_pg_qry(sql, db, query="select * from dbo.{}".format(test_sql_to_pg_qry_table),
#                               dest_table=test_sql_to_pg_qry_table, dest_schema='working', print_cmd=True)
#
#         # Assert sql_to_pg_qry successful and correct length
#         assert db.table_exists(schema='working', table=test_sql_to_pg_qry_table)
#         assert len(db.dfquery('select * from working.{}'.format(test_sql_to_pg_qry_table))) == 2
#
#         # Assert df equality
#         sql_df = sql.dfquery("""
#         select * from dbo.{}
#         order by test_col1
#         """.format(test_sql_to_pg_qry_table)).infer_objects().replace('\s+', '', regex=True)
#
#         pg_df = db.dfquery("""
#         select * from working.{}
#         order by test_col1
#         """.format(test_sql_to_pg_qry_table)).infer_objects().replace('\s+', '', regex=True)
#
#         sql_df.columns = [c.lower() for c in list(sql_df.columns)]
#
#         # Assert
#         pd.testing.assert_frame_equal(sql_df, pg_df.drop(['ogc_fid', 'geom'], axis=1), check_column_type=False,
#                                       check_dtype=False)
#
#         # Cleanup
#         db.drop_table(schema='working', table=test_sql_to_pg_qry_table)
#         sql.drop_table(schema='dbo', table=test_sql_to_pg_qry_table)
#
#     def test_sql_to_pg_qry_no_dest_table_input(self):
#         return
#
#     def test_sql_to_pg_qry_empty_query_error(self):
#         return
#
#     def test_sql_to_pg_qry_empty_wrong_layer_error(self):
#         return
#
#     def test_sql_to_pg_qry_empty_overwrite_error(self):
#         return
#
#     # Note: temporary functionality will be tested separately!
#     # Still to test: LDAP, print_cmd
#
#
# class TestSqlToPg:
#     def test_sql_to_pg_basic_table(self):
#         db.drop_table(db.default_schema, test_sql_to_pg_table)
#         # Assert table doesn't exist in pg
#         assert not db.table_exists(table=test_sql_to_pg_table)
#
#         # Add test_table
#         sql.drop_table(schema='dbo', table=test_sql_to_pg_table)
#         sql.query("""
#          create table dbo.{} (test_col1 int, test_col2 int);
#          insert into dbo.{} VALUES(1, 2);
#          insert into dbo.{} VALUES(3, 4);
#          """.format(test_sql_to_pg_table, test_sql_to_pg_table, test_sql_to_pg_table))
#
#         # Sql_to_pg
#         data_io.sql_to_pg(sql, db, org_table=test_sql_to_pg_table, org_schema='dbo', dest_table=test_sql_to_pg_table,
#                           print_cmd=True)
#
#         # Assert sql_to_pg was successful; table exists in pg
#         assert db.table_exists(table=test_sql_to_pg_table)
#
#         # Assert df equality
#         sql_df = sql.dfquery("""
#         select * from dbo.{} order by test_col1
#         """.format(test_sql_to_pg_table)).infer_objects().replace('\s+', '', regex=True)
#
#         pg_df = db.dfquery("""
#         select * from {} order by test_col1
#         """.format(test_sql_to_pg_table)).infer_objects().replace('\s+', '', regex=True)
#
#         sql_df.columns = [c.lower() for c in list(sql_df.columns)]
#
#         # Assert
#         pd.testing.assert_frame_equal(sql_df, pg_df.drop(['geom', 'ogc_fid'], axis=1),
#                                       check_dtype=False, check_column_type=False)
#
#         # Cleanup
#         db.drop_table(schema=db.default_schema, table=test_sql_to_pg_table)
#         sql.drop_table(schema='dbo', table=test_sql_to_pg_table)
#
#     def test_sql_to_pg_dest_schema_name(self):
#         # Assert table doesn't exist in pg
#         db.drop_table('working', test_sql_to_pg_table)
#         assert not db.table_exists(schema='working', table=test_sql_to_pg_table)
#
#         # Add test_table
#         sql.drop_table(schema='dbo', table=test_sql_to_pg_table)
#         sql.query("""
#          create table dbo.{} (test_col1 int, test_col2 int);
#          insert into dbo.{} VALUES(1, 2);
#          insert into dbo.{} VALUES(3, 4);
#          """.format(test_sql_to_pg_table, test_sql_to_pg_table, test_sql_to_pg_table))
#
#         # Sql_to_pg
#         data_io.sql_to_pg(sql, db, org_table=test_sql_to_pg_table, org_schema='dbo', dest_table=test_sql_to_pg_table,
#                           dest_schema='working', print_cmd=True)
#
#         # Assert sql_to_pg was successful; table exists in pg
#         assert db.table_exists(schema='working', table=test_sql_to_pg_table)
#
#         # Assert df equality
#         sql_df = sql.dfquery("""
#         select * from dbo.{} order by test_col1
#         """.format(test_sql_to_pg_table)).infer_objects().replace('\s+', '', regex=True)
#
#         pg_df = db.dfquery("""
#         select * from working.{} order by test_col1
#         """.format(test_sql_to_pg_table)).infer_objects().replace('\s+', '', regex=True)
#
#         sql_df.columns = [c.lower() for c in list(sql_df.columns)]
#
#         # Assert
#         pd.testing.assert_frame_equal(sql_df, pg_df.drop(['geom', 'ogc_fid'], axis=1),
#                                       check_dtype=False,
#                                       check_column_type=False)
#
#         # Cleanup
#         db.drop_table(schema='working', table=test_sql_to_pg_table)
#         sql.drop_table(schema='dbo', table=test_sql_to_pg_table)
#
#     def test_sql_to_pg_org_schema_name(self):
#         # TODO: test with non-DBO table
#         return
#
#     def test_sql_to_pg_spatial(self):
#         # TODO: when adding spatial features like SRID via a_srs, test spatial
#         return
#
#     def test_sql_to_pg_wrong_layer_error(self):
#         return
#
#     def test_sql_to_pg_error(self):
#         return
#
#     # Note: temporary functionality will be tested separately!
#     # Still to test: LDAP, print_cmd


class TestPgToPg:
    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_pg(pg_dbc)

    def test_pg_to_pg_basic_table(self):
        # Must have RIS DB info in db_config.cfg [SECOND_PG_DB] section
        ris_dbc = pysqldb.DbConnect(db_type=config.get('SECOND_PG_DB', 'TYPE'),
                                host=config.get('SECOND_PG_DB', 'SERVER'),
                                db_name=config.get('SECOND_PG_DB', 'DB_NAME'),
                                username=config.get('SECOND_PG_DB', 'DB_USER'),
                                password=config.get('SECOND_PG_DB', 'DB_PASSWORD'))

        pg_dbc.drop_table(schema_name='working', table_name=test_pg_to_pg_tbl)
        ris_dbc.drop_table(schema_name='working', table_name=test_pg_to_pg_tbl)

        # Create table
        pg_dbc.query("""
            create table working.{test_table} as 
            select * 
            from working.{table} 
            limit 10 
        """.format(test_table=test_pg_to_pg_tbl, table=pg_table_name))

        # Assert tables don't already exist in destination
        assert pg_dbc.table_exists(schema_name='working', table_name=test_pg_to_pg_tbl)
        assert not ris_dbc.table_exists(schema_name='working', table_name=test_pg_to_pg_tbl)

        # pg_to_pg
        data_io.pg_to_pg(pg_dbc, ris_dbc, src_schema_name='working', src_table_name=test_pg_to_pg_tbl, dest_schema_name='working',
                         print_cmd=True)

        # Assert pg_to_pg successful
        assert pg_dbc.table_exists(schema_name='working', table_name=test_pg_to_pg_tbl)
        assert ris_dbc.table_exists(schema_name='working', table_name=test_pg_to_pg_tbl)

        # Assert db equality
        ris_df = ris_dbc.dfquery("""
              select * 
              from working.{table}
          """.format(table=test_pg_to_pg_tbl)).infer_objects()

        pg_df = pg_dbc.dfquery("""
              select * 
              from working.{table}
          """.format(table=test_pg_to_pg_tbl)).infer_objects()

        # Assert
        pd.testing.assert_frame_equal(ris_df.drop(['geom', 'ogc_fid'], axis=1), pg_df.drop(['geom'], axis=1),
                                      check_exact=False,
                                      check_less_precise=True)

        # Cleanup
        pg_dbc.drop_table(schema_name='working', table_name=test_pg_to_pg_tbl)
        ris_dbc.drop_table(schema_name='working', table_name=test_pg_to_pg_tbl)

    def test_pg_to_pg_basic_name_table(self):
        # Must have RIS DB info in db_config.cfg [SECOND_PG_DB] section
        ris_dbc = pysqldb.DbConnect(db_type=config.get('SECOND_PG_DB', 'TYPE'),
                                host=config.get('SECOND_PG_DB', 'SERVER'),
                                db_name=config.get('SECOND_PG_DB', 'DB_NAME'),
                                username=config.get('SECOND_PG_DB', 'DB_USER'),
                                password=config.get('SECOND_PG_DB', 'DB_PASSWORD'))

        pg_dbc.drop_table(schema_name='working', table_name=test_pg_to_pg_tbl)

        test_pg_to_pg_tbl_other = test_pg_to_pg_tbl + '_another_name'
        ris_dbc.drop_table(schema_name='working', table_name=test_pg_to_pg_tbl_other)

        # Create table for testing in ris
        pg_dbc.query("""
            create table working.{test_table} as 
            select * 
            from working.{table} 
            limit 10 
        """.format(test_table=test_pg_to_pg_tbl, table=pg_table_name))

        # Assert final table doesn't already exist
        assert not ris_dbc.table_exists(schema_name='working', table_name=test_pg_to_pg_tbl_other)
        assert pg_dbc.table_exists(schema_name='working', table_name=test_pg_to_pg_tbl)

        # pg_to_pg
        data_io.pg_to_pg(pg_dbc, ris_dbc, src_schema_name='working', src_table_name=test_pg_to_pg_tbl,
                         dest_schema_name='working', dest_table_name=test_pg_to_pg_tbl_other, print_cmd=True)

        # Assert pg_to_pg was successful
        assert pg_dbc.table_exists(schema_name='working', table_name=test_pg_to_pg_tbl)
        assert ris_dbc.table_exists(schema_name='working', table_name=test_pg_to_pg_tbl_other)

        # Assert db equality
        ris_df = ris_dbc.dfquery("""
            select * 
            from working.{table}
        """.format(table=test_pg_to_pg_tbl_other)).infer_objects()

        pg_df = pg_dbc.dfquery("""
            select * 
            from working.{table}
        """.format(table=test_pg_to_pg_tbl)).infer_objects()

        # Assert
        pd.testing.assert_frame_equal(ris_df.drop(['geom', 'ogc_fid'], axis=1), pg_df.drop(['geom'], axis=1),
                                      check_exact=False,
                                      check_less_precise=True)

        # Cleanup
        ris_dbc.drop_table(schema_name='working', table_name=test_pg_to_pg_tbl_other)
        pg_dbc.drop_table(schema_name='working', table_name=test_pg_to_pg_tbl)

        # Note: temporary functionality will be tested separately!
        # Still to test: LDAP, print_cmd

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(pg_dbc)
        pg_dbc.cleanup_new_tables()
        ms_dbconn.cleanup_new_tables()


class TestPgToPgQry:
    def test_pg_to_pg_qry_basic_table(self):
        ris_dbc = pysqldb.DbConnect(db_type=config.get('SECOND_PG_DB', 'TYPE'),
                                   host=config.get('SECOND_PG_DB', 'SERVER'),
                                   db_name=config.get('SECOND_PG_DB', 'DB_NAME'),
                                   username=config.get('SECOND_PG_DB', 'DB_USER'),
                                   password=config.get('SECOND_PG_DB', 'DB_PASSWORD'))

        # Assert pg table doesn't exist
        pg_dbc.drop_table(schema_name=pg_dbc.default_schema, table_name=test_pg_to_pg_qry_table)
        assert not pg_dbc.table_exists(table_name=test_pg_to_pg_qry_table)

        # Add test_table
        ris_dbc.drop_table(schema_name='public', table_name=test_pg_to_pg_qry_table)
        ris_dbc.query("""
        create table public.{table} (test_col1 int, test_col2 int);
        insert into public.{table} VALUES(1, 2);
        insert into public.{table} VALUES(3, 4);
        """.format(table=test_pg_to_pg_qry_table))

        # sql_to_pg_qry
        data_io.pg_to_pg_qry(ris_dbc, pg_dbc, query="select * from public.{table}".format(table=test_pg_to_pg_qry_table),
                             dest_table_name=test_pg_to_pg_qry_table, print_cmd=True, is_spatial=False)

        # Assert sql to pg query was successful (table exists)
        assert pg_dbc.table_exists(table_name=test_pg_to_pg_qry_table)

        # Assert df equality
        ris_df = ris_dbc.dfquery("""
        select * from public.{table}
        order by test_col1
        """.format(table=test_pg_to_pg_qry_table)).infer_objects().replace('\s+', '', regex=True)

        pg_df = pg_dbc.dfquery("""
        select * from {table}
        order by test_col1
        """.format(table=test_pg_to_pg_qry_table)).infer_objects().replace('\s+', '', regex=True)

        ris_df.columns = [c.lower() for c in list(ris_df.columns)]

        # Assert
        pd.testing.assert_frame_equal(ris_df, pg_df.drop(['ogc_fid'], axis=1), check_dtype=False,
                                      check_column_type=False)

        # Cleanup
        pg_dbc.drop_table(schema_name=pg_dbc.default_schema, table_name=test_pg_to_pg_qry_table)
        ris_dbc.drop_table(schema_name='public', table_name=test_pg_to_pg_qry_table)

    def test_pg_to_pg_qry_dest_schema(self):
        src_dbc = pysqldb.DbConnect(db_type=config.get('SECOND_PG_DB', 'TYPE'),
                                   host=config.get('SECOND_PG_DB', 'SERVER'),
                                   db_name=config.get('SECOND_PG_DB', 'DB_NAME'),
                                   username=config.get('SECOND_PG_DB', 'DB_USER'),
                                   password=config.get('SECOND_PG_DB', 'DB_PASSWORD'))

        # Assert doesn't exist already
        pg_dbc.drop_table(schema_name='working', table_name=test_pg_to_pg_qry_table)
        assert not pg_dbc.table_exists(schema_name='working', table_name=test_pg_to_pg_qry_table)

        # Add test_table
        src_dbc.drop_table(schema_name='working', table_name=test_pg_to_pg_qry_table)
        src_dbc.query("""
        create table working.{table} (test_col1 int, test_col2 int);
        insert into working.{table} VALUES(1, 2);
        insert into working.{table} VALUES(3, 4);
        """.format(table=test_pg_to_pg_qry_table))

        # sql_to_pg_qry
        data_io.pg_to_pg_qry(src_dbc, pg_dbc, query="select * from working.{table}".format(table=test_pg_to_pg_qry_table),
                             dest_table_name=test_pg_to_pg_qry_table, dest_schema_name='working', print_cmd=True)

        # Assert sql_to_pg_qry successful and correct length
        assert pg_dbc.table_exists(schema='working', table_name=test_pg_to_pg_qry_table)
        assert len(pg_dbc.dfquery('select * from working.{table}'.format(table=test_pg_to_pg_qry_table))) == 2

        # Assert df equality
        src_df = src_dbc.dfquery("""
        select * from working.{table}
        order by test_col1
        """.format(table=test_pg_to_pg_qry_table)).infer_objects().replace('\s+', '', regex=True)

        pg_df = pg_dbc.dfquery("""
        select * from working.{table}
        order by test_col1
        """.format(table=test_pg_to_pg_qry_table)).infer_objects().replace('\s+', '', regex=True)

        src_df.columns = [c.lower() for c in list(src_df.columns)]

        # Assert
        pd.testing.assert_frame_equal(src_df, pg_df.drop(['ogc_fid'], axis=1), check_column_type=False,
                                      check_dtype=False)

        # Cleanup
        pg_dbc.drop_table(schema_name='working', table_name=test_pg_to_pg_qry_table)
        ms_dbconn.drop_table(schema_name='dbo', table_name=test_pg_to_pg_qry_table)

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