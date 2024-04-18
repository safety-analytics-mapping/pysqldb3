import os
import random

import configparser
import pandas as pd

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

sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                        server=config.get('SQL_DB', 'SERVER'),
                        database=config.get('SQL_DB', 'DB_NAME'),
                        user=config.get('SQL_DB', 'DB_USER'),
                        password=config.get('SQL_DB', 'DB_PASSWORD'),
                        allow_temp_tables=True)

pg_table_name = 'pg_test_table_{}'.format(db.user)
test_pg_to_sql_table ='tst_pg_to_sql_tbl_{}'.format(db.user)
test_sql_to_pg_qry_table = 'tst_sql_to_pg_qry_table_{}'.format(db.user)
test_sql_to_pg_qry_spatial_table = 'tst_sql_to_pg_qry_spatial_table_{}'.format(db.user)
test_sql_to_pg_table = 'tst_sql_to_pg_table_{}'.format(db.user)
test_pg_to_pg_tbl = 'tst_pg_to_pg_tbl_{}'.format(db.user)
test_pg_to_pg_qry_table = 'tst_pg_to_pg_qry_table_{}'.format(db.user)

pg_schema = 'working'
sql_schema = 'dbo'

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
#         assert db.table_exists(table=test_pg_to_sql_table, schema=schema)
#
#         # Assert not in sql yet
#         org_schema = schema
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
#         db.drop_table(table=test_pg_to_sql_table, schema=schema)
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
#         assert db.table_exists(table=test_pg_to_sql_table, schema=schema)
#
#         # Assert not in sql yet
#         sql.drop_table(schema=sql.default_schema, table=test_pg_to_sql_table)
#         assert not sql.table_exists(table=test_pg_to_sql_table)
#         assert not sql.table_exists(table=dest_name)
#
#         # Move to sql from pg
#         data_io.pg_to_sql(db, sql, org_schema=schema, org_table=test_pg_to_sql_table, dest_table=dest_name,
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
#         db.drop_table(table=test_pg_to_sql_table, schema=schema)
#
#     def test_pg_to_sql_spatial_table(self):
#         org_schema = schema
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
#         assert db.table_exists(table=test_pg_to_sql_table, schema=schema)
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
#         db.drop_table(table=test_pg_to_sql_table, schema=schema)
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
class TestSqlToPgQry:

    def test_sql_to_pg_qry_basic_table(self):
        # Assert pg table doesn't exist
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)
        assert not db.table_exists(table=test_sql_to_pg_qry_table)

        sql.drop_table(sql_schema, test_sql_to_pg_qry_table)
        sql.query(f"""
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

        sql_df.columns = [c.lower() for c in list(sql_df.columns)]

        # Assert
        pd.testing.assert_frame_equal(sql_df, pg_df.drop(['geom', 'ogc_fid'], axis = 1),
                                    check_dtype=False,
                                      check_column_type=False)

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)
        # sql.query(f"""drop table if exists ##{test_sql_to_pg_qry_table}""")

    def test_sql_to_pg_qry_basic_with_comments_table(self):
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

        sql_df.columns = [c.lower() for c in list(sql_df.columns)]
        

        # Assert
        pd.testing.assert_frame_equal(sql_df, pg_df.drop(['geom', 'ogc_fid'], axis = 1),
                                    check_dtype=False,
                                      check_column_type=False)

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)

    def test_sql_to_pg_qry_spatial(self):
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_spatial_table)
    
        assert not db.table_exists(table=test_sql_to_pg_qry_spatial_table, schema = pg_schema)
        assert not db.table_exists(table=test_sql_to_pg_qry_table, schema = pg_schema)
        
        sql.query(f"""
            create table ##{test_sql_to_pg_qry_spatial_table} 
                (test_col1 int, test_col2 int, test_geom geometry);
            insert into ##{test_sql_to_pg_qry_spatial_table} (test_col1, test_col2, test_geom) 
                VALUES (1, 2, CAST(geometry::Point(700890, 123456, 2236) AS VARCHAR));
            insert into ##{test_sql_to_pg_qry_spatial_table} (test_col1, test_col2, test_geom) 
                VALUES (3, 4, CAST(geometry::Point(912763, 119434, 2236) AS VARCHAR));
        """)

        data_io.sql_to_pg_qry(sql, db, query=f"""
                                               SELECT * --comments within the query
                                                FROM ##{test_sql_to_pg_qry_spatial_table} -- geom here                
                                                -- end here""",
                              dest_table= test_sql_to_pg_qry_spatial_table,
                              dest_schema = pg_schema)
        
        data_io.sql_to_pg_qry(sql, db, query=f"""-- comments within the query
                                               SELECT * FROM ##{test_sql_to_pg_qry_table}-- use nodeid as the unique key
                                                -- includes geom    
                                            """,
                              dest_table=test_sql_to_pg_qry_table,
                              dest_schema = pg_schema,
                              spatial=True)
        
        assert db.table_exists(table=test_sql_to_pg_qry_table, schema = pg_schema)
        assert len(db.dfquery(f'select * from {pg_schema}.{test_sql_to_pg_qry_table}')) == 2
        
        assert db.table_exists(table=test_sql_to_pg_qry_spatial_table, schema = pg_schema)
        assert len(db.dfquery(f'select * from {pg_schema}.{test_sql_to_pg_qry_spatial_table}')) == 2
        
        spatial_df = db.dfquery(f"""
        select *
        from {pg_schema}.{test_sql_to_pg_qry_spatial_table}
        """)
        
        print(spatial_df)
        
        not_spatial_df = db.dfquery(f"""
                select *
                from {pg_schema}.{test_sql_to_pg_qry_table}
        """)
        
        print(not_spatial_df)
    
        joined_df = spatial_df.merge(not_spatial_df, on='test_col1')
    
        print(joined_df)
    
        assert len(spatial_df) == len(not_spatial_df) and len(joined_df) == len(
             joined_df[joined_df['geom_x'] != joined_df['geom_y']])
    
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_spatial_table)
        sql.query(f"""DROP TABLE IF EXISTS ##{test_sql_to_pg_qry_spatial_table}""")

    def test_sql_to_pg_qry_dest_schema(self):
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

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_qry_table)
        sql.query(f"""drop table if exists ##{test_sql_to_pg_qry_table}""") # run query for global temp table

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
    def test_sql_to_pg_basic_table(self):
        db.drop_table(pg_schema, test_sql_to_pg_table)
        # Assert table doesn't exist in pg
        assert not db.table_exists(table=test_sql_to_pg_table, schema = pg_schema)

        # Add test_table
        sql.drop_table(schema=sql_schema, table=test_sql_to_pg_table)
        sql.query("""
         create table dbo.{} (test_col1 int, test_col2 int);
         insert into dbo.{} VALUES(1, 2);
         insert into dbo.{} VALUES(3, 4);
         """.format(test_sql_to_pg_table, test_sql_to_pg_table, test_sql_to_pg_table))

        # Sql_to_pg
        data_io.sql_to_pg(sql, db, org_table=test_sql_to_pg_table, org_schema=sql_schema, dest_table=test_sql_to_pg_table,
                          dest_schema = pg_schema, print_cmd=True)

        # Assert sql_to_pg was successful; table exists in pg
        assert db.table_exists(table=test_sql_to_pg_table, schema=pg_schema)

        # Assert df equality
        sql_df = sql.dfquery("""
        select * from dbo.{} order by test_col1
        """.format(test_sql_to_pg_table)).infer_objects().replace('\s+', '', regex=True)

        pg_df = db.dfquery("""
        select * from {}.{} order by test_col1
        """.format(pg_schema, test_sql_to_pg_table)).infer_objects().replace('\s+', '', regex=True)

        sql_df.columns = [c.lower() for c in list(sql_df.columns)]

        # Assert
        pd.testing.assert_frame_equal(sql_df, pg_df.drop(['geom', 'ogc_fid'], axis=1),
                                      check_dtype=False, check_column_type=False)

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_table)
        sql.drop_table(schema=sql_schema, table=test_sql_to_pg_table)

    def test_sql_to_pg_dest_schema_name(self):
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
        sql_df = sql.dfquery("""
        select * from {}.{} order by test_col1
        """.format(sql_schema, test_sql_to_pg_table)).infer_objects().replace('\s+', '', regex=True)

        pg_df = db.dfquery("""
        select * from {}.{} order by test_col1
        """.format(pg_schema, test_sql_to_pg_table)).infer_objects().replace('\s+', '', regex=True)

        sql_df.columns = [c.lower() for c in list(sql_df.columns)]

        # Assert
        pd.testing.assert_frame_equal(sql_df, pg_df.drop(['geom', 'ogc_fid'], axis=1),
                                      check_dtype=False,
                                      check_column_type=False)

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_sql_to_pg_table)
        sql.drop_table(schema=sql_schema, table=test_sql_to_pg_table)

    def test_sql_to_pg_org_schema_name(self):
        # TODO: test with non-DBO table
        return

    def test_sql_to_pg_spatial(self):
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

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_pg_to_pg_tbl)
        ris.drop_table(schema=pg_schema, table=test_pg_to_pg_tbl)

    def test_pg_to_pg_basic_name_table(self):
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

        # Note: temporary functionality will be tested separately!
        # Still to test: LDAP, print_cmd

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(db)
        db.cleanup_new_tables()
        sql.cleanup_new_tables()


class TestPgToPgQry:
    def test_pg_to_pg_qry_basic_table(self):
        org_pg = pysqldb.DbConnect(type=config.get('SECOND_PG_DB', 'TYPE'),
                                server=config.get('SECOND_PG_DB', 'SERVER'),
                                database=config.get('SECOND_PG_DB', 'DB_NAME'),
                                user=config.get('SECOND_PG_DB', 'DB_USER'),
                                password=config.get('SECOND_PG_DB', 'DB_PASSWORD'))

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

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)
        org_pg.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)

    def test_pg_to_pg_qry_basic_with_comments_table(self):
        org_pg = pysqldb.DbConnect(type=config.get('SECOND_PG_DB', 'TYPE'),
                                server=config.get('SECOND_PG_DB', 'SERVER'),
                                database=config.get('SECOND_PG_DB', 'DB_NAME'),
                                user=config.get('SECOND_PG_DB', 'DB_USER'),
                                password=config.get('SECOND_PG_DB', 'DB_PASSWORD'))

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

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)
        org_pg.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)

    def test_pg_to_pg_qry_dest_schema(self):
        org_pg = pysqldb.DbConnect(type=config.get('SECOND_PG_DB', 'TYPE'),
                                   server=config.get('SECOND_PG_DB', 'SERVER'),
                                   database=config.get('SECOND_PG_DB', 'DB_NAME'),
                                   user=config.get('SECOND_PG_DB', 'DB_USER'),
                                   password=config.get('SECOND_PG_DB', 'DB_PASSWORD'))

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

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_pg_to_pg_qry_table)
        org_pg.drop_table(schema = pg_schema, table = test_pg_to_pg_qry_table)


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