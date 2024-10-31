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

sql2 = pysqldb.DbConnect(type=config.get('SECOND_SQL_DB', 'TYPE'),
                        server=config.get('SECOND_SQL_DB', 'SERVER'),
                        database=config.get('SECOND_SQL_DB', 'DB_NAME'),
                        user=config.get('SECOND_SQL_DB', 'DB_USER'),
                        password=config.get('SECOND_SQL_DB', 'DB_PASSWORD'),
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

test_org_schema = 'risadmin'
test_dest_schema = 'dbo'
test_sql_to_sql_tbl_to = 'tst_sql_to_sql_to_tbl_{}'.format(db.user)
test_sql_to_sql_tbl_from = 'tst_sql_to_sql_from_tbl_{}'.format(db.user)

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
        
        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants 
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_sql_to_pg_qry_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

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
        
        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants 
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_sql_to_pg_qry_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

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
        
        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants 
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_sql_to_pg_qry_spatial_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"
    
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
        
        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants 
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_sql_to_pg_qry_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

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

        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants 
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_sql_to_pg_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

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
        
        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants 
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_sql_to_pg_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

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

        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants 
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_pg_to_pg_qry_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

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

        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants 
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_pg_to_pg_qry_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

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
        
        assert db.dfquery(f"""SELECT bool_or(CASE WHEN GRANTEE IN ('PUBLIC') THEN True ELSE False END)
                            FROM information_schema.role_table_grants 
                            WHERE table_schema = '{pg_schema}' and table_name = '{test_pg_to_pg_qry_table}'""").values[0][0]  == True, "Dest table permissions not set to PUBLIC"

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
    # Still to test: LDAP, print_cmd    # Still to test: LDAP, print_cmd
class TestSqlToSqlQry:
    
    def test_sql_to_sql_basic_table(self):

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

        # drop ouptut table
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)

    def test_sql_to_sql_qry(self):

        """
        Test to ensure that sql_to_sql query works
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

        output_table = sql2.dfquery(f"select * from {test_dest_schema}.{test_sql_to_sql_tbl_to};").drop('ogr_fid', axis = 1)

        assert sql2.table_exists(table = test_sql_to_sql_tbl_from, schema=test_org_schema)

        # assert that the tables are the same
        assert list(geometry_table.columns) == list(output_table.columns)
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
        
        output_table = sql2.dfquery(f"select * from {test_dest_schema}.{test_sql_to_sql_tbl_to}").drop('ogr_fid', axis = 1)

        assert sql2.table_exists(table = test_sql_to_sql_tbl_to, schema = test_dest_schema)

        # assert that the tables are the same
        assert reference_table.shape == output_table.shape
        assert list(reference_table.columns) == list(output_table.columns)
        assert reference_table.equals(output_table)

        # clean up
        sql2.drop_table(test_dest_schema, test_sql_to_sql_tbl_to)
        sql.drop_table(test_org_schema, test_sql_to_sql_tbl_from)

    def test_sql_to_sql_basic_long_names(self):

        sql2.drop_table(schema= test_dest_schema, table = test_sql_to_sql_tbl_from)

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
        output_table = sql2.dfquery(f"select * from {test_dest_schema}.{test_sql_to_sql_tbl_to};").drop('ogr_fid', axis = 1)

        # assert that the tables are the same
        assert list(reference_table.columns) == list(output_table.columns)
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
        assert list(ref_table.columns) == list(output_table.columns)
        assert ref_table.shape == output_table.shape

        # clean up
        sql2.drop_table(schema = test_dest_schema, table = test_sql_to_sql_tbl_to)
        sql.drop_table(test_org_schema, test_sql_to_sql_tbl_from)