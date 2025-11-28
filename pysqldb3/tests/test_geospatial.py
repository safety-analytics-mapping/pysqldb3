import os
import configparser
import pandas as pd
import subprocess
import shlex
import pytest

from .. import pysqldb3 as pysqldb
from .. import geospatial as s
from ..sql import *
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
                        password=config.get('SQL_DB', 'DB_PASSWORD'),
                        allow_temp_tables=True,
                        # use_native_driver=False # this is needed for spatial data types
                        )

pg_table_name = f'pg_test_table_{db.user}'
test_read_gpkg_table_name = f'test_read_gpkg_table_{db.user}'
test_write_gpkg_table_name = f'test_write_gpkg_table_{db.user}'
test_reuploaded_table_name = f'test_write_reuploaded_{db.user}'
test_layer1 = f'test_layer1_{db.user}'
test_layer2 = f'test_layer2_{db.user}'
test_read_shp_table_name = f'test_read_shp_table_{db.user}'
test_write_shp_table_name = f'test_write_shp_table_{db.user}'
test_reuploaded_table_name = f'test_write_reuploaded_{db.user}'
test_feature_class_table_name = f'test_feature_class_{db.user}'

FOLDER_PATH = helpers.DIR

ms_schema = 'dbo'
pg_schema = 'working'

fgdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data/lion/lion.gdb')
fc = 'node.shp'

db_int, db_geom, sql_int, sql_geom = helpers.identify_default_dtypes(db, sql, ms_schema)

class TestReadgpkgPG:
    @classmethod
    def setup_class(cls):
        helpers.set_up_geopackage(db.user)
        helpers.set_up_test_table_pg(db)

    def test_input_gpkg_basic(self):

        gpkg_name = "testgpkg.gpkg"

        # Assert successful
        assert gpkg_name in os.listdir(FOLDER_PATH)
        db.drop_table(schema=pg_schema, table=test_read_gpkg_table_name)

        # Read gpkg to new, test table
        s.upload_geospatial(path=FOLDER_PATH + '//' + gpkg_name, dbo=db, gpkg_tbl = test_layer1,
                                table=test_read_gpkg_table_name, schema=pg_schema, print_cmd=True)

        # Assert read_gpkg happened successfully and contents are correct
        assert db.table_exists(schema=pg_schema, table = test_read_gpkg_table_name)

        table_df = db.dfquery(f"""
                                select * from {pg_schema}.{test_read_gpkg_table_name}
                                """)

        assert set(table_df.columns) == {'gid', 'some_value', 'fid', 'geom'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently but mean the same (after mapping on QGIS)
        diff_df = db.dfquery(f"""
        select distinct st_distance(raw_inputs.geom,
                            st_transform(st_setsrid(end_table.geom, 4326),2263)
                            )::int as distance
        from (
            select 1 as id, st_setsrid(st_point(1015329.1, 213793.1), 2263) as geom
            union
            select 2 as id, st_setsrid(st_point(1015428.1, 213086.1), 2263) as geom
        ) raw_inputs
        join {pg_schema}.{test_read_gpkg_table_name} end_table
                    on raw_inputs.id=end_table.gid::int
        """)

        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

        assert db.tables_created[-1] == (db.server, db.database, pg_schema, test_read_gpkg_table_name)

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_read_gpkg_table_name)
        
    def test_read_gpkg_basic_multigpkgtbl(self):

        """
        Test for multiple tables within a geopackage
        """

        gpkg_name = "testgpkg.gpkg"

        # Assert successful
        # Assert successful
        assert gpkg_name in os.listdir(FOLDER_PATH)
        db.drop_table(schema=pg_schema, table=test_layer1)
        db.drop_table(schema=pg_schema, table=test_layer2)

        # no gpkg_tbl argument so that it bulk uploads
        s.upload_geospatial(path=FOLDER_PATH, dbo=db, schema=pg_schema, input_file=gpkg_name, print_cmd=True)

        # Assert read_gpkg happened successfully and contents are correct
        assert db.table_exists(schema=pg_schema, table = test_layer1)
        assert db.table_exists(schema=pg_schema, table = test_layer2)

        table_df = db.dfquery(f"""
                                select * from {pg_schema}.{test_layer1}
                                """)

        assert set(table_df.columns) == {'gid', 'some_value', 'fid', 'geom'}
        assert len(table_df) == 2

        table_df2 = db.dfquery(f"""
                                select * from {pg_schema}.{test_layer2}
                                """)
        assert set(table_df2.columns) == {'gid', 'some_value', 'fid', 'geom'}
        assert len(table_df2) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently but mean the same (after mapping on QGIS)
        diff_df = db.dfquery(f"""
        select distinct st_distance(raw_inputs.geom,
                            st_transform(st_setsrid(end_table.geom, 4326),2263)
                            )::int as distance
        from (
            select 1 as id, st_setsrid(st_point(1015329.1, 213793.1), 2263) as geom
            union
            select 2 as id, st_setsrid(st_point(1015428.1, 213086.1), 2263) as geom
        ) raw_inputs
        join {pg_schema}.{test_layer1} end_table
                    on raw_inputs.id=end_table.gid::int
        """)

        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

        diff_df2 = db.dfquery(f"""
        select distinct st_distance(raw_inputs.geom,
                            st_transform(st_setsrid(end_table.geom, 4326),2263)
                            )::int as distance
        from (
            select 1 as id, st_setsrid(st_point(1015329.1, 213793.1), 2263) as geom
            union
            select 2 as id, st_setsrid(st_point(1015428.1, 213086.1), 2263) as geom
        ) raw_inputs
        join {pg_schema}.{test_layer2} end_table
                    on raw_inputs.id=end_table.gid::int
        """)

        assert len(diff_df2) == 1
        assert int(diff_df2.iloc[0]['distance']) == 0

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_layer1)
        db.drop_table(schema=pg_schema, table=test_layer2)

    def test_list_all_gpkg_tables(self):

        gpkg_name = "testgpkg.gpkg"
    
        s_list = s.list_gpkg_tables(path=os.path.join(FOLDER_PATH, gpkg_name))

        assert s_list == [test_layer1, test_layer2]

    def test_read_gpkg_no_schema(self):
        gpkg_name = "testgpkg.gpkg"

        # Assert successful
        assert gpkg_name in os.listdir(FOLDER_PATH)
        db.drop_table(schema=pg_schema, table=test_layer1)
        db.drop_table(schema=pg_schema, table=test_layer2)

        # Read gpkg to new, test table
        s.upload_geospatial(dbo=db, path=FOLDER_PATH, input_file = gpkg_name, gpkg_tbl = test_layer2,
                                table = test_read_gpkg_table_name, print_cmd=True)

        # Assert read_gpkg happened successfully and contents are correct
        assert db.table_exists(schema=db.default_schema, table=test_read_gpkg_table_name)
        table_df = db.dfquery(f'select * from {db.default_schema}.{test_read_gpkg_table_name}')

        assert set(table_df.columns) == {'some_value', 'fid', 'gid', 'geom'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently but mean the same (after mapping on QGIS)
        diff_df = db.dfquery(f"""
        select distinct
        st_distance(raw_inputs.geom, st_transform(st_setsrid(end_table.geom, 4326),2263))::int distance
        from (
            select 1 as id, st_setsrid(st_point(1015329.1, 213793.1), 2263) as geom
            union
            select 2 as id, st_setsrid(st_point(1015428.1, 213086.1), 2263) as geom
        ) raw_inputs
        join {db.default_schema}.{test_read_gpkg_table_name} end_table
        on raw_inputs.id=end_table.gid::int
        """)

        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

        # Cleanup
        db.drop_table(schema=db.default_schema, table=test_read_gpkg_table_name)

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_geopackage()


class TestReadgpkgMS:
    @classmethod
    def setup_class(cls):
        helpers.set_up_geopackage(db.user)

    def test_read_gpkg_basic(self):
        gpkg_name = "testgpkg.gpkg"

        # Assert successful
        assert gpkg_name in os.listdir(FOLDER_PATH)

        # remove temp table from MS SQL Server if it already exists
        sql.query(f"drop table if exists {ms_schema}.{test_read_gpkg_table_name}")

        # Read gpkg to new, test table
        s.upload_geospatial(dbo=sql, path=FOLDER_PATH + '//' + gpkg_name, gpkg_tbl = test_layer2,
                                table=test_read_gpkg_table_name, schema=ms_schema, print_cmd=True)

        # Assert read_gpkg happened successfully and contents are correct
        assert sql.table_exists(schema = ms_schema, table=test_read_gpkg_table_name)

        # todo: this fails because odbc 17 driver isnt supporting geometry
        table_df = sql.dfquery(f'select * from {ms_schema}.{test_read_gpkg_table_name}')

        assert set(table_df.columns) == {'fid', 'gid', 'some_value', 'geom'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently but mean the same (after mapping on QGIS)
        diff_df = sql.dfquery(f"""
        select distinct raw_inputs.geom.STDistance(end_table.geom) as distance
        from (
            (select 1 as id, geometry::Point(-73.88782477721676, 40.75343453961836, 2263) as geom)
            union all
            (select 2 as id, geometry::Point(-73.88747073046778, 40.75149365677327, 2263) as geom)
        ) raw_inputs
        join {ms_schema}.{test_read_gpkg_table_name} end_table
        on raw_inputs.id=end_table.gid
        """)

        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

        assert sql.tables_created[-1] == (sql.server, sql.database, ms_schema, test_read_gpkg_table_name)

        # Cleanup
        sql.query(f"drop table if exists {ms_schema}.{test_read_gpkg_table_name}")

    def test_read_gpkg_basic_multigpkgtbl(self):

        """
        Test for multiple tables within a geopackage
        """

        gpkg_name = "testgpkg.gpkg"

        # Assert successful
        assert gpkg_name in os.listdir(FOLDER_PATH)
        sql.query(f"drop table if exists {ms_schema}.{test_layer1}")
        sql.query(f"drop table if exists {ms_schema}.{test_layer2}")

        # no gpkg_tbl argument so that it bulk uploads
        s.upload_geospatial(dbo=sql, path=FOLDER_PATH, input_file = gpkg_name, schema=ms_schema, print_cmd=True)

        # Assert read_gpkg happened successfully and contents are correct
        assert sql.table_exists(schema = ms_schema, table = test_layer1)
        assert sql.table_exists(schema = ms_schema, table = test_layer2)

        # test contents
        table_df1 = sql.dfquery(f'select * from {ms_schema}.{test_layer1}')
        table_df2 = sql.dfquery(f'select * from {ms_schema}.{test_layer2}')

        assert set(table_df1.columns) == set(table_df2.columns)
        assert len(table_df1) == len(table_df2)

        # Cleanup
        sql.query(f"drop table if exists {ms_schema}.{test_layer1}")
        sql.query(f"drop table if exists {ms_schema}.{test_layer2}")

    def test_read_gpkg_no_table(self):
        gpkg_name = "testgpkg.gpkg"

        # Assert successful
        assert gpkg_name in os.listdir(FOLDER_PATH)

        # drop temp table if exists
        sql.query(f"drop table if exists {ms_schema}.{test_layer1}")
        sql.query(f"drop table if exists {ms_schema}.{test_layer2}")

        # Read gpkg to new, test table
        s.upload_geospatial(path=FOLDER_PATH, input_file=gpkg_name, dbo=sql, schema=ms_schema, print_cmd=True)

        # Assert read_gpkg happened successfully and contents are correct
        assert sql.table_exists(schema=ms_schema, table= test_layer1)
        table_df = sql.dfquery(f'select * from {ms_schema}.{test_layer1}')
        assert set(table_df.columns) == {'fid', 'gid', 'some_value', 'geom'}
        assert len(table_df) == 2

        assert sql.table_exists(schema=ms_schema, table= test_layer2)
        table_df2 = sql.dfquery(f'select * from {ms_schema}.{test_layer2}')
        assert set(table_df2.columns) == {'fid', 'gid', 'some_value', 'geom'}
        assert len(table_df2) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently but mean the same (after mapping on QGIS)
        diff_df = sql.dfquery(f"""
        select distinct raw_inputs.geom.STDistance(end_table.geom) as distance
        from (            
            (select 1 as id, geometry::Point( -73.88782477721676, 40.75343453961836, 2263) as geom)
            union all
            (select 2 as id, geometry::Point(-73.88747073046778, 40.75149365677327, 2263) as geom)
        ) raw_inputs
        join {ms_schema}.{test_layer1} end_table
        on raw_inputs.id=cast(end_table.gid as int)
        """)
        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

        diff_df2 = sql.dfquery(f"""
        select distinct raw_inputs.geom.STDistance(end_table.geom) as distance
        from (            
            (select 1 as id, geometry::Point( -73.88782477721676, 40.75343453961836, 2263) as geom)
            union all
            (select 2 as id, geometry::Point(-73.88747073046778, 40.75149365677327, 2263) as geom)
        ) raw_inputs
        join {ms_schema}.{test_layer2} end_table
        on raw_inputs.id=cast(end_table.gid as int)
        """)
        assert len(diff_df2) == 1
        assert int(diff_df2.iloc[0]['distance']) == 0

        # Cleanup
        sql.query(f'drop table if exists {ms_schema}.{test_layer1}')
        sql.query(f'drop table if exists {ms_schema}.{test_layer2}')


    def test_read_gpkg_no_schema(self):
        gpkg_name = "testgpkg.gpkg"

        # Assert successful
        assert gpkg_name in os.listdir(FOLDER_PATH)
        sql.drop_table(schema=sql.default_schema, table=test_read_gpkg_table_name)

        # Read gpkg to new, test table
        s.upload_geospatial(dbo=sql, path=FOLDER_PATH, input_file=gpkg_name, gpkg_tbl = test_layer1, table=test_read_gpkg_table_name, print_cmd=True)

        # Assert read_gpkg happened successfully and contents are correct
        assert sql.table_exists(schema=sql.default_schema, table=test_read_gpkg_table_name)
        table_df = sql.dfquery(f'select * from {sql.default_schema}.{test_read_gpkg_table_name}')
        assert set(table_df.columns) == {'fid', 'gid', 'some_value', 'geom'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently but mean the same (after mapping on QGIS)
        diff_df = sql.dfquery(f"""
        select distinct raw_inputs.geom.STDistance(end_table.geom) as distance
        from (
            (select 1 as id, geometry::Point(-73.88782477721676, 40.75343453961836, 2263) as geom)
            union all
            (select 2 as id, geometry::Point(-73.88747073046778, 40.75149365677327, 2263) as geom)
        ) raw_inputs
        join {sql.default_schema}.{test_read_gpkg_table_name} end_table
        on raw_inputs.id=cast(end_table.gid as int)
        """)

        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

        # Cleanup
        sql.drop_table(schema=sql.default_schema, table=test_read_gpkg_table_name)

    def test_read_gpkg_no_output_name(self):
        gpkg_name = "testgpkg.gpkg"

        # Assert successful
        assert gpkg_name in os.listdir(FOLDER_PATH)

        # remove temp table from MS SQL Server if it already exists
        sql.query(f"drop table if exists {ms_schema}.{test_layer1}")

        # Read gpkg to new, test table
        s.upload_geospatial(dbo=sql, path=FOLDER_PATH, input_file=gpkg_name, gpkg_tbl=test_layer1, schema=ms_schema, print_cmd=True)

        # Assert read_gpkg happened successfully and contents are correct
        assert sql.table_exists(schema = ms_schema, table= test_layer1)

        # todo: this fails because odbc 17 driver isnt supporting geometry
        table_df = sql.dfquery(f'select * from {ms_schema}.{test_layer1}')

        assert set(table_df.columns) == {'fid', 'gid', 'some_value', 'geom'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently but mean the same (after mapping on QGIS)
        diff_df = sql.dfquery(f"""
        select distinct raw_inputs.geom.STDistance(end_table.geom) as distance
        from (
            (select 1 as id, geometry::Point(-73.88782477721676, 40.75343453961836, 2263) as geom)
            union all
            (select 2 as id, geometry::Point(-73.88747073046778, 40.75149365677327, 2263) as geom)
        ) raw_inputs
        join {ms_schema}.{test_layer1} end_table
        on raw_inputs.id=end_table.gid
        """)

        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

        # Cleanup
        sql.query(f"drop table if exists {ms_schema}.{test_layer1}")

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_geopackage()
        helpers.clean_up_test_table_pg(db)


class TestWritegpkgPG:
    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_pg(db)

    def test_write_gpkg_table(self):
        db.query(f"""
        drop table if exists {pg_schema}.{test_write_gpkg_table_name};

        create table {pg_schema}.{test_write_gpkg_table_name} as
        select *
        from {pg_schema}.{pg_table_name}
        order by id
        limit 100
        """)

        gpkg_name = 'testgpkg.gpkg'

        # Write gpkg
        s.write_geospatial(path=os.path.join(FOLDER_PATH, gpkg_name), dbo=db, schema=pg_schema, table=test_write_gpkg_table_name, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        # Reupload as table
        s.upload_geospatial(dbo = db, path=FOLDER_PATH, input_file = gpkg_name, gpkg_tbl = test_write_gpkg_table_name,
                                schema=pg_schema, table = test_reuploaded_table_name, print_cmd=True)

        # Assert equality
        db_df = db.dfquery(f"select * from {pg_schema}.{pg_table_name} order by id limit 100")
        gpkg_uploaded_df = db.dfquery(f"select * from {pg_schema}.{test_reuploaded_table_name} order by id")

        assert len(db_df) == len(gpkg_uploaded_df)

        # Some columns changed names since gpkgfiles have a character limit of 10
        mutual_columns = set(db_df.columns).intersection(gpkg_uploaded_df.columns) - {'fid', 'geom'}
        pd.testing.assert_frame_equal(db_df[list(mutual_columns)], gpkg_uploaded_df[list(mutual_columns)],
                                      check_like=True, check_names=False, check_dtype=False,
                                      check_datetimelike_compat=True)

        # Assert before/after geom columns are all 0 ft from each other, even if represented differently
        dist_df = db.dfquery(f"""
        select distinct st_distance(st_setsrid(b.geom, 2263), a.geom) as distance
        from {pg_schema}.{pg_table_name} b
        join {pg_schema}.{test_reuploaded_table_name} a
        on b.id=a.id
        """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0

        # clean up
        db.drop_table(schema=pg_schema, table=test_write_gpkg_table_name)
        db.drop_table(schema=pg_schema, table=test_reuploaded_table_name)
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))

    def test_write_gpkg_overwrite(self):
        db.query(f"""
        drop table if exists {pg_schema}.{test_write_gpkg_table_name};

        create table {pg_schema}.{test_write_gpkg_table_name} as
        select *
        from {pg_schema}.{pg_table_name}
        order by id
        limit 100
        """)

        gpkg_name = 'testgpkg.gpkg'

        # Write gpkg
        s.write_geospatial(path=os.path.join(FOLDER_PATH, gpkg_name), dbo=db, schema=pg_schema, table=test_write_gpkg_table_name, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        # overwrite the table as a version with 2 rows
        db.query(f"""
        drop table if exists {pg_schema}.{test_write_gpkg_table_name};

        create table {pg_schema}.{test_write_gpkg_table_name} as
        select *
        from {pg_schema}.{pg_table_name}
        order by id
        limit 2
        """)
        s.write_geospatial(path=os.path.join(FOLDER_PATH, gpkg_name), dbo=db, schema=pg_schema,
                           table=test_write_gpkg_table_name, overwrite = True, print_cmd=True) # overwrite to 2 rows

        # Reupload as table
        s.upload_geospatial(dbo = db, path=FOLDER_PATH, schema=pg_schema, input_file=gpkg_name, gpkg_tbl = test_write_gpkg_table_name,
                         table = test_reuploaded_table_name, print_cmd=True)

        # Assert equality
        db_df = db.dfquery(f"select * from {pg_schema}.{pg_table_name} order by id limit 2")
        gpkg_uploaded_df = db.dfquery(f"select * from {pg_schema}.{test_reuploaded_table_name} order by id")

        assert len(db_df) == len(gpkg_uploaded_df) # 2 == 2

        # Some columns changed names since gpkgfiles have a character limit of 10
        mutual_columns = set(db_df.columns).intersection(gpkg_uploaded_df.columns) - {'fid', 'geom'}
        pd.testing.assert_frame_equal(db_df[list(mutual_columns)], gpkg_uploaded_df[list(mutual_columns)],
                                      check_like=True, check_names=False, check_dtype=False,
                                      check_datetimelike_compat=True)

        # Assert before/after geom columns are all 0 ft from each other, even if represented differently
        dist_df = db.dfquery(f"""
        select distinct st_distance(st_setsrid(b.geom, 2263), a.geom) as distance
        from {pg_schema}.{pg_table_name} b
        join {pg_schema}.{test_reuploaded_table_name} a
        on b.id=a.id
        """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0

        # clean up
        db.drop_table(schema=pg_schema, table=test_write_gpkg_table_name)
        db.drop_table(schema=pg_schema, table=test_reuploaded_table_name)
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))

    def test_write_gpkg_add_table(self):

        db.query(f"""
        drop table if exists {pg_schema}.{test_write_gpkg_table_name};

        create table {pg_schema}.{test_write_gpkg_table_name} as
        select *
        from {pg_schema}.{pg_table_name}
        order by id
        limit 100
        """)

        gpkg_name = 'testgpkg.gpkg'

        # Write gpkg
        s.write_geospatial(path=FOLDER_PATH + '//' + gpkg_name, dbo=db, schema=pg_schema,
                           gpkg_tbl = test_reuploaded_table_name, table= test_write_gpkg_table_name, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        # add another (differently-named) table to the same gpkg
        db.query(f"""
        drop table if exists {pg_schema}.{test_write_gpkg_table_name}_2;

        create table {pg_schema}.{test_write_gpkg_table_name}_2 as
        select *
        from {pg_schema}.{pg_table_name}
        order by id
        limit 2
        """)
        s.write_geospatial(dbo=db, schema=pg_schema, table = test_write_gpkg_table_name + '_2', path=os.path.join(FOLDER_PATH, gpkg_name),
                           gpkg_tbl = test_reuploaded_table_name + '_2', overwrite = False, print_cmd=True) # add another table

        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name)) # assert that the table is still there

        # Reupload both tables from the same geopackage (since we are uploading under a different name, we can't use bulk upload function here)
        s.upload_geospatial(path=FOLDER_PATH, dbo = db, schema=pg_schema, input_file=gpkg_name, print_cmd=True)

        # Assert equality
        db_df = db.dfquery(f"select * from {pg_schema}.{pg_table_name} order by id limit 100")
        gpkg_uploaded_df = db.dfquery(f"select * from {pg_schema}.{test_reuploaded_table_name} order by id")
        db_df2 = db.dfquery(f"select * from {pg_schema}.{pg_table_name} order by id limit 2")
        gpkg_uploaded_df2 = db.dfquery(f"select * from {pg_schema}.{test_reuploaded_table_name}_2 order by id")

        assert len(db_df) == len(gpkg_uploaded_df)
        assert len(db_df2) == len(gpkg_uploaded_df2)

        # Some columns changed names since gpkgfiles have a character limit of 10
        mutual_columns = set(db_df.columns).intersection(gpkg_uploaded_df.columns) - {'fid', 'geom'}
        pd.testing.assert_frame_equal(db_df[list(mutual_columns)], gpkg_uploaded_df[list(mutual_columns)],
                                      check_like=True, check_names=False, check_dtype=False,
                                      check_datetimelike_compat=True)

        mutual_columns2 = set(db_df2.columns).intersection(gpkg_uploaded_df2.columns) - {'fid', 'geom'}
        pd.testing.assert_frame_equal(db_df2[list(mutual_columns2)], gpkg_uploaded_df2[list(mutual_columns2)],
                                      check_like=True, check_names=False, check_dtype=False,
                                      check_datetimelike_compat=True)

        # Assert before/after geom columns are all 0 ft from each other, even if represented differently
        dist_df = db.dfquery(f"""
        select distinct st_distance(st_setsrid(b.geom, 2263), a.geom) as distance
        from {pg_schema}.{pg_table_name} b
        join {pg_schema}.{test_reuploaded_table_name} a
        on b.id=a.id
        """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0

        dist_df2 = db.dfquery(f"""
        select distinct st_distance(st_setsrid(b.geom, 2263), a.geom) as distance
        from {pg_schema}.{pg_table_name} b
        join {pg_schema}.{test_reuploaded_table_name}_2 a
        on b.id=a.id
        """)

        assert len(dist_df2) == 1
        assert dist_df2.iloc[0]['distance'] == 0

        # clean up
        db.drop_table(schema=pg_schema, table=test_write_gpkg_table_name)
        db.drop_table(schema=pg_schema, table=test_reuploaded_table_name)
        db.drop_table(schema=pg_schema, table=test_write_gpkg_table_name + '_2')
        db.drop_table(schema=pg_schema, table=test_reuploaded_table_name + '_2')
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))


    def test_write_gpkg_dates_table(self):
            
        gpkg_name = 'testgpkg.gpkg'

        db.drop_table(schema = pg_schema, table = test_write_gpkg_table_name)
        db.drop_table(schema = pg_schema, table = test_reuploaded_table_name)

        db.query(f"""

                create table {pg_schema}.{test_write_gpkg_table_name} as
                select *,
                        now() as dt_col,
                        cast('2020-01-01' as date) as next_dt_col
                from {pg_schema}.{pg_table_name}
                order by id
                limit 100
                """)
        
        s.write_geospatial(path=os.path.join(FOLDER_PATH, gpkg_name), dbo=db, schema=pg_schema, table=test_write_gpkg_table_name,
                           gpkg_tbl = test_write_gpkg_table_name)

        # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        # check the date columns
        cmd_gpkg = f'ogrinfo -so -al "{FOLDER_PATH}/{gpkg_name}"'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT)
        assert 'dt_col_dt: Date' in str(ogr_response_gpkg), "'dt_col_dt column is not a Date data type when it should be"

        # clean up
        db.drop_table(schema=pg_schema, table=test_write_gpkg_table_name)
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))
    
    def test_write_gpkg_table_pth(self):
        db.drop_table(pg_schema, test_write_gpkg_table_name)
        db.query(f"""
        create table {pg_schema}.{test_write_gpkg_table_name} as
        select *
        from {pg_schema}.{pg_table_name}
        order by id
        limit 100
        """)

        gpkg_name = 'testgpkg.gpkg'

        # Write gpkg
        s.write_geospatial(path=os.path.join(FOLDER_PATH, gpkg_name), dbo=db, table=test_write_gpkg_table_name, schema=pg_schema, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        # Reupload as table
        s.upload_geospatial(dbo = db, path=FOLDER_PATH, input_file = gpkg_name, schema=pg_schema,
                         table=test_reuploaded_table_name, gpkg_tbl = test_write_gpkg_table_name, print_cmd=True)

        # Assert equality
        db_df = db.dfquery(f"select * from {pg_schema}.{pg_table_name} order by id limit 100")
        gpkg_uploaded_df = db.dfquery(f"select * from {pg_schema}.{test_reuploaded_table_name} order by id")

        assert len(db_df) == len(gpkg_uploaded_df)

        # Some columns changed names since gpkgfiles have a character limit of 10
        mutual_columns = set(db_df.columns).intersection(gpkg_uploaded_df.columns) - {'fid', 'geom'}
        pd.testing.assert_frame_equal(db_df[list(mutual_columns)], gpkg_uploaded_df[list(mutual_columns)],
                                      check_like=True, check_names=False, check_dtype=False,
                                      check_datetimelike_compat=True)

        # Assert before/after geom columns are all 0 ft from each other, even if represented differently
        dist_df = db.dfquery(f"""
        select distinct st_distance(st_setsrid(b.geom, 2263), a.geom) as distance
        from {pg_schema}.{pg_table_name} b
        join {pg_schema}.{test_reuploaded_table_name} a
        on b.id=a.id
        """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0

        db.drop_table(schema=pg_schema, table=test_write_gpkg_table_name)
        db.drop_table(schema=pg_schema, table=test_reuploaded_table_name)

        # clean up
        db.drop_table(schema=pg_schema, table=test_write_gpkg_table_name)
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))

    def test_write_gpkg_table_pth_w_name(self):
        db.query(f"""
        drop table if exists {pg_schema}.{test_write_gpkg_table_name};

        create table {pg_schema}.{test_write_gpkg_table_name} as
        select *
        from {pg_schema}.{pg_table_name}
        order by id
        limit 100
        """)

        gpkg_name = 'testgpkg.gpkg'

        # Write gpkg
        s.write_geospatial(path=os.path.join(FOLDER_PATH, gpkg_name), dbo=db, table=test_write_gpkg_table_name, schema=pg_schema, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        # Reupload as table
        s.upload_geospatial(dbo = db, path=FOLDER_PATH, input_file=gpkg_name ,schema=pg_schema,
                        table=test_reuploaded_table_name, gpkg_tbl = test_write_gpkg_table_name, print_cmd=True)

        # Assert equality
        db_df = db.dfquery(f"select * from {pg_schema}.{pg_table_name} order by id limit 100")
        gpkg_uploaded_df = db.dfquery(f"select * from {pg_schema}.{test_reuploaded_table_name} order by id")

        assert len(db_df) == len(gpkg_uploaded_df)

        # Some columns changed names since gpkgfiles have a character limit of 10
        mutual_columns = set(db_df.columns).intersection(gpkg_uploaded_df.columns) - {'fid', 'geom'}
        pd.testing.assert_frame_equal(db_df[list(mutual_columns)], gpkg_uploaded_df[list(mutual_columns)],
                                      check_like=True, check_names=False, check_dtype=False,
                                      check_datetimelike_compat=True)

        # Assert before/after geom columns are all 0 ft from each other, even if represented differently
        dist_df = db.dfquery(f"""
        select distinct st_distance(st_setsrid(b.geom, 2263), a.geom) as distance
        from {pg_schema}.{pg_table_name} b
        join {pg_schema}.{test_reuploaded_table_name} a
        on b.id=a.id
        """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0

        db.drop_table(schema=pg_schema, table=test_write_gpkg_table_name)
        db.drop_table(schema=pg_schema, table=test_reuploaded_table_name)

        # clean up
        db.drop_table(schema=pg_schema, table=test_write_gpkg_table_name)
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))

    def test_write_gpkg_query(self):

        gpkg_name = 'testgpkg.gpkg'

        # Write gpkg
        s.write_geospatial(dbo=db, query=f"""select * from {pg_schema}.{pg_table_name} order by id limit 100""",
                            path=os.path.join(FOLDER_PATH, gpkg_name), gpkg_tbl = test_write_gpkg_table_name, print_cmd=True)

        # Check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        # Reupload as table
        s.upload_geospatial(dbo = db, path=FOLDER_PATH, input_file=gpkg_name, schema=pg_schema,
                         table=test_reuploaded_table_name, gpkg_tbl = test_write_gpkg_table_name, print_cmd=True)

        # Assert equality
        db_df = db.dfquery(f"select * from {pg_schema}.{pg_table_name} order by id limit 100")
        gpkg_uploaded_df = db.dfquery(f"select * from {pg_schema}.{test_reuploaded_table_name} order by id")

        assert len(db_df) == len(gpkg_uploaded_df)

        # Some columns changed names since gpkgfiles have a character limit of 10
        mutual_columns = set(db_df.columns).intersection(gpkg_uploaded_df.columns) - {'fid', 'geom'}
        pd.testing.assert_frame_equal(db_df[list(mutual_columns)], gpkg_uploaded_df[list(mutual_columns)],
                                      check_like=True, check_names=False, check_dtype=False,
                                      check_datetimelike_compat=True)

        # Assert before/after geom columns are all 0 ft from each other, even if represented differently
        dist_df = db.dfquery(f"""
        select distinct st_distance(st_setsrid(b.geom, 2263), a.geom) as distance
        from {pg_schema}.{pg_table_name} b
        join {pg_schema}.{test_reuploaded_table_name} a
        on b.id=a.id
        """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0

                # clean up
        db.drop_table(schema=pg_schema, table=test_write_gpkg_table_name)
        db.drop_table(schema=pg_schema, table=test_reuploaded_table_name)
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(db)
        helpers.clean_up_geopackage()


class TestWritegpkgMS:

    def test_write_gpkg_table(self):
        sql.drop_table(schema=ms_schema, table=test_write_gpkg_table_name)

        # Add test_table
        sql.query(f"""
        create table {ms_schema}.{test_write_gpkg_table_name} (test_col1 int, test_col2 int, dte datetime, geom geometry);
        insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(1, 2, current_timestamp, geometry::Point(985831.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(3, 4, current_timestamp, geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        gpkg_name = 'test_write.gpkg'

        # Write gpkg
        s.write_geospatial(dbo=sql, schema = ms_schema, path=os.path.join(FOLDER_PATH, gpkg_name), table=test_write_gpkg_table_name, print_cmd=True)

        # # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        # Reupload as table
        s.upload_geospatial(dbo = sql, path=FOLDER_PATH, input_file=gpkg_name, schema = ms_schema, table=test_reuploaded_table_name,
                          gpkg_tbl = test_write_gpkg_table_name, print_cmd=True)

        # # Assert equality
        db_df = sql.dfquery(f"select top 10 * from {ms_schema}.{test_write_gpkg_table_name} order by test_col1")
        gpkg_uploaded_df = sql.dfquery(f"select top 10 * from {ms_schema}.{test_reuploaded_table_name} order by test_col1")

        assert len(db_df) == len(gpkg_uploaded_df)

        # Some columns may change names since gpkgfiles have a character limit of 10
        pd.testing.assert_frame_equal(db_df[['test_col1', 'test_col2']],
                                      gpkg_uploaded_df[['test_col1', 'test_col2']],
                                      check_column_type=False,
                                      check_dtype=False)

        # Assert before/after geom columns are all 0 ft from each other, even if represented differently
        dist_df = sql.dfquery(f"""
        select distinct b.geom.STDistance(a.geom) as distance
        from {ms_schema}.{test_write_gpkg_table_name} b
        join {ms_schema}.{test_reuploaded_table_name} a
            on b.test_col1=a.test_col1
        """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0

        # Clean up
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name};")
        sql.query(f"drop table if exists {ms_schema}.{test_reuploaded_table_name};")

    def test_write_gpkg_dates_table(self):
        
        sql.drop_table(schema=ms_schema, table=test_write_gpkg_table_name)

        # Add test_table
        sql.query(f"""
        create table {ms_schema}.{test_write_gpkg_table_name} (test_col1 int, test_col2 int, dte datetime, geom geometry);
        insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(1, 2, current_timestamp, geometry::Point(985831.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(3, 4, current_timestamp, geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        gpkg_name = 'test_write.gpkg'

        # Write gpkg
        s.write_geospatial(dbo=sql, schema = ms_schema, path=os.path.join(FOLDER_PATH, gpkg_name), 
                           gpkg_tbl = test_write_gpkg_table_name, table=test_write_gpkg_table_name, print_cmd=True)

        # # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        # run ogrinfo for data types
        cmd_gpkg = f'ogrinfo -so -al "{FOLDER_PATH}/{gpkg_name}"'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT)
        assert 'dte_dt: Date' in str(ogr_response_gpkg), "'dte_dt column is not a Date data type when it should be"

        # Clean up
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name};")

    def test_write_gpkg_overwrite(self):
        sql.drop_table(schema=ms_schema, table=test_write_gpkg_table_name)

        # Add test_table
        sql.query(f"""
         drop table if exists {ms_schema}.{test_write_gpkg_table_name};
        create table {ms_schema}.{test_write_gpkg_table_name} (test_col1 int, test_col2 int, geom geometry);
        insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        gpkg_name = 'test_write.gpkg'

        # Write gpkg. Gpkg table will be named the same as the query table
        s.write_geospatial(path=os.path.join(FOLDER_PATH, gpkg_name), dbo=sql, schema = ms_schema, table=test_write_gpkg_table_name, print_cmd=True)

        # # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        ### ADD THE SECOND TABLE THAT IS SHORTER VERSION ###
        # Add test_table (slightly different values)
        sql.query(f"""
        drop table if exists {ms_schema}.{test_write_gpkg_table_name}_2;
        create table {ms_schema}.{test_write_gpkg_table_name}_2 (test_col3 int, test_col4 int, geom geometry);
        insert into {ms_schema}.{test_write_gpkg_table_name}_2 VALUES(5, 6, geometry::Point(985830.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_gpkg_table_name}_2 VALUES(7, 8, geometry::Point(985830.79200444, 203371.60461367, 2263));
        """)

        # Write gpkg. Gpkg table will be named the same as the previous table to overwrite it
        s.write_geospatial(dbo=sql, schema = ms_schema, table=test_write_gpkg_table_name + '_2',
                          path=os.path.join(FOLDER_PATH, gpkg_name), gpkg_tbl = test_write_gpkg_table_name, overwrite = True, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        # Reupload as table
        s.upload_geospatial(dbo = sql, path=FOLDER_PATH, input_file=gpkg_name, schema = ms_schema,
                          gpkg_tbl = test_write_gpkg_table_name, table=test_reuploaded_table_name, print_cmd=True)

        # # Assert equality
        db_df = sql.dfquery(f"select top 10 * from {ms_schema}.{test_write_gpkg_table_name}_2 order by test_col3")
        gpkg_uploaded_df = sql.dfquery(f"select top 10 * from {ms_schema}.{test_reuploaded_table_name} order by test_col3")

        assert len(db_df) == len(gpkg_uploaded_df)

        # Some columns may change names since gpkgfiles have a character limit of 10
        pd.testing.assert_frame_equal(db_df[['test_col3', 'test_col4']],
                                      gpkg_uploaded_df[['test_col3', 'test_col4']],
                                      check_column_type=False,
                                      check_dtype=False)

        # Assert before/after geom columns are all 0 ft from each other, even if represented differently
        dist_df = sql.dfquery(f"""
        select distinct b.geom.STDistance(a.geom) as distance
        from {ms_schema}.{test_reuploaded_table_name} b
        join {ms_schema}.{test_reuploaded_table_name} a
            on b.test_col3=a.test_col3
        """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0

        # Clean up
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name};")
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}_2;")
        sql.query(f"drop table if exists {ms_schema}.{test_reuploaded_table_name};")

    def test_write_gpkg_add_table(self):
        sql.drop_table(schema=ms_schema, table=test_write_gpkg_table_name)

        # Add test_table
        sql.query(f"""
            drop table if exists {ms_schema}.{test_write_gpkg_table_name};
            create table {ms_schema}.{test_write_gpkg_table_name} (test_col1 int, test_col2 int, geom geometry);
            insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
            insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        gpkg_name = 'test_write.gpkg'

        # Write gpkg. Gpkg table will be named the same as the query table
        s.write_geospatial(dbo=sql, schema = ms_schema, table= test_write_gpkg_table_name,
                           path=os.path.join(FOLDER_PATH, gpkg_name), gpkg_tbl = test_reuploaded_table_name, print_cmd=True)

        # # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        ### ADD THE SECOND TABLE THAT IS SHORTER VERSION ###
        # Add test_table (slightly different values)
        sql.query(f"""
        drop table if exists {ms_schema}.{test_write_gpkg_table_name}_2;
        create table {ms_schema}.{test_write_gpkg_table_name}_2 (test_col3 int, test_col4 int, geom geometry);
        insert into {ms_schema}.{test_write_gpkg_table_name}_2 VALUES(5, 6, geometry::Point(985830.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_gpkg_table_name}_2 VALUES(7, 8, geometry::Point(985830.79200444, 203371.60461367, 2263));
        """)

        # Write gpkg.
        s.write_geospatial(dbo=sql, schema = ms_schema, table = test_write_gpkg_table_name + '_2',
                           path=os.path.join(FOLDER_PATH, gpkg_name), gpkg_tbl=test_reuploaded_table_name + '_2', print_cmd=True) # add second table

        # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        # Reupload as tables using bulk upload function
        s.upload_geospatial(dbo = sql, path=FOLDER_PATH, input_file=gpkg_name, schema = ms_schema, print_cmd=True)

        # # Assert equality
        db_df = sql.dfquery(f"select top 10 * from {ms_schema}.{test_write_gpkg_table_name} order by test_col1")
        db_df2 = sql.dfquery(f"select top 10 * from {ms_schema}.{test_write_gpkg_table_name}_2 order by test_col3")
        gpkg_uploaded_df = sql.dfquery(f"select top 10 * from {ms_schema}.{test_reuploaded_table_name} order by test_col1")
        gpkg_uploaded_df2 = sql.dfquery(f"select top 10 * from {ms_schema}.{test_reuploaded_table_name}_2 order by test_col3")

        assert len(db_df) == len(gpkg_uploaded_df)
        assert len(db_df2) == len(gpkg_uploaded_df2)

        pd.testing.assert_frame_equal(db_df[['test_col1', 'test_col2']],
                                      gpkg_uploaded_df[['test_col1', 'test_col2']],
                                      check_column_type=False,
                                      check_dtype=False)
        
        pd.testing.assert_frame_equal(db_df2[['test_col3', 'test_col4']],
                                      gpkg_uploaded_df2[['test_col3', 'test_col4']],
                                      check_column_type=False,
                                      check_dtype=False)

        # Assert before/after geom columns are all 0 ft from each other, even if represented differently
        dist_df = sql.dfquery(f"""
        select distinct b.geom.STDistance(a.geom) as distance
        from {ms_schema}.{test_reuploaded_table_name} b
        join {ms_schema}.{test_reuploaded_table_name} a
            on b.test_col1=a.test_col1
        """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0

        # Clean up
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name};")
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}_2;")
        sql.query(f"drop table if exists {ms_schema}.{test_reuploaded_table_name};")

    def test_write_gpkg_table_pth(self):

        # drop temp table if exists 
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}")
 
        # Add test_table
        sql.query(f"""
        create table {ms_schema}.{test_write_gpkg_table_name} (test_col1 int, test_col2 int, geom geometry);
        insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)
 
        gpkg_name = 'test_write.gpkg'

        # Write gpkg
        s.write_geospatial(dbo=sql, path = os.path.join(FOLDER_PATH, gpkg_name), table= test_write_gpkg_table_name, gpkg_tbl = test_write_gpkg_table_name, schema=ms_schema, print_cmd=True)
 
        # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))
 
        # Reupload as table
        s.upload_geospatial(dbo = sql, path=os.path.join(FOLDER_PATH, gpkg_name), gpkg_tbl = test_write_gpkg_table_name,
                 schema=ms_schema, table=test_reuploaded_table_name, print_cmd=True)

        # Assert equality
        db_df = sql.dfquery(f"select top 10 * from {ms_schema}.{test_write_gpkg_table_name} order by test_col1")
        gpkg_uploaded_df = sql.dfquery(f"select top 10 * from {ms_schema}.{test_reuploaded_table_name} order by test_col1")

        assert len(db_df) == len(gpkg_uploaded_df)

        # Some columns may change names since gpkgfiles have a character limit of 10
        pd.testing.assert_frame_equal(db_df[['test_col1', 'test_col2']],
                                      gpkg_uploaded_df[['test_col1', 'test_col2']],
                                      check_column_type=False,
                                      check_dtype=False)
 
        # Assert before/after geom columns are all 0 ft from each other, even if represented differently
        dist_df = sql.dfquery(f"""
        select distinct b.geom.STDistance(a.geom) as distance
        from {ms_schema}.{test_write_gpkg_table_name} b
        join {ms_schema}.{test_reuploaded_table_name} a
        on b.test_col1=a.test_col1
        """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0
 
        # Clean up
        sql.drop_table(schema = ms_schema, table = test_write_gpkg_table_name)
        sql.drop_table(schema = ms_schema, table = test_reuploaded_table_name)
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))
    
    def test_write_gpkg_query(self):

        gpkg_name = 'testgpkg.gpkg'
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}")

        # Add test_table
        sql.query(f"""
        create table {ms_schema}.{test_write_gpkg_table_name} (test_col1 int, test_col2 int, geom geometry);
        insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        # Write gpkg
        s.write_geospatial(dbo=sql, query=f"""select top 10 * from {ms_schema}.{test_write_gpkg_table_name} order by test_col1""",
                            path= os.path.join(FOLDER_PATH, gpkg_name), gpkg_tbl = test_write_gpkg_table_name, print_cmd=True)

        # Check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        # Reupload as table
        s.upload_geospatial(dbo = sql, path=FOLDER_PATH + '//' + gpkg_name, gpkg_tbl = test_write_gpkg_table_name,
                            schema=ms_schema, table=test_reuploaded_table_name, print_cmd=True)

        # Assert equality
        db_df = sql.dfquery(f"select top 10 * from {ms_schema}.{test_write_gpkg_table_name} order by test_col1")
        gpkg_uploaded_df = sql.dfquery(f"select top 10 * from {ms_schema}.{test_reuploaded_table_name} order by test_col1")

        assert len(db_df) == len(gpkg_uploaded_df)

        # Some columns changed names since gpkgfiles have a character limit of 10
        pd.testing.assert_frame_equal(db_df[['test_col1', 'test_col2']],
                                      gpkg_uploaded_df[['test_col1', 'test_col2']],
                                      check_column_type=False,
                                      check_dtype=False)

        # Assert before/after geom columns are all 0 ft from each other, even if represented differently
        dist_df = sql.dfquery(f"""
                select distinct b.geom.STDistance(a.geom) as distance
                from {ms_schema}.{test_write_gpkg_table_name} b
                join {ms_schema}.{test_reuploaded_table_name} a
                on b.test_col1=a.test_col1
                """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0

        # Clean up
        sql.query(f"drop table if exists {ms_schema}.{test_reuploaded_table_name}")
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}")

        os.remove(os.path.join(FOLDER_PATH, gpkg_name))
        
    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(sql)
        helpers.clean_up_geopackage()

class TestGpkgShpConversion:
    @classmethod
        
    def test_convert_gpkg_to_shp_file(self):

        gpkg_name = 'gpkg_to_shp.gpkg'
        shp_name = test_write_gpkg_table_name + '.shp'

        # no shape file name needs to be specified because the table(s) within the gpkg are not necessarily named the same thing
        sql.drop_table(schema=ms_schema, table=test_write_gpkg_table_name)
        # write a query
        sql.query(f"""
                create table {ms_schema}.{test_write_gpkg_table_name} (test_col1 int, test_col2 int, geom geometry);
                insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
                insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
                """)
        
        assert sql.table_exists(schema = ms_schema, table = test_write_gpkg_table_name)

        # write geopackage file
        s.write_geospatial(dbo=sql, table= test_write_gpkg_table_name, schema=ms_schema,
                           path = os.path.join(FOLDER_PATH, gpkg_name), print_cmd=True)

        # # Check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))
        
        # run function to convert geopackage to shape file
        s.geospatial_convert(input_path = FOLDER_PATH + '//' + gpkg_name, gpkg_tbl = test_write_gpkg_table_name,
                             output_file = shp_name, print_cmd = True)

        # assert that a shape output file exists. It will not match the name of the geopackage because the tables inside the package canbe different
        assert os.path.isfile(os.path.join(FOLDER_PATH, shp_name))

        # check that the shapefile and gpkg have the same output
        cmd_gpkg = f'ogrinfo "{FOLDER_PATH}/{gpkg_name}" -sql "SELECT test_col1 FROM {test_write_gpkg_table_name} LIMIT 1" -q'
        cmd_shp = f'ogrinfo "{FOLDER_PATH}/{shp_name}" -sql "SELECT test_col1 FROM {test_write_gpkg_table_name} LIMIT 1" -q'

        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT)
        ogr_response_shp = subprocess.check_output(shlex.split(cmd_shp), stderr=subprocess.STDOUT)
        
        assert 'test_col1 (Integer) = 1' in str(ogr_response_gpkg) and 'test_col1 (Integer) = 1' in str(ogr_response_shp), "cannot find 'test_col1 (Integer) = 1' statement in both the shapefile and gpkg queries"

        # remove geopackage and shape files
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(FOLDER_PATH, test_write_gpkg_table_name + '.' + ext))

        sql.drop_table(schema = ms_schema, table = test_write_gpkg_table_name)

    def test_convert_gpkg_to_shp_file_bulk(self):

        gpkg_name = 'gpkg_to_shp.gpkg'
        shp_name = test_write_gpkg_table_name + '.shp'

        # no shape file name needs to be specified because the table(s) within the gpkg are not necessarily named the same thing

        # write a query
        sql.query(f"""drop table if exists {ms_schema}.{test_write_gpkg_table_name};
                create table {ms_schema}.{test_write_gpkg_table_name} (test_col1 int, test_col2 int, geom geometry);
                insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
                insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
                """)

        # write geopackage file
        s.write_geospatial(dbo=sql, table= test_write_gpkg_table_name, schema=ms_schema, path = os.path.join(FOLDER_PATH, gpkg_name), print_cmd=True)

        # add an extra table to test multiple
        sql.query(f"""drop table if exists {ms_schema}.{test_write_gpkg_table_name}_2;
                create table {ms_schema}.{test_write_gpkg_table_name}_2 (test_col5 int, test_col6 int, geom geometry);
                insert into {ms_schema}.{test_write_gpkg_table_name}_2 VALUES(5, 6, geometry::Point(985830.79200444, 203371.60461367, 2263));
                insert into {ms_schema}.{test_write_gpkg_table_name}_2 VALUES(7, 8, geometry::Point(985830.79200444, 203371.60461367, 2263));
                """)
        
        assert sql.table_exists(schema = ms_schema, table = test_write_gpkg_table_name)
        assert sql.table_exists(schema = ms_schema, table = f"{test_write_gpkg_table_name}_2")

        s.write_geospatial(dbo=sql, schema = ms_schema, table = test_write_gpkg_table_name + '_2',
                           path = os.path.join(FOLDER_PATH, gpkg_name), print_cmd=True) # this will append 

        # Check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))
        
        # run function to convert geopackage to shape file
        s.gpkg_to_shp_bulk(input_path = FOLDER_PATH, input_file=gpkg_name, print_cmd = True)

        # assert that a shape output file exists. It will not match the name of the geopackage because the tables inside the package canbe different
        assert os.path.isfile(os.path.join(FOLDER_PATH, shp_name))
        assert os.path.isfile(os.path.join(FOLDER_PATH, test_write_gpkg_table_name + '_2.shp'))

        # check that the shapefile and gpkg have the same output
        cmd_gpkg = f'ogrinfo "{FOLDER_PATH}/{gpkg_name}" -sql "SELECT test_col1 FROM {test_write_gpkg_table_name} LIMIT 1" -q'
        cmd_shp = f'ogrinfo "{FOLDER_PATH}/{shp_name}" -sql "SELECT test_col1 FROM {test_write_gpkg_table_name} LIMIT 1" -q'
        cmd_gpkg2 = f'ogrinfo "{FOLDER_PATH}/{gpkg_name}" -sql "SELECT test_col5 FROM {test_write_gpkg_table_name}_2 LIMIT 1" -q'
        cmd_shp2 = f'ogrinfo "{FOLDER_PATH}/{test_write_gpkg_table_name}_2.shp" -sql "SELECT test_col5 FROM {test_write_gpkg_table_name}_2 LIMIT 1" -q'

        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT)
        ogr_response_shp = subprocess.check_output(shlex.split(cmd_shp), stderr=subprocess.STDOUT)
        ogr_response_gpkg2 = subprocess.check_output(shlex.split(cmd_gpkg2), stderr=subprocess.STDOUT)
        ogr_response_shp2 = subprocess.check_output(shlex.split(cmd_shp2), stderr=subprocess.STDOUT)
        
        assert 'test_col1 (Integer) = 1' in str(ogr_response_gpkg) and 'test_col1 (Integer) = 1' in str(ogr_response_shp), \
            "cannot find 'test_col1 (Integer) = 1' statement in both the shapefile and gpkg queries"
        assert 'test_col5 (Integer) = 5' in str(ogr_response_gpkg2) and 'test_col5 (Integer) = 5' in str(ogr_response_shp2), \
            "cannot find 'test_col5 (Integer) = 5' statement in both the shapefile and gpkg queries"

        # remove geopackage and shape files
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}")
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}_2")
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(FOLDER_PATH, test_write_gpkg_table_name + '.' + ext))
            os.remove(os.path.join(FOLDER_PATH, test_write_gpkg_table_name + '_2.' + ext))

    def test_convert_shp_to_gpkg_file(self):
        gpkg_name = 'gpkg_to_shp.gpkg'
        shp_name = test_write_gpkg_table_name + '.shp'

        # create shapefile (we don't import Shapefile.py so we create a GPKG, convert to SHP, and then delete the GPKG)
        sql.query(f"""drop table if exists {ms_schema}.{test_write_gpkg_table_name};
                create table {ms_schema}.{test_write_gpkg_table_name} (test_col1 int, test_col2 int, geom geometry);
                insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
                insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
                """)

        # create shp file and then convert to geopackage file
        s.write_geospatial(dbo=sql, table= test_write_gpkg_table_name, schema=ms_schema,
                           path = os.path.join(FOLDER_PATH, shp_name), print_cmd=True)
        
        s.geospatial_convert(input_path = FOLDER_PATH,
                             input_file = shp_name,
                             output_file = gpkg_name, gpkg_tbl = test_write_gpkg_table_name, print_cmd = True)

        # drop SQL table and GPKG
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}")
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))

        assert os.path.exists(os.path.join(FOLDER_PATH, gpkg_name)) == False # confirm that the gpkg is removed
        assert os.path.isfile(os.path.join(FOLDER_PATH, shp_name)) # confirm that the shp file exists

        # run function to convert Shapefile to GPKG
        s.geospatial_convert(input_path=FOLDER_PATH, input_file = shp_name,
                             output_file = gpkg_name, gpkg_tbl = test_write_gpkg_table_name, print_cmd = True)

        # assert that the output file exists and that it matches the geopackage
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        # assert that the data is the same
        cmd_gpkg = f'ogrinfo "{FOLDER_PATH}/{gpkg_name}" -sql "SELECT test_col1 FROM {test_write_gpkg_table_name} LIMIT 1" -q'
        cmd_shp = f'ogrinfo "{FOLDER_PATH}/{shp_name}" -sql "SELECT test_col1 FROM {test_write_gpkg_table_name} LIMIT 1" -q'

        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT)
        ogr_response_shp = subprocess.check_output(shlex.split(cmd_shp), stderr=subprocess.STDOUT)
        
        assert 'test_col1 (Integer) = 1' in str(ogr_response_gpkg) and 'test_col1 (Integer) = 1' in str(ogr_response_shp), "cannot find 'test_col1 (Integer) = 1' statement in both the shapefile and gpkg queries"

        # don't remove gpkg output to reuse for subsequent test
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}")

    def test_convert_shp_to_existing_gpkg_file(self):

        # copy the same shp file as an additional table in the gpkg
        # this test confirms that the update function for the gpkg is working correctly

        gpkg_name = 'gpkg_to_shp.gpkg'
        shp_name = test_write_gpkg_table_name 

        # Check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, shp_name + '.shp'))

        # run function to convert Shapefile to GPKG
        s.geospatial_convert(input_path=FOLDER_PATH, input_file = shp_name + '.shp',
                             output_file = gpkg_name, gpkg_tbl = f'{test_write_gpkg_table_name}_2')

        # assert that the output file exists and that it matches the geopackage
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        # assert that the data is the same
        cmd_gpkg = f'ogrinfo "{FOLDER_PATH}/{gpkg_name}" -sql "SELECT test_col1 FROM {test_write_gpkg_table_name}_2 LIMIT 1" -q'
        cmd_shp = f'ogrinfo "{FOLDER_PATH}/{shp_name}.shp" -sql "SELECT test_col1 FROM {test_write_gpkg_table_name} LIMIT 1" -q'

        ogr_response_gpkg_2 = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT)
        ogr_response_shp_2 = subprocess.check_output(shlex.split(cmd_shp), stderr=subprocess.STDOUT)

        # todo add (Integer64) option
        assert 'test_col1 (Integer) = 1' in str(ogr_response_gpkg_2) and 'test_col1 (Integer) = 1' in str(ogr_response_shp_2), "cannot find 'test_col1 (Integer) = 1' statement in both the shapefile and gpkg queries"

        # remove shape file
        for ext in ('.dbf', '.prj', '.shx', '.shp'):
            try:
                os.remove(os.path.join(FOLDER_PATH, shp_name + ext))
            except Exception as e:
                print(e)

        # remove gpkg output
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))

    def test_convert_gdb_to_gpkg_file(self):
        gpkg_name = 'gpkg_to_gdb.gpkg'

        # check that GDB exists
        assert os.path.isdir(fgdb)
        assert not os.path.exists(os.path.join(fgdb, gpkg_name))

        s.geospatial_convert(input_path = fgdb,
                             feature_class = 'node',
                             output_file = gpkg_name,
                             gpkg_tbl = test_write_gpkg_table_name, print_cmd = True)

        assert os.path.exists(os.path.join(FOLDER_PATH + '/lion', gpkg_name)) # confirm that the gpkg is removed

        # assert that the data is the same
        cmd_gpkg = f'ogrinfo "{FOLDER_PATH}/lion/{gpkg_name}" -sql "SELECT NODEID FROM {test_write_gpkg_table_name} ORDER BY NODEID LIMIT 10" -q'
        cmd_gdb = f'ogrinfo "{fgdb}" -sql "SELECT NODEID FROM node ORDER BY NODEID LIMIT 10" -q'

        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT)
        ogr_response_gdb = subprocess.check_output(shlex.split(cmd_gdb), stderr=subprocess.STDOUT)
        
        print(ogr_response_gpkg)
        print(ogr_response_gdb)
        assert 'NODEID (Integer) = 1' in str(ogr_response_gpkg) and 'NODEID (Integer) = 1' in str(ogr_response_gdb), "cannot find 'NODEID (Integer) = 1' statement in the gdb and gpkg queries"

        os.remove(os.path.join(FOLDER_PATH + '/lion', gpkg_name))

    def test_convert_gdb_to_existing_gpkg_file(self):

        # copy the same shp file as an additional table in the gpkg
        # this test confirms that the update function for the gpkg is working correctly

        gpkg_name = 'gpkg_to_gdb.gpkg'

        assert os.path.isdir(fgdb)
        assert not os.path.exists(os.path.join(FOLDER_PATH + '/lion', gpkg_name))

        # create the first gpkg
        s.geospatial_convert(input_path = fgdb,
                             feature_class = 'lion',
                             output_file = gpkg_name,
                             gpkg_tbl = test_write_gpkg_table_name, print_cmd = True)
        
        # run function to convert GDB to GPKG and add as a second set of tables
        s.geospatial_convert(input_path = fgdb, feature_class = 'node', output_file = gpkg_name, gpkg_tbl = f'{test_write_gpkg_table_name}_2')

        # assert that the output file exists and that it matches the geopackage
        assert os.path.isfile(os.path.join(FOLDER_PATH + '/lion', gpkg_name))

        # assert that the data is the same
        cmd_gpkg = f'ogrinfo "{FOLDER_PATH}/lion/{gpkg_name}" -sql "SELECT NODEID FROM {test_write_gpkg_table_name}_2 ORDER BY NODEID LIMIT 1" -q'
        cmd_shp = f'ogrinfo "{fgdb}" -sql "SELECT NODEID FROM node ORDER BY NODEID LIMIT 1" -q'

        ogr_response_gpkg_2 = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT)
        ogr_response_gdb_2 = subprocess.check_output(shlex.split(cmd_shp), stderr=subprocess.STDOUT)
        
        assert 'NODEID (Integer) = 1' in str(ogr_response_gpkg_2) and 'NODEID (Integer) = 1' in str(ogr_response_gdb_2), "cannot find 'NODEID (Integer) = 1' statement in the gdb and gpkg queries"

        # remove gpkg output
        os.remove(os.path.join(FOLDER_PATH + '/lion', gpkg_name))
        
    @classmethod
    def teardown_class(cls):
        helpers.clean_up_geopackage()
        helpers.clean_up_shapefile()


class TestReadShpPG:
    @classmethod
    def setup_class(cls):
        helpers.set_up_shapefile()
        helpers.set_up_test_table_pg(db)

    def test_read_shp_basic(self):
        fp = FOLDER_PATH
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        db.drop_table(schema=pg_schema, table=test_read_shp_table_name)

        # Read shp to new, test table
        s.upload_geospatial(dbo=db, path=fp, schema=pg_schema, input_file=shp_name, table=test_read_shp_table_name, print_cmd=True)

        # Assert read_shp happened successfully and contents are correct
        assert db.table_exists(schema=pg_schema, table=test_read_shp_table_name)
        table_df = db.dfquery(f'select * from {pg_schema}.{test_read_shp_table_name}')

        assert set(table_df.columns) == {'gid', 'some_value', 'geom', 'ogc_fid'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently but mean the same (after mapping on QGIS)
        diff_df = db.dfquery(f"""
        select distinct st_distance(raw_inputs.geom,
        st_transform(st_setsrid(end_table.geom, 4326),2263)
        )::int as distance
        from (
            select 1 as id, st_setsrid(st_point(1015329.1, 213793.1), 2263) as geom
            union
            select 2 as id, st_setsrid(st_point(1015428.1, 213086.1), 2263) as geom
        ) raw_inputs
        join {pg_schema}.{test_read_shp_table_name} end_table
        on raw_inputs.id=end_table.gid::int
        """)

        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_read_shp_table_name)

    def test_read_shp_zip(self):
        fp = FOLDER_PATH + '/test.zip'
        shp_name = "test.shp"

        # Make sure table doesn't already exist
        db.drop_table(pg_schema, test_read_shp_table_name)
        assert not db.table_exists(schema=pg_schema, table=test_read_shp_table_name)

        # Read shp to new, test table
        s.upload_geospatial(dbo=db, path=fp, schema=pg_schema, input_file=shp_name, table=test_read_shp_table_name,
                                print_cmd=True)

        # Assert read_shp happened successfully and contents are correct
        assert db.table_exists(schema=pg_schema, table=test_read_shp_table_name)
        table_df = db.dfquery(f'select * from {pg_schema}.{test_read_shp_table_name}')

        assert set(table_df.columns) == {'gid', 'some_value', 'geom', 'ogc_fid'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently
        # but mean the same (after mapping on QGIS)
        diff_df = db.dfquery(f"""
        select distinct st_distance(raw_inputs.geom,
        st_transform(st_setsrid(end_table.geom, 4326),2263)
        )::int as distance
        from (
            select 1 as id, st_setsrid(st_point(1015329.1, 213793.1), 2263) as geom
            union
            select 2 as id, st_setsrid(st_point(1015428.1, 213086.1), 2263) as geom
        ) raw_inputs
        join {pg_schema}.{test_read_shp_table_name} end_table
        on raw_inputs.id=end_table.gid::int
        """)

        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

    def test_read_shp_to_table_zip(self):
        fp = FOLDER_PATH + '/test.zip'
        shp_name = "test.shp"

        # Make sure table doesn't alredy exist
        db.drop_table(pg_schema, test_read_shp_table_name)
        assert not db.table_exists(schema=pg_schema, table=test_read_shp_table_name)

        # Assert successful
        db.drop_table(schema=pg_schema, table=test_read_shp_table_name)

        # Read shp to new, test table
        db.shp_to_table(path=fp, schema=pg_schema, shp_name=shp_name, table=test_read_shp_table_name, print_cmd=True)

        # Assert read_shp happened successfully and contents are correct
        assert db.table_exists(schema=pg_schema, table=test_read_shp_table_name)
        table_df = db.dfquery(f'select * from {pg_schema}.{test_read_shp_table_name}')

        assert set(table_df.columns) == {'gid', 'some_value', 'geom', 'ogc_fid'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently
        # but mean the same (after mapping on QGIS)
        diff_df = db.dfquery(f"""
        select distinct st_distance(raw_inputs.geom,
        st_transform(st_setsrid(end_table.geom, 4326),2263)
        )::int as distance
        from (
            select 1 as id, st_setsrid(st_point(1015329.1, 213793.1), 2263) as geom
            union
            select 2 as id, st_setsrid(st_point(1015428.1, 213086.1), 2263) as geom
        ) raw_inputs
        join {pg_schema}.{test_read_shp_table_name} end_table
        on raw_inputs.id=end_table.gid::int
        """)

        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

    def test_read_shp_other_compressed(self):
        fp = FOLDER_PATH + "\\test.7z"
        shp_name = "test.shp"

        # Make sure table doesn't already exist
        db.drop_table(pg_schema, test_read_shp_table_name)
        assert not db.table_exists(schema=pg_schema, table=test_read_shp_table_name)

        # Read the .7z archive directly into PostGIS
        s.upload_geospatial(
            dbo=db,
            path=fp,
            schema=pg_schema,
            input_file=shp_name,
            table=test_read_shp_table_name,
            print_cmd=True
        )

        # Check table creation and content
        assert db.table_exists(schema=pg_schema, table=test_read_shp_table_name)
        table_df = db.dfquery(f'select * from {pg_schema}.{test_read_shp_table_name}')
        assert set(table_df.columns) == {'gid', 'some_value', 'geom', 'ogc_fid'}
        assert len(table_df) == 2

        # Check geometry consistency
        diff_df = db.dfquery(f"""
        select distinct st_distance(raw_inputs.geom,
        st_transform(st_setsrid(end_table.geom, 4326),2263)
        )::int as distance
        from (
            select 1 as id, st_setsrid(st_point(1015329.1, 213793.1), 2263) as geom
            union
            select 2 as id, st_setsrid(st_point(1015428.1, 213086.1), 2263) as geom
        ) raw_inputs
        join {pg_schema}.{test_read_shp_table_name} end_table
        on raw_inputs.id = end_table.gid::int
        """)

        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

    def test_read_shp_no_table(self):
        fp = FOLDER_PATH
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        db.drop_table(schema=pg_schema, table="test")

        # Read shp to new, test table
        s.upload_geospatial(dbo=db, path=fp, schema=pg_schema, input_file=shp_name, print_cmd=True)

        # Assert read_shp happened successfully and contents are correct
        assert db.table_exists(schema=pg_schema, table='test')
        table_df = db.dfquery(f'select * from {pg_schema}.test')

        assert set(table_df.columns) == {'some_value', 'ogc_fid', 'gid', 'geom'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently but mean the same (after mapping on QGIS)
        diff_df = db.dfquery(f"""
        select distinct st_distance(raw_inputs.geom, st_transform(st_setsrid(end_table.geom, 4326),2263))::int as distance
        from (
            select 1 as id, st_setsrid(st_point(1015329.1, 213793.1), 2263) as geom
            union
            select 2 as id, st_setsrid(st_point(1015428.1, 213086.1), 2263) as geom
        ) raw_inputs
        join {pg_schema}.test end_table
        on raw_inputs.id=end_table.gid::int
        """)
        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

        # Cleanup
        db.drop_table(schema=pg_schema, table='test')

    def test_read_shp_no_schema(self):
        fp = FOLDER_PATH
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        db.drop_table(schema=db.default_schema, table=test_read_shp_table_name)

        # Read shp to new, test table
        s.upload_geospatial(dbo=db, path=fp, input_file=shp_name, table=test_read_shp_table_name, print_cmd=True)

        # Assert read_shp happened successfully and contents are correct
        assert db.table_exists(schema=db.default_schema, table=test_read_shp_table_name)
        table_df = db.dfquery(f'select * from {test_read_shp_table_name}')

        assert set(table_df.columns) == {'some_value', 'ogc_fid', 'gid', 'geom'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently but mean the same (after mapping on QGIS)
        diff_df = db.dfquery(f"""
        select distinct
        st_distance(raw_inputs.geom, st_transform(st_setsrid(end_table.geom, 4326),2263))::int distance
        from (
            select 1 as id, st_setsrid(st_point(1015329.1, 213793.1), 2263) as geom
            union
            select 2 as id, st_setsrid(st_point(1015428.1, 213086.1), 2263) as geom
        ) raw_inputs
        join {test_read_shp_table_name} end_table
        on raw_inputs.id=end_table.gid::int
        """)

        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

        # Cleanup
        db.drop_table(schema=db.default_schema, table=test_read_shp_table_name)


    @classmethod
    def teardown_class(cls):
        helpers.clean_up_shapefile()
        helpers.clean_up_test_table_pg(db)


class TestReadShpMS:
    @classmethod
    def setup_class(cls):
        helpers.set_up_shapefile()

    def test_read_shp_basic(self):
        fp = FOLDER_PATH
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        sql.drop_table(schema=ms_schema, table=test_read_shp_table_name)

        # Read shp to new, test table
        s.upload_geospatial(dbo=sql, path=fp, table=test_read_shp_table_name, schema=ms_schema, input_file=shp_name, print_cmd=True)

        # Assert read_shp happened successfully and contents are correct
        assert sql.table_exists(schema=ms_schema, table=test_read_shp_table_name)

        # todo: this fails because odbc 17 driver isnt supporting geometry
        table_df = sql.dfquery(f'select * from {ms_schema}.{test_read_shp_table_name}')

        assert set(table_df.columns) == {'ogr_fid', 'gid', 'some_value', 'geom'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently but mean the same (after mapping on QGIS)
        diff_df = sql.dfquery(f"""
        select distinct raw_inputs.geom.STDistance(end_table.geom) as distance
        from (
            (select 1 as id, geometry::Point(-73.88782477721676, 40.75343453961836, 2263) as geom)
            union all
            (select 2 as id, geometry::Point(-73.88747073046778, 40.75149365677327, 2263) as geom)
        ) raw_inputs
        join {ms_schema}.{test_read_shp_table_name} end_table
        on raw_inputs.id=end_table.gid
        """)

        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

        assert sql.tables_created[-1] == (sql.server, sql.database, ms_schema, test_read_shp_table_name)

        # Cleanup
        sql.drop_table(schema=ms_schema, table=test_read_shp_table_name)

    def test_read_shp_zip(self):

        fp = FOLDER_PATH + '/test.zip'
        shp_name = "test.shp"

        # Make sure table doesn't alredy exist
        sql.drop_table(ms_schema, test_read_shp_table_name)
        assert not db.table_exists(schema=ms_schema, table=test_read_shp_table_name)

        # Assert successful
        sql.drop_table(schema=ms_schema, table=test_read_shp_table_name)

        # Read shp to new, test table
        s.upload_geospatial(dbo=sql, path=fp, table=test_read_shp_table_name, schema=ms_schema, input_file=shp_name, print_cmd=True)

        # Assert read_shp happened successfully and contents are correct
        assert sql.table_exists(schema=ms_schema, table=test_read_shp_table_name)
        table_df = sql.dfquery(f'select * from {ms_schema}.{test_read_shp_table_name}')

        assert set(table_df.columns) == {'gid', 'some_value', 'geom', 'ogr_fid'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently
        # but mean the same (after mapping on QGIS)
        diff_df = sql.dfquery(f"""
                select distinct raw_inputs.geom.STDistance(end_table.geom) as distance
                from (
                    (select 1 as id, geometry::Point(-73.88782477721676, 40.75343453961836, 2263) as geom)
                    union all
                    (select 2 as id, geometry::Point(-73.88747073046778, 40.75149365677327, 2263) as geom)
                ) raw_inputs
                join {ms_schema}.{test_read_shp_table_name} end_table
                on raw_inputs.id=end_table.gid
                """)

        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

    def test_read_shp_zip_2(self):

        fp = FOLDER_PATH + '/test.zip'
        shp_name = "test.shp"

        # Make sure table doesn't alredy exist
        sql.drop_table(ms_schema, test_read_shp_table_name)
        assert not db.table_exists(schema=ms_schema, table=test_read_shp_table_name)

        # Assert successful
        sql.drop_table(schema=ms_schema, table=test_read_shp_table_name)

        # Read shp to new, test table
        sql.shp_to_table(path=fp, table=test_read_shp_table_name, schema=ms_schema, shp_name=shp_name, print_cmd=True)

        # Assert read_shp happened successfully and contents are correct
        assert sql.table_exists(schema=ms_schema, table=test_read_shp_table_name)
        table_df = sql.dfquery(f'select * from {ms_schema}.{test_read_shp_table_name}')

        assert set(table_df.columns) == {'gid', 'some_value', 'geom', 'ogr_fid'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently
        # but mean the same (after mapping on QGIS)
        diff_df = sql.dfquery(f"""
                select distinct raw_inputs.geom.STDistance(end_table.geom) as distance
                from (
                    (select 1 as id, geometry::Point(-73.88782477721676, 40.75343453961836, 2263) as geom)
                    union all
                    (select 2 as id, geometry::Point(-73.88747073046778, 40.75149365677327, 2263) as geom)
                ) raw_inputs
                join {ms_schema}.{test_read_shp_table_name} end_table
                on raw_inputs.id=end_table.gid
                """)

        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

    def test_read_shp_other_compressed(self):
        fp = FOLDER_PATH + "\\test.7z"
        shp_name = "test.shp"

        # Make sure table doesn't already exist
        sql.drop_table(pg_schema, test_read_shp_table_name)
        assert not sql.table_exists(schema=pg_schema, table=test_read_shp_table_name)

        # Read the .7z archive directly into PostGIS
        s.upload_geospatial(
            dbo=sql,
            path=fp,
            schema=ms_schema,
            input_file=shp_name,
            table=test_read_shp_table_name,
            print_cmd=True
        )

        # Check table creation and content
        assert sql.table_exists(schema=ms_schema, table=test_read_shp_table_name)
        table_df = sql.dfquery(f'select * from {ms_schema}.{test_read_shp_table_name}')
        assert set(table_df.columns) == {'ogr_fid', 'gid', 'some_value', 'geom'}
        assert len(table_df) == 2

        # Check geometry consistency
        diff_df = sql.dfquery(f"""
                select distinct raw_inputs.geom.STDistance(end_table.geom) as distance
                from (
                    (select 1 as id, geometry::Point(-73.88782477721676, 40.75343453961836, 2263) as geom)
                    union all
                    (select 2 as id, geometry::Point(-73.88747073046778, 40.75149365677327, 2263) as geom)
                ) raw_inputs
                join {ms_schema}.{test_read_shp_table_name} end_table
                on raw_inputs.id=end_table.gid
                """)

        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

    def test_read_shp_no_table(self):
        fp = FOLDER_PATH
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        sql.drop_table(schema=ms_schema, table='test')

        # Read shp to new, test table
        s.upload_geospatial(dbo=sql, path=fp, schema=ms_schema, input_file=shp_name, print_cmd=True)

        # Assert read_shp happened successfully and contents are correct
        assert sql.table_exists(schema=ms_schema, table='test')
        table_df = sql.dfquery(f'select * from {ms_schema}.test')
        assert set(table_df.columns) == {'ogr_fid', 'gid', 'some_value', 'geom'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently but mean the same (after mapping on QGIS)
        diff_df = sql.dfquery(f"""
        select distinct raw_inputs.geom.STDistance(end_table.geom) as distance
        from (            
            (select 1 as id, geometry::Point( -73.88782477721676, 40.75343453961836, 2263) as geom)
            union all
            (select 2 as id, geometry::Point(-73.88747073046778, 40.75149365677327, 2263) as geom)
        ) raw_inputs
        join {ms_schema}.test end_table
        on raw_inputs.id=cast(end_table.gid as int)
        """)
        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

        # Cleanup
        sql.drop_table(schema=ms_schema, table='test')

    def test_read_shp_no_schema(self):
        fp = FOLDER_PATH
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        sql.drop_table(schema=sql.default_schema, table=test_read_shp_table_name)

        # Read shp to new, test table
        s.upload_geospatial(dbo=sql, path=fp, table=test_read_shp_table_name, input_file=shp_name, print_cmd=True)

        # Assert read_shp happened successfully and contents are correct
        assert sql.table_exists(schema=sql.default_schema, table=test_read_shp_table_name)
        table_df = sql.dfquery(f'select * from {test_read_shp_table_name}')
        assert set(table_df.columns) == {'ogr_fid', 'gid', 'some_value', 'geom'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently but mean the same (after mapping on QGIS)
        diff_df = sql.dfquery(f"""
        select distinct raw_inputs.geom.STDistance(end_table.geom) as distance
        from (
            (select 1 as id, geometry::Point(-73.88782477721676, 40.75343453961836, 2263) as geom)
            union all
            (select 2 as id, geometry::Point(-73.88747073046778, 40.75149365677327, 2263) as geom)
        ) raw_inputs
        join {test_read_shp_table_name} end_table
        on raw_inputs.id=cast(end_table.gid as int)
        """)

        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

        # Cleanup
        sql.drop_table(schema=sql.default_schema, table=test_read_shp_table_name)

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_shapefile()
        helpers.clean_up_test_table_sql(sql)


class TestWriteShpPG:
    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_pg(db)

    def test_write_shp_table(self):
        db.query(f"""
        drop table if exists {pg_schema}.{test_write_shp_table_name};

        create table {pg_schema}.{test_write_shp_table_name} as
        select *
        from {pg_schema}.{pg_table_name}
        order by id
        limit 100
        """)

        fp = FOLDER_PATH
        shp_name = 'test_write.shp'

        # Write shp
        s.write_geospatial(dbo=db, path=fp + '//' + shp_name, table=test_write_shp_table_name, schema=pg_schema, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(fp, shp_name))

        db.shp_to_table(path=fp, shp_name=shp_name, schema=pg_schema, table=test_reuploaded_table_name, print_cmd=True)
 
        # Assert equality
        db_df = db.dfquery(f"select * from {pg_schema}.{test_write_shp_table_name} order by id limit 100")
        shp_uploaded_df = db.dfquery(f"select * from {pg_schema}.{test_reuploaded_table_name} order by id")
 
        assert len(db_df) == len(shp_uploaded_df)

        # Some columns changed names since shpfiles have a character limit of 10
        mutual_columns = set(db_df.columns).intersection(shp_uploaded_df.columns) - {'ogc_fid', 'geom'}
        pd.testing.assert_frame_equal(db_df[list(mutual_columns)], shp_uploaded_df[list(mutual_columns)],
                                      check_like=True, check_names=False, check_dtype=False,
                                      check_datetimelike_compat=True)

        # Assert before/after geom columns are all 0 ft from each other, even if represented differently
        dist_df = db.dfquery(f"""
        select distinct st_distance(st_setsrid(b.geom, 2263), a.geom) as distance
        from {pg_schema}.{test_write_shp_table_name} b
        join {pg_schema}.{test_reuploaded_table_name} a
            on b.id=a.id
        """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0
        
        # clean up
        db.drop_table(schema=pg_schema, table=test_write_shp_table_name)
        db.drop_table(schema=pg_schema, table=test_reuploaded_table_name)
 
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fp, shp_name.replace('shp', ext)))

    def test_write_shp_table_pth(self):

        db.drop_table(pg_schema, test_write_shp_table_name)

        db.query(f"""
        create table {pg_schema}.{test_write_shp_table_name} as
        select *
        from {pg_schema}.{pg_table_name}
        order by id
        limit 100
        """)
 
        fp = FOLDER_PATH
        shp_name = 'test_write.shp'
 
        # Write shp
        s.write_geospatial(dbo = db, path = fp + '//' + shp_name, schema = pg_schema, table = test_write_shp_table_name, print_cmd=True)
 
        # Assert successful
        assert os.path.isfile(os.path.join(fp, shp_name))
 
        # Reupload as table
        s.upload_geospatial(dbo = db, path=fp, input_file=shp_name, schema=pg_schema, table=test_reuploaded_table_name, print_cmd=True)

        # Assert equality
        db_df = db.dfquery(f"select * from {pg_schema}.{test_write_shp_table_name} order by id limit 100")
        shp_uploaded_df = db.dfquery(f"select * from {pg_schema}.{test_reuploaded_table_name} order by id")

        assert len(db_df) == len(shp_uploaded_df)

        # Some columns changed names since shpfiles have a character limit of 10
        mutual_columns = set(db_df.columns).intersection(shp_uploaded_df.columns) - {'ogc_fid', 'geom'}
        pd.testing.assert_frame_equal(db_df[list(mutual_columns)], shp_uploaded_df[list(mutual_columns)],
                                      check_like=True, check_names=False, check_dtype=False,
                                      check_datetimelike_compat=True)

        # Assert before/after geom columns are all 0 ft from each other, even if represented differently
        dist_df = db.dfquery(f"""
        select distinct st_distance(st_setsrid(b.geom, 2263), a.geom) as distance
        from {pg_schema}.{test_write_shp_table_name} b
        join {pg_schema}.{test_reuploaded_table_name} a
        on b.id=a.id
        """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0

        # clean up
        db.drop_table(schema=pg_schema, table=test_write_shp_table_name)
        db.drop_table(schema=pg_schema, table=test_reuploaded_table_name)

        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fp, shp_name.replace('shp', ext)))

    def test_write_shp_table_pth_w_name(self):
        db.query(f"""
        drop table if exists {pg_schema}.{test_write_shp_table_name};

        create table {pg_schema}.{test_write_shp_table_name} as
        select *
        from {pg_schema}.{pg_table_name}
        order by id
        limit 100
        """)

        shp_name = 'test_write.shp'

        # Write shp
        s.write_geospatial(dbo=db, path=os.path.join(FOLDER_PATH, shp_name), table=test_write_shp_table_name, schema=pg_schema, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, shp_name))

        # Reupload as table
        s.upload_geospatial(dbo = db, path=FOLDER_PATH, input_file=shp_name ,schema=pg_schema,
                        table=test_reuploaded_table_name, print_cmd=True)

        # Assert equality
        db_df = db.dfquery(f"select * from {pg_schema}.{pg_table_name} order by id limit 100")
        shp_uploaded_df = db.dfquery(f"select * from {pg_schema}.{test_reuploaded_table_name} order by id")

        assert len(db_df) == len(shp_uploaded_df)

        # Some columns changed names since shpfiles have a character limit of 10
        mutual_columns = set(db_df.columns).intersection(shp_uploaded_df.columns) - {'ogc_fid', 'geom'}
        pd.testing.assert_frame_equal(db_df[list(mutual_columns)], shp_uploaded_df[list(mutual_columns)],
                                      check_like=True, check_names=False, check_dtype=False,
                                      check_datetimelike_compat=True)

        # Assert before/after geom columns are all 0 ft from each other, even if represented differently
        dist_df = db.dfquery(f"""
        select distinct st_distance(st_setsrid(b.geom, 2263), a.geom) as distance
        from {pg_schema}.{pg_table_name} b
        join {pg_schema}.{test_reuploaded_table_name} a
        on b.id=a.id
        """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0

        db.drop_table(schema=pg_schema, table=test_write_shp_table_name)
        db.drop_table(schema=pg_schema, table=test_reuploaded_table_name)

        # clean up
        db.drop_table(schema=pg_schema, table=test_write_shp_table_name)

        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(FOLDER_PATH, shp_name.replace('shp', ext)))

    def test_write_shp_query(self):
        fp = FOLDER_PATH
        shp_name = 'test_write.shp'

        # Write shp
        s.write_geospatial(dbo=db, path=os.path.join(fp, shp_name),
                      query=f"""select * from {pg_schema}.{pg_table_name} order by id limit 100""", print_cmd=True)

        # Check table in folder
        assert os.path.isfile(os.path.join(fp, shp_name))

        # Reupload as table
        s.upload_geospatial(dbo = db, path=fp, input_file=shp_name, schema=pg_schema, table=test_reuploaded_table_name, print_cmd=True)

        # Assert equality
        db_df = db.dfquery(f"select * from {pg_schema}.{pg_table_name} order by id limit 100")
        shp_uploaded_df = db.dfquery(f"select * from {pg_schema}.{test_reuploaded_table_name} order by id")

        assert len(db_df) == len(shp_uploaded_df)

        # Some columns changed names since shpfiles have a character limit of 10
        mutual_columns = set(db_df.columns).intersection(shp_uploaded_df.columns) - {'ogc_fid', 'geom'}
        pd.testing.assert_frame_equal(db_df[list(mutual_columns)], shp_uploaded_df[list(mutual_columns)],
                                      check_like=True, check_names=False, check_dtype=False,
                                      check_datetimelike_compat=True)

        # Assert before/after geom columns are all 0 ft from each other, even if represented differently
        dist_df = db.dfquery(f"""
        select distinct st_distance(st_setsrid(b.geom, 2263), a.geom) as distance
        from {pg_schema}.{pg_table_name} b
        join {pg_schema}.{test_reuploaded_table_name} a
        on b.id=a.id
        """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0

        db.drop_table(schema=pg_schema, table=test_write_shp_table_name)
        db.drop_table(schema=pg_schema, table=test_reuploaded_table_name)

        # clean up
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(FOLDER_PATH, shp_name.replace('shp', ext)))

    def test_write_shp_dates_table(self):

        shp_name = 'test_write.shp'

        db.query(f"""
                drop table if exists {pg_schema}.{test_write_shp_table_name};

                create table {pg_schema}.{test_write_shp_table_name} as
                select  now() as dt_col,
                        current_date as dt_col2,
                        cast('2020-01-01' as date) as next_dt_col,
                        geom
                from {pg_schema}.{pg_table_name};

                """)
        
        s.write_geospatial(path=os.path.join(FOLDER_PATH, shp_name), dbo=db, schema=pg_schema, table=test_write_shp_table_name, overwrite = True)
        
        # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, shp_name))

        # check that Date column is set as a Date type
        cmd_shp = f'ogrinfo -so -al "{FOLDER_PATH}/{shp_name}" '
        ogr_response_shp = subprocess.check_output(shlex.split(cmd_shp), stderr=subprocess.STDOUT)   
        assert 'dt_col_dt: Date' in str(ogr_response_shp), "'dt_col_dt column is not a Date data type when it should be"

        # clean up
        db.drop_table(schema=pg_schema, table=test_write_shp_table_name)
        os.remove(os.path.join(FOLDER_PATH, shp_name))

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(db)
        helpers.clean_up_shapefile()


class TestWriteShpMS:
    
    def test_write_shp_table(self):
        sql.drop_table(schema=ms_schema, table=test_write_shp_table_name)

        # Add test_table
        sql.query(f"""
        create table {ms_schema}.{test_write_shp_table_name} (test_col1 int, test_col2 int, geom geometry);
        insert into {ms_schema}.{test_write_shp_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_shp_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        fp = FOLDER_PATH
        shp_name = 'test_write.shp'

        # Write shp
        s.write_geospatial(dbo=sql, path= os.path.join(fp, shp_name), table=test_write_shp_table_name, schema=ms_schema, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(fp, shp_name))

        # Reupload as table
        s.upload_geospatial(dbo = sql, path=fp, input_file=shp_name, schema=ms_schema, table=test_reuploaded_table_name, print_cmd=True)

        # Assert equality
        db_df = sql.dfquery(f"select top 10 * from {ms_schema}.{test_write_shp_table_name} order by test_col1")
        shp_uploaded_df = sql.dfquery(f"select top 10 * from {ms_schema}.{test_reuploaded_table_name} order by test_col1")

        assert len(db_df) == len(shp_uploaded_df)

        # Some columns may change names since shpfiles have a character limit of 10
        pd.testing.assert_frame_equal(db_df[['test_col1', 'test_col2']],
                                      shp_uploaded_df[['test_col1', 'test_col2']],
                                      check_column_type=False,
                                      check_dtype=False)

        # Assert before/after geom columns are all 0 ft from each other, even if represented differently
        dist_df = sql.dfquery(f"""
        select distinct b.geom.STDistance(a.geom) as distance
        from {ms_schema}.{test_write_shp_table_name} b
        join {ms_schema}.{test_reuploaded_table_name} a
        on b.test_col1=a.test_col1
        """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0

        # Clean up
        sql.drop_table(schema=ms_schema, table=test_write_shp_table_name)
        sql.drop_table(schema=ms_schema, table=test_reuploaded_table_name)

        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fp, shp_name.replace('shp', ext)))
            except Exception as e:
                print(e)

    def test_write_shp_dates_table(self):
        sql.drop_table(schema=ms_schema, table=test_write_shp_table_name)

        # Add test_table
        sql.query(f"""
        create table {ms_schema}.{test_write_shp_table_name} (test_col1 int, test_col2 int, dte datetime, geom geometry);
        insert into {ms_schema}.{test_write_shp_table_name} VALUES(1, 2, current_timestamp, geometry::Point(985831.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_shp_table_name} VALUES(3, 4, current_timestamp, geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        fp = FOLDER_PATH
        shp_name = 'test_write.shp'

        # Write shp
        s.write_geospatial(dbo=sql, path= os.path.join(fp, shp_name), table=test_write_shp_table_name, schema=ms_schema, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(fp, shp_name))

        # check that Date column is set as a Date type
        cmd_shp = f'ogrinfo -so -al "{FOLDER_PATH}/{shp_name}" '
        ogr_response_shp = subprocess.check_output(shlex.split(cmd_shp), stderr=subprocess.STDOUT)   
        assert 'dte_dt: Date' in str(ogr_response_shp), "'dte_dt column is not a Date data type when it should be"
  
        # Clean up
        sql.drop_table(schema=ms_schema, table=test_write_shp_table_name)

        # for ext in ('dbf', 'prj', 'shx', 'shp'):
        #     try:
        #         os.remove(os.path.join(fp, shp_name.replace('shp', ext)))
        #     except Exception as e:
        #         print(e)

    def test_write_shp_table_pth(self):
 
        sql.drop_table(schema=ms_schema, table=test_write_shp_table_name)

        # Add test_table
        sql.query(f"""
        create table {ms_schema}.{test_write_shp_table_name} (test_col1 int, test_col2 int, geom geometry);
        insert into {ms_schema}.{test_write_shp_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_shp_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        fp = FOLDER_PATH
        shp_name = 'test_write.shp'

        # Write shp
        s.write_geospatial(dbo=sql, path= os.path.join(fp, shp_name), table=test_write_shp_table_name, schema=ms_schema, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(fp, shp_name))

        # Reupload as table
 
        s.upload_geospatial(dbo=sql, path=fp+'\\'+shp_name, schema=ms_schema, table=test_reuploaded_table_name, print_cmd=True)

        # Assert equality
        db_df = sql.dfquery(f"select top 10 * from {ms_schema}.{test_write_shp_table_name} order by test_col1")
        shp_uploaded_df = sql.dfquery(f"select top 10 * from {ms_schema}.{test_reuploaded_table_name} order by test_col1")
 
        assert len(db_df) == len(shp_uploaded_df)

        # Some columns may change names since shpfiles have a character limit of 10
        pd.testing.assert_frame_equal(db_df[['test_col1', 'test_col2']],
                                      shp_uploaded_df[['test_col1', 'test_col2']],
                                      check_column_type=False,
                                      check_dtype=False)

        # Assert before/after geom columns are all 0 ft from each other, even if represented differently
        dist_df = sql.dfquery(f"""
        select distinct b.geom.STDistance(a.geom) as distance
        from {ms_schema}.{test_write_shp_table_name} b
        join {ms_schema}.{test_reuploaded_table_name} a
            on b.test_col1=a.test_col1
        """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0

        # Clean up
        sql.drop_table(schema=ms_schema, table=test_write_shp_table_name)
        sql.drop_table(schema=ms_schema, table=test_reuploaded_table_name)
 
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fp, shp_name.replace('shp', ext)))
            except Exception as e:
                print(e)

    def test_write_shp_query(self):
        fp = FOLDER_PATH
        shp_name = 'test_write.shp'
        sql.drop_table(schema=ms_schema, table=test_write_shp_table_name)

        # Add test_table
        sql.query(f"""
        create table {ms_schema}.{test_write_shp_table_name} (test_col1 int, test_col2 int, geom geometry);
        insert into {ms_schema}.{test_write_shp_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_shp_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        # Write shp
        s.write_geospatial(dbo=sql, path= os.path.join(fp, shp_name),
                           query=f"""select top 10 * from {ms_schema}.{test_write_shp_table_name} order by test_col1""", print_cmd=True)

        # Check table in folder
        assert os.path.isfile(os.path.join(fp, shp_name))

        # Reupload as table
        s.upload_geospatial(dbo = sql, path=fp, input_file=shp_name, schema=ms_schema, table=test_reuploaded_table_name, print_cmd=True)

        # Assert equality
        db_df = sql.dfquery(f"select top 10 * from {ms_schema}.{test_write_shp_table_name} order by test_col1")
        shp_uploaded_df = sql.dfquery(f"select top 10 * from {ms_schema}.{test_reuploaded_table_name} order by test_col1")

        assert len(db_df) == len(shp_uploaded_df)

        # Some columns changed names since shpfiles have a character limit of 10
        pd.testing.assert_frame_equal(db_df[['test_col1', 'test_col2']],
                                      shp_uploaded_df[['test_col1', 'test_col2']],
                                      check_column_type=False,
                                      check_dtype=False)

        # Assert before/after geom columns are all 0 ft from each other, even if represented differently
        dist_df = sql.dfquery(f"""
                select distinct b.geom.STDistance(a.geom) as distance
                from {ms_schema}.{test_write_shp_table_name} b
                join {ms_schema}.{test_reuploaded_table_name} a
                on b.test_col1=a.test_col1
                """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0

        # Clean up
        sql.drop_table(schema=ms_schema, table=test_write_shp_table_name)
        sql.drop_table(schema=ms_schema, table=test_reuploaded_table_name)

        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fp, shp_name.replace('shp', ext)))
            except Exception as e:
                print(e)

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(sql)
        helpers.clean_up_shapefile()

class TestFeatureClassToTablePg:
    @classmethod
    def setup_class(cls):
        helpers.set_up_feature_class()

    def test_import_fc_basic(self):
        db.drop_table(table=test_feature_class_table_name, schema=db.default_schema)
        assert not db.table_exists(test_feature_class_table_name, schema=db.default_schema)

        db.feature_class_to_table(path = fgdb, table = test_feature_class_table_name, schema=None, feature_class=fc)
        assert db.table_exists(test_feature_class_table_name, schema=db.default_schema)

        db.drop_table(db.default_schema, test_feature_class_table_name)

    def test_import_fc_new_name(self):
        db.drop_table(table = test_feature_class_table_name, schema=db.default_schema)
        assert not db.table_exists(test_feature_class_table_name, schema=db.default_schema)

        db.feature_class_to_table(path = fgdb, table=test_feature_class_table_name, schema=None, feature_class = fc)
        assert db.table_exists(test_feature_class_table_name, schema=db.default_schema)

        db.drop_table(db.default_schema, test_feature_class_table_name)

    def test_import_fc_complex_file_exts(self):

        db.drop_table(table=test_feature_class_table_name, schema=db.default_schema)
        assert not db.table_exists(test_feature_class_table_name, schema=db.default_schema)

        db.feature_class_to_table(path = FOLDER_PATH + "/nyclion_21d.zip/lion/lion.gdb", table = test_feature_class_table_name, schema=None, feature_class=fc)
        assert db.table_exists(test_feature_class_table_name, schema=db.default_schema)

        db.drop_table(db.default_schema, test_feature_class_table_name)

    def test_import_fc_new_name_schema(self):

        db.drop_table(table = test_feature_class_table_name, schema=pg_schema)
        assert not db.table_exists(test_feature_class_table_name, schema=pg_schema)

        db.feature_class_to_table(path = fgdb, table=test_feature_class_table_name, feature_class = fc, schema=pg_schema)
        assert db.table_exists(test_feature_class_table_name, schema=pg_schema)

        db.query(f"select * from {pg_schema}.__temp_log_table_{db.user}__ where table_name = '{test_feature_class_table_name}'")
        assert len(db.data) == 1

        db.drop_table(pg_schema, test_feature_class_table_name)

    def test_import_fc_new_name_schema_srid(self):

        db.drop_table(table=test_feature_class_table_name, schema=pg_schema)
        assert not db.table_exists(test_feature_class_table_name, schema=pg_schema)

        db.feature_class_to_table(path = fgdb, table=test_feature_class_table_name, feature_class=fc, schema=pg_schema, srid=4326)
        assert db.table_exists(test_feature_class_table_name, schema=pg_schema)

        db.query(f'select distinct st_srid(geom) from {pg_schema}.{test_feature_class_table_name}')
        assert db.data[0][0] == 4326

        db.drop_table(pg_schema, test_feature_class_table_name)

    def test_import_fc_new_name_data_check(self):
        db.drop_table(table=test_feature_class_table_name, schema=db.default_schema)
        assert not db.table_exists(test_feature_class_table_name, schema=db.default_schema)

        db.feature_class_to_table(path = fgdb, table=test_feature_class_table_name, schema=None, feature_class=fc)
        assert db.table_exists(test_feature_class_table_name, schema=db.default_schema)

        db.query(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{test_feature_class_table_name}'
            AND table_schema = '{db.default_schema}'
        """)

        columns = {i[0] for i in db.data}
        types = {i[1] for i in db.data}

        # identify the uploaded geometry column name, because it can be named geom or Shape depending on user's computer

        assert {'vintersect', 'objectid', 'geom', 'nodeid'}.issubset(columns)
        assert {db_int, db_int, 'character varying', db_geom}.issubset(types)
        # check non geom data
        db.query(f"""
                    select nodeid, vintersect, st_astext(geom, 1) geom from {db.default_schema}.{test_feature_class_table_name} where nodeid in (88, 98, 100)
                """)

        row_values = [(88, 'VirtualIntersection', 'MULTIPOINT(914145.1 126536.1)'),
                    (98, '', 'MULTIPOINT(914714.8 126499.8)'),
                    (100, 'VirtualIntersection', 'MULTIPOINT(914872 126696.6)')]

        # assert db.data == row_values
        for c in range(len(db.data)):
            for r in range(len(db.data[c])):
                assert row_values[c][r] == db.data[c][r]

        # check geom matches (less than 1 ft off
        db.query(f"""
            select st_distance(st_setsrid(ST_GeometryN(geom, 1), 2263),
                st_setsrid(st_makepoint(914145.1,126536.1, 2263),2263))
            from {db.default_schema}.{test_feature_class_table_name}
            where nodeid=88
        """)
        assert db.data[0][0] < 1

        db.query(f"""
            select st_distance(st_setsrid(ST_GeometryN(geom, 1), 2263),
                st_setsrid(st_makepoint(920184.0, 138084.1, 2263),2263))
            from {db.default_schema}.{test_feature_class_table_name}
            where nodeid=888
        """)

        assert db.data[0][0] < 1

        db.drop_table(db.default_schema, test_feature_class_table_name)

    def test_import_fc_new_name_schema_no_fc(self):

        db.drop_table(table=test_feature_class_table_name, schema=db.default_schema)
        assert not db.table_exists(test_feature_class_table_name, schema=db.default_schema)

        try:
            db.feature_class_to_table(path = fgdb, table=test_feature_class_table_name, feature_class=fc, schema=pg_schema)
        except:
            assert not db.table_exists(test_feature_class_table_name, schema=pg_schema)

        db.drop_table(pg_schema, test_feature_class_table_name)

    def test_import_fc_new_name_schema_private(self):
        private_table = test_feature_class_table_name + '_priv'

        db.drop_table(table=private_table, schema=pg_schema)
        assert not db.table_exists(private_table, schema=pg_schema)

        db.feature_class_to_table(path = fgdb, table=private_table, feature_class=fc, schema=pg_schema, private=True)
        assert db.table_exists(private_table, schema=pg_schema)

        db.query(f"""
            select distinct grantee from information_schema.table_privileges
            where table_name = '{private_table}'
            and table_schema='{pg_schema}'
        """, strict=False)
        assert len(db.data) == 1

        db.drop_table(pg_schema, private_table)

    def test_import_fc_new_name_schema_tmp(self):
        not_temp_table = test_feature_class_table_name + '_tmp'

        db.drop_table(table=not_temp_table, schema=pg_schema)
        assert not db.table_exists(not_temp_table, schema=pg_schema)

        db.feature_class_to_table(path = fgdb, table=not_temp_table, feature_class=fc, schema=pg_schema, temp=False)
        assert db.table_exists(not_temp_table, schema=pg_schema)

        db.query(f"select * from {pg_schema}.__temp_log_table_{db.user}__ where table_name = '{not_temp_table}'")
        assert len(db.data) == 0

        db.drop_table(pg_schema, not_temp_table)

    def test_import_fc_extra_cmd(self):
        db.drop_table(table=test_feature_class_table_name, schema=db.default_schema)
        assert not db.table_exists(test_feature_class_table_name, schema=db.default_schema)

        db.feature_class_to_table(fgdb, test_feature_class_table_name, schema=None, feature_class='lion', extra_cmd='-nlt MULTILINESTRING')
        assert db.table_exists(test_feature_class_table_name, schema=db.default_schema)

        db.drop_table(db.default_schema, test_feature_class_table_name)

    @classmethod
    def teardown_class(cls):
        db.cleanup_new_tables()


class TestFeatureClassToTableMs:
    @classmethod
    def setup_class(cls):
        helpers.set_up_feature_class()

    def test_import_fc_basic(self):
        sql.drop_table(table=test_feature_class_table_name, schema=sql.default_schema)
        assert not sql.table_exists(test_feature_class_table_name, schema=sql.default_schema)

        sql.feature_class_to_table(path = fgdb, table=test_feature_class_table_name, schema=None, feature_class=fc, print_cmd=True,skip_failures='-skip_failures')
        assert sql.table_exists(test_feature_class_table_name, schema=sql.default_schema)

        sql.drop_table(sql.default_schema, test_feature_class_table_name)

    def test_import_fc_new_name(self):
        sql.drop_table(table=test_feature_class_table_name, schema=sql.default_schema)
        assert not sql.table_exists(test_feature_class_table_name, schema=sql.default_schema)

        sql.feature_class_to_table(path = fgdb, table=test_feature_class_table_name, schema=None, feature_class=fc, skip_failures='-skip_failures')
        assert sql.table_exists(test_feature_class_table_name, schema=sql.default_schema)

        sql.drop_table(sql.default_schema, test_feature_class_table_name)

    def test_import_fc_complex_file_exts(self):

        sql.drop_table(table=test_feature_class_table_name, schema=sql.default_schema)
        assert not sql.table_exists(test_feature_class_table_name, schema=sql.default_schema)

        sql.feature_class_to_table(path = FOLDER_PATH + "/nyclion_21d.zip/lion/lion.gdb", table = test_feature_class_table_name, schema=None, feature_class=fc, skip_failures='-skip_failures')
        assert sql.table_exists(test_feature_class_table_name, schema=sql.default_schema)

        sql.drop_table(sql.default_schema, test_feature_class_table_name)

    def test_import_fc_new_name_schema(self):

        sql.drop_table(table=test_feature_class_table_name, schema=ms_schema)
        assert not sql.table_exists(test_feature_class_table_name, schema=ms_schema)

        sql.feature_class_to_table(path = fgdb, table=test_feature_class_table_name, feature_class=fc, schema=ms_schema, skip_failures='-skip_failures')
        assert sql.table_exists(test_feature_class_table_name, schema=ms_schema)

        sql.drop_table(ms_schema, test_feature_class_table_name)

    def test_import_fc_new_name_schema_srid(self):

        sql.drop_table(table=test_feature_class_table_name, schema=ms_schema)
        assert not sql.table_exists(test_feature_class_table_name, schema=ms_schema)

        sql.feature_class_to_table(path = fgdb, table=test_feature_class_table_name, feature_class=fc, schema = ms_schema, srid=4326, skip_failures='-skip_failures')
        assert sql.table_exists(test_feature_class_table_name, schema = ms_schema)

        sql.query(f"select distinct geom.STSrid from {ms_schema}.{test_feature_class_table_name}")
        assert sql.data[0][0] == 4326

        sql.drop_table(ms_schema, test_feature_class_table_name)

    def test_import_fc_new_name_data_check(self):
        sql.drop_table(table=test_feature_class_table_name, schema=sql.default_schema)

        assert not sql.table_exists(test_feature_class_table_name, schema=sql.default_schema)
        sql.feature_class_to_table(path = fgdb, table=test_feature_class_table_name, schema=None, feature_class=fc, skip_failures='-skip_failures')

        assert sql.table_exists(test_feature_class_table_name, schema=sql.default_schema)

        # run the query to identify all column names and types from the test table
        sql.query(f"""
                select column_name, data_type
                from INFORMATION_SCHEMA.COLUMNS
                where table_name = '{test_feature_class_table_name}'
                and table_schema='{sql.default_schema}'
        """)

        columns = {i[0] for i in sql.data}
        types = {i[1] for i in sql.data}

        assert {'objectid', 'geom', 'nodeid', 'vintersect'}.issubset(columns)
        assert {sql_int, sql_geom, 'nvarchar'}.issubset(types)

        # check non geom data
        sql.query(f"""select nodeid, vintersect, geom.STAsText() geom from {sql.default_schema}.{test_feature_class_table_name} where nodeid in (88, 98, 100)
                        """)

        row_values = [(88, 'VirtualIntersection', 'MULTIPOINT ((914145.06807594 126536.07138967514))'),
                    (98, '', 'MULTIPOINT ((914714.79952293634 126499.80801236629))'),
                    (100, 'VirtualIntersection', 'MULTIPOINT ((914872.03410968184 126696.62913236022))')]

        for c in range(len(sql.data)):
            for r in range(len(sql.data[c])):
                assert row_values[c][r] == sql.data[c][r]

        # check geom matches (less than 1 ft off)
        sql.query(f"""
            select geom.STGeometryN(1).STDistance(geometry::Point(914145.1,126536.1, 2263))
            from {sql.default_schema}.{test_feature_class_table_name}
            where nodeid=88
        """)
        assert sql.data[0][0] < 1

        sql.query(f"""
            select geom.STGeometryN(1).STDistance(geometry::Point(920184.0, 138084.1, 2263))
            from {sql.default_schema}.{test_feature_class_table_name}
            where nodeid=888
                """)
        assert sql.data[0][0] < 1

        sql.drop_table(sql.default_schema, test_feature_class_table_name)

    def test_import_fc_new_name_schema_no_fc(self):
        schema = 'working'

        sql.drop_table(table=test_feature_class_table_name, schema=schema)
        assert not sql.table_exists(test_feature_class_table_name, schema=schema)

        try:
            sql.feature_class_to_table(path = fgdb, table = test_feature_class_table_name, feature_class=fc, schema=schema, skip_failures='-skip_failures')
        except:
            assert not sql.table_exists(test_feature_class_table_name, schema=schema)

        sql.drop_table(schema, test_feature_class_table_name)

    def test_import_fc_new_name_schema_temp(self):

        sql.drop_table(table=test_feature_class_table_name, schema = ms_schema)
        assert not sql.table_exists(test_feature_class_table_name, schema=ms_schema)

        sql.feature_class_to_table(path = fgdb, table = test_feature_class_table_name, feature_class=fc, schema=ms_schema, temp=False,
        skip_failures='-skip_failures')
        assert sql.table_exists(test_feature_class_table_name, schema=ms_schema)

        sql.query(f"select * from {ms_schema}.__temp_log_table_{sql.user}__ where table_name = '{test_feature_class_table_name}'")
        assert len(sql.data) == 0

        sql.drop_table(ms_schema, test_feature_class_table_name)

    def test_import_fc_new_name_schema_private(self):

        sql.drop_table(table=test_feature_class_table_name, schema = ms_schema)
        assert not sql.table_exists(test_feature_class_table_name, schema=ms_schema)

        sql.feature_class_to_table(path = fgdb, table=test_feature_class_table_name, feature_class=fc,
                                   schema = ms_schema, private=True, skip_failures='-skip_failures')
        assert sql.table_exists(table = test_feature_class_table_name, schema = ms_schema)

        sql.query(f"""
            EXEC sp_table_privileges @table_name = '{test_feature_class_table_name}';
            """)
        sql.drop_table(ms_schema, test_feature_class_table_name)

    def test_import_fc_extra_cmd(self):
        sql.drop_table(table=test_feature_class_table_name, schema=ms_schema)
        assert not sql.table_exists(test_feature_class_table_name, schema=ms_schema)

        sql.feature_class_to_table(path = fgdb, table = test_feature_class_table_name, feature_class = fc, schema=ms_schema,
                                    extra_cmd='-nlt MULTILINESTRING')
        assert sql.table_exists(test_feature_class_table_name, schema=ms_schema)

        sql.drop_table(ms_schema, test_feature_class_table_name)

    @classmethod
    def teardown_class(cls):
        sql.cleanup_new_tables()

class TestSHPDeleteIndexPG:
    @classmethod
    def setup_class(cls):
        # Setup; create sample file
        helpers.set_up_shapefile()

    def test_shp_delete_index_pg_basic(self):
        fp = FOLDER_PATH
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        db.drop_table(schema=pg_schema, table=test_read_shp_table_name)

        # Assert no indexes to start
        indexes_df = db.dfquery(DEL_INDICES_QUERY_PG.format(s=pg_schema, t=test_read_shp_table_name))
        assert len(indexes_df) == 0

        # Read shp to new, test table
        s.upload_geospatial(dbo=db, path=fp, input_file=shp_name, table=test_read_shp_table_name, schema=pg_schema)

        # Assert two indexes were made; one for PK
        indexes_df = db.dfquery(DEL_INDICES_QUERY_PG.format(s=pg_schema, t=test_read_shp_table_name))
        assert len(indexes_df) == 2
        assert len(indexes_df[indexes_df['index_name'].str.contains('pkey')]) == 1

        # Call del_indexes
        s.del_indexes(dbo = sql, schema = pg_schema, table = test_read_shp_table_name)
        db.query(f"alter table {pg_schema}.{test_read_shp_table_name} drop column geom;") # remove geometry column since it's not an index

        # Assert one indexes left; contains pkey
        indexes_df = db.dfquery(DEL_INDICES_QUERY_PG.format(s=pg_schema, t=test_read_shp_table_name))
        assert len(indexes_df[indexes_df['index_name'].str.contains('pkey')]) == 1
        assert len(indexes_df) == 1

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_read_shp_table_name)

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_shapefile()


class TestSHPDeleteIndexMS:
    @classmethod
    def setup_class(cls):
        # Setup; create sample file
        helpers.set_up_shapefile()

    def test_shp_delete_index_ms_basic(self):
        fp = FOLDER_PATH
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        sql.drop_table(schema=ms_schema, table=test_read_shp_table_name)

        # Assert no indexes to start
        indexes_df = sql.dfquery(DEL_INDICES_QUERY_MS.format(s=ms_schema, t=test_read_shp_table_name))
        assert len(indexes_df) == 0

        # Read shp to new, test table
        s.upload_geospatial(dbo=sql, path=fp, input_file=shp_name, table=test_read_shp_table_name, schema='dbo')

        # Assert one index was made; one for PK
        indexes_df = sql.dfquery(DEL_INDICES_QUERY_MS.format(s=ms_schema, t=test_read_shp_table_name))
        assert len(indexes_df) == 1
        assert len(indexes_df[indexes_df['index_name'].str.contains('PK')]) == 1

        # Call del_indexes
        s.del_indexes(dbo = sql, schema = ms_schema, table = test_read_shp_table_name)

        # Assert still one index left; contains PK
        indexes_df = sql.dfquery(DEL_INDICES_QUERY_MS.format(s=ms_schema, t=test_read_shp_table_name))
        assert len(indexes_df) == 1
        assert len(indexes_df[indexes_df['index_name'].str.contains('PK')]) == 1

        # Cleanup
        sql.drop_table(schema='dbo', table=test_read_shp_table_name)

    def test_shp_delete_index_ms_multiple(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        sql.drop_table(schema=ms_schema, table=test_read_shp_table_name)

        # Assert no indexes to start
        indexes_df = sql.dfquery(DEL_INDICES_QUERY_MS.format(s=ms_schema, t=test_read_shp_table_name))
        assert len(indexes_df) == 0

        # Read shp to new, test table
        s.upload_geospatial(dbo=sql, path=fp, input_file=shp_name, table=test_read_shp_table_name, schema=ms_schema)

        # Add one more
        sql.query(f"CREATE INDEX IX_{test_read_shp_table_name} ON {ms_schema}.{test_read_shp_table_name} (ogr_fid)")

        # Assert one index was made; one for PK
        indexes_df = sql.dfquery(DEL_INDICES_QUERY_MS.format(s=ms_schema, t=test_read_shp_table_name))
        assert len(indexes_df) == 2
        assert len(indexes_df[indexes_df['index_name'].str.contains('PK')]) == 1

        # Call del_indexes
        s.del_indexes(dbo = sql, schema = ms_schema, table = test_read_shp_table_name)

        # Assert still one index left; contains PK
        indexes_df = sql.dfquery(DEL_INDICES_QUERY_MS.format(s=ms_schema, t=test_read_shp_table_name))
        assert len(indexes_df) == 1
        assert len(indexes_df[indexes_df['index_name'].str.contains('PK')]) == 1

        # Cleanup
        sql.drop_table(schema=ms_schema, table=test_read_shp_table_name)

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_shapefile()