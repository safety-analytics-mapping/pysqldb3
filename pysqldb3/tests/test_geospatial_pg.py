import os
import configparser
import pandas as pd
import subprocess
import shlex

from .. import pysqldb3 as pysqldb
from .. import geospatial as s
from . import helpers


# todo - issues #######################################################################
# MS tests are failing becuase the odbc 17 driver seems unable to read geometry fields
# #####################################################################################

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                       server=config.get('PG_DB', 'SERVER'),
                       database=config.get('PG_DB', 'DB_NAME'),
                       user=config.get('PG_DB', 'DB_USER'),
                       password=config.get('PG_DB', 'DB_PASSWORD'))

pg_table_name = f'pg_test_table_{db.user}'
test_read_gpkg_table_name = f'test_read_gpkg_table_{db.user}'
test_write_gpkg_table_name = f'test_write_gpkg_table_{db.user}'
test_reuploaded_table_name = f'test_write_reuploaded_{db.user}'
test_layer1 = f'test_layer1_{db.user}'
test_layer2 = f'test_layer2_{db.user}'
test_read_shp_table_name = f'test_read_shp_table_{db.user}'
test_write_shp_table_name = f'test_write_shp_table_{db.user}'
test_reuploaded_table_name = f'test_write_reuploaded_{db.user}'

FOLDER_PATH = helpers.DIR

pg_schema = 'working'


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
        s.input_geospatial_file(dbo=db, path=fp, schema=pg_schema, input_file=shp_name, table=test_read_shp_table_name, print_cmd=True)

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

        fp = FOLDER_PATH + '\\shp_test.zip'
        fp = fp.replace('/', '\\')
        shp_name = "test.shp"

        # Make sure table doesn't alredy exist
        db.drop_table(pg_schema, test_read_shp_table_name)
        assert not db.table_exists(schema=pg_schema, table=test_read_shp_table_name)

        # Assert successful
        db.drop_table(schema=pg_schema, table=test_read_shp_table_name)

        # Read shp to new, test table
        s.input_geospatial_file(dbo=db, path=fp, schema=pg_schema, input_file=shp_name, table=test_read_shp_table_name, print_cmd=True, zip=True)

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
        fp = FOLDER_PATH + '\\test.7z'
        fp = fp.replace('/', '\\')
        shp_name = "test.shp"

        # Make sure table doesn't already exist
        db.drop_table(pg_schema, test_read_shp_table_name)
        assert not db.table_exists(schema=pg_schema, table=test_read_shp_table_name)

        # Read the .7z archive directly into PostGIS
        s.input_geospatial_file(
            dbo=db,
            path=fp,
            schema=pg_schema,
            input_file=shp_name,
            table=test_read_shp_table_name,
            print_cmd=True,
            zip=False
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
        s.input_geospatial_file(dbo=db, path=fp, schema=pg_schema, input_file=shp_name, print_cmd=True)

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
        s.input_geospatial_file(dbo=db, path=fp, input_file=shp_name, table=test_read_shp_table_name, print_cmd=True)

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

