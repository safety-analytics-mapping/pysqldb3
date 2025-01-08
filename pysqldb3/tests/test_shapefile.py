import os
import configparser
import pandas as pd

from .. import pysqldb3 as pysqldb
from ..shapefile import Shapefile
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
                        allow_temp_tables=True
                        )

pg_table_name = f'pg_test_table_{db.user}'
test_read_shp_table_name = f'test_read_shp_table_{db.user}'
test_write_shp_table_name = f'test_write_shp_table_{db.user}'
test_reuploaded_table_name = f'test_write_reuploaded_{db.user}'

ms_schema = 'dbo'
pg_schema = 'working'


class TestReadShpPG:
    @classmethod
    def setup_class(cls):
        helpers.set_up_shapefile()
        helpers.set_up_test_table_pg(db)

    def test_read_shp_basic(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        db.drop_table(schema=pg_schema, table=test_read_shp_table_name)

        # Read shp to new, test table
        s = Shapefile(dbo=db, path=fp, shp_name=shp_name, table=test_read_shp_table_name, schema=pg_schema)
        s.read_shp(print_cmd=True)

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
        # fp = r'C:/Users/SHostetter/Desktop/GIS/nyad_21c.zip\yad_21c'
        # fp = 'C:/Users/SHostetter/Desktop/GIS/GIS.zip'
        # shp_name = "Hisa_yellow_10_14.shp"

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__))) + '/test_data/shp_test.zip'
        shp_name = "test.shp"

        # Make sure table doesn't alredy exist
        db.drop_table(pg_schema, test_read_shp_table_name)
        assert not db.table_exists(schema=pg_schema, table=test_read_shp_table_name)

        # Assert successful
        db.drop_table(schema=pg_schema, table=test_read_shp_table_name)

        # Read shp to new, test table
        s = Shapefile(dbo=db, path=fp, shp_name=shp_name, table=test_read_shp_table_name, schema=pg_schema)
        s.read_shp(print_cmd=True, zip=True)

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


    """
    NEED TO CHANGE TEST FILE TO CHANGE TABLE NAME
    """
    def test_read_shp_no_table(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        db.drop_table(schema=pg_schema, table="test")

        # Read shp to new, test table
        s = Shapefile(dbo=db, path=fp, shp_name=shp_name, schema=pg_schema)
        s.read_shp(print_cmd=True)

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
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        db.drop_table(schema=db.default_schema, table=test_read_shp_table_name)

        # Read shp to new, test table
        s = Shapefile(dbo=db, path=fp, shp_name=shp_name, table=test_read_shp_table_name)
        s.read_shp(print_cmd=True)

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

    def test_read_shp_precision(self):
        return

    def test_read_shp_private(self):
        # TODO: pending permissions defaults convo
        return

    def test_read_temp(self):
        # TODO: pending temp functionality
        return

    def test_read_shp_encoding(self):
        # TODO: add test with fix to special characters
        return

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_shapefile()
        helpers.clean_up_test_table_pg(db)


class TestReadShpMS:
    @classmethod
    def setup_class(cls):
        helpers.set_up_shapefile()

    def test_read_shp_basic(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        sql.drop_table(schema=ms_schema, table=test_read_shp_table_name)

        # Read shp to new, test table
        s = Shapefile(dbo=sql, path=fp, shp_name=shp_name, table=test_read_shp_table_name, schema=ms_schema)
        s.read_shp(print_cmd=True)

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

        # Cleanup
        sql.drop_table(schema=ms_schema, table=test_read_shp_table_name)

    def test_read_shp_zip(self):
        # fp = r'C:/Users/SHostetter/Desktop/GIS/nyad_21c.zip\yad_21c'
        # fp = 'C:/Users/SHostetter/Desktop/GIS/GIS.zip'
        # shp_name = "Hisa_yellow_10_14.shp"

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__))) + '/test_data/shp_test.zip'
        shp_name = "test.shp"

        # Make sure table doesn't alredy exist
        sql.drop_table(ms_schema, test_read_shp_table_name)
        assert not db.table_exists(schema=ms_schema, table=test_read_shp_table_name)

        # Assert successful
        sql.drop_table(schema=ms_schema, table=test_read_shp_table_name)

        # Read shp to new, test table
        s = Shapefile(dbo=sql, path=fp, shp_name=shp_name, table=test_read_shp_table_name, schema=ms_schema)
        s.read_shp(print_cmd=True, zip=True)

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

    """
    NEED TO CHANGE TEST FILE TO CHANGE TABLE NAME
    """
    def test_read_shp_no_table(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        sql.drop_table(schema=ms_schema, table='test')

        # Read shp to new, test table
        s = Shapefile(dbo=sql, path=fp, shp_name=shp_name, schema=ms_schema)
        s.read_shp(print_cmd=True)

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
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        sql.drop_table(schema=sql.default_schema, table=test_read_shp_table_name)

        # Read shp to new, test table
        s = Shapefile(dbo=sql, path=fp, shp_name=shp_name, table=test_read_shp_table_name)
        s.read_shp(print_cmd=True)

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

    def test_read_shp_precision(self):
        return

    def test_read_shp_private(self):
        # TODO: pending permissions defaults convo
        return

    def test_read_temp(self):
        # TODO: pending temp functionality
        return

    def test_read_shp_encoding(self):
        # TODO: add test with fix to special characters
        return

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_shapefile()


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

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        shp_name = 'test_write.shp'

        # Write shp
        s = Shapefile(dbo=db, path=fp, shp_name=shp_name, table=test_write_shp_table_name, schema=pg_schema)
        s.write_shp(print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(fp, shp_name))

        # Reupload as table
        db.shp_to_table(path=fp, shp_name=shp_name, schema=pg_schema, table=test_reuploaded_table_name, print_cmd=True)

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

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        shp_name = 'test_write.shp'

        # Write shp
        s = Shapefile(dbo=db, path=fp, shp_name=shp_name, table=test_write_shp_table_name, schema=pg_schema)
        s.write_shp(print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(fp, shp_name))

        # Reupload as table
        db.shp_to_table(path=fp+'\\'+shp_name, schema=pg_schema, table=test_reuploaded_table_name, print_cmd=True)

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

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        shp_name = 'test_write.shp'

        # Write shp
        s = Shapefile(dbo=db, path=fp, shp_name=shp_name, table=test_write_shp_table_name, schema=pg_schema)
        s.write_shp(print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(fp, shp_name))

        # Reupload as table
        db.shp_to_table(path=fp+'\\'+'err_'+shp_name, shp_name=shp_name ,schema=pg_schema,
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
            os.remove(os.path.join(fp, shp_name.replace('shp', ext)))

    def test_write_shp_query(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        shp_name = 'test_write.shp'

        # Write shp
        s = Shapefile(dbo=db, path=fp, shp_name=shp_name,
                      query=f"""select * from {pg_schema}.{pg_table_name} order by id limit 100""")
        s.write_shp(print_cmd=True)

        # Check table in folder
        assert os.path.isfile(os.path.join(fp, shp_name))

        # Reupload as table
        db.shp_to_table(path=fp, shp_name=shp_name, schema=pg_schema, table=test_reuploaded_table_name, print_cmd=True)

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
            os.remove(os.path.join(fp, shp_name.replace('shp', ext)))

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(db)


class TestWriteShpMS:
    def test_write_shp_table(self):
        sql.drop_table(schema=ms_schema, table=test_write_shp_table_name)

        # Add test_table
        sql.query(f"""
        create table {ms_schema}.{test_write_shp_table_name} (test_col1 int, test_col2 int, geom geometry);
        insert into {ms_schema}.{test_write_shp_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_shp_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = 'test_write.shp'

        # Write shp
        s = Shapefile(dbo=sql, path=fp, shp_name=shp_name, table=test_write_shp_table_name, schema=ms_schema)
        s.write_shp(print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(fp, shp_name))

        # Reupload as table
        sql.shp_to_table(path=fp, shp_name=shp_name, schema=ms_schema, table=test_reuploaded_table_name, print_cmd=True)

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
        from {ms_schema}.{test_reuploaded_table_name} b
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

    def test_write_shp_table_pth(self):
        sql.drop_table(schema=ms_schema, table=test_write_shp_table_name)

        # Add test_table
        sql.query(f"""
        create table {ms_schema}.{test_write_shp_table_name} (test_col1 int, test_col2 int, geom geometry);
        insert into {ms_schema}.{test_write_shp_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_shp_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        shp_name = 'test_write.shp'

        # Write shp
        s = Shapefile(dbo=sql, path=fp, shp_name=shp_name, table=test_write_shp_table_name,
                      schema=ms_schema)
        s.write_shp(print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(fp, shp_name))

        # Reupload as table
        sql.shp_to_table(path=fp+'\\'+shp_name, schema=ms_schema, table=test_reuploaded_table_name, print_cmd=True)

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
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = 'test_write.shp'
        sql.drop_table(schema=ms_schema, table=test_write_shp_table_name)

        # Add test_table
        sql.query(f"""
        create table {ms_schema}.{test_write_shp_table_name} (test_col1 int, test_col2 int, geom geometry);
        insert into {ms_schema}.{test_write_shp_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_shp_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        # Write shp
        s = Shapefile(dbo=sql, path=fp, shp_name=shp_name,
                      query=f"""select top 10 * from {ms_schema}.{test_write_shp_table_name} order by test_col1""")
        s.write_shp(print_cmd=True)

        # Check table in folder
        assert os.path.isfile(os.path.join(fp, shp_name))

        # Reupload as table
        sql.shp_to_table(path=fp, shp_name=shp_name, schema=ms_schema, table=test_reuploaded_table_name, print_cmd=True)

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
