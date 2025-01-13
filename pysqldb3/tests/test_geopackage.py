import os
import configparser
import pandas as pd
import subprocess
import shlex

from .. import pysqldb3 as pysqldb
from ..geopackage import Geopackage
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


ms_schema = 'dbo'
pg_schema = 'working'

class TestReadgpkgPG:
    @classmethod
    def setup_class(cls):
        helpers.set_up_geopackage(db.user)
        helpers.set_up_test_table_pg(db)

    def test_read_gpkg_basic(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data/'
        gpkg_name = "testgpkg.gpkg"

        # Assert successful
        assert gpkg_name in os.listdir(fp)
        db.drop_table(schema=pg_schema, table=test_read_gpkg_table_name)

        # Read gpkg to new, test table
        s = Geopackage(path=fp, gpkg_name=gpkg_name, gpkg_tbl = test_layer1)
        s.read_gpkg(dbo=db, table=test_read_gpkg_table_name, schema=pg_schema, print_cmd=True)

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

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_read_gpkg_table_name)
        
    def test_read_gpkg_basic_multigpkgtbl(self):

        """
        Test for multiple tables within a geopackage
        """

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data/'
        gpkg_name = "testgpkg.gpkg"

        # Assert successful
        # Assert successful
        assert gpkg_name in os.listdir(fp)
        db.drop_table(schema=pg_schema, table=test_layer1)
        db.drop_table(schema=pg_schema, table=test_layer2)

        # no gpkg_tbl argument so that it bulk uploads
        s = Geopackage(path=fp, gpkg_name=gpkg_name)
        s.read_gpkg_bulk(dbo=db, schema=pg_schema, print_cmd=True)

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
        
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data/'
        gpkg_name = "testgpkg.gpkg"
        s = Geopackage(path=fp, gpkg_name=gpkg_name)
        s_list = s.list_gpkg_tables()

        assert s_list == [test_layer1, test_layer2]

    def test_read_gpkg_no_schema(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        gpkg_name = "testgpkg.gpkg"

        # Assert successful
        assert gpkg_name in os.listdir(fp)
        db.drop_table(schema=pg_schema, table=test_layer1)
        db.drop_table(schema=pg_schema, table=test_layer2)

        # Read gpkg to new, test table
        s = Geopackage(path=fp, gpkg_name=gpkg_name, gpkg_tbl = test_layer2)
        s.read_gpkg(dbo=db, table = test_read_gpkg_table_name, print_cmd=True)

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

    # def test_read_gpkg_precision(self):
    #     return

    # def test_read_gpkg_private(self):
    #     # TODO: pending permissions defaults convo
    #     return

    def test_read_temp(self):
        # TODO: pending temp functionality
        return

    def test_read_gpkg_encoding(self):
        # TODO: add test with fix to special characters
        return

    @classmethod
    @classmethod
    def teardown_class(cls):
        helpers.clean_up_geopackage()
        # helpers.clean_up_test_table_pg(db)


class TestReadgpkgMS:
    @classmethod
    def setup_class(cls):
        helpers.set_up_geopackage(db.user)

    def test_read_gpkg_basic(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'\\test_data'
        gpkg_name = "testgpkg.gpkg"

        # Assert successful
        assert gpkg_name in os.listdir(fp)

        # remove temp table from MS SQL Server if it already exists
        sql.query(f"drop table if exists {ms_schema}.{test_read_gpkg_table_name}")

        # Read gpkg to new, test table
        s = Geopackage(path=fp, gpkg_name=gpkg_name, gpkg_tbl = test_layer2)
        s.read_gpkg(dbo=sql, table=test_read_gpkg_table_name, schema=ms_schema, print_cmd=True)

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

        # Cleanup
        sql.query(f"drop table if exists {ms_schema}.{test_read_gpkg_table_name}")

    def test_read_gpkg_basic_multigpkgtbl(self):

        """
        Test for multiple tables within a geopackage
        """
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data/'
        gpkg_name = "testgpkg.gpkg"

        # Assert successful
        assert gpkg_name in os.listdir(fp)
        sql.query(f"drop table if exists {ms_schema}.{test_layer1}")
        sql.query(f"drop table if exists {ms_schema}.{test_layer2}")

        # no gpkg_tbl argument so that it bulk uploads
        s = Geopackage(path=fp, gpkg_name=gpkg_name)
        s.read_gpkg_bulk(dbo=sql, schema=ms_schema, print_cmd=True)

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
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        gpkg_name = "testgpkg.gpkg"

        # Assert successful
        assert gpkg_name in os.listdir(fp)

        # drop temp table if exists
        sql.query(f"drop table if exists {ms_schema}.{test_layer1}")
        sql.query(f"drop table if exists {ms_schema}.{test_layer2}")

        # Read gpkg to new, test table
        s = Geopackage(path=fp, gpkg_name=gpkg_name)
        s.read_gpkg_bulk(dbo=sql, schema=ms_schema, print_cmd=True)

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
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        gpkg_name = "testgpkg.gpkg"

        # Assert successful
        assert gpkg_name in os.listdir(fp)
        sql.drop_table(schema=sql.default_schema, table=test_read_gpkg_table_name)

        # Read gpkg to new, test table
        s = Geopackage(path=fp, gpkg_name=gpkg_name, gpkg_tbl = test_layer1)
        s.read_gpkg(dbo=sql, table=test_read_gpkg_table_name, print_cmd=True)

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
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'\\test_data'
        gpkg_name = "testgpkg.gpkg"

        # Assert successful
        assert gpkg_name in os.listdir(fp)

        # remove temp table from MS SQL Server if it already exists
        sql.query(f"drop table if exists {ms_schema}.{test_layer1}")

        # Read gpkg to new, test table
        s = Geopackage(path=fp, gpkg_name=gpkg_name, gpkg_tbl = test_layer1)
        s.read_gpkg(dbo=sql, schema=ms_schema, print_cmd=True)

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

    # def test_read_gpkg_precision(self):
    #     return

    # def test_read_gpkg_private(self):
    #     # TODO: pending permissions defaults convo
    #     return

    # def test_read_temp(self):
    #     # TODO: pending temp functionality
    #     return

    # def test_read_gpkg_encoding(self):
    #     # TODO: add test with fix to special characters
    #     return

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

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        gpkg_name = 'testgpkg.gpkg'

        # Write gpkg
        s = Geopackage(path=fp, gpkg_name=gpkg_name)
        s.write_gpkg(dbo=db, schema=pg_schema, table=test_write_gpkg_table_name, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(fp, gpkg_name))

        # Reupload as table
        db.gpkg_to_table(path=fp, schema=pg_schema, table = test_reuploaded_table_name, gpkg_name = gpkg_name, gpkg_tbl = test_write_gpkg_table_name, print_cmd=True)

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
        os.remove(os.path.join(fp, gpkg_name))

    def test_write_gpkg_overwrite(self):
        db.query(f"""
        drop table if exists {pg_schema}.{test_write_gpkg_table_name};

        create table {pg_schema}.{test_write_gpkg_table_name} as
        select *
        from {pg_schema}.{pg_table_name}
        order by id
        limit 100
        """)

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        gpkg_name = 'testgpkg.gpkg'

        # Write gpkg
        s = Geopackage(path=fp, gpkg_name=gpkg_name)
        s.write_gpkg(dbo=db, schema=pg_schema, table=test_write_gpkg_table_name, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(fp, gpkg_name))

        # overwrite the table as a version with 2 rows
        db.query(f"""
        drop table if exists {pg_schema}.{test_write_gpkg_table_name};

        create table {pg_schema}.{test_write_gpkg_table_name} as
        select *
        from {pg_schema}.{pg_table_name}
        order by id
        limit 2
        """)
        s = Geopackage(path=fp, gpkg_name=gpkg_name)
        s.write_gpkg(dbo=db, schema=pg_schema, table=test_write_gpkg_table_name, overwrite = True, print_cmd=True) # overwrite to 2 rows

        # Reupload as table
        db.gpkg_to_table(path=fp, schema=pg_schema, gpkg_name=gpkg_name, gpkg_tbl = test_write_gpkg_table_name,
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
        os.remove(os.path.join(fp, gpkg_name))

    def test_write_gpkg_add_table(self):

        db.query(f"""
        drop table if exists {pg_schema}.{test_write_gpkg_table_name};

        create table {pg_schema}.{test_write_gpkg_table_name} as
        select *
        from {pg_schema}.{pg_table_name}
        order by id
        limit 100
        """)

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        gpkg_name = 'testgpkg.gpkg'

        # Write gpkg
        s = Geopackage(path=fp, gpkg_name=gpkg_name)
        s.write_gpkg(dbo=db, schema=pg_schema, gpkg_tbl = test_reuploaded_table_name, table= test_write_gpkg_table_name, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(fp, gpkg_name))

        # add another (differently-named) table to the same gpkg
        db.query(f"""
        drop table if exists {pg_schema}.{test_write_gpkg_table_name}_2;

        create table {pg_schema}.{test_write_gpkg_table_name}_2 as
        select *
        from {pg_schema}.{pg_table_name}
        order by id
        limit 2
        """)
        s = Geopackage(path=fp, gpkg_name=gpkg_name)
        s.write_gpkg(dbo=db, schema=pg_schema, table = test_write_gpkg_table_name + '_2', gpkg_tbl = test_reuploaded_table_name + '_2', overwrite = False, print_cmd=True) # add another table

        assert os.path.isfile(os.path.join(fp, gpkg_name)) # assert that the table is still there

        # Reupload both tables from the same geopackage (since we are uploading under a different name, we can't use bulk upload function here)
        db.gpkg_to_table_bulk(path=fp, schema=pg_schema, gpkg_name=gpkg_name, print_cmd=True) 

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
        os.remove(os.path.join(fp, gpkg_name))

    def test_write_gpkg_table_pth(self):
        db.drop_table(pg_schema, test_write_gpkg_table_name)
        db.query(f"""
        create table {pg_schema}.{test_write_gpkg_table_name} as
        select *
        from {pg_schema}.{pg_table_name}
        order by id
        limit 100
        """)

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        gpkg_name = 'testgpkg.gpkg'

        # Write gpkg
        s = Geopackage(path=fp, gpkg_name=gpkg_name)
        s.write_gpkg(dbo=db, table=test_write_gpkg_table_name, schema=pg_schema, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(fp, gpkg_name))

        # Reupload as table
        db.gpkg_to_table(path=fp+'\\'+gpkg_name, gpkg_name = gpkg_name, schema=pg_schema, table=test_reuploaded_table_name, gpkg_tbl = test_write_gpkg_table_name, print_cmd=True)

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
        os.remove(os.path.join(fp, gpkg_name))

    def test_write_gpkg_table_pth_w_name(self):
        db.query(f"""
        drop table if exists {pg_schema}.{test_write_gpkg_table_name};

        create table {pg_schema}.{test_write_gpkg_table_name} as
        select *
        from {pg_schema}.{pg_table_name}
        order by id
        limit 100
        """)

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        gpkg_name = 'testgpkg.gpkg'

        # Write gpkg
        s = Geopackage(path=fp, gpkg_name=gpkg_name)
        s.write_gpkg(dbo=db, table=test_write_gpkg_table_name, schema=pg_schema, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(fp, gpkg_name))

        # Reupload as table
        db.gpkg_to_table(path=fp+'\\'+'err_'+gpkg_name, gpkg_name=gpkg_name ,schema=pg_schema,
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
        os.remove(os.path.join(fp, gpkg_name))

    def test_write_gpkg_query(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        gpkg_name = 'testgpkg.gpkg'

        # Write gpkg
        s = Geopackage(path=fp, gpkg_name=gpkg_name)
        s.write_gpkg(dbo=db, query=f"""select * from {pg_schema}.{pg_table_name} order by id limit 100""",
                    gpkg_tbl = test_write_gpkg_table_name, print_cmd=True)

        # Check table in folder
        assert os.path.isfile(os.path.join(fp, gpkg_name))

        # Reupload as table
        db.gpkg_to_table(path=fp, gpkg_name=gpkg_name, schema=pg_schema,
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
        os.remove(os.path.join(fp, gpkg_name))

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(db)
        helpers.clean_up_geopackage()


class TestWritegpkgMS:
    def test_write_gpkg_table(self):
        sql.drop_table(schema=ms_schema, table=test_write_gpkg_table_name)

        # Add test_table
        sql.query(f"""
        create table {ms_schema}.{test_write_gpkg_table_name} (test_col1 int, test_col2 int, geom geometry);
        insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'\\test_data'
        gpkg_name = 'test_write.gpkg'

        # Write gpkg
        s = Geopackage(path=fp, gpkg_name=gpkg_name)
        s.write_gpkg(dbo=sql, schema = ms_schema, table=test_write_gpkg_table_name, print_cmd=True)

        # # Assert successful
        assert os.path.isfile(os.path.join(fp, gpkg_name))

        # Reupload as table
        sql.gpkg_to_table(path=fp, gpkg_name=gpkg_name, schema = ms_schema, table=test_reuploaded_table_name,
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
        from {ms_schema}.{test_reuploaded_table_name} b
        join {ms_schema}.{test_reuploaded_table_name} a
            on b.test_col1=a.test_col1
        """)

        assert len(dist_df) == 1
        assert dist_df.iloc[0]['distance'] == 0

        # Clean up
        os.remove(os.path.join(fp, gpkg_name))
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

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'\\test_data'
        gpkg_name = 'test_write.gpkg'

        # Write gpkg. Gpkg table will be named the same as the query table
        s = Geopackage(path=fp, gpkg_name=gpkg_name)
        s.write_gpkg(dbo=sql, schema = ms_schema, table=test_write_gpkg_table_name, print_cmd=True)

        # # Assert successful
        assert os.path.isfile(os.path.join(fp, gpkg_name))

        ### ADD THE SECOND TABLE THAT IS SHORTER VERSION ###
        # Add test_table (slightly different values)
        sql.query(f"""
        drop table if exists {ms_schema}.{test_write_gpkg_table_name}_2;
        create table {ms_schema}.{test_write_gpkg_table_name}_2 (test_col3 int, test_col4 int, geom geometry);
        insert into {ms_schema}.{test_write_gpkg_table_name}_2 VALUES(5, 6, geometry::Point(985830.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_gpkg_table_name}_2 VALUES(7, 8, geometry::Point(985830.79200444, 203371.60461367, 2263));
        """)

        # Write gpkg. Gpkg table will be named the same as the previous table to overwrite it
        s = Geopackage(path=fp,
                       gpkg_name=gpkg_name,
                        gpkg_tbl = test_write_gpkg_table_name)
        s.write_gpkg(dbo=sql, schema = ms_schema, table=test_write_gpkg_table_name + '_2', overwrite = True, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(fp, gpkg_name))

        # Reupload as table
        sql.gpkg_to_table(path=fp, gpkg_name=gpkg_name, schema = ms_schema, 
                          gpkg_tbl = test_write_gpkg_table_name +'_2', table=test_reuploaded_table_name, print_cmd=True)

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
        os.remove(os.path.join(fp, gpkg_name))
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

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'\\test_data'
        gpkg_name = 'test_write.gpkg'

        # Write gpkg. Gpkg table will be named the same as the query table
        s = Geopackage(path=fp, gpkg_name=gpkg_name)
        s.write_gpkg(dbo=sql, schema = ms_schema, table= test_write_gpkg_table_name, gpkg_tbl = test_reuploaded_table_name, print_cmd=True)

        # # Assert successful
        assert os.path.isfile(os.path.join(fp, gpkg_name))

        ### ADD THE SECOND TABLE THAT IS SHORTER VERSION ###
        # Add test_table (slightly different values)
        sql.query(f"""
        drop table if exists {ms_schema}.{test_write_gpkg_table_name}_2;
        create table {ms_schema}.{test_write_gpkg_table_name}_2 (test_col3 int, test_col4 int, geom geometry);
        insert into {ms_schema}.{test_write_gpkg_table_name}_2 VALUES(5, 6, geometry::Point(985830.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_gpkg_table_name}_2 VALUES(7, 8, geometry::Point(985830.79200444, 203371.60461367, 2263));
        """)

        # Write gpkg.
        s = Geopackage(path=fp, gpkg_name=gpkg_name, )
        s.write_gpkg(dbo=sql, schema = ms_schema, gpkg_tbl=test_reuploaded_table_name + '_2', table = test_write_gpkg_table_name + '_2', print_cmd=True) # add second table

        # Assert successful
        assert os.path.isfile(os.path.join(fp, gpkg_name))

        # Reupload as tables using bulk upload function
        sql.gpkg_to_table_bulk(path=fp, gpkg_name=gpkg_name, schema = ms_schema, print_cmd=True)

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
        os.remove(os.path.join(fp, gpkg_name))
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name};")
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}_2;")
    
    def test_write_gpkg_table_pth(self):
        # drop temp table if exists 
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}")

        # Add test_table
        sql.query(f"""
        create table {ms_schema}.{test_write_gpkg_table_name} (test_col1 int, test_col2 int, geom geometry);
        insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        gpkg_name = 'test_write.gpkg'

        # Write gpkg
        s = Geopackage(path=fp, gpkg_name=gpkg_name)
        s.write_gpkg(dbo=sql, table= test_write_gpkg_table_name, gpkg_tbl = test_write_gpkg_table_name, schema=ms_schema, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(fp, gpkg_name))

        # Reupload as table
        sql.gpkg_to_table(path=fp+'\\'+gpkg_name, gpkg_name = gpkg_name, gpkg_tbl = test_write_gpkg_table_name,
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
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}")
        sql.query(f"drop table if exists {ms_schema}.{test_reuploaded_table_name}")

        os.remove(os.path.join(fp, gpkg_name))

    def test_write_gpkg_query(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        gpkg_name = 'testgpkg.gpkg'
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}")

        # Add test_table
        sql.query(f"""
        create table {ms_schema}.{test_write_gpkg_table_name} (test_col1 int, test_col2 int, geom geometry);
        insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
        insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        # Write gpkg
        s = Geopackage(path=fp, gpkg_name=gpkg_name, gpkg_tbl = test_write_gpkg_table_name)
        s.write_gpkg(dbo=sql, query=f"""select top 10 * from {ms_schema}.{test_write_gpkg_table_name} order by test_col1""",
                        gpkg_tbl = test_write_gpkg_table_name, print_cmd=True)

        # Check table in folder
        assert os.path.isfile(os.path.join(fp, gpkg_name))

        # Reupload as table
        sql.gpkg_to_table(path=fp, gpkg_name=gpkg_name, gpkg_tbl = test_write_gpkg_table_name,
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

        os.remove(os.path.join(fp, gpkg_name))
        
    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(db)

class TestGpkgShpConversion:
    @classmethod
        
    def test_convert_gpkg_to_shp_file(self):

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        gpkg_name = 'gpkg_to_shp.gpkg'

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
        s = Geopackage(path = fp, gpkg_name=gpkg_name)
        s.write_gpkg(dbo=sql, table= test_write_gpkg_table_name, schema=ms_schema, print_cmd=True)

        # # Check table in folder
        assert os.path.isfile(os.path.join(fp, gpkg_name))
        
        # run function to convert geopackage to shape file
        s.gpkg_to_shp(export_path = fp, gpkg_tbl = test_write_gpkg_table_name, print_cmd = True)

        # assert that a shape output file exists. It will not match the name of the geopackage because the tables inside the package canbe different
        assert os.path.isfile(os.path.join(fp, test_write_gpkg_table_name + '.shp'))

        # check that the shapefile and gpkg have the same output
        cmd_gpkg = f'ogrinfo "{fp}/{gpkg_name}" -sql "SELECT test_col1 FROM {test_write_gpkg_table_name} LIMIT 1" -q'
        cmd_shp = f'ogrinfo "{fp}/{test_write_gpkg_table_name}.shp" -sql "SELECT test_col1 FROM {test_write_gpkg_table_name} LIMIT 1" -q'

        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT)
        ogr_response_shp = subprocess.check_output(shlex.split(cmd_shp), stderr=subprocess.STDOUT)
        
        assert 'test_col1 (Integer) = 1' in str(ogr_response_gpkg) and 'test_col1 (Integer) = 1' in str(ogr_response_shp), "cannot find 'test_col1 (Integer) = 1' statement in both the shapefile and gpkg queries"

        # remove geopackage and shape files
        os.remove(os.path.join(fp, gpkg_name))
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fp, test_write_gpkg_table_name + '.' + ext))

        sql.drop_table(schema = ms_schema, table = test_write_gpkg_table_name)
    
    def test_convert_gpkg_to_shp_custom_path(self):

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        gpkg_name = 'gpkg_to_shp.gpkg'

        # no shape file name needs to be specified because the table(s) within the gpkg are not necessarily named the same thing

        # write a query
        sql.query(f"""drop table if exists {ms_schema}.{test_write_gpkg_table_name};
                create table {ms_schema}.{test_write_gpkg_table_name} (test_col1 int, test_col2 int, geom geometry);
                insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
                insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
                """)

        # write geopackage file
        s = Geopackage(path = fp, gpkg_name=gpkg_name)
        s.write_gpkg(dbo=sql, table= test_write_gpkg_table_name, schema=ms_schema, print_cmd=True)

        # Check table in folder
        assert os.path.isfile(os.path.join(fp, gpkg_name))
        # run function to convert geopackage to shape file
        s.gpkg_to_shp_bulk(export_path = fp)
 
        # assert that a shape output file exists. It will not match the name of the geopackage because the tables inside the package canbe different
        assert os.path.isfile(os.path.join(fp , test_write_gpkg_table_name + '.shp'))

        # check that the shapefile and gpkg have the same output
        cmd_gpkg = f'ogrinfo "{fp}/{gpkg_name}" -sql "SELECT test_col1 FROM {test_write_gpkg_table_name} LIMIT 1" -q'
        cmd_shp = f'ogrinfo "{fp}/{test_write_gpkg_table_name}.shp" -sql "SELECT test_col1 FROM {test_write_gpkg_table_name} LIMIT 1" -q'

        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT)
        ogr_response_shp = subprocess.check_output(shlex.split(cmd_shp), stderr=subprocess.STDOUT)
        
        assert 'test_col1 (Integer) = 1' in str(ogr_response_gpkg) and 'test_col1 (Integer) = 1' in str(ogr_response_shp), "cannot find 'test_col1 (Integer) = 1' statement in both the shapefile and gpkg queries"

        # remove geopackage and shape files
        os.remove(os.path.join(fp, gpkg_name))
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fp, test_write_gpkg_table_name + '.' + ext))

        sql.drop_table(schema = ms_schema, table = test_write_gpkg_table_name)

    def test_convert_gpkg_to_shp_file_bulk(self):

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__))) + '/test_data'
        gpkg_name = 'gpkg_to_shp.gpkg'

        # no shape file name needs to be specified because the table(s) within the gpkg are not necessarily named the same thing

        # write a query
        sql.query(f"""drop table if exists {ms_schema}.{test_write_gpkg_table_name};
                create table {ms_schema}.{test_write_gpkg_table_name} (test_col1 int, test_col2 int, geom geometry);
                insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
                insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
                """)

        # write geopackage file
        s = Geopackage(path = fp, gpkg_name=gpkg_name)
        s.write_gpkg(dbo=sql, table= test_write_gpkg_table_name, schema=ms_schema, print_cmd=True)

        # add an extra table to test multiple
        sql.query(f"""drop table if exists {ms_schema}.{test_write_gpkg_table_name}_2;
                create table {ms_schema}.{test_write_gpkg_table_name}_2 (test_col5 int, test_col6 int, geom geometry);
                insert into {ms_schema}.{test_write_gpkg_table_name}_2 VALUES(5, 6, geometry::Point(985830.79200444, 203371.60461367, 2263));
                insert into {ms_schema}.{test_write_gpkg_table_name}_2 VALUES(7, 8, geometry::Point(985830.79200444, 203371.60461367, 2263));
                """)
        
        assert sql.table_exists(schema = ms_schema, table = test_write_gpkg_table_name)
        assert sql.table_exists(schema = ms_schema, table = f"{test_write_gpkg_table_name}_2")

        s = Geopackage(path = fp, gpkg_name=gpkg_name)
        s.write_gpkg(dbo=sql, schema = ms_schema, table = test_write_gpkg_table_name + '_2', print_cmd=True) # this will append 

        # Check table in folder
        assert os.path.isfile(os.path.join(fp, gpkg_name))
        
        # run function to convert geopackage to shape file
        s = Geopackage(path = fp, gpkg_name=gpkg_name)
        s.gpkg_to_shp_bulk(print_cmd = True)

        # assert that a shape output file exists. It will not match the name of the geopackage because the tables inside the package canbe different
        assert os.path.isfile(os.path.join(fp, test_write_gpkg_table_name + '.shp'))
        assert os.path.isfile(os.path.join(fp, test_write_gpkg_table_name + '_2.shp'))

        # check that the shapefile and gpkg have the same output
        cmd_gpkg = f'ogrinfo "{fp}/{gpkg_name}" -sql "SELECT test_col1 FROM {test_write_gpkg_table_name} LIMIT 1" -q'
        cmd_shp = f'ogrinfo "{fp}/{test_write_gpkg_table_name}.shp" -sql "SELECT test_col1 FROM {test_write_gpkg_table_name} LIMIT 1" -q'
        cmd_gpkg2 = f'ogrinfo "{fp}/{gpkg_name}" -sql "SELECT test_col5 FROM {test_write_gpkg_table_name}_2 LIMIT 1" -q'
        cmd_shp2 = f'ogrinfo "{fp}/{test_write_gpkg_table_name}_2.shp" -sql "SELECT test_col5 FROM {test_write_gpkg_table_name}_2 LIMIT 1" -q'

        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT)
        ogr_response_shp = subprocess.check_output(shlex.split(cmd_shp), stderr=subprocess.STDOUT)
        ogr_response_gpkg2 = subprocess.check_output(shlex.split(cmd_gpkg2), stderr=subprocess.STDOUT)
        ogr_response_shp2 = subprocess.check_output(shlex.split(cmd_shp2), stderr=subprocess.STDOUT)
        
        assert 'test_col1 (Integer) = 1' in str(ogr_response_gpkg) and 'test_col1 (Integer) = 1' in str(ogr_response_shp), \
            "cannot find 'test_col1 (Integer) = 1' statement in both the shapefile and gpkg queries"
        assert 'test_col5 (Integer) = 5' in str(ogr_response_gpkg2) and 'test_col5 (Integer) = 5' in str(ogr_response_shp2), \
            "cannot find 'test_col5 (Integer) = 5' statement in both the shapefile and gpkg queries"

        # remove geopackage and shape files
        os.remove(os.path.join(fp, gpkg_name))
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}")
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}_2")
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fp, test_write_gpkg_table_name + '.' + ext))
            os.remove(os.path.join(fp, test_write_gpkg_table_name + '_2.' + ext))

    def test_convert_shp_to_gpkg_file(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        gpkg_name = 'gpkg_to_shp.gpkg'
        shp_name = test_write_gpkg_table_name

        # create shapefile (we don't import Shapefile.py so we create a GPKG, convert to SHP, and then delete the GPKG)
        sql.query(f"""drop table if exists {ms_schema}.{test_write_gpkg_table_name};
                create table {ms_schema}.{test_write_gpkg_table_name} (test_col1 int, test_col2 int, geom geometry);
                insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
                insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
                """)

        # write geopackage file
        s = Geopackage(path = fp, gpkg_name=gpkg_name)
        s.write_gpkg(dbo=sql, table= test_write_gpkg_table_name, schema=ms_schema, print_cmd=True)
        s.gpkg_to_shp(export_path = fp, gpkg_tbl = test_write_gpkg_table_name, print_cmd = True)

        # drop SQL table and GPKG
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}")
        os.remove(os.path.join(fp, gpkg_name))

        assert os.path.exists(os.path.join(fp, gpkg_name)) == False # confirm that the gpkg is removed
        assert os.path.isfile(os.path.join(fp, shp_name + '.shp')) # confirm that the shp file exists

        # run function to convert Shapefile to GPKG
        s = Geopackage(path=fp, gpkg_name=gpkg_name)
        s.shp_to_gpkg(gpkg_tbl = test_write_gpkg_table_name, shp_name = shp_name + '.shp', print_cmd = True)

        # assert that the output file exists and that it matches the geopackage
        assert os.path.isfile(os.path.join(fp, gpkg_name))

        # assert that the data is the same
        cmd_gpkg = f'ogrinfo "{fp}/{gpkg_name}" -sql "SELECT test_col1 FROM {test_write_gpkg_table_name} LIMIT 1" -q'
        cmd_shp = f'ogrinfo "{fp}/{shp_name}.shp" -sql "SELECT test_col1 FROM {test_write_gpkg_table_name} LIMIT 1" -q'

        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT)
        ogr_response_shp = subprocess.check_output(shlex.split(cmd_shp), stderr=subprocess.STDOUT)
        
        assert 'test_col1 (Integer) = 1' in str(ogr_response_gpkg) and 'test_col1 (Integer) = 1' in str(ogr_response_shp), "cannot find 'test_col1 (Integer) = 1' statement in both the shapefile and gpkg queries"

        # don't remove gpkg output to reuse for subsequent test
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}")

    def test_convert_shp_to_existing_gpkg_file(self):

        # copy the same shp file as an additional table in the gpkg
        # this test confirms that the update function for the gpkg is working correctly

        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        gpkg_name = 'gpkg_to_shp.gpkg'
        shp_name = test_write_gpkg_table_name

        # Check table in folder
        assert os.path.isfile(os.path.join(fp, shp_name) + '.shp')

        # run function to convert Shapefile to GPKG
        s = Geopackage(path=fp, gpkg_name=gpkg_name)
        s.shp_to_gpkg(shp_name = shp_name + '.shp', gpkg_tbl = f'{test_write_gpkg_table_name}_2')

        # assert that the output file exists and that it matches the geopackage
        assert os.path.isfile(os.path.join(fp, gpkg_name))

        # assert that the data is the same
        cmd_gpkg = f'ogrinfo "{fp}/{gpkg_name}" -sql "SELECT test_col1 FROM {test_write_gpkg_table_name}_2 LIMIT 1" -q'
        cmd_shp = f'ogrinfo "{fp}/{shp_name}.shp" -sql "SELECT test_col1 FROM {test_write_gpkg_table_name} LIMIT 1" -q'

        ogr_response_gpkg_2 = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT)
        ogr_response_shp_2 = subprocess.check_output(shlex.split(cmd_shp), stderr=subprocess.STDOUT)
        
        assert 'test_col1 (Integer) = 1' in str(ogr_response_gpkg_2) and 'test_col1 (Integer) = 1' in str(ogr_response_shp_2), "cannot find 'test_col1 (Integer) = 1' statement in both the shapefile and gpkg queries"

        # remove shape file
        for ext in ('.dbf', '.prj', '.shx', '.shp'):
            try:
                os.remove(os.path.join(fp, shp_name + ext))
            except Exception as e:
                print(e)

        # remove gpkg output
        os.remove(os.path.join(fp, gpkg_name))
        
    @classmethod
    def teardown_class(cls):
        helpers.clean_up_geopackage()