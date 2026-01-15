# -*- coding: utf-8 -*-
import os
import configparser
import subprocess
import shlex

from .. import pysqldb3 as pysqldb
from .. import geospatial as s
from . import helpers

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                       server=config.get('PG_DB', 'SERVER'),
                       database=config.get('PG_DB', 'DB_NAME'),
                       user=config.get('PG_DB', 'DB_USER'),
                       password=config.get('PG_DB', 'DB_PASSWORD'),
                       allow_temp_tables=True
                       )

sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                        server=config.get('SQL_DB', 'SERVER'),
                        database=config.get('SQL_DB', 'DB_NAME'),
                        user=config.get('SQL_DB', 'DB_USER'),
                        password=config.get('SQL_DB', 'DB_PASSWORD'),
                        allow_temp_tables=True)

test_table_shp = f'__testing_query_to_geospatial_{db.user}__'
test_write_shp_table_name = f'test_write_shp_table_{db.user}'
test_write_gpkg_table_name = f'test_write_gpkg_table_{db.user}'
pg_table_name = f'pg_test_table_{db.user}'
test_table = f'__testing_query_to_geospatial_{db.user}__'
test_table_shp = f'__testing_query_to_geospatial_{db.user}__'

pg_schema = 'working'
ms_schema = 'dbo'

FOLDER_PATH = helpers.DIR


class TestWriteShp:

    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_pg(db)

    def test_write_shp_longdate_table_pg(self):

        shp_name = 'test_write.shp'

        db.query(f"""
                drop table if exists {pg_schema}.{test_write_shp_table_name};
                create table {pg_schema}.{test_write_shp_table_name} as
                select  cast('1/1/2000 11:50:00' as timestamp) as long_dt_column123,
                        cast('8/4/2004 7:20:00' as timestamp) as long_dt_column1234,
                        geom
                from {pg_schema}.{pg_table_name};
                """)
        
        s.write_geospatial(path=os.path.join(FOLDER_PATH, shp_name), dbo=db, schema=pg_schema, table=test_write_shp_table_name, overwrite = True)

        # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, shp_name))

        # check that Date column is set as a Date type
        cmd_shp = f'ogrinfo "{FOLDER_PATH}/{shp_name}" -sql "select * from test_write limit 1"'
        ogr_response_shp = subprocess.check_output(shlex.split(cmd_shp), stderr=subprocess.STDOUT)   
        
        assert f'long_dt_dt (Date) = 2000/01/01' in str(ogr_response_shp), f"long_dt_dt column is not returning the correct Date & Value"
        assert f'long_d1_dt (Date) = 2004/08/04' in str(ogr_response_shp), f"long_d1_dt column is not returning the correct Date & Value"
        assert f'long_dt_tm (String) = 11:50:00' in str(ogr_response_shp), f"long_dt_tm column is not returning the correct Time & Value"
        assert f'long_d1_tm (String) = 07:20:00' in str(ogr_response_shp), f"long_d1_tm column is not returning the correct Time & Value"

    def test_write_shp_longdate_table_ms_v1(self):

        ## THIS TEST IS PASSES IF DATE AND TIME COLUMNS ARE CREATED FOR SQL SERVER SHP ##

        sql.drop_table(schema=ms_schema, table=test_write_shp_table_name)

        # Add test_table
        sql.query(f"""
        create table {ms_schema}.{test_write_shp_table_name} (test_col1 int, long_dt_column123 datetime, long_dt_column1234 datetime, geom geometry);
        insert into {ms_schema}.{test_write_shp_table_name} VALUES(1, '1/1/2000 11:50:00', '8/4/2004 7:20:00',
                                                                geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        fp = FOLDER_PATH
        shp_name = 'test_write.shp'
        
        # Write shp
        s.write_geospatial(dbo=sql, path= os.path.join(fp, shp_name), table=test_write_shp_table_name, schema=ms_schema, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(fp, shp_name))

        # check that Date column is set as a Date type
        cmd_shp = f'ogrinfo "{FOLDER_PATH}/{shp_name}" -sql "select * from test_write limit 1"'
        ogr_response_shp = subprocess.check_output(shlex.split(cmd_shp), stderr=subprocess.STDOUT)   

        assert f'long_dt_dt (Date) = 2000/01/01' in str(ogr_response_shp), f"long_dt_dt column is not returning the correct Date & Value"
        assert f'long_d1_dt (Date) = 2004/08/04' in str(ogr_response_shp), f"long_d1_dt column is not returning the correct Date & Value"
        assert f'long_dt_tm (String) = 11:50:00' in str(ogr_response_shp), f"long_dt_tm column is not returning the correct Time & Value"
        assert f'long_d1_tm (String) = 07:20:00' in str(ogr_response_shp), f"long_d1_tm column is not returning the correct Time & Value"

        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(FOLDER_PATH, shp_name.replace('shp', ext)))
            except:
                pass
        
    def test_write_shp_longdate_table_ms_v2(self):

        ## THIS TEST IS PASSES IF DATETIME COLUMNS ARE LEFT AS IS FOR SQL SERVER SHP##

        sql.drop_table(schema=ms_schema, table=test_write_shp_table_name)

        # Add test_table
        sql.query(f"""
        create table {ms_schema}.{test_write_shp_table_name} (test_col1 int, long_dt_column123 datetime, long_dt_column1234 datetime, geom geometry);
        insert into {ms_schema}.{test_write_shp_table_name} VALUES(1, '1/1/2000 11:50:00', '8/4/2004 7:20:00',
                                                                geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        fp = FOLDER_PATH
        shp_name = 'test_write.shp'

        # Write shp
        s.write_geospatial(dbo=sql, path= os.path.join(fp, shp_name), table=test_write_shp_table_name, schema=ms_schema, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(fp, shp_name))

        # check that Date column is set as a Date type
        cmd_shp = f'ogrinfo "{FOLDER_PATH}/{shp_name}" -sql "select * from test_write limit 1"'
        ogr_response_shp = subprocess.check_output(shlex.split(cmd_shp), stderr=subprocess.STDOUT)   

        assert f'long_dt_co (Date) = 2000/01/01' in str(ogr_response_shp), f"long_dt_co column is not returning the correct Date & Value"

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_shapefile()
        helpers.clean_up_test_table_sql(sql)
        helpers.clean_up_test_table_pg(db)

class TestWriteGpkg:

    def test_write_gpkg_longdate_table_pg(self):
        db.query(f"""
                drop table if exists {pg_schema}.{test_write_gpkg_table_name};
                create table {pg_schema}.{test_write_gpkg_table_name} as
                select  cast('10/14/2010 19:12:00' as timestamp) dtformatted2010,
                        cast('7/11/1988 03:05:00' as timestamp) dtformatted201054
                """)
        gpkg_name = 'testgpkg.gpkg'
        # Write gpkg
        s.write_geospatial(path=os.path.join(FOLDER_PATH, gpkg_name), dbo=db, schema=pg_schema, table=test_write_gpkg_table_name, print_cmd=True)
        # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))
        # Assert that date columns are in the proper format
        cmd_shp = f'ogrinfo "{FOLDER_PATH}/{gpkg_name}" -sql "select * from {test_write_gpkg_table_name}"'
        ogr_response_shp = subprocess.check_output(shlex.split(cmd_shp), stderr=subprocess.STDOUT)   

        assert 'dtforma_dt (Date) = 2010/10/14' in str(ogr_response_shp), "'dtforma_dt column is not returning the correct Date and Type"
        assert 'dtforma_tm (String) = 19:12:00' in str(ogr_response_shp), "'dtforma_tm column is not returning the correct Type and Time"
        assert 'dtform1_dt (Date) = 1988/07/11' in str(ogr_response_shp), "'dtform1_dt column is not returning the correct Date and Type"
        assert 'dtform1_tm (String) = 03:05:00' in str(ogr_response_shp), "'dtform1_tm column is not returning the correct Type and Time"
        
        # clean up
        db.drop_table(schema=pg_schema, table=test_write_gpkg_table_name)
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))
        assert not os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))
        
    def test_write_gpkg_longdate_table_ms_v1(self):
        
        gpkg_name = 'testgpkg.gpkg'
        if os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name)):
            os.remove(os.path.join(FOLDER_PATH, gpkg_name))
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}")

        # Add test_table
        sql.query(f"""
        create table {ms_schema}.{test_write_gpkg_table_name} (test_col1 int, long_dt_column123 datetime, long_dt_column1234 datetime, geom geometry);
        insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(1, '1/1/2000 11:50:00', '8/4/2004 7:20:00',
                                                                geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        # Write gpkg
        s.write_geospatial(dbo=sql, query=f"select * from {ms_schema}.{test_write_gpkg_table_name}",
                            path= os.path.join(FOLDER_PATH, gpkg_name), gpkg_tbl = test_write_gpkg_table_name, print_cmd=True)

        # Check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        cmd_gpkg = f'ogrinfo "{FOLDER_PATH}/{gpkg_name}" -sql "SELECT * FROM {test_write_gpkg_table_name} LIMIT 1" -q'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT)
        
        assert f'long_dt_dt (Date) = 2000/01/01' in str(ogr_response_gpkg), f"long_dt_dt column is not returning the correct Date & Value"
        assert f'long_d1_dt (Date) = 2004/08/04' in str(ogr_response_gpkg), f"long_d1_dt column is not returning the correct Date & Value"
        assert f'long_dt_tm (String) = 11:50:00' in str(ogr_response_gpkg), f"long_dt_tm column is not returning the correct Time & Value"
        assert f'long_d1_tm (String) = 07:20:00' in str(ogr_response_gpkg), f"long_d1_tm column is not returning the correct Time & Value"

        os.remove(os.path.join(FOLDER_PATH, gpkg_name))
        assert not os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))
        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}")
        
    def test_write_gpkg_longdate_table_ms_v2(self):
        
        gpkg_name = 'testgpkg.gpkg'

        # need this step in case you are testing for v1 to fail
        if os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name)):
            os.remove(os.path.join(FOLDER_PATH, gpkg_name))

        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}")

        # Add test_table
        sql.query(f"""
        create table {ms_schema}.{test_write_gpkg_table_name} (test_col1 int, long_dt_column123 datetime, long_dt_column1234 datetime, geom geometry);
        insert into {ms_schema}.{test_write_gpkg_table_name} VALUES(1, '1/1/2000 11:50:00', '8/4/2004 7:20:00',
                                                                geometry::Point(985831.79200444, 203371.60461367, 2263));
        """)

        # Write gpkg
        s.write_geospatial(dbo=sql, query=f"select * from {ms_schema}.{test_write_gpkg_table_name}",
                            path= os.path.join(FOLDER_PATH, gpkg_name), gpkg_tbl = test_write_gpkg_table_name, print_cmd=True)

        # Check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        cmd_gpkg = f'ogrinfo "{FOLDER_PATH}/{gpkg_name}" -sql "SELECT * FROM {test_write_gpkg_table_name} LIMIT 1" -q'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT)
        
        assert f'long_dt_column123 (DateTime) = 2000/01/01 11:50:00' in str(ogr_response_gpkg), f"long_dt_column123 column is not returning the correct Date & Value"
        assert f'long_dt_column1234 (DateTime) = 2004/08/04 07:20:00' in str(ogr_response_gpkg), f"long_dt_column1234 is not returning the correct Date & Value"

        sql.query(f"drop table if exists {ms_schema}.{test_write_gpkg_table_name}")
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))
        assert not os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))
        
    @classmethod
    def teardown_class(cls):
        helpers.clean_up_geopackage()
        helpers.clean_up_test_table_sql(sql)
        helpers.clean_up_test_table_pg(db)

class TestQueryToShp:
    @classmethod
    def setup_class(cls):
        helpers.set_up_schema(sql, ms_schema=ms_schema)

    def test_query_to_shp_data_long_ms_v1(self):

        fldr = FOLDER_PATH
        shp = 'test.shp'

        sql.drop_table(ms_schema, test_table_shp)
        sql.drop_table(ms_schema, test_table_shp + 'QA')

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table_shp} (fld1 int,
            fld2 varchar(MAX),
            fld3 varchar(MAX),
            longfld4 datetime,
            fld5 float,
            fld6 geometry);

            INSERT INTO {ms_schema}.{test_table_shp}
             VALUES (1,
             'test text',
             '{'test ' * 51}',
             CURRENT_TIMESTAMP, 123.456, geometry::Point(1015329.1, 213793.1, 2263 ))
        """)  # The shapefile maximum field width is 254 lt set to 255
        assert sql.table_exists(test_table_shp, schema=ms_schema)

        # table to shp
        sql.query_to_shp(f"select * from {ms_schema}.{test_table_shp}", path=os.path.join(fldr, shp), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # import shp to db to compare
        sql.shp_to_table(path=fldr, shp_name = shp, table=test_table_shp + 'QA', schema=ms_schema, print_cmd=True)

        sql.query(f"""
        select
            case when t1.fld2 = t2.fld2 then 1 else 0 end,
            case when left(t1.fld3, 254) = t2.fld3 then 1 else 0 end,
            case when cast(t1.longfld4 as date)=t2.longfld_dt then 1 else 0 end, -- extra column called _dt
            case when cast(t1.longfld4 as time)=t2.longfld_tm then 1 else 0 end, -- extra column called _tm
            case when t1.fld5 = t2.fld5 then 1 else 0 end,
            case when t1.fld6.STDistance(t2.geom) < 1  then 1 else 0 end-- default name from pysqldb
        from {ms_schema}.{test_table_shp} t1
        join {ms_schema}.{test_table_shp}QA t2
        on t1.fld1=t2.fld1
        """)
        assert set(sql.data[0]) == {1}

        # clean up
        sql.drop_table(ms_schema, test_table_shp)
        sql.drop_table(ms_schema, test_table_shp + 'QA')

        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_data_long_ms_v2(self):

        fldr = FOLDER_PATH
        shp = 'test.shp'

        sql.drop_table(ms_schema, test_table_shp)
        sql.drop_table(ms_schema, test_table_shp + 'QA')

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table_shp} (fld1 int,
            fld2 varchar(MAX),
            fld3 varchar(MAX),
            longfld4 datetime,
            fld5 float,
            fld6 geometry);

            INSERT INTO {ms_schema}.{test_table_shp}
             VALUES (1,
             'test text',
             '{'test ' * 51}',
             CURRENT_TIMESTAMP, 123.456, geometry::Point(1015329.1, 213793.1, 2263 ))
        """)  # The shapefile maximum field width is 254 lt set to 255
        assert sql.table_exists(test_table_shp, schema=ms_schema)

        # table to shp
        sql.query_to_shp(f"select * from {ms_schema}.{test_table_shp}", path=os.path.join(fldr, shp), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # import shp to db to compare
        sql.shp_to_table(path=fldr, shp_name = shp, table=test_table_shp + 'QA', schema=ms_schema, print_cmd=True)

        sql.query(f"""
        select
            case when t1.fld2 = t2.fld2 then 1 else 0 end,
            case when left(t1.fld3, 254) = t2.fld3 then 1 else 0 end,
            case when cast(t1.longfld4 as date) =t2.longfld4 then 1 else 0 end, -- DateTime in Shp turns into a Date?
            case when t1.fld5 = t2.fld5 then 1 else 0 end,
            case when t1.fld6.STDistance(t2.geom) < 1  then 1 else 0 end-- default name from pysqldb
        from {ms_schema}.{test_table_shp} t1
        join {ms_schema}.{test_table_shp}QA t2
        on t1.fld1=t2.fld1
        """)
        assert set(sql.data[0]) == {1}

        # clean up
        sql.drop_table(ms_schema, test_table_shp)
        sql.drop_table(ms_schema, test_table_shp + 'QA')

        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_data_longcolumn_pg(self):
        fldr = FOLDER_PATH
        shp = 'test.shp'

        db.drop_table(schema = pg_schema, table = test_table)
        db.drop_table(schema = pg_schema, table = test_table + 'QA')
        # create table
        db.query(f"""
            CREATE TABLE {pg_schema}.{test_table} (fld1 int,
            fld2 text,
            fld3 text,
            longfld4 timestamp,
            fld5 float,
            fld6 geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1,
             'test text',
             '{'test ' * 51}',
             now(), 123.456, st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)  # The shapefile maximum field width is 254 lt set to 255
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.query_to_shp(f"select * from {pg_schema}.{test_table}", path=os.path.join(fldr, shp), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # import shp to db to compare
        db.shp_to_table(path=fldr, shp_name = shp, table=test_table + 'QA', schema=pg_schema, print_cmd=True)

        db.query(f"""
        select
            t1.fld2 = t2.fld2,
            left(t1.fld3, 254) = t2.fld3,
            t1.longfld4::date = t2.longfld_dt, -- shapefiles cannot store datetimes
            t1.longfld4::time = t2.longfld_tm::time, -- shapefiles cannot store datetimes
            t1.fld5 = t2.fld5,
            st_distance(t1.fld6, t2.geom) < 1 -- deafult name from pysqldb
        from {pg_schema}.{test_table} t1
        join {pg_schema}.{test_table}qa t2
        on t1.fld1=t2.fld1
        """)

        assert set(db.data[0]) == {True}

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        db.drop_table(schema=pg_schema, table=test_table + 'qa')

        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except Exception as e:
                print(e)

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_shapefile()
        helpers.clean_up_geopackage()
        helpers.clean_up_test_table_sql(sql)

class TestQueryToGpkg:

    def test_query_to_gpkg_data_longcolumn_pg(self):

        gpkg = 'testgpkg.gpkg'
        
        db.drop_table(schema=pg_schema, table=test_table)
        db.drop_table(schema=pg_schema, table=test_table + 'QA')

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (fld1 int,
            fld2 text,
            fld3 text,
            longfld4 timestamp,
            fld5 float,
            fld6 geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1,
             'test text',
             '{'test ' * 51}',
             now(), 123.456, st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)  # The shapefile maximum field width is 254 lt set to 255
        assert db.table_exists(test_table, schema=pg_schema)

        # table to gpkg
        db.query_to_gpkg(query = f"select * from {pg_schema}.{test_table}", gpkg_tbl = test_table,
                         path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # import gpkg to db to compare
        db.gpkg_to_table(path=FOLDER_PATH, gpkg_name = gpkg, gpkg_tbl = test_table, schema = pg_schema, table = test_table + 'QA', print_cmd=True)

        db.query(f"""
        select
            t1.fld2 = t2.fld2,
            left(t1.fld3, 254) = left(t2.fld3, 254),
            t1.longfld4::date = t2.longfld_dt, -- shapefiles cannot store datetimes
            t1.longfld4::time = t2.longfld_tm::time, -- shapefiles cannot store datetimes
            t1.fld5 = t2.fld5,
            st_distance(t1.fld6, t2.fld6) < 1 -- deafult name from pysqldb
        from {pg_schema}.{test_table} t1
        join {pg_schema}.{test_table}QA t2
        on t1.fld1=t2.fld1
        """)
        assert set(db.data[0]) == {True}

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        db.drop_table(schema=pg_schema, table=test_table + 'QA')

        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_gpkg_data_long_ms(self):

        gpkg = 'testgpkg.gpkg'

        sql.drop_table(ms_schema, test_table)
        sql.drop_table(ms_schema, test_table + 'QA')

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (fld1 int,
            fld2 varchar(MAX),
            fld3 varchar(MAX),
            longfld4 datetime,
            fld5 float,
            fld6 geometry);

            INSERT INTO {ms_schema}.{test_table}
             VALUES (1,
             'test text',
             '{'test ' * 51}',
             CURRENT_TIMESTAMP, 123.456, geometry::Point(1015329.1, 213793.1, 2263 ))
        """)  # The shapefile maximum field width is 254 lt set to 255
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to gpkg
        sql.query_to_gpkg(query = f"select * from {ms_schema}.{test_table}", gpkg_tbl = test_table,
                          path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # import gpkg to db to compare
        sql.gpkg_to_table(path=FOLDER_PATH, gpkg_name = gpkg, gpkg_tbl = test_table, table=test_table + 'QA', schema=ms_schema, print_cmd=True)

        sql.query(f"""
        select
            case when t1.fld2 = t2.fld2 then 1 else 0 end,
            case when left(t1.fld3, 254) = left(t2.fld3, 254) then 1 else 0 end,
            case when t1.longfld4=t2.longfld4 then 1 else 0 end,
            case when t1.fld5 = t2.fld5 then 1 else 0 end,
            case when t1.fld6.STDistance(t2.fld6) < 1 then 1 else 0 end-- default name from pysqldb
        from {ms_schema}.{test_table} t1
        join {ms_schema}.{test_table}QA t2
        on t1.fld1=t2.fld1
        """)
        assert set(sql.data[0]) == {1}

        # clean up
        sql.drop_table(ms_schema, test_table)
        sql.drop_table(ms_schema, test_table + 'QA')

        os.remove(os.path.join(FOLDER_PATH, gpkg))

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(sql, schema=ms_schema)
        sql.query(f"drop table {ms_schema}.{sql.log_table}")
        sql.cleanup_new_tables()
        helpers.clean_up_geopackage()