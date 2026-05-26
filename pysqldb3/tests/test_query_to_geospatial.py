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

test_table = f'__testing_query_to_geospatial_{db.user}__'
test_table_shp = f'__testing_query_to_geospatial_{db.user}__'

ms_schema = 'dbo'
pg_schema = 'working'

db_int, db_geom = helpers.identify_default_dtypes(db, pg_schema)
sql_int, sql_geom = helpers.identify_default_dtypes(sql, ms_schema)

FOLDER_PATH = helpers.DIR

class TestQueryToGpkgPg:

    def test_query_to_gpkg_basic(self):

        gpkg = 'testgpkg.gpkg'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to gpkg
        db.query_to_gpkg(query = f"select * from {pg_schema}.{test_table}", gpkg_tbl = test_table,
                         path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True, srid=2263)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # Manually check SRID of projection file and verify it contains 2263
        cmd = r'gdalsrsinfo {}\{}'.format(FOLDER_PATH, gpkg).replace('\\', '/')
        ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
        assert b'"EPSG",2263' in ogr_response or b'"EPSG","2263"' in ogr_response

        # clean up
        db.drop_table(pg_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_gpkg_multitable(self):

        gpkg = 'testgpkg.gpkg'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
         """)
        
        assert db.table_exists(test_table, schema=pg_schema)

        # add first table
        db.query_to_gpkg(query = f"select * from {pg_schema}.{test_table}", gpkg_tbl = test_table,
                            path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True, srid=2263)
        # add second table to the same gpkg
        db.query_to_gpkg(query = f"select * from {pg_schema}.{test_table}", gpkg_tbl = test_table + '_2',
                         path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True, srid=2263)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # Check that both tables appear in geopackage
        cmd_gpkg = f'ogrinfo {FOLDER_PATH}/{gpkg} -sql "SELECT id FROM {test_table}_2 LIMIT 1" -q'
        cmd_gpkg2 = f'ogrinfo  {FOLDER_PATH}/{gpkg} -sql "SELECT id FROM {test_table}_2 LIMIT 1" -q'

        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg.replace('\\', '/')), stderr=subprocess.STDOUT)
        ogr_response_gpkg2 = subprocess.check_output(shlex.split(cmd_gpkg2.replace('\\', '/')), stderr=subprocess.STDOUT)
        
        assert 'id (Integer) = 1' in str(ogr_response_gpkg) and 'id (Integer) = 1' in str(ogr_response_gpkg2), "Geopackage does not contain multiple tables"

        # clean up
        db.drop_table(pg_schema, test_table)
        db.drop_table(pg_schema, test_table + '_2')
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_gpkg_overwrite(self):

        gpkg = 'testgpkg.gpkg'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
            VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
         """)
        
        assert db.table_exists(test_table, schema=pg_schema)

        db.query_to_gpkg(query = f"select * from {pg_schema}.{test_table}", gpkg_tbl = test_table,
                         path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True, srid=2263)

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table}_2;
            CREATE TABLE {pg_schema}.{test_table}_2 (id2 int, txt2 text, dte2 timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}_2
             VALUES (2, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
         """)
        
        # overwrite the table
        db.query_to_gpkg(query = f"select * from {pg_schema}.{test_table}_2", gpkg_tbl = test_table, overwrite = True,
                            path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True, srid=2263)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # this should fail
        cmd_gpkg = f'ogrinfo {FOLDER_PATH}/{gpkg} -sql "SELECT id FROM {test_table} LIMIT 1" -q'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg.replace('\\', '/')), stderr=subprocess.STDOUT)
        assert 'ERROR' in str(ogr_response_gpkg), "table was not overwritten in the geopackage"

        # check that the overwritten table works
        cmd_gpkg2 = f'ogrinfo {FOLDER_PATH}/{gpkg} -sql "SELECT id2 FROM {test_table} LIMIT 1" -q'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg2.replace('\\', '/')), stderr=subprocess.STDOUT)
        assert 'id2 (Integer) = 2' in str(ogr_response_gpkg), "table was not overwritten in the geopackage"

        # clean up
        db.drop_table(pg_schema, test_table)
        db.drop_table(pg_schema, test_table + '_2')
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_gpkg_basic_pth_and_gpkg_1(self):
        gpkg = 'testgpkg.gpkg'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
            VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to gpkg - make sure output_file overwrites any gpkg in the path
        db.query_to_gpkg(query = f"select * from {pg_schema}.{test_table}", path=os.path.join(FOLDER_PATH, gpkg), gpkg_tbl = test_table, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # Manually check SRID of projection file and verify it contains 2263
        cmd = f'ogrinfo {os.path.join(FOLDER_PATH, gpkg)} -sql "SELECT geom FROM {test_table}"'.replace('\\', '/')
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\\', '/')), stderr=subprocess.STDOUT)
        assert b'"EPSG",2263' in ogr_response

        # clean up
        db.drop_table(pg_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_gpkg_basic_pth_and_gpkg_2(self):

        gpkg = 'test_gpkg.gpkg'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to gpkg - make sure gpkg_tbl overwrites any gpkg in the path
        db.query_to_gpkg(query = f"select * from {pg_schema}.{test_table}", gpkg_tbl = test_table,
                         path=os.path.join(FOLDER_PATH, 'test_' + gpkg), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, 'test_' + gpkg))

        # clean up
        db.drop_table(pg_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, 'test_' + gpkg))

    def test_query_to_gpkg_basic_quotes(self):
        gpkg = 'testgpkg.gpkg'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, "txt" text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to gpkg
        db.query_to_gpkg(query = f"select * from {pg_schema}.{test_table}", gpkg_tbl = test_table,
                         path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_gpkg_basic_funky_field_names(self):
        gpkg = 'testgpkg.gpkg'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, "t.txt" text, "1t txt" text, "t txt" text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text','test text','test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to gpkg
        db.query_to_gpkg(query = f"select * from {pg_schema}.{test_table}", gpkg_tbl = test_table,
                         path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_gpkg_basic_long_names(self):
        gpkg = 'testgpkg.gpkg'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id_name_one int, "123text name one" text,
            "text@name-two~three four five six seven" text,
            current_date_time timestamp,
            "x-coord" float,
            geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', 'test text', now(), 123.456, st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to gpkg
        db.query_to_gpkg(f"select * from {pg_schema}.{test_table}", gpkg_tbl = test_table,
                         path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_gpkg_basic_no_data(self):
        gpkg = 'testgpkg.gpkg'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to gpkg
        db.query_to_gpkg(query = f"select * from {pg_schema}.{test_table} limit 0", gpkg_tbl = test_table,
                         path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))


    def test_query_to_gpkg_data(self):
        gpkg = 'testgpkg.gpkg'

        if os.path.isfile(os.path.join(FOLDER_PATH, gpkg)):
            os.remove(os.path.join(FOLDER_PATH, gpkg))

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (fld1 int,
            fld2 text,
            fld3 text,
            fld4 timestamp,
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

        # Assert that date columns are in the proper format
        cmd_gpkg = f'ogrinfo "{FOLDER_PATH}/{gpkg}" -sql "select * from {test_table}"'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT) 

        # Difficult to assert the same current timestamp so we assert only that column and type are correct.
        assert 'fld1 (Integer) = 1' in str(ogr_response_gpkg), "GPKG - fld1 not returning correct datatype or value"
        assert 'fld2 (String) = test text' in str(ogr_response_gpkg), "GPKG - fld2 not returning correct datatype or value"
        assert 'fld3 (String) = test test test' in str(ogr_response_gpkg), "GPKG - fld3 not returning correct datatype or value"
        assert 'fld4 (DateTime) = ' in str(ogr_response_gpkg), "GPKG - fld4 not returning correct datatype or value"
        assert 'fld5 (Real) = 123.456' in str(ogr_response_gpkg), "GPKG - fld5 not returning correct datatype or value"
        assert 'POINT (1015329.1 213793.1)' in str(ogr_response_gpkg), "GPKG - fld6 not returning correct datatype or value"

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        db.drop_table(schema=pg_schema, table=test_table + 'QA')

        os.remove(os.path.join(FOLDER_PATH, gpkg))
        
    def test_query_to_gpkg_data_longcolumn(self):

        gpkg = 'testgpkg.gpkg'

        if os.path.isfile(os.path.join(FOLDER_PATH, gpkg)):
            os.remove(os.path.join(FOLDER_PATH, gpkg))
        
        db.drop_table(schema=pg_schema, table=test_table)

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

        # Assert that date columns are in the proper format
        cmd_gpkg = f'ogrinfo "{FOLDER_PATH}/{gpkg}" -sql "select * from {test_table}"'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT) 

        # Difficult to assert the same current timestamp so we assert only that column and type are correct.
        assert 'fld1 (Integer) = 1' in str(ogr_response_gpkg), "GPKG - fld1 not returning correct datatype or value"
        assert 'fld2 (String) = test text' in str(ogr_response_gpkg), "GPKG - fld2 not returning correct datatype or value"
        assert 'fld3 (String) = test test test' in str(ogr_response_gpkg), "GPKG - fld3 not returning correct datatype or value"
        assert 'longfld4 (DateTime) = ' in str(ogr_response_gpkg), "GPKG longfld4 column not in DateTime format"
        assert 'fld5 (Real) = 123.456' in str(ogr_response_gpkg), "GPKG - fld5 not returning correct datatype or value"
        assert 'POINT (1015329.1 213793.1)' in str(ogr_response_gpkg), "GPKG - fld6 not returning correct datatype or value"

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)

        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_geospatial_bad_query(self):
        gpkg = 'testgpkg'

        # This should fail
        try:
            db.query_to_gpkg(query="select * from table_does_not_exist", gpkg_tbl = test_table,
                             path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True)
        except:
            Failed = True
        # check table in not folder
        assert Failed
        assert not os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_gpkg_date_basic(self):

        db.query(f"""
                drop table if exists {pg_schema}.{test_table};
                create table {pg_schema}.{test_table} as
                select  1 as id, 
                        cast('10/14/2010 19:12:00' as timestamp) test_date,
                        st_setsrid(st_point(1015428.1, 213086.1), 2263) as geom
                """)
        
        assert db.table_exists(test_table, schema = pg_schema)

        gpkg = 'testgpkg.gpkg'

        if os.path.isfile(os.path.join(FOLDER_PATH, gpkg)):
            os.remove(os.path.join(FOLDER_PATH, gpkg))

        # Write gpkg
        db.query_to_gpkg(path=os.path.join(FOLDER_PATH, gpkg), query = f"select * from {pg_schema}.{test_table}",
                         gpkg_tbl=test_table, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # Assert that date columns are in the proper format
        cmd_gpkg = f'ogrinfo "{FOLDER_PATH}/{gpkg}" -sql "select * from {test_table}"'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT) 

        assert 'test_date (DateTime) = 2010/10/14 19:12:00' in str(ogr_response_gpkg), "GPKG test_date not in DateTime format"

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))
        assert not os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(db)
        helpers.clean_up_geopackage()

class TestQueryToGpkgMs:
    @classmethod
    def setup_class(cls):
        helpers.set_up_schema(sql, ms_schema=ms_schema)

    def test_query_to_gpkg_basic(self):
        gpkg = 'testgpkg.gpkg'
        sql.drop_table(schema=ms_schema, table=test_table)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to gpkg
        sql.query_to_gpkg(f"select * from {ms_schema}.{test_table}", gpkg_tbl = test_table,
                          path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True, srid=2263)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # Manually check SRID of projection file and verify it contains 2263
        cmd = r'gdalsrsinfo {}\{}'.format(FOLDER_PATH, gpkg).replace('\\', '/')
        ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
        assert b'"EPSG",2263' in ogr_response or b'"EPSG","2263"' in ogr_response

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))
        
    def test_query_to_gpkg_multitable(self):

        gpkg = 'testgpkg.gpkg'
        sql.drop_table(schema=ms_schema, table=test_table)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=ms_schema)
        sql.query_to_gpkg(f"select * from {ms_schema}.{test_table}", gpkg_tbl = test_table,
                          path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True, srid=2263)

        sql.drop_table(schema=ms_schema, table=test_table)
        # add similar table under a different name in the same gpkg
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id3 int, txt text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table}
            (id3, txt, dte, geom)
             VALUES (3, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=ms_schema)
        sql.query_to_gpkg(query = f"select * from {ms_schema}.{test_table}", gpkg_tbl = test_table + '_2',
                          path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True, srid=2263)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # Check that both tables appear in geopackage
        cmd_gpkg = f'ogrinfo {FOLDER_PATH}/{gpkg} -sql "SELECT id FROM {test_table} LIMIT 1" -q'
        cmd_gpkg2 = f'ogrinfo {FOLDER_PATH}/{gpkg} -sql "SELECT id3 FROM {test_table}_2 LIMIT 1" -q'

        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg.replace('\\', '/')), stderr=subprocess.STDOUT)
        ogr_response_gpkg2 = subprocess.check_output(shlex.split(cmd_gpkg2.replace('\\', '/')), stderr=subprocess.STDOUT)
        
        assert 'id (Integer) = 1' in str(ogr_response_gpkg) and 'id3 (Integer) = 3' in str(ogr_response_gpkg2), "geopackage does not contain multiple tables"

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_gpkg_overwrite(self):
        gpkg = 'testgpkg.gpkg'
        sql.drop_table(schema=ms_schema, table=test_table)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        sql.query_to_gpkg(f"select * from {ms_schema}.{test_table}", gpkg_tbl = test_table,
                          path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True, srid=2263)

        # create new, slightly different table
        sql.drop_table(schema=ms_schema, table=test_table)
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id3 int, txt text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table}
            (id3, txt, dte, geom)
             VALUES (3, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=ms_schema)

        # overwrite the same table
        sql.query_to_gpkg(query = f"select * from {ms_schema}.{test_table}", gpkg_tbl = test_table,
                          path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True, srid=2263)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # this should fail
        cmd_gpkg = f'ogrinfo {FOLDER_PATH}/{gpkg} -sql "SELECT id FROM {test_table} LIMIT 1" -q'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg.replace('\\', '/')), stderr=subprocess.STDOUT)
        assert 'ERROR' in str(ogr_response_gpkg), "table was not overwritten in the geopackage"

        # check that the overwritten table works
        cmd_gpkg2 = f'ogrinfo {FOLDER_PATH}/{gpkg} -sql "SELECT id3 FROM {test_table} LIMIT 1" -q'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg2.replace('\\', '/')), stderr=subprocess.STDOUT)
        assert 'id3 (Integer) = 3' in str(ogr_response_gpkg), "table was not overwritten in the geopackage"

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

        assert not os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_gpkg_basic_pth_and_name_1(self):
        gpkg = 'testgpkg.gpkg'
        sql.drop_table(schema=ms_schema, table=test_table)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to geospatial - make sure geospatial overwrites any gpkg in the path
        sql.query_to_gpkg(f"select * from {ms_schema}.{test_table}", gpkg_tbl = test_table,
                        path= os.path.join(FOLDER_PATH, gpkg), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # Manually check SRID of projection file and verify it contains 2263
        cmd = r'gdalsrsinfo {}\{}'.format(FOLDER_PATH, gpkg).replace('\\', '/')
        ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
        assert b'"EPSG",2263' in ogr_response

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

        assert not os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_geospatial_basic_pth_and_name_2(self):

        gpkg = 'testgpkg.gpkg'
        sql.drop_table(schema=ms_schema, table=test_table)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to gpkg
        sql.query_to_gpkg(query = f"select * from {ms_schema}.{test_table}", path=os.path.join(FOLDER_PATH, gpkg), gpkg_tbl = test_table, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_gpkg_basic_pth_and_name(self):

        gpkg = 'testgpkg.gpkg'
        sql.drop_table(schema=ms_schema, table=test_table)
 
        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id int, txt text, dte datetime, geom geometry);
            INSERT INTO {ms_schema}.{test_table}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
 
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to gpkg - make sure Geopackage overwrites any gpkg in the path
        sql.query_to_gpkg(f"select * from {ms_schema}.{test_table}", gpkg_tbl = test_table,
                        path= os.path.join(FOLDER_PATH, gpkg), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_geospatial_basic_brackets(self):
        
        gpkg = 'testgpkg.gpkg'
        sql.drop_table(schema=ms_schema, table=test_table)

        # create table
        sql.query(f"""
                    CREATE TABLE {ms_schema}.{test_table} (id int, [txt] text, dte datetime, geom geometry);

                    INSERT INTO {ms_schema}.{test_table}
                    (id, txt, dte, geom)
                     VALUES (1, 'test text', CURRENT_TIMESTAMP,
                     geometry::Point(1015329.1, 213793.1, 2263))
                """)
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to gpkg
        sql.query_to_gpkg(f"select * from {ms_schema}.{test_table}", gpkg_tbl = test_table,
                          path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))


    def test_query_to_geospatial_basic_funky_field_names(self):
        gpkg = 'testgpkg.gpkg'

        sql.drop_table(schema = ms_schema, table = test_table)
        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id int, [t.txt] text, [1t txt] text, [t_txt] text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table}
            (id, [t.txt], [1t txt], [t_txt], dte, geom)
            VALUES (1, 'test text','test text','test text', CURRENT_TIMESTAMP,
            geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to gpkg
        sql.query_to_gpkg(f"select * from {ms_schema}.{test_table}", gpkg_tbl = test_table,
                          path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_geospatial_basic_long_names(self):
        gpkg = 'testgpkg.gpkg'
        sql.drop_table(schema=ms_schema, table=test_table)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id_name_one int,
            [123text name one] text,
            [text@name-two~three four five six seven] text,
            current_date_time datetime,
            [x-coord] float,
            geom geometry);

            INSERT INTO {ms_schema}.{test_table}
            VALUES (1, 'test text', 'test text', CURRENT_TIMESTAMP,
            123.456, geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to gpkg
        sql.query_to_gpkg(f"select * from {ms_schema}.{test_table}", gpkg_tbl = test_table,
                          path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_gpkg_basic_no_data(self):

        gpkg = 'testgpkg.gpkg'
        sql.drop_table(schema=ms_schema, table=test_table)
        assert not sql.table_exists(table=test_table, schema=ms_schema)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table}
                 VALUES (1, 'test text', cast(CURRENT_TIMESTAMP as datetime), geometry::Point(1015329.1, 213793.1, 2263 ))
        """)

        assert sql.table_exists(test_table, schema=ms_schema)

        # table to gpkg
        sql.query_to_gpkg(f"select top 0 * from {ms_schema}.{test_table}", gpkg_tbl = test_table,
                          path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_gpkg_data(self):

        gpkg = 'testgpkg.gpkg'

        sql.drop_table(ms_schema, test_table)
        sql.drop_table(ms_schema, test_table + 'QA')

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (fld1 int,
            fld2 varchar(MAX),
            fld3 varchar(MAX),
            fld4 datetime,
            fld5 float,
            fld6 geometry);

            INSERT INTO {ms_schema}.{test_table}
             VALUES (1,
             'test text',
             '{'test ' * 51}',
             CURRENT_TIMESTAMP, 123.456, geometry::Point(1015329.1, 213793.1, 2263 ))
        """) # The shapefile maximum field width is 254 lt set to 255
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to gpkg
        sql.query_to_gpkg(query = f"select * from {ms_schema}.{test_table}", gpkg_tbl = test_table,
                          path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # compare result using ogrinfo
        cmd_gpkg = f'ogrinfo "{FOLDER_PATH}/{gpkg}" -sql "select * from {test_table}"'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT) 

        # hard to assert current_timestamp value so we just test this field / type exists
        assert 'fld1 (Integer) = 1' in str(ogr_response_gpkg), "GPKG - fld1 not returning correct datatype or value"
        assert 'fld2 (String) = test text' in str(ogr_response_gpkg), "GPKG - fld2 not returning correct datatype or value"
        assert 'fld3 (String) = test test test' in str(ogr_response_gpkg), "GPKG - fld3 not returning correct datatype or value"
        assert 'fld4 (DateTime) = ' in str(ogr_response_gpkg), "GPKG - fld4 not in DateTime format"
        assert 'fld5 (Real) = 123.456' in str(ogr_response_gpkg), "GPKG - fld5 not returning correct datatype or value"
        assert 'POINT (1015329.1 213793.1)' in str(ogr_response_gpkg), "GPKG - fld6 not returning correct datatype or value"
        
        # clean up
        sql.drop_table(ms_schema, test_table)
        sql.drop_table(ms_schema, test_table + 'QA')

        os.remove(os.path.join(FOLDER_PATH, gpkg))
            
    def test_query_to_gpkg_data_long(self):

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

        # compare result using ogrinfo
        cmd_gpkg = f'ogrinfo "{FOLDER_PATH}/{gpkg}" -sql "select * from {test_table}"'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT) 

        # hard to assert current_timestamp value so we just test this field / type exists
        assert 'fld1 (Integer) = 1' in str(ogr_response_gpkg), "GPKG - fld1 not returning correct datatype or value"
        assert 'fld2 (String) = test text' in str(ogr_response_gpkg), "GPKG - fld2 not returning correct datatype or value"
        assert 'fld3 (String) = test test test' in str(ogr_response_gpkg), "GPKG - fld3 not returning correct datatype or value"
        assert 'longfld4 (DateTime) = ' in str(ogr_response_gpkg), "GPKG date not in DateTime format"
        assert 'fld5 (Real) = 123.456' in str(ogr_response_gpkg), "GPKG - fld5 not returning correct datatype or value"
        assert 'POINT (1015329.1 213793.1)' in str(ogr_response_gpkg), "GPKG - fld6 not returning correct datatype or value"
        
        # clean up
        sql.drop_table(ms_schema, test_table)

        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_gpkg_bad_query(self):
        gpkg = 'test'

        # This should fail
        try:
            sql.query_to_gpkg(query="select * from table_does_not_exist", gpkg_tbl = 'table_does_not_exist',
                              path=os.path.join(FOLDER_PATH, gpkg), print_cmd=True)
        except:
            Failed = True
        # check table in not folder
        assert Failed
        assert not os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

    def test_query_to_gpkg_date_basic(self):

        sql.query(f"drop table if exists {ms_schema}.{test_table}")

        sql.query(f"""
                create table {ms_schema}.{test_table}
                    (id int, test_date datetime, geom geometry);
                insert into {ms_schema}.{test_table} VALUES(1, '1/1/2000 11:50:00',
                                                                geometry::Point(985831.79200444, 203371.60461367, 2263));
                """)
        
        assert sql.table_exists(test_table, schema = ms_schema)

        gpkg_name = 'testgpkg.gpkg'

        if os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name)):
            os.remove(os.path.join(FOLDER_PATH, gpkg_name))

        # Write gpkg
        sql.query_to_gpkg(path=os.path.join(FOLDER_PATH, gpkg_name), query = f"select * from {ms_schema}.{test_table}",
                         gpkg_tbl=test_table, print_cmd=True)

        # Assert successful
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

        # Assert that date columns are in the proper format
        cmd_gpkg = f'ogrinfo "{FOLDER_PATH}/{gpkg_name}" -sql "select * from {test_table}"'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT) 

        assert 'test_date (DateTime) = 2000/01/01 11:50:00' in str(ogr_response_gpkg), "Appears in incorrect format"

        # clean up
        sql.drop_table(schema=ms_schema, table=test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg_name))
        assert not os.path.isfile(os.path.join(FOLDER_PATH, gpkg_name))

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(sql, schema=ms_schema)
        sql.query(f"drop table {ms_schema}.{sql.log_table}")
        sql.cleanup_new_tables()
        helpers.clean_up_geopackage()

class TestQueryToShpPg:
    def test_query_to_shp_basic(self):
        fldr = FOLDER_PATH
        shp = 'test.shp'

        db.drop_table(schema = pg_schema, table = test_table)
        # create table
        db.query(f"""
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, dt2 date, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(),  cast('2020-01-01' as date), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.query_to_shp(f"select * from {pg_schema}.{test_table}", path=fldr + '/' + shp, print_cmd=True, srid=2263)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # Manually check SRID of projection file and verify it contains 2263
        cmd = r'gdalsrsinfo {}\{}'.format(fldr, shp).replace('\\', '/')
        ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
        assert b'"EPSG",2263' in ogr_response or b'"EPSG","2263"' in ogr_response

        # check the data types
        table_types = db.get_table_columns(test_table, schema = pg_schema)
        types = [x[1] for x in table_types]
        assert {db_int, 'text', 'timestamp without time zone', 'date', db_geom}.issubset(types)

        # clean up
        db.drop_table(pg_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_query_to_shp_basic_pth(self):
        fldr = FOLDER_PATH
        shp = 'test.shp'

        db.drop_table(schema = pg_schema, table = test_table)
        # create table
        db.query(f"""
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.query_to_shp(f"select * from {pg_schema}.{test_table}", path=fldr+'\\'+shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(pg_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_query_to_shp_basic_pth_and_shp_1(self):
        fldr = FOLDER_PATH
        shp = 'test.shp'

        db.drop_table(schema = pg_schema, table = test_table)
        # create table
        db.query(f"""
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp - make sure command overwrites any shp in the path
        db.query_to_shp(f"select * from {pg_schema}.{test_table}", path=fldr+'\\'+shp, print_cmd=True)
        
        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # Manually check SRID of projection file and verify it contains 2263
        cmd = r'gdalsrsinfo {}\{}'.format(fldr, shp).replace('\\', '/')
        ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
        assert b'"EPSG",2263' in ogr_response

        # clean up
        db.drop_table(pg_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_query_to_shp_basic_pth_and_shp_2(self):

        fldr = FOLDER_PATH
        shp = 'test.shp'

        db.drop_table(schema = pg_schema, table = test_table)
        # create table
        db.query(f"""
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp - make sure output_file overwrites any shp in the path
        db.query_to_shp(f"select * from {pg_schema}.{test_table}", path=fldr+'\\'+'test_'+shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, 'test_' + shp))

        # clean up
        db.drop_table(pg_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, 'test_' + shp.replace('shp', ext)))

    def test_query_to_shp_basic_quotes(self):
        fldr = FOLDER_PATH
        shp = 'test.shp'

        db.drop_table(schema = pg_schema, table = test_table)
        # create table
        db.query(f"""
            CREATE TABLE {pg_schema}.{test_table} (id int, "txt" text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.query_to_shp(f"select * from {pg_schema}.{test_table}", path=fldr + '/' + shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_query_to_shp_basic_funky_field_names(self):
        fldr = FOLDER_PATH
        shp = 'test.shp'

        db.drop_table(schema = pg_schema, table = test_table)
        # create table
        db.query(f"""
            CREATE TABLE {pg_schema}.{test_table} (id int, "t.txt" text, "1t txt" text, "t txt" text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text','test text','test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.query_to_shp(f"select * from {pg_schema}.{test_table}", path=fldr + '/' + shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_query_to_shp_basic_long_names(self):
        fldr = FOLDER_PATH
        shp = 'test.shp'

        db.drop_table(schema = pg_schema, table = test_table)
        # create table
        db.query(f"""
            CREATE TABLE {pg_schema}.{test_table} (id_name_one int, "123text name one" text,
            "text@name-two~three four five six seven" text,
            current_date_time timestamp,
            "x-coord" float,
            geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', 'test text', now(), 123.456, st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.query_to_shp(f"select * from {pg_schema}.{test_table}", path=fldr + '/' + shp, print_cmd=True)
        
        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_query_to_shp_basic_no_data(self):

        fldr = FOLDER_PATH
        shp = 'test.shp'

        db.drop_table(schema = pg_schema, table = test_table)
        # create table
        db.query(f"""
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.query_to_shp(f"select * from {pg_schema}.{test_table} limit 0", path=os.path.join(fldr, shp), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except Exception as e:
                print(e)

    def test_query_to_shp_data(self):
        fldr = FOLDER_PATH
        shp = 'test.shp'

        db.drop_table(schema = pg_schema, table = test_table)
        # create table
        db.query(f"""
            CREATE TABLE {pg_schema}.{test_table} (fld1 int,
            fld2 text,
            fld3 text,
            fld4 timestamp,
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
        db.shp_to_table(path=os.path.join(fldr, shp), table=test_table + 'QA', schema=pg_schema, print_cmd=True)

        assert db.table_exists(schema = pg_schema, table = test_table + 'QA')

        db.query(f"""
        select
            t1.fld2 = t2.fld2,
            left(t1.fld3, 254) = t2.fld3,
            t1.fld4::date = t2.fld4_dt, -- shapefiles cannot store datetimes
            t1.fld4::time = t2.fld4_tm::time, -- shapefiles cannot store datetimes
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

    def test_query_to_shp_data_longcolumn(self):
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
        db.shp_to_table(path= os.path.join(fldr, shp), table=test_table + 'QA', schema=pg_schema, print_cmd=True)

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

    def test_query_to_geospatial_bad_query(self):
        fldr = FOLDER_PATH
        shp = 'test'

        # This should fail
        try:
            db.query_to_shp(query="select * from table_does_not_exist", path=os.path.join(fldr, shp), print_cmd=True)
        except:
            Failed = True
        # check table in not folder
        assert Failed

    def teardown_class(cls):
        helpers.clean_up_test_table_pg(db)
        db.cleanup_new_tables()
        helpers.clean_up_geopackage()
        helpers.clean_up_shapefile()


class TestQueryToShpMs:
    @classmethod
    def setup_class(cls):
        helpers.set_up_schema(sql, ms_schema=ms_schema)

    def test_query_to_shp_basic(self):
        fldr = FOLDER_PATH
        shp = 'test.shp'
        sql.drop_table(schema=ms_schema, table=test_table_shp)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table_shp} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table_shp}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table_shp, schema=ms_schema)

        # table to shp
        sql.query_to_shp(f"select * from {ms_schema}.{test_table}", path=os.path.join(fldr, shp), print_cmd=True, srid=2263)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # Manually check SRID of projection file and verify it contains 2263
        cmd = r'gdalsrsinfo {}\{}'.format(fldr, shp).replace('\\', '/')
        ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
        assert b'"EPSG",2263' in ogr_response or b'"EPSG","2263"' in ogr_response

        # check the data types
        table_types = sql.get_table_columns(test_table, schema = ms_schema)
        types = [x[1] for x in table_types]

        assert {sql_int, 'text', 'datetime', sql_geom}.issubset(types)
        
        # clean up
        sql.drop_table(ms_schema, test_table_shp)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_pth_and_name(self):
        fldr = FOLDER_PATH
        shp = 'test.shp'
        sql.drop_table(schema=ms_schema, table=test_table_shp)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table_shp} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table_shp}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table_shp, schema=ms_schema)

        # table to shp - make sure output_file overwrites any shp in the path
        sql.query_to_shp(f"select * from {ms_schema}.{test_table}", path=fldr + '\\' + 'test_' + shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, 'test_' + shp))

        # Manually check SRID of projection file and verify it contains 2263
        cmd = r'gdalsrsinfo {}\{}'.format(fldr, 'test_' + shp).replace('\\', '/')
        ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
        assert b'"EPSG",2263' in ogr_response

        # clean up
        sql.drop_table(ms_schema, test_table_shp)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, 'test_' + shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_pth(self):
        fldr = FOLDER_PATH
        shp = 'test.shp'
        sql.drop_table(schema=ms_schema, table=test_table)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to shp
        sql.query_to_shp(f"select * from {ms_schema}.{test_table_shp}", path=fldr + '\\' + shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(ms_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_brackets(self):

        fldr = FOLDER_PATH
        shp = 'test.shp'
        sql.drop_table(schema=ms_schema, table=test_table_shp)

        # create table
        sql.query(f"""
                    CREATE TABLE {ms_schema}.{test_table_shp} (id int, [txt] text, dte datetime, geom geometry);

                    INSERT INTO {ms_schema}.{test_table_shp}
                    (id, txt, dte, geom)
                     VALUES (1, 'test text', CURRENT_TIMESTAMP,
                     geometry::Point(1015329.1, 213793.1, 2263))
                """)
        assert sql.table_exists(test_table_shp, schema=ms_schema)

        # table to shp
        sql.query_to_shp(f"select * from {ms_schema}.{test_table_shp}", path=os.path.join(fldr, shp), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(ms_schema, test_table_shp)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_funky_field_names(self):
        fldr = FOLDER_PATH
        shp = 'test.shp'

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table_shp} (id int, [t.txt] text, [1t txt] text, [t_txt] text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table_shp}
            (id, [t.txt], [1t txt], [t_txt], dte, geom)
            VALUES (1, 'test text','test text','test text', CURRENT_TIMESTAMP,
            geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table_shp, schema=ms_schema)

        # table to shp
        sql.query_to_shp(f"select * from {ms_schema}.{test_table_shp}", path=os.path.join(fldr, shp), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(ms_schema, test_table_shp)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_long_names(self):

        fldr = FOLDER_PATH
        shp = 'test.shp'
        sql.drop_table(schema=ms_schema, table=test_table_shp)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table_shp} (id_name_one int,
            [123text name one] text,
            [text@name-two~three four five six seven] text,
            current_date_time datetime,
            [x-coord] float,
            geom geometry);

            INSERT INTO {ms_schema}.{test_table_shp}
            VALUES (1, 'test text', 'test text', CURRENT_TIMESTAMP,
            123.456, geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table_shp, schema=ms_schema)

        # table to shp
        sql.query_to_shp(f"select * from {ms_schema}.{test_table_shp}", path=os.path.join(fldr, shp), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(ms_schema, test_table_shp)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_no_data(self):

        fldr = FOLDER_PATH
        shp = 'test.shp'
        sql.drop_table(schema=ms_schema, table=test_table_shp)
        assert not sql.table_exists(table=test_table, schema=ms_schema)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table_shp} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table_shp}
                 VALUES (1, 'test text', cast(CURRENT_TIMESTAMP as datetime), geometry::Point(1015329.1, 213793.1, 2263 ))
        """)

        assert sql.table_exists(test_table_shp, schema=ms_schema)

        # table to shp
        sql.query_to_shp(f"select top 0 * from {ms_schema}.{test_table_shp}", path=os.path.join(fldr, shp), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(ms_schema, test_table_shp)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

        sql.drop_table(schema = ms_schema, table = test_table_shp)

    def test_query_to_shp_data(self):

        fldr = FOLDER_PATH
        shp = 'test.shp'

        sql.drop_table(ms_schema, test_table_shp)
        sql.drop_table(ms_schema, test_table_shp + 'QA')

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table_shp} (fld1 int,
            fld2 varchar(MAX),
            fld3 varchar(MAX),
            fld4 datetime,
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
        sql.shp_to_table(path=os.path.join(fldr, shp), table=test_table_shp + 'QA', schema=ms_schema, print_cmd=True)

        sql.query(f"""
        select
            case when t1.fld2 = t2.fld2 then 1 else 0 end,
            case when left(t1.fld3, 254) = t2.fld3 then 1 else 0 end,
            case when cast(t1.fld4 as date)=t2.fld4_dt then 1 else 0 end, -- shapefiles cannot store datetimes
            case when cast(t1.fld4 as time)=t2.fld4_tm then 1 else 0 end, -- shapefiles cannot store datetimes
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

    def test_query_to_shp_data_long(self):

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
        sql.shp_to_table(path=os.path.join(fldr, shp), table=test_table_shp + 'QA', schema=ms_schema, print_cmd=True)

        sql.query(f"""
        select
            case when t1.fld2 = t2.fld2 then 1 else 0 end,
            case when left(t1.fld3, 254) = t2.fld3 then 1 else 0 end,
            case when cast(t1.longfld4 as date)=t2.longfld_dt then 1 else 0 end, -- shapefiles cannot store datetimes
            case when cast(t1.longfld4 as time)=t2.longfld_tm then 1 else 0 end, -- shapefiles cannot store datetimes
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

    def test_query_to_geospatial_bad_query(self):
        fldr = FOLDER_PATH
        shp = 'test'

        # This should fail
        try:
            sql.query_to_shp(query="select * from table_does_not_exist", path=fldr + '/' + shp + '.shp', print_cmd=True)
        except:
            Failed = True
        # check table in not folder
        assert Failed

    @classmethod
    def teardown_class(cls):
        # helpers.clean_up_test_table_sql(sql)
        sql.drop_table(schema = ms_schema, table = sql.log_table)
        # sql.cleanup_new_tables()
        helpers.clean_up_geopackage()
        helpers.clean_up_shapefile()
