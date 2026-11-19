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

class TestTableToGpkgPg:
    def test_table_to_gpkg_basic(self):

        gpkg = 'test.gpkg'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(table = test_table, schema=pg_schema)

        # table to gpkg
        db.table_to_gpkg(table = test_table, schema=pg_schema,
                         path=os.path.join(FOLDER_PATH, gpkg, test_table), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # clean up
        db.drop_table(pg_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_table_to_gpkg_muiltitable(self):

        gpkg = 'test.gpkg'
        
        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(table = test_table, schema=pg_schema)

        # table to gpkg
        db.table_to_gpkg(table = test_table, schema=pg_schema,
                         path=os.path.join(FOLDER_PATH, gpkg, test_table), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # create second table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id_test int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (2, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(table = test_table, schema=pg_schema)

        # table to gpkg
        db.table_to_gpkg(table = test_table, schema=pg_schema,
                         path=os.path.join(FOLDER_PATH, gpkg, f'{test_table}_2'), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # check the number of tables within the geopackage
        cmd_gpkg_1 = f'ogrinfo "{FOLDER_PATH}/{gpkg}" -sql "SELECT id FROM {test_table} LIMIT 1" -q '
        cmd_gpkg_2 = f'ogrinfo "{FOLDER_PATH}/{gpkg}" -sql "SELECT id_test FROM {test_table}_2 LIMIT 1" -q'

        ogr_response_gpkg_1 = subprocess.check_output(shlex.split(cmd_gpkg_1), stderr=subprocess.STDOUT)
        ogr_response_gpkg_2 = subprocess.check_output(shlex.split(cmd_gpkg_2), stderr=subprocess.STDOUT)

        assert 'id (Integer) = 1' in str(ogr_response_gpkg_1) \
                and 'id_test (Integer) = 2' in str(ogr_response_gpkg_2), "Cannot find 2 tables in the same geopackage"

        # clean up
        db.drop_table(pg_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_table_gpkg_overwrite(self):

        gpkg = 'test.gpkg'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(table = test_table, schema=pg_schema)

        # table to gpkg
        db.table_to_gpkg(table = test_table, schema=pg_schema,
                         path = os.path.join(FOLDER_PATH, gpkg, test_table), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # create a slightly different table and overwrite this
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id2 int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (2, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(table = test_table, schema=pg_schema)

        # overwrite the table with the same name, but the table has different values
        db.table_to_gpkg(table = test_table, schema=pg_schema,
                         path= os.path.join(FOLDER_PATH, gpkg, test_table), print_cmd=True)

        # this should fail
        cmd_gpkg = f'ogrinfo {FOLDER_PATH}/{gpkg} -sql "SELECT id FROM {test_table} LIMIT 1" -q'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg.replace('\\','/')), stderr=subprocess.STDOUT)
        assert 'ERROR' in str(ogr_response_gpkg), "table was not overwritten in the geopackage"

        # check that the overwritten table works
        cmd_gpkg2 = f'ogrinfo {FOLDER_PATH}/{gpkg} -sql "SELECT id2 FROM {test_table} LIMIT 2" -q'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg2.replace('\\','/')), stderr=subprocess.STDOUT)
        assert 'id2 (Integer) = 2' in str(ogr_response_gpkg), "table was not overwritten in the geopackage"

        # clean up
        db.drop_table(pg_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_table_to_gpkg_basic_pth(self):

        gpkg = 'testgpkg.gpkg'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(table = test_table, schema=pg_schema)

        # table to gpkg
        db.table_to_gpkg(table = test_table, schema=pg_schema,
                         path=os.path.join(FOLDER_PATH, gpkg, test_table), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # clean up
        db.drop_table(pg_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_table_to_gpkg_basic_pth_w_name(self):

        gpkg = 'testgpkg.gpkg'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(table = test_table, schema=pg_schema)

        # table to gpkg
        db.table_to_gpkg(table = test_table, schema=pg_schema,
                         path=os.path.join(FOLDER_PATH, gpkg, test_table), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # clean up
        db.drop_table(pg_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))
        

    def test_table_to_gpkg_basic_quotes(self):

        gpkg = test_table+'.gpkg'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, "txt" text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(table = test_table, schema=pg_schema)

        # table to gpkg
        db.table_to_gpkg(table = test_table, schema = pg_schema,
                         path=os.path.join(FOLDER_PATH, gpkg, test_table), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))


class TestTableTogpkgMs:
    @classmethod
    def setup_class(cls):
        helpers.set_up_schema(sql)


    def test_table_gpkg_basic(self):

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
        assert sql.table_exists(table = test_table, schema=ms_schema)

        # table to gpkg
        sql.table_to_gpkg(table = test_table, schema=ms_schema,
                          path= os.path.join(FOLDER_PATH, gpkg, test_table), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_table_gpkg_multitable(self):

        gpkg = 'testgpkg.gpkg'
        sql.drop_table(schema=ms_schema, table=test_table)

        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(table = test_table, schema=ms_schema)

        # table to gpkg
        sql.table_to_gpkg(table = test_table, schema=ms_schema, 
                          path= os.path.join(FOLDER_PATH, gpkg, test_table), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))
        
        # create the second table
        sql.drop_table(schema=ms_schema, table=test_table + '_2')
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table}_2 (id_test int, txt text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table}_2
            (id_test, txt, dte, geom)
             VALUES (2, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(table = test_table, schema=ms_schema)

        # add second table to gpkg
        sql.table_to_gpkg(table = test_table + '_2', schema=ms_schema,
                          path= os.path.join(FOLDER_PATH, gpkg, test_table + '_2'), print_cmd=True)

        # check gpkg in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # check the number of tables within the geopackage
        cmd_gpkg_1 = f'ogrinfo {FOLDER_PATH}/{gpkg} -sql "SELECT id FROM {test_table} LIMIT 1" -q '
        cmd_gpkg_2 = f'ogrinfo {FOLDER_PATH}/{gpkg} -sql "SELECT id_test FROM {test_table}_2 LIMIT 1" -q'

        ogr_response_gpkg_1 = subprocess.check_output(shlex.split(cmd_gpkg_1.replace('\\','/')), stderr=subprocess.STDOUT)
        ogr_response_gpkg_2 = subprocess.check_output(shlex.split(cmd_gpkg_2.replace('\\','/')), stderr=subprocess.STDOUT)

        assert 'id (Integer) = 1' in str(ogr_response_gpkg_1) and 'id_test (Integer) = 2' in str(ogr_response_gpkg_2), "Cannot find 2 tables in the same geopackage"

        # clean up
        sql.drop_table(ms_schema, test_table)
        sql.drop_table(ms_schema, test_table + '_2')
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_table_gpkg_overwrite(self):

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
        assert sql.table_exists(table = test_table, schema=ms_schema)

        # table to gpkg
        sql.table_to_gpkg(table = test_table, schema=ms_schema,
                          path= os.path.join(FOLDER_PATH, gpkg, test_table), print_cmd=True)

        # create a second table with slightly different data and overwrite it
        sql.drop_table(schema=ms_schema, table=test_table)
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id3 int, txt text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table}
            (id3, txt, dte, geom)
             VALUES (3, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(table = test_table, schema=ms_schema)
        sql.table_to_gpkg(table = test_table, schema=ms_schema, 
                          path= os.path.join(FOLDER_PATH, gpkg, test_table), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # this should fail
        cmd_gpkg = f'ogrinfo {FOLDER_PATH}/{gpkg} -sql "SELECT id FROM {test_table} LIMIT 1" -q'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg.replace('\\','/')), stderr=subprocess.STDOUT)
        assert 'ERROR' in str(ogr_response_gpkg), "table was not overwritten in the geopackage"

        # check that the overwritten table works
        cmd_gpkg2 = f'ogrinfo {FOLDER_PATH}/{gpkg} -sql "SELECT id3 FROM {test_table} LIMIT 1" -q'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg2.replace('\\','/')), stderr=subprocess.STDOUT)
        assert 'id3 (Integer) = 3' in str(ogr_response_gpkg), "table was not overwritten in the geopackage"

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

      
    def test_table_to_gpkg_basic_pth(self):

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
        assert sql.table_exists(table = test_table, schema=ms_schema)

        # table to gpkg
        sql.table_to_gpkg(table = test_table, schema=ms_schema,
                          path= os.path.join(FOLDER_PATH, gpkg, test_table), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_table_to_gpkg_basic_pth_w_name(self):

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
        assert sql.table_exists(table = test_table, schema=ms_schema)

        # table to gpkg
        sql.table_to_gpkg(table = test_table, schema=ms_schema,
                          path= os.path.join(FOLDER_PATH, gpkg, test_table), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    def test_table_to_gpkg_basic_brackets(self):

        gpkg = test_table+'.gpkg'
        # sql.drop_table(table=test_table, schema=ms_schema)

        # create table
        sql.query(f"""
                    CREATE TABLE {ms_schema}.{test_table} (id int, [txt] text, dte datetime, geom geometry);

                    INSERT INTO {ms_schema}.{test_table}
                    (id, txt, dte, geom)
                     VALUES (1, 'test text', CURRENT_TIMESTAMP,
                     geometry::Point(1015329.1, 213793.1, 2263))
                """)
        assert sql.table_exists(table = test_table, schema=ms_schema)

        # table to gpkg
        sql.table_to_gpkg(table = test_table, schema=ms_schema,
                          path= os.path.join(FOLDER_PATH, gpkg, test_table), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, gpkg))

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(FOLDER_PATH, gpkg))

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(sql, schema=ms_schema)
        sql.cleanup_new_tables()


class TestTableToShpPg:
    def test_table_to_shp_basic(self):

        shp = 'test.shp'

        db.drop_table(schema = pg_schema, table = test_table)
        # create table
        db.query(f"""
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(table = test_table, schema=pg_schema)

        # table to shp
        db.table_to_shp(table = test_table, schema=pg_schema,
                        path= os.path.join(FOLDER_PATH, shp), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, shp))

        # clean up
        db.drop_table(pg_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(FOLDER_PATH, shp.replace('shp', ext)))

    def test_table_to_shp_basic_pth(self):

        shp = 'test.shp'

        db.drop_table(schema = pg_schema, table = test_table)
        # create table
        db.query(f"""
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(table = test_table, schema=pg_schema)

        # table to shp
        db.table_to_shp(table = test_table, schema=pg_schema,
                        path=FOLDER_PATH + '\\' + shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, shp))

        # clean up
        db.drop_table(pg_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(FOLDER_PATH, shp.replace('shp', ext)))

    def test_table_to_shp_basic_pth_w_name(self):

        shp = 'test.shp'

        db.drop_table(schema = pg_schema, table = test_table)
        # create table
        db.query(f"""
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(table = test_table, schema=pg_schema)

        # table to shp
        db.table_to_shp(table = test_table, schema=pg_schema, 
                        path= os.path.join(FOLDER_PATH, shp), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, shp))

        # clean up
        db.drop_table(pg_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(FOLDER_PATH, shp.replace('shp', ext)))

    def test_table_to_shp_basic_quotes(self):
   
        shp = test_table+'.shp'

        db.drop_table(schema = pg_schema, table = test_table)
        # create table
        db.query(f"""
            CREATE TABLE {pg_schema}.{test_table} (id int, "txt" text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(table = test_table, schema=pg_schema)

        # table to shp
        db.table_to_shp(table = test_table, schema = pg_schema,
                        path= os.path.join(FOLDER_PATH, shp), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, shp))

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(FOLDER_PATH, shp.replace('.shp', '.'+ext)))


class TestTableToShpMs:
    @classmethod
    def setup_class(cls):
        helpers.set_up_schema(sql)


    def test_table_to_shp_basic(self):

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
        assert sql.table_exists(table = test_table, schema=ms_schema)

        # table to shp
        sql.table_to_shp(table = test_table, schema=ms_schema,
                         path= os.path.join(FOLDER_PATH, shp), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, shp))

        # clean up
        sql.drop_table(ms_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(FOLDER_PATH, shp.replace('shp', ext)))
            except:
                pass

    def test_table_to_shp_basic_pth(self):

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
        assert sql.table_exists(table = test_table, schema=ms_schema)

        # table to shp
        sql.table_to_shp(table = test_table, schema=ms_schema,
                         path= os.path.join(FOLDER_PATH, shp), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, shp))

        # clean up
        sql.drop_table(ms_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(FOLDER_PATH, shp.replace('shp', ext)))
            except:
                pass

    def test_table_to_shp_basic_pth_w_name(self):

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
        assert sql.table_exists(table = test_table, schema=ms_schema)

        # table to shp
        sql.table_to_shp(table = test_table, schema=ms_schema, 
                         path= os.path.join(FOLDER_PATH, shp), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, shp))

        # clean up
        sql.drop_table(ms_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(FOLDER_PATH, shp.replace('shp', ext)))
            except:
                pass

    def test_table_to_shp_basic_brackets(self):
        
        shp = test_table+'.shp'
        sql.drop_table(table=test_table, schema=ms_schema)

        # create table
        sql.query(f"""
                    CREATE TABLE {ms_schema}.{test_table} (id int, [txt] text, dte datetime, geom geometry);

                    INSERT INTO {ms_schema}.{test_table}
                    (id, txt, dte, geom)
                     VALUES (1, 'test text', CURRENT_TIMESTAMP,
                     geometry::Point(1015329.1, 213793.1, 2263))
                """.format(s=ms_schema, t=test_table))
        assert sql.table_exists(table = test_table, schema=ms_schema)

        # table to shp
        sql.table_to_shp(table = test_table, schema=ms_schema,
                         path=os.path.join(FOLDER_PATH, shp), print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(FOLDER_PATH, shp))

        # clean up
        sql.drop_table(ms_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(FOLDER_PATH, shp.replace('shp', ext)))
            except:
                pass

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(sql, schema=ms_schema)
        sql.drop_table(schema = ms_schema, table = sql.log_table)
        sql.cleanup_new_tables()