# -*- coding: utf-8 -*-
import os
import configparser
import subprocess
import shlex

from .. import pysqldb3 as pysqldb
from . import helpers

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

pg_dbconn = pysqldb.DbConnect(db_type=config.get('PG_DB', 'TYPE'),
                              host=config.get('PG_DB', 'SERVER'),
                              db_name=config.get('PG_DB', 'DB_NAME'),
                              username=config.get('PG_DB', 'DB_USER'),
                              password=config.get('PG_DB', 'DB_PASSWORD'),
                              allow_temp_tables=True
                              )

ms_dbconn = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                              host=config.get('SQL_DB', 'SERVER'),
                              db_name=config.get('SQL_DB', 'DB_NAME'),
                              username=config.get('SQL_DB', 'DB_USER'),
                              password=config.get('SQL_DB', 'DB_PASSWORD'),
                              allow_temp_tables=True)

# ldap_sql = pysqldb.DbConnect(db_type='MS',
#                              database='CLION',
#                              server='DOTGISSQL01',
#                              ldap=True)

test_table = '__testing_query_to_shp_{user}__'.format(user=pg_dbconn.username)

ms_schema_name = 'risadmin'
pg_schema_name = 'working'


class TestQueryToShpPg:
    def test_query_to_shp_basic(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        pg_dbconn.query("""
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {schema}.{table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(schema=pg_schema_name, table=test_table))
        assert pg_dbconn.table_exists(table_name=test_table, schema_name=pg_schema_name)

        # table to shp
        pg_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=pg_schema_name, table=test_table), shpfile_name=shp, path=dir, print_cmd=True, srid=2263)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # Manually check SRID of projection file and verify it contains 2263
        cmd = r'gdalsrsinfo {dir}\{shp}'.format(dir=dir, shp=shp).replace('\\', '/')
        ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
        assert b'"EPSG",2263' in ogr_response

        # clean up
        pg_dbconn.drop_table(schema_name=pg_schema_name, table_name=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(dir, shp.replace('shp', ext)))

    def test_query_to_shp_basic_pth(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        pg_dbconn.query("""
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {schema}.{table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(schema=pg_schema_name, table=test_table))
        assert pg_dbconn.table_exists(table_name=test_table, schema_name=pg_schema_name)

        # table to shp
        pg_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=pg_schema_name, table=test_table), path=dir + '\\' + shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        pg_dbconn.drop_table(pg_schema_name, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(dir, shp.replace('shp', ext)))

    def test_query_to_shp_basic_pth_and_shp(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        pg_dbconn.query("""
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {schema}.{table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(schema=pg_schema_name, table=test_table))
        assert pg_dbconn.table_exists(table_name=test_table, schema_name=pg_schema_name)

        # table to shp - make sure shp_name overwrites any shp in the path
        pg_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=pg_schema_name, table=test_table), shpfile_name=shp,
                               path=f'{dir}\\test_{shp}', print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # Manually check SRID of projection file and verify it contains 2263
        cmd = r'gdalsrsinfo {dir}\{shp}'.format(dir=dir, shp=shp).replace('\\', '/')
        ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
        assert b'"EPSG",2263' in ogr_response

        # clean up
        pg_dbconn.drop_table(pg_schema_name, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(dir, shp.replace('shp', ext)))

    def test_query_to_shp_basic_pth(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        pg_dbconn.query("""
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {schema}.{table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(schema=pg_schema_name, table=test_table))
        assert pg_dbconn.table_exists(table_name=test_table, schema_name=pg_schema_name)

        # table to shp
        pg_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=pg_schema_name, table=test_table),
                               path=dir + '\\' + shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        pg_dbconn.drop_table(pg_schema_name, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(dir, shp.replace('shp', ext)))

    def test_query_to_shp_basic_pth_and_shp(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        pg_dbconn.query("""
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {schema}.{table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(schema=pg_schema_name, table=test_table))
        assert pg_dbconn.table_exists(table_name=test_table, schema_name=pg_schema_name)

        # table to shp - make sure shp_name overwrites any shp in the path
        pg_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=pg_schema_name, table=test_table), shpfile_name=shp,
                               path=dir+'\\'+'test_'+shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        pg_dbconn.drop_table(schema_name=pg_schema_name, table_name=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(dir, shp.replace('shp', ext)))

    def test_query_to_shp_basic_quotes(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        pg_dbconn.query("""
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table} (id int, "txt" text, dte timestamp, geom geometry(Point));

            INSERT INTO {schema}.{table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(schema=pg_schema_name, table=test_table))
        assert pg_dbconn.table_exists(table_name=test_table, schema_name=pg_schema_name)

        # table to shp
        pg_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=pg_schema_name, table=test_table),
                               shpfile_name=shp, path=dir, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        pg_dbconn.drop_table(schema_name=pg_schema_name, table_name=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(dir, shp.replace('shp', ext)))

    def test_query_to_shp_basic_funky_field_names(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        pg_dbconn.query("""
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table} (id int, "t.txt" text, "1t txt" text, "t txt" text, dte timestamp, geom geometry(Point));

            INSERT INTO {schema}.{table}
             VALUES (1, 'test text','test text','test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(schema=pg_schema_name, table=test_table))
        assert pg_dbconn.table_exists(table_name=test_table, schema_name=pg_schema_name)

        # table to shp
        pg_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=pg_schema_name, table=test_table), shpfile_name=shp, path=dir, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        pg_dbconn.drop_table(schema_name=pg_schema_name, table_name=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(dir, shp.replace('shp', ext)))

    def test_query_to_shp_basic_long_names(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        pg_dbconn.query("""
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table} (id_name_one int, "123text name one" text,
            "text@name-two~three four five six seven" text,
            current_date_time timestamp,
            "x-coord" float,
            geom geometry(Point));

            INSERT INTO {schema}.{table}
             VALUES (1, 'test text', 'test text', now(), 123.456, st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(schema=pg_schema_name, table=test_table))
        assert pg_dbconn.table_exists(table_name=test_table, schema_name=pg_schema_name)

        # table to shp
        pg_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=pg_schema_name, table=test_table), shpfile_name=shp, path=dir, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        pg_dbconn.drop_table(schema_name=pg_schema_name, table_name=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(dir, shp.replace('shp', ext)))

    def test_query_to_shp_basic_no_data(self):

        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        pg_dbconn.query("""
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {schema}.{table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(schema=pg_schema_name, table=test_table))
        assert pg_dbconn.table_exists(table_name=test_table, schema_name=pg_schema_name)

        # table to shp
        pg_dbconn.query_to_shp("select * from {schema}.{table} limit 0".format(schema=pg_schema_name, table=test_table),
                               shpfile_name=shp, path=dir, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        pg_dbconn.drop_table(schema_name=pg_schema_name, table_name=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(dir, shp.replace('shp', ext)))
            except Exception as e:
                print(e)

    def test_query_to_shp_data(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        pg_dbconn.query("""
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table} (fld1 int,
            fld2 text,
            fld3 text,
            fld4 timestamp,
            fld5 float,
            fld6 geometry(Point));

            INSERT INTO {schema}.{table}
             VALUES (1,
             'test text',
             '{long_string}',
             now(), 123.456, st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(schema=pg_schema_name, table=test_table, long_string='test ' * 51))  # The shapefile maximum field width is 254 lt set to 255
        assert pg_dbconn.table_exists(test_table, schema=pg_schema_name)

        # table to shp
        pg_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=pg_schema_name, table=test_table), shpfile_name=shp, path=dir, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # import shp to db to compare
        pg_dbconn.shp_to_table(path=dir, table_name=test_table + 'QA', schema_name=pg_schema_name,
                               shpfile_name=shp, print_cmd=True)

        pg_dbconn.query("""
        select
            t1.fld2 = t2.fld2,
            left(t1.fld3, 254) = t2.fld3,
            t1.fld4::date = t2.fld4_dt, -- shapefiles cannot store datetimes
            t1.fld4::time = t2.fld4_tm::time, -- shapefiles cannot store datetimes
            t1.fld5 = t2.fld5,
            st_distance(t1.fld6, t2.geom) < 1 -- deafult name from pysqldb
        from {schema}.{table} t1
        join {schema}.{table}QA t2
        on t1.fld1=t2.fld1
        """.format(schema=pg_schema_name, table=test_table))
        assert set(pg_dbconn.data[0]) == {True}

        # clean up
        pg_dbconn.drop_table(schema_name=pg_schema_name, table_name=test_table)
        pg_dbconn.drop_table(schema_name=pg_schema_name, table_name=test_table + 'QA')

        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(dir, shp.replace('shp', ext)))
            except Exception as e:
                print(e)

    def test_query_to_shp_data_longcolumn(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        pg_dbconn.query("""
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table} (fld1 int,
            fld2 text,
            fld3 text,
            longfld4 timestamp,
            fld5 float,
            fld6 geometry(Point));

            INSERT INTO {schema}.{table}
             VALUES (1,
             'test text',
             '{long_string}',
             now(), 123.456, st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(schema=pg_schema_name, table=test_table, long_string='test ' * 51))  # The shapefile maximum field width is 254 lt set to 255
        assert pg_dbconn.table_exists(table_name=test_table, schema_name=pg_schema_name)

        # table to shp
        pg_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=pg_schema_name, table=test_table),
                               shpfile_name=shp, path=dir, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # import shp to db to compare
        pg_dbconn.shp_to_table(path=dir, table_name=test_table + 'QA', schema_name=pg_schema_name,
                               shpfile_name=shp, print_cmd=True)

        pg_dbconn.query("""
        select
            t1.fld2 = t2.fld2,
            left(t1.fld3, 254) = t2.fld3,
            t1.longfld4::date = t2.longfld_dt, -- shapefiles cannot store datetimes
            t1.longfld4::time = t2.longfld_tm::time, -- shapefiles cannot store datetimes
            t1.fld5 = t2.fld5,
            st_distance(t1.fld6, t2.geom) < 1 -- deafult name from pysqldb
        from {schema}.{table} t1
        join {schema}.{table}QA t2
        on t1.fld1=t2.fld1
        """).format(schema=pg_schema_name, table_name=test_table)
        assert set(pg_dbconn.data[0]) == {True}

        # clean up
        pg_dbconn.drop_table(schema_name=pg_schema_name, table_name=test_table)
        pg_dbconn.drop_table(schema_name=pg_schema_name, table_name=test_table + 'QA')

        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(dir, shp.replace('shp', ext)))
            except Exception as e:
                print(e)

    def test_query_to_shp_sc(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test'

        # This is encoded as utf8 and then ogr default's to LATIN1 encoding
        pg_dbconn.query_to_shp(query=u"select '©' as sc", shpfile_name=shp, path=dir, print_cmd=True)

        # Check table in folder
        assert os.path.isfile(os.path.join(dir, shp + '.dbf'))

        # Upload shp (with special character)
        pg_dbconn.shp_to_table(path=dir, shpfile_name=shp + '.dbf', schema_name=pg_schema_name, table_name=test_table)

        # This will only work if ENCODED/DECODED properly; otherwise, it will be scrambled.
        # Though ogr uses LATIN1, our PG server stores things using UTF8; this is decoded and then encoded as LATIN1 to get the initial character.
        assert list(pg_dbconn.dfquery("""select sc from {schema}.{table}""".format(schema=pg_schema_name, table=test_table))['sc'])[0].encode('latin1') == '©'.encode('latin1')

        # clean up
        pg_dbconn.drop_table(schema_name=pg_schema_name, table_name=test_table)
        os.remove(os.path.join(dir, shp + '.dbf'))

    def test_query_to_shp_bad_query(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test'

        # This should fail
        try:
            pg_dbconn.query_to_shp(query="select * from table_does_not_exist", shpfile_name=shp, path=fldr, print_cmd=True)
        except:
            fail = True
        # check table in not folder
        assert fail
        assert not os.path.isfile(os.path.join(fldr, shp + '.dbf'))


class TestQueryToShpMs:
    # @classmethod
    # def setup_class(cls):
    #     helpers.set_up_schema(sql, ms_schema=ms_schema)

    def test_query_to_shp_basic(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        ms_dbconn.drop_table(schema_name=ms_schema_name, table_name=test_table)

        # create table
        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {schema}.{table}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """.format(schema=ms_schema_name, table=test_table))
        assert ms_dbconn.table_exists(table_name=test_table, schema_name=ms_schema_name)

        # table to shp
        ms_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=ms_schema_name, table=test_table), shpfile_name=shp, path=dir, print_cmd=True, srid=2263)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # Manually check SRID of projection file and verify it contains 2263
        cmd = r'gdalsrsinfo {dir}\{shpfile}'.format(dir=dir, shpfile=shp).replace('\\', '/')
        ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
        assert b'"EPSG",2263' in ogr_response

        # clean up
        ms_dbconn.drop_table(ms_schema_name, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(dir, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_pth(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        ms_dbconn.drop_table(schema_name=ms_schema_name, table_name=test_table)

        # create table
        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {schema}.{table}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """.format(schema=ms_schema_name, table=test_table))
        assert ms_dbconn.table_exists(table_name=test_table, schema_name=ms_schema_name)

        # table to shp
        ms_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=ms_schema_name, table=test_table), path=dir + '\\' + shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        ms_dbconn.drop_table(ms_schema_name, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(dir, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_pth_and_name(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        ms_dbconn.drop_table(schema_name=ms_schema_name, table_name=test_table)

        # create table
        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {schema}.{table}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """.format(schema=ms_schema_name, table=test_table))
        assert ms_dbconn.table_exists(table_name=test_table, schema_name=ms_schema_name)

        # table to shp - make sure shp_name overwrites any shp in the path
        ms_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=ms_schema_name, table=test_table), shpfile_name=shp,
                               path=f'{dir}\\test_{shp}', print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # Manually check SRID of projection file and verify it contains 2263
        cmd = r'gdalsrsinfo {dir}\{shp}'.format(dir=dir, shp=shp).replace('\\', '/')
        ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
        assert b'"EPSG",2263' in ogr_response

        # clean up
        ms_dbconn.drop_table(ms_schema_name, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(dir, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_pth(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        ms_dbconn.drop_table(schema_name=ms_schema_name, table_name=test_table)

        # create table
        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {schema}.{table}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """.format(schema=ms_schema_name, table=test_table))
        assert ms_dbconn.table_exists(table_name=test_table, schema_name=ms_schema_name)

        # table to shp
        ms_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=ms_schema_name, table=test_table),
                         f'{dir}\\{shp}', print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        ms_dbconn.drop_table(ms_schema_name, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(dir, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_pth_and_name(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        ms_dbconn.drop_table(schema_name=ms_schema_name, table_name=test_table)

        # create table
        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {schema}.{table}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """.format(schema=ms_schema_name, table=test_table))
        assert ms_dbconn.table_exists(table_name=test_table, schema_name=ms_schema_name)

        # table to shp - make sure shp_name overwrites any shp in the path
        ms_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=ms_schema_name, table=test_table),
                               shpfile_name=shp, path=dir + '\\' + 'test_' + shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        ms_dbconn.drop_table(ms_schema_name, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(dir, shp.replace('shp', ext)))
            except:
                pass

    # def test_ldap(self): # todo: need ldap db to test
    #     fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
    #     shp = 'test.shp'
    #
    #     ldap_sql.query_to_shp("""
    #     select 1 as test_col1, 2 as test_col2, geometry::Point(985831.79200444, 203371.60461367, 2263) as geom
    #     union all
    #     select 3 as test_col1, 4 as test_col2, geometry::Point(985831.79200444, 203371.60461367, 2263) as geom
    #     """, shp_name=shp, path=fldr, print_cmd=True)
    #
    #     assert os.path.isfile(os.path.join(fldr, shp))
    #
    #     for ext in ('dbf', 'prj', 'shx', 'shp'):
    #         try:
    #             os.remove(os.path.join(fldr, shp.replace('shp', ext)))
    #         except Exception as e:
    #             print(e)
    #
    #     ldap_sql.drop_table(schema=ldap_sql.default_schema, table='test_table')

    def test_query_to_shp_basic_brackets(self):
        schema_name = 'dbo'
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        ms_dbconn.drop_table(schema_name=schema_name, table_name=test_table)

        # create table
        ms_dbconn.query("""
                    CREATE TABLE {schema}.{table} (id int, [txt] text, dte datetime, geom geometry);

                    INSERT INTO {schema}.{table} (id, txt, dte, geom) VALUES (1, 'test text', CURRENT_TIMESTAMP,
                    geometry::Point(1015329.1, 213793.1, 2263))
                """.format(schema=schema_name, table=test_table))
        assert ms_dbconn.table_exists(table_name=test_table, schema_name=schema_name)

        # table to shp
        ms_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=schema_name, table=test_table), shpfile_name=shp,
                               path=dir, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        ms_dbconn.drop_table(schema_name=schema_name, table_name=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(dir, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_funky_field_names(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (id int, [t.txt] text, [1t txt] text, [t_txt] text, dte datetime, geom geometry);

            INSERT INTO {schema}.{table}
            (id, [t.txt], [1t txt], [t_txt], dte, geom)
            VALUES (1, 'test text','test text','test text', CURRENT_TIMESTAMP,
            geometry::Point(1015329.1, 213793.1, 2263 ))
        """.format(schema=ms_schema_name, table=test_table))
        assert ms_dbconn.table_exists(table_name=test_table, schema_name=ms_schema_name)

        # table to shp
        ms_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=ms_schema_name, table=test_table),
                               shpfile_name=shp, path=dir, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        ms_dbconn.drop_table(schema_name=ms_schema_name, table_name=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(dir, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_long_names(self):
        schema = 'dbo'
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        ms_dbconn.drop_table(schema_name=schema, table_name=test_table)

        # create table
        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (id_name_one int,
            [123text name one] text,
            [text@name-two~three four five six seven] text,
            current_date_time datetime,
            [x-coord] float,
            geom geometry);

            INSERT INTO {schema}.{table}
            VALUES (1, 'test text', 'test text', CURRENT_TIMESTAMP,
            123.456, geometry::Point(1015329.1, 213793.1, 2263 ))
        """.format(schema=schema, table=test_table))
        assert ms_dbconn.table_exists(table_name=test_table, schema_name=schema)

        # table to shp
        ms_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=schema, table=test_table),
                               shpfile_name=shp, path=dir, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        ms_dbconn.drop_table(schema_name=schema, table_name=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(dir, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_no_data(self):
        schema = 'dbo'
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        ms_dbconn.drop_table(schema_name=schema, table_name=test_table)
        assert not ms_dbconn.table_exists(table_name=test_table, schema_name=schema)

        # create table
        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {schema}.{table}
                 VALUES (1, 'test text', cast(CURRENT_TIMESTAMP as datetime), geometry::Point(1015329.1, 213793.1, 2263))
        """.format(schema=schema, table=test_table))

        assert ms_dbconn.table_exists(table_name=test_table, schema_name=schema)

        # table to shp
        ms_dbconn.query_to_shp("select top 0 * from {schema}.{table}".format(schema=schema, table=test_table),
                               shpfile_name=shp, path=dir, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        ms_dbconn.drop_table(schema_name=schema, table_name=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(dir, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_data(self):
        schema = 'dbo'
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        ms_dbconn.drop_table(schema_name=schema, table_name=test_table)
        ms_dbconn.drop_table(schema_name=schema, table_name=test_table + 'QA')

        # create table
        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (fld1 int,
            fld2 varchar(MAX),
            fld3 varchar(MAX),
            fld4 datetime,
            fld5 float,
            fld6 geometry);

            INSERT INTO {schema}.{table}
             VALUES (1,
             'test text',
             '{long_table}',
             CURRENT_TIMESTAMP, 123.456, geometry::Point(1015329.1, 213793.1, 2263 ))
        """.format(schema=schema, table=test_table, long_table='test ' * 51))  # The shapefile maximum field width is 254 lt set to 255
        assert ms_dbconn.table_exists(table_name=test_table, schema_name=schema)

        # table to shp
        ms_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=schema, table=test_table), shpfile_name=shp, path=dir, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # import shp to db to compare
        ms_dbconn.shp_to_table(path=dir, table_name=test_table + 'QA', schema_name=schema, shpfile_name=shp, print_cmd=True)

        ms_dbconn.query("""
        select
            case when t1.fld2 = t2.fld2 then 1 else 0 end,
            case when left(t1.fld3, 254) = t2.fld3 then 1 else 0 end,
            case when cast(t1.fld4 as date)=t2.fld4_dt then 1 else 0 end, -- shapefiles cannot store datetimes
            case when cast(t1.fld4 as time)=t2.fld4_tm then 1 else 0 end, -- shapefiles cannot store datetimes
            case when t1.fld5 = t2.fld5 then 1 else 0 end,
            case when t1.fld6.STDistance(t2.geom) < 1  then 1 else 0 end-- default name from pysqldb
        from {schema}.{table} t1
        join {schema}.{table}QA t2
        on t1.fld1=t2.fld1
        """.format(schema=schema, table=test_table))
        assert set(ms_dbconn.data[0]) == {1}

        # clean up
        ms_dbconn.drop_table(schema_name=schema, table_name=test_table)
        ms_dbconn.drop_table(schema_name=schema, table_name=test_table + 'QA')

        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(dir, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_data_long(self):
        schema = 'dbo'
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        ms_dbconn.drop_table(schema_name=schema, table_name=test_table)
        ms_dbconn.drop_table(schema_name=schema, table_name=test_table + 'QA')

        # create table
        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (fld1 int,
            fld2 varchar(MAX),
            fld3 varchar(MAX),
            longfld4 datetime,
            fld5 float,
            fld6 geometry);

            INSERT INTO {schema}.{table}
             VALUES (1,
             'test text',
             '{long_table}',
             CURRENT_TIMESTAMP, 123.456, geometry::Point(1015329.1, 213793.1, 2263 ))
        """.format(schema=schema, table=test_table, long_table='test ' * 51))  # The shapefile maximum field width is 254 lt set to 255
        assert ms_dbconn.table_exists(table_name=test_table, schema_name=schema)

        # table to shp
        ms_dbconn.query_to_shp("select * from {schema}.{table}".format(schema=schema, table=test_table), shpfile_name=shp, path=dir, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # import shp to db to compare
        ms_dbconn.shp_to_table(path=dir, table_name=test_table + 'QA', schema_name=schema, shpfile_name=shp, print_cmd=True)

        ms_dbconn.query("""
        select
            case when t1.fld2 = t2.fld2 then 1 else 0 end,
            case when left(t1.fld3, 254) = t2.fld3 then 1 else 0 end,
            case when cast(t1.longfld4 as date)=t2.longfld_dt then 1 else 0 end, -- shapefiles cannot store datetimes
            case when cast(t1.longfld4 as time)=t2.longfld_tm then 1 else 0 end, -- shapefiles cannot store datetimes
            case when t1.fld5 = t2.fld5 then 1 else 0 end,
            case when t1.fld6.STDistance(t2.geom) < 1  then 1 else 0 end-- default name from pysqldb
        from {schema}.{table} t1
        join {schema}.{table}QA t2
        on t1.fld1=t2.fld1
        """.format(schema=schema, table=test_table))
        assert set(ms_dbconn.data[0]) == {1}

        # clean up
        ms_dbconn.drop_table(schema_name=schema, table_name=test_table)
        ms_dbconn.drop_table(schema_name=schema, table_name=test_table + 'QA')

        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(dir, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_sc(self):
        schema = 'dbo'
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test'

        # This is encoded in UTF8 and then uses ogr's SQL default LATIN1
        ms_dbconn.query_to_shp(query=u"select '©' as sc", shpfile_name=shp, path=dir, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp + '.dbf'))

        # Upload shp (with special character)
        ms_dbconn.shp_to_table(path=dir, shpfile_name=shp + '.dbf', schema_name=schema, table_name=test_table)

        # This will only work if ENCODED/DECODED properly; otherwise, it will be scrambled.
        # ogr and SQL Server use/default to LATIN1; thus, encoding our string in LATIN1 will result in the correct character
        assert (list(ms_dbconn.dfquery("""select sc from {schema}.{table}""".format(
            schema=schema, table=test_table))['sc'])[0]).encode('latin1') == '©'.encode('latin1')

        # clean up
        ms_dbconn.drop_table(schema_name=schema, table_name=test_table)
        os.remove(os.path.join(dir, shp + '.dbf'))

    def test_query_to_shp_bad_query(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test'

        # This should fail
        try:
            ms_dbconn.query_to_shp(query="select * from table_does_not_exist", shpfile_name=shp, path=dir, print_cmd=True)
        except:
            fail = True
        # check table in not folder
        assert fail
        assert not os.path.isfile(os.path.join(dir, shp + '.dbf'))

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(ms_dbconn, schema=ms_schema_name)
        ms_dbconn.query("drop table {schema}.{table}".format(schema=ms_schema_name, table=ms_dbconn.log_table))
        ms_dbconn.cleanup_new_tables()
        # helpers.clean_up_schema(sql, ms_schema)

