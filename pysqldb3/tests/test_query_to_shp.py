# -*- coding: utf-8 -*-
import os
import configparser
import subprocess
import shlex

from .. import pysqldb3 as pysqldb
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

# ldap_sql = pysqldb.DbConnect(type='MS',
#                              database='CLION',
#                              server='DOTGISSQL01',
#                              ldap=True)

test_table = f'__testing_query_to_shp_{db.user}__'

ms_schema = 'risadmin'
pg_schema = 'working'


class TestQueryToShpPg:
    def test_query_to_shp_basic(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));
            INSERT INTO {pg_schema}.{test_table}
                VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.query_to_shp(f"select * from {pg_schema}.{test_table}", shp_name=shp, path=fldr, print_cmd=True, srid=2263)

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

    def test_query_to_shp_basic_pth(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));
            INSERT INTO {pg_schema}.{test_table}
                VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.query_to_shp(f"select * from {pg_schema}.{test_table}", path=f'{fldr}\\{shp}', print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(pg_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_query_to_shp_basic_pth_and_shp(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));
            INSERT INTO {pg_schema}.{test_table}
                VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp - make sure shp_name overwrites any shp in the path
        db.query_to_shp(f"select * from {pg_schema}.{test_table}", shp_name=shp, path=f'{fldr}\\test_{shp}', print_cmd=True)

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

    def test_query_to_shp_basic_pth(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));
            INSERT INTO {pg_schema}.{test_table}
                VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.query_to_shp(f"select * from {pg_schema}.{test_table}", path=f'{fldr}\\{shp}', print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(pg_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_query_to_shp_basic_pth_and_shp(self):

        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));
            INSERT INTO {pg_schema}.{test_table}
                VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp - make sure shp_name overwrites any shp in the path
        db.query_to_shp(f"select * from {pg_schema}.{test_table}", shp_name=shp, path=f'{fldr}\\test_{shp}', print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(pg_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_query_to_shp_basic_quotes(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, "txt" text, dte timestamp, geom geometry(Point));
            INSERT INTO {pg_schema}.{test_table}
                VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.query_to_shp(f"select * from {pg_schema}.{test_table}", shp_name=shp, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_query_to_shp_basic_funky_field_names(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, "t.txt" text, "1t txt" text, "t txt" text, dte timestamp, geom geometry(Point));
            INSERT INTO {pg_schema}.{test_table}
                VALUES (1, 'test text','test text','test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.query_to_shp(f"select * from {pg_schema}.{test_table}", shp_name=shp, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_query_to_shp_basic_long_names(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

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

        # table to shp
        db.query_to_shp(f"select * from {pg_schema}.{test_table}", shp_name=shp, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_query_to_shp_basic_no_data(self):

        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));
            INSERT INTO {pg_schema}.{test_table}
                VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.query_to_shp(f"select * from {pg_schema}.{test_table} limit 0", shp_name=shp, path=fldr, print_cmd=True)

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
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        lt = 'text' * 51
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
                '{lt}',
                now(), 123.456, st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)  # The shapefile maximum field width is 254 lt set to 255
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.query_to_shp(f"select * from {pg_schema}.{test_table}", shp_name=shp, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # import shp to db to compare
        db.shp_to_table(path=fldr, table=test_table + 'QA', schema=pg_schema, shp_name=shp, print_cmd=True)

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
        db.drop_table(schema=pg_schema, table=f'{test_table}qa')

        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except Exception as e:
                print(e)

    def test_query_to_shp_data_longcolumn(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        lt = 'test ' * 51
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
             '{lt}',
             now(), 123.456, st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)  # The shapefile maximum field width is 254 lt set to 255
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.query_to_shp(f"select * from {pg_schema}.{test_table}", shp_name=shp, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # import shp to db to compare
        db.shp_to_table(path=fldr, table=f'{test_table}qa', schema=pg_schema, shp_name=shp, print_cmd=True)


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
        db.drop_table(schema=pg_schema, table=f'{test_table}qa')

        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except Exception as e:
                print(e)

    def test_query_to_shp_sc(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test'

        # This is encoded as utf8 and then ogr default's to LATIN1 encoding
        db.query_to_shp(query=u"select '©' as sc",  shp_name=shp, path=fldr, print_cmd=True)

        # Check table in folder
        assert os.path.isfile(os.path.join(fldr, f'{shp}.dbf'))

        # Upload shp (with special character)
        db.shp_to_table(path=fldr, shp_name=f'{shp}.dbf', schema=pg_schema, table=test_table)

        # This will only work if ENCODED/DECODED properly; otherwise, it will be scrambled.
        # Though ogr uses LATIN1, our PG server stores things using UTF8; this is decoded and then encoded as LATIN1 to get the initial character.
        assert list(db.dfquery(f"""select sc from {pg_schema}.{test_table}""")['sc'])[0].encode('latin1') == '©'.encode('latin1')

        # clean up
        db.drop_table(pg_schema, test_table)
        os.remove(os.path.join(fldr, f'{shp}.dbf'))

    def test_query_to_shp_bad_query(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test'

        # This should fail
        try:
            db.query_to_shp(query="select * from table_does_not_exist", shp_name=shp, path=fldr, print_cmd=True)
        except:
            Failed = True
        # check table in not folder
        assert Failed
        assert not os.path.isfile(os.path.join(fldr, f'{shp}.dbf'))


class TestQueryToShpMs:
    # @classmethod
    # def setup_class(cls):
    #     helpers.set_up_schema(sql, ms_schema=ms_schema)

    def test_query_to_shp_basic(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        sql.drop_table(schema=ms_schema, table=test_table)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id int, txt text, dte datetime, geom geometry);
            INSERT INTO {ms_schema}.{test_table} (id, txt, dte, geom)
                VALUES (1, 'test text', CURRENT_TIMESTAMP, geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to shp
        sql.query_to_shp(f"select * from {ms_schema}.{test_table}", shp_name=shp, path=fldr, print_cmd=True, srid=2263)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # Manually check SRID of projection file and verify it contains 2263
        cmd = r'gdalsrsinfo {}\{}'.format(fldr, shp).replace('\\', '/')
        ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
        assert b'"EPSG",2263' in ogr_response

        # clean up
        sql.drop_table(ms_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_pth(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        sql.drop_table(schema=ms_schema, table=test_table)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id int, txt text, dte datetime, geom geometry);
            INSERT INTO {ms_schema}.{test_table} (id, txt, dte, geom)
                VALUES (1, 'test text', CURRENT_TIMESTAMP, geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to shp
        sql.query_to_shp(f"select * from {ms_schema}.{test_table}", path=f'{fldr}\\{shp}', print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(ms_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_pth_and_name(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        sql.drop_table(schema=ms_schema, table=test_table)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id int, txt text, dte datetime, geom geometry);
            INSERT INTO {ms_schema}.{test_table} (id, txt, dte, geom)
                VALUES (1, 'test text', CURRENT_TIMESTAMP, geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to shp - make sure shp_name overwrites any shp in the path
        sql.query_to_shp(f"select * from {ms_schema}.{test_table}", shp_name=shp, path=f'{fldr}\\test_{shp}', print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # Manually check SRID of projection file and verify it contains 2263
        cmd = r'gdalsrsinfo {}\{}'.format(fldr, shp).replace('\\', '/')
        ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
        assert b'"EPSG",2263' in ogr_response

        # clean up
        sql.drop_table(ms_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_pth(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        sql.drop_table(schema=ms_schema, table=test_table)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id int, txt text, dte datetime, geom geometry);
            INSERT INTO {ms_schema}.{test_table} (id, txt, dte, geom)
                VALUES (1, 'test text', CURRENT_TIMESTAMP, geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to shp
        sql.query_to_shp(f"select * from {ms_schema}.{test_table}", path=f'{fldr}\\{shp}', print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(ms_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_pth_and_name(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        sql.drop_table(schema=ms_schema, table=test_table)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id int, txt text, dte datetime, geom geometry);
            INSERT INTO {ms_schema}.{test_table} (id, txt, dte, geom)
                VALUES (1, 'test text', CURRENT_TIMESTAMP,
                geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to shp - make sure shp_name overwrites any shp in the path
        sql.query_to_shp(f"select * from {ms_schema}.{test_table}", shp_name=shp, path=f'{fldr}\\test_{shp}', print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(ms_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
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
        schema = 'dbo'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        sql.drop_table(schema=schema, table=test_table)

        # create table
        sql.query(f"""
            CREATE TABLE {schema}.{test_table} (id int, [txt] text, dte datetime, geom geometry);
            INSERT INTO {schema}.{test_table}
            (id, txt, dte, geom)
                VALUES (1, 'test text', CURRENT_TIMESTAMP,
                geometry::Point(1015329.1, 213793.1, 2263))
        """)
        assert sql.table_exists(test_table, schema=schema)

        # table to shp
        sql.query_to_shp(f"select * from {schema}.{test_table}", shp_name=shp, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_funky_field_names(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id int, [t.txt] text, [1t txt] text, [t_txt] text, dte datetime, geom geometry);
            INSERT INTO {ms_schema}.{test_table} (id, [t.txt], [1t txt], [t_txt], dte, geom)
                VALUES (1, 'test text','test text','test text', CURRENT_TIMESTAMP, geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to shp
        sql.query_to_shp(f"select * from {ms_schema}.{test_table}", shp_name=shp, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(ms_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_long_names(self):
        schema = 'dbo'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        sql.drop_table(schema=schema, table=test_table)

        # create table
        sql.query(f"""
            CREATE TABLE {schema}.{test_table} (id_name_one int,
            [123text name one] text,
            [text@name-two~three four five six seven] text,
            current_date_time datetime,
            [x-coord] float,
            geom geometry);

            INSERT INTO {schema}.{test_table}
            VALUES (1, 'test text', 'test text', CURRENT_TIMESTAMP,
            123.456, geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=schema)

        # table to shp
        sql.query_to_shp(f"select * from {schema}.{test_table}", shp_name=shp, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_basic_no_data(self):
        schema = 'dbo'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        sql.drop_table(schema=schema, table=test_table)
        assert not sql.table_exists(table=test_table, schema=schema)

        # create table
        sql.query(f"""
            CREATE TABLE {schema}.{test_table} (id int, txt text, dte datetime, geom geometry);
            INSERT INTO {schema}.{test_table}
                 VALUES (1, 'test text', cast(CURRENT_TIMESTAMP as datetime), geometry::Point(1015329.1, 213793.1, 2263 ))
        """)

        assert sql.table_exists(test_table, schema=schema)

        # table to shp
        sql.query_to_shp(f"select top 0 * from {schema}.{test_table}", shp_name=shp, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_data(self):
        schema = 'dbo'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        sql.drop_table(schema, test_table)
        sql.drop_table(schema, f'{test_table}qa')

        # create table
        lt = 'test ' * 51
        sql.query(f"""
            CREATE TABLE {schema}.{test_table} (fld1 int,
            fld2 varchar(MAX),
            fld3 varchar(MAX),
            fld4 datetime,
            fld5 float,
            fld6 geometry);

            INSERT INTO {schema}.{test_table}
                VALUES (1,
                'test text',
                '{lt}',
                CURRENT_TIMESTAMP, 123.456, geometry::Point(1015329.1, 213793.1, 2263 ))
        """)  # The shapefile maximum field width is 254 lt set to 255
        assert sql.table_exists(test_table, schema=schema)

        # table to shp
        sql.query_to_shp(f"select * from {schema}.{test_table}", shp_name=shp, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # import shp to db to compare
        sql.shp_to_table(path=fldr, table=f'{test_table}qa', schema=schema, shp_name=shp, print_cmd=True)

        sql.query(f"""
        select
            case when t1.fld2 = t2.fld2 then 1 else 0 end,
            case when left(t1.fld3, 254) = t2.fld3 then 1 else 0 end,
            case when cast(t1.fld4 as date)=t2.fld4_dt then 1 else 0 end, -- shapefiles cannot store datetimes
            case when cast(t1.fld4 as time)=t2.fld4_tm then 1 else 0 end, -- shapefiles cannot store datetimes
            case when t1.fld5 = t2.fld5 then 1 else 0 end,
            case when t1.fld6.STDistance(t2.geom) < 1  then 1 else 0 end-- default name from pysqldb
        from {schema}.{test_table} t1
        join {schema}.{test_table}qa t2
            on t1.fld1=t2.fld1
        """)
        assert set(sql.data[0]) == {1}

        # clean up
        sql.drop_table(schema, test_table)
        sql.drop_table(schema, test_table + 'qa')

        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_data_long(self):
        schema = 'dbo'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        sql.drop_table(schema, test_table)
        sql.drop_table(schema, f'{test_table}qa')

        # create table
        lt = 'test ' * 51
        sql.query(f"""
            CREATE TABLE {schema}.{test_table} (fld1 int,
                fld2 varchar(MAX),
                fld3 varchar(MAX),
                longfld4 datetime,
                fld5 float,
                fld6 geometry);

            INSERT INTO {schema}.{test_table}
            VALUES (1,
                'test text',
                '{lt}',
                CURRENT_TIMESTAMP, 123.456, geometry::Point(1015329.1, 213793.1, 2263 ))
        """)  # The shapefile maximum field width is 254 lt set to 255
        assert sql.table_exists(test_table, schema=schema)

        # table to shp
        sql.query_to_shp(f"select * from {schema}.{test_table}", shp_name=shp, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # import shp to db to compare
        sql.shp_to_table(path=fldr, table=f'{test_table}qa', schema=schema, shp_name=shp, print_cmd=True)

        sql.query(f"""
        select
            case when t1.fld2 = t2.fld2 then 1 else 0 end,
            case when left(t1.fld3, 254) = t2.fld3 then 1 else 0 end,
            case when cast(t1.longfld4 as date)=t2.longfld_dt then 1 else 0 end, -- shapefiles cannot store datetimes
            case when cast(t1.longfld4 as time)=t2.longfld_tm then 1 else 0 end, -- shapefiles cannot store datetimes
            case when t1.fld5 = t2.fld5 then 1 else 0 end,
            case when t1.fld6.STDistance(t2.geom) < 1  then 1 else 0 end-- default name from pysqldb
        from {schema}.{test_table} t1
            join {schema}.{test_table}qa t2
            on t1.fld1=t2.fld1
        """)
        assert set(sql.data[0]) == {1}

        # clean up
        sql.drop_table(schema, test_table)
        sql.drop_table(schema, f'{test_table}qa')

        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_query_to_shp_sc(self):
        schema = 'dbo'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test'

        # This is encoded in UTF8 and then uses ogr's SQL default LATIN1
        sql.query_to_shp(query=u"select '©' as sc",  shp_name=shp, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, f'{shp}.dbf'))

        # Upload shp (with special character)
        sql.shp_to_table(path=fldr, shp_name=f'{shp}.dbf', schema=schema, table=test_table)

        # This will only work if ENCODED/DECODED properly; otherwise, it will be scrambled.
        # ogr and SQL Server use/default to LATIN1; thus, encoding our string in LATIN1 will result in the correct character
        assert (list(sql.dfquery(f"select sc from {schema}.{test_table}")['sc'])[0]).encode('latin1') == '©'.encode('latin1')

        # clean up
        sql.drop_table(schema, test_table)
        os.remove(os.path.join(fldr, f'{shp}.dbf'))

    def test_query_to_shp_bad_query(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test'

        # This should fail
        try:
            sql.query_to_shp(query="select * from table_does_not_exist", shp_name=shp, path=fldr, print_cmd=True)
        except:
            Failed = True
        # check table in not folder
        assert Failed
        assert not os.path.isfile(os.path.join(fldr, f'{shp}.dbf'))

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(sql, schema=ms_schema)
        sql.query(f"drop table {ms_schema}.{sql.log_table}")
        sql.cleanup_new_tables()
        # helpers.clean_up_schema(sql, ms_schema)
