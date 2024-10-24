import os
import configparser
from .. import pysqldb3 as pysqldb
from . import helpers
import shlex
import subprocess

#################################################################################
                        # only doing basic tests since it just calls
                        # query_to_gpkg and that is already tested
#################################################################################


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

test_table = '__testing_query_to_gpkg_{}__'.format(db.user)
ms_schema = 'risadmin'
pg_schema = 'working'


class TestTableToGpkgPg:
    def test_table_to_gpkg_basic(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        gpkg = 'test.gpkg'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to gpkg
        db.table_to_gpkg(test_table, schema=pg_schema, gpkg_name=gpkg, gpkg_tbl = test_table, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, gpkg))

        # clean up
        db.drop_table(pg_schema, test_table)
        os.remove(os.path.join(fldr, gpkg))

    def test_table_to_gpkg_muiltitable(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        gpkg = 'test.gpkg'
        
        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to gpkg
        db.table_to_gpkg(test_table, schema=pg_schema, gpkg_name=gpkg, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, gpkg))

        # create second table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id_test int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (2, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to gpkg
        db.table_to_gpkg(test_table, schema=pg_schema, gpkg_name=gpkg, gpkg_tbl = f'{test_table}_2', path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, gpkg))

        # check the number of tables within the geopackage
        cmd_gpkg_1 = f'ogrinfo "{fldr}/{gpkg}" -sql "SELECT id FROM {test_table} LIMIT 1" -q '
        cmd_gpkg_2 = f'ogrinfo "{fldr}/{gpkg}" -sql "SELECT id_test FROM {test_table}_2 LIMIT 1" -q'

        ogr_response_gpkg_1 = subprocess.check_output(shlex.split(cmd_gpkg_1), stderr=subprocess.STDOUT)
        ogr_response_gpkg_2 = subprocess.check_output(shlex.split(cmd_gpkg_2), stderr=subprocess.STDOUT)

        assert 'id (Integer) = 1' in str(ogr_response_gpkg_1) and 'id_test (Integer) = 2' in str(ogr_response_gpkg_2), "Cannot find 2 tables in the same geopackage"

        # clean up
        db.drop_table(pg_schema, test_table)
        os.remove(os.path.join(fldr, gpkg))

    def test_table_gpkg_overwrite(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        gpkg = 'test.gpkg'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to gpkg
        db.table_to_gpkg(test_table, schema=pg_schema, gpkg_name=gpkg, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, gpkg))

        # create a slightly different table and overwrite this
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id2 int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (2, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # overwrite the table with the same name, but the table has different values
        db.table_to_gpkg(table = test_table, schema=pg_schema, gpkg_name=gpkg, path=fldr, print_cmd=True)

        # this should fail
        cmd_gpkg = f'ogrinfo ./test_data/{gpkg} -sql "SELECT id FROM {test_table} LIMIT 1" -q'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT)
        assert 'ERROR' in str(ogr_response_gpkg), "table was not overwritten in the geopackage"

        # check that the overwritten table works
        cmd_gpkg2 = f'ogrinfo ./test_data/{gpkg} -sql "SELECT id2 FROM {test_table} LIMIT 2" -q'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg2), stderr=subprocess.STDOUT)
        assert 'id2 (Integer) = 2' in str(ogr_response_gpkg), "table was not overwritten in the geopackage"

        # clean up
        db.drop_table(pg_schema, test_table)
        os.remove(os.path.join(fldr, gpkg))

    def test_table_to_gpkg_basic_pth(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
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
        db.table_to_gpkg(test_table, schema=pg_schema, path=fldr + '\\' + gpkg, gpkg_name = gpkg, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, gpkg))

        # clean up
        db.drop_table(pg_schema, test_table)
        os.remove(os.path.join(fldr, gpkg))

    def test_table_to_gpkg_basic_pth_w_name(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
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
        db.table_to_gpkg(test_table, schema=pg_schema, gpkg_name=gpkg, path=fldr + '\\' + gpkg, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, gpkg))

        # clean up
        db.drop_table(pg_schema, test_table)
        os.remove(os.path.join(fldr, gpkg))
        

    def test_table_to_gpkg_basic_quotes(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        gpkg = test_table+'.gpkg'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{test_table};
            CREATE TABLE {pg_schema}.{test_table} (id int, "txt" text, dte timestamp, geom geometry(Point));

            INSERT INTO {pg_schema}.{test_table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=pg_schema)

        # table to gpkg
        db.table_to_gpkg(table = test_table, schema = pg_schema, path=fldr, gpkg_name=gpkg, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, gpkg))

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        os.remove(os.path.join(fldr, gpkg))


class TestTableTogpkgMs:
    @classmethod
    def setup_class(cls):
        helpers.set_up_schema(sql)


    def test_table_to_gpkg_basic(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
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
        sql.table_to_gpkg(test_table, schema=ms_schema, gpkg_name=gpkg, gpkg_tbl = test_table, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, gpkg))

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(fldr, gpkg))

    def test_table_to_gpkg_multitable(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        gpkg = 'testgpkg.gpkg'
        sql.drop_table(schema=ms_schema, table=test_table)

        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to gpkg
        sql.table_to_gpkg(table = test_table, schema=ms_schema, gpkg_name=gpkg, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, gpkg))
        
        # create the second table
        sql.drop_table(schema=ms_schema, table=test_table + '_2')
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table}_2 (id_test int, txt text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table}_2
            (id_test, txt, dte, geom)
             VALUES (2, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=ms_schema)

        # add second table to gpkg
        sql.table_to_gpkg(test_table + '_2', schema=ms_schema, gpkg_name=gpkg, gpkg_tbl = test_table + '_2', path=fldr, print_cmd=True)

        # check gpkg in folder
        assert os.path.isfile(os.path.join(fldr, gpkg))

        # check the number of tables within the geopackage
        cmd_gpkg_1 = f'ogrinfo ./test_data/{gpkg} -sql "SELECT id FROM {test_table} LIMIT 1" -q '
        cmd_gpkg_2 = f'ogrinfo ./test_data/{gpkg} -sql "SELECT id_test FROM {test_table}_2 LIMIT 1" -q'

        ogr_response_gpkg_1 = subprocess.check_output(shlex.split(cmd_gpkg_1), stderr=subprocess.STDOUT)
        ogr_response_gpkg_2 = subprocess.check_output(shlex.split(cmd_gpkg_2), stderr=subprocess.STDOUT)

        assert 'id (Integer) = 1' in str(ogr_response_gpkg_1) and 'id_test (Integer) = 2' in str(ogr_response_gpkg_2), "Cannot find 2 tables in the same geopackage"

        # clean up
        sql.drop_table(ms_schema, test_table)
        sql.drop_table(ms_schema, test_table + '_2')
        os.remove(os.path.join(fldr, gpkg))

    def test_table_to_gpkg_overwrite(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
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
        sql.table_to_gpkg(table = test_table, schema=ms_schema, gpkg_name=gpkg, path=fldr, print_cmd=True)

        # create a second table with slightly different data and overwrite it
        sql.drop_table(schema=ms_schema, table=test_table)
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_table} (id3 int, txt text, dte datetime, geom geometry);

            INSERT INTO {ms_schema}.{test_table}
            (id3, txt, dte, geom)
             VALUES (3, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=ms_schema)
        sql.table_to_gpkg(table = test_table, schema=ms_schema, gpkg_name=gpkg, gpkg_tbl = test_table, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, gpkg))

        # this should fail
        cmd_gpkg = f'ogrinfo ./test_data/{gpkg} -sql "SELECT id FROM {test_table} LIMIT 1" -q'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg), stderr=subprocess.STDOUT)
        assert 'ERROR' in str(ogr_response_gpkg), "table was not overwritten in the geopackage"

        # check that the overwritten table works
        cmd_gpkg2 = f'ogrinfo ./test_data/{gpkg} -sql "SELECT id3 FROM {test_table} LIMIT 1" -q'
        ogr_response_gpkg = subprocess.check_output(shlex.split(cmd_gpkg2), stderr=subprocess.STDOUT)
        assert 'id3 (Integer) = 3' in str(ogr_response_gpkg), "table was not overwritten in the geopackage"

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(fldr, gpkg))

      
    def test_table_to_gpkg_basic_pth(self):

        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
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
        sql.table_to_gpkg(table = test_table, schema=ms_schema, path=fldr + '\\' + gpkg, gpkg_name = gpkg, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, gpkg))

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(fldr, gpkg))

    def test_table_to_gpkg_basic_pth_w_name(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
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
        sql.table_to_gpkg(table = test_table, schema=ms_schema, gpkg_name=gpkg ,path=fldr + '\\' + 'err_'+gpkg, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, gpkg))

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(fldr, gpkg))

    def test_table_to_gpkg_basic_brackets(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
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
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to gpkg
        sql.table_to_gpkg(table = test_table, schema=ms_schema, path=fldr, gpkg_name=gpkg, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, gpkg))

        # clean up
        sql.drop_table(ms_schema, test_table)
        os.remove(os.path.join(fldr, gpkg))

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(sql, schema=ms_schema)
        sql.query(f"drop table {ms_schema}.{sql.log_table}")
        sql.cleanup_new_tables()
        # helpers.clean_up_schema(sql, schema=ms_schema)
