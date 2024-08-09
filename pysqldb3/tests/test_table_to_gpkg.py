import os
import configparser
from .. import pysqldb3 as pysqldb
from . import helpers

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
        db.table_to_gpkg(test_table, schema=pg_schema, gpkg_name=gpkg, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, gpkg))

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
        db.table_to_gpkg(test_table, schema=pg_schema, gpkg_name=gpkg, path=fldr + '\\' + 'err_' + gpkg, print_cmd=True)

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
        db.table_to_gpkg(test_table, pg_schema, path=fldr, gpkg_name=gpkg, print_cmd=True)

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
        sql.table_to_gpkg(test_table, schema=ms_schema, gpkg_name=gpkg, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, gpkg))

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
        sql.table_to_gpkg(test_table, schema=ms_schema, path=fldr + '\\' + gpkg, gpkg_name = gpkg, print_cmd=True)

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
        sql.table_to_gpkg(test_table, schema=ms_schema, gpkg_name=gpkg ,path=fldr + '\\' + 'err_'+gpkg, print_cmd=True)

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
        sql.table_to_gpkg(test_table, schema=ms_schema, path=fldr, gpkg_name=gpkg, print_cmd=True)

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
