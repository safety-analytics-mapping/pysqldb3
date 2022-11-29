import os
import configparser
from .. import pysqldb3 as pysqldb
from . import helpers

#################################################################################
                        # only doing basic tests since it just calls
                        # query_to_shp and that is already tested
#################################################################################


config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

pg_dbconn = pysqldb.DbConnect(db_type=config.get('PG_DB', 'TYPE'),
                              host=config.get('PG_DB', 'SERVER'),
                              db_name=config.get('PG_DB', 'DB_NAME'),
                              username=config.get('PG_DB', 'DB_USER'),
                              password=config.get('PG_DB', 'DB_PASSWORD'),
                              allow_temp_tables=True
                              )

sql = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
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
ms_schema = 'risadmin'
pg_schema = 'working'


class TestTableToShpPg:
    def test_table_to_shp_basic(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        pg_dbconn.query("""
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {schema}.{table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(schema=pg_schema, table=test_table))
        assert pg_dbconn.table_exists(table_name=test_table, schema_name=pg_schema)

        # table to shp
        pg_dbconn.table_to_shp(test_table, schema_name=pg_schema, shpfile_name=shp, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        pg_dbconn.drop_table(pg_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_table_to_shp_basic_pth(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        pg_dbconn.query("""
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {schema}.{table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(schema=pg_schema, table=test_table))
        assert pg_dbconn.table_exists(test_table, schema=pg_schema)

        # table to shp
        pg_dbconn.table_to_shp(test_table, schema_name=pg_schema, path=f'{dir}\\{shp}', print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        pg_dbconn.drop_table(pg_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(dir, shp.replace('shp', ext)))

    def test_table_to_shp_basic_pth_w_name(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        pg_dbconn.query("""
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {schema}.{table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(schema=pg_schema, table=test_table))
        assert pg_dbconn.table_exists(table_name=test_table, schema=pg_schema)

        # table to shp
        pg_dbconn.table_to_shp(table_name=test_table, schema_name=pg_schema, shpfile_name=shp, path=dir + '\\' + 'err_' + shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        pg_dbconn.drop_table(schema_name=pg_schema, table_name=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(dir, shp.replace('shp', ext)))

    def test_table_to_shp_basic_quotes(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = test_table + '.shp'

        # create table
        pg_dbconn.query("""
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table} (id int, "txt" text, dte timestamp, geom geometry(Point));

            INSERT INTO {schema}.{table}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(schema=pg_schema, table=test_table))
        assert pg_dbconn.table_exists(table_name=test_table, schema_name=pg_schema)

        # table to shp
        pg_dbconn.table_to_shp(table_name=test_table, schema_name=pg_schema, path=dir, shpfile_name=shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        pg_dbconn.drop_table(schema_name=pg_schema, table_name=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(dir, shp.replace('.shp', '.'+ext)))


class TestTableToShpMs:
    @classmethod
    def setup_class(cls):
        helpers.set_up_schema(sql)


    def test_table_to_shp_basic(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        sql.drop_table(schema_name=ms_schema, table_name=test_table)

        # create table
        sql.query("""
            CREATE TABLE {schema}.{table} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {schema}.{table}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """.format(schema=ms_schema, table=test_table))
        assert sql.table_exists(table_name=test_table, schema=ms_schema)

        # table to shp
        sql.table_to_shp(table_name=test_table, schema_name=ms_schema, shpfile_name=shp, path=dir, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        sql.drop_table(ms_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(dir, shp.replace('shp', ext)))
            except:
                pass

    def test_table_to_shp_basic_pth(self):

        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        sql.drop_table(schema_name=ms_schema, table_name=test_table)

        # create table
        sql.query("""
            CREATE TABLE {schema}.{table} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {schema}.{table}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """.format(schema=ms_schema, table=test_table))
        assert sql.table_exists(table_name=test_table, schema=ms_schema)

        # table to shp
        sql.table_to_shp(table_name=test_table, schema_name=ms_schema, path=f'{dir}\\{shp}', print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        sql.drop_table(schema_name=ms_schema, table_name=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(dir, shp.replace('shp', ext)))
            except:
                pass

    def test_table_to_shp_basic_pth_w_name(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        sql.drop_table(schema_name=ms_schema, table_name=test_table)
        # create table
        sql.query("""
            CREATE TABLE {schema}.{table} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {schema}.{table}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """.format(schema=ms_schema, table=test_table))
        assert sql.table_exists(table_name=test_table, schema_name=ms_schema)

        # table to shp
        sql.table_to_shp(table_name=test_table, schema_name=ms_schema, shpfile_name=shp, path=f'{dir}\\err_{shp}', print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        sql.drop_table(schema_name=ms_schema, table_name=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(dir, shp.replace('shp', ext)))
            except:
                pass

    def test_table_to_shp_basic_brackets(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = test_table+'.shp'
        sql.drop_table(table_name=test_table, schema_name=ms_schema)

        # create table
        sql.query("""
                    CREATE TABLE {schema}.{table} (id int, [txt] text, dte datetime, geom geometry);

                    INSERT INTO {schema}.{table}
                    (id, txt, dte, geom)
                     VALUES (1, 'test text', CURRENT_TIMESTAMP,
                     geometry::Point(1015329.1, 213793.1, 2263))
                """.format(schema=ms_schema, table=test_table))
        assert sql.table_exists(table_name=test_table, schema_name=ms_schema)

        # table to shp
        sql.table_to_shp(table_name=test_table, schema_name=ms_schema, path=dir, shpfile_name=shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(dir, shp))

        # clean up
        sql.drop_table(schema_name=ms_schema, table_name=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(dir, shp.replace('shp', ext)))
            except:
                pass

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(sql, schema=ms_schema)
        sql.query("drop table {schema}.{table}".format(schema=ms_schema, table=sql.log_table))
        sql.cleanup_new_tables()
        # helpers.clean_up_schema(sql, schema=ms_schema)
