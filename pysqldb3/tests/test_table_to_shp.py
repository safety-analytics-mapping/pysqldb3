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

test_table = '__testing_query_to_shp_{}__'.format(db.user)
ms_schema = 'risadmin'
pg_schema = 'working'


class TestTableToShpPg:
    def test_table_to_shp_basic(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        db.query("""
            DROP TABLE IF EXISTS {s}.{t};
            CREATE TABLE {s}.{t} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {s}.{t}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(s=pg_schema, t=test_table))
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.table_to_shp(test_table, schema=pg_schema, shp_name=shp, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(pg_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_table_to_shp_basic_pth(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        db.query("""
            DROP TABLE IF EXISTS {s}.{t};
            CREATE TABLE {s}.{t} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {s}.{t}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(s=pg_schema, t=test_table))
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.table_to_shp(test_table, schema=pg_schema, path=fldr + '\\' + shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(pg_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_table_to_shp_basic_pth_w_name(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        db.query("""
            DROP TABLE IF EXISTS {s}.{t};
            CREATE TABLE {s}.{t} (id int, txt text, dte timestamp, geom geometry(Point));

            INSERT INTO {s}.{t}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(s=pg_schema, t=test_table))
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.table_to_shp(test_table, schema=pg_schema, shp_name=shp, path=fldr + '\\' + 'err_' + shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(pg_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_table_to_shp_basic_quotes(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = test_table+'.shp'

        # create table
        db.query("""
            DROP TABLE IF EXISTS {s}.{t};
            CREATE TABLE {s}.{t} (id int, "txt" text, dte timestamp, geom geometry(Point));

            INSERT INTO {s}.{t}
             VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """.format(s=pg_schema, t=test_table))
        assert db.table_exists(test_table, schema=pg_schema)

        # table to shp
        db.table_to_shp(test_table, pg_schema, path=fldr, shp_name=shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(schema=pg_schema, table=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('.shp', '.'+ext)))


class TestTableToShpMs:
    @classmethod
    def setup_class(cls):
        helpers.set_up_schema(sql)


    def test_table_to_shp_basic(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        sql.drop_table(schema=ms_schema, table=test_table)

        # create table
        sql.query("""
            CREATE TABLE {s}.{t} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {s}.{t}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """.format(s=ms_schema, t=test_table))
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to shp
        sql.table_to_shp(test_table, schema=ms_schema, shp_name=shp, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(ms_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_table_to_shp_basic_pth(self):

        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        sql.drop_table(schema=ms_schema, table=test_table)

        # create table
        sql.query("""
            CREATE TABLE {s}.{t} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {s}.{t}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """.format(s=ms_schema, t=test_table))
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to shp
        sql.table_to_shp(test_table, schema=ms_schema, path=fldr + '\\' + shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(ms_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_table_to_shp_basic_pth_w_name(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        sql.drop_table(schema=ms_schema, table=test_table)
        # create table
        sql.query("""
            CREATE TABLE {s}.{t} (id int, txt text, dte datetime, geom geometry);

            INSERT INTO {s}.{t}
            (id, txt, dte, geom)
             VALUES (1, 'test text', CURRENT_TIMESTAMP,
             geometry::Point(1015329.1, 213793.1, 2263 ))
        """.format(s=ms_schema, t=test_table))
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to shp
        sql.table_to_shp(test_table, schema=ms_schema, shp_name=shp ,path=fldr + '\\' + 'err_'+shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(ms_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_table_to_shp_basic_brackets(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = test_table+'.shp'
        sql.drop_table(table=test_table, schema=ms_schema)

        # create table
        sql.query("""
                    CREATE TABLE {s}.{t} (id int, [txt] text, dte datetime, geom geometry);

                    INSERT INTO {s}.{t}
                    (id, txt, dte, geom)
                     VALUES (1, 'test text', CURRENT_TIMESTAMP,
                     geometry::Point(1015329.1, 213793.1, 2263))
                """.format(s=ms_schema, t=test_table))
        assert sql.table_exists(test_table, schema=ms_schema)

        # table to shp
        sql.table_to_shp(test_table, schema=ms_schema, path=fldr, shp_name=shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(ms_schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(sql, schema=ms_schema)
        sql.query("drop table {}.{}".format('pytest', sql.log_table))
        sql.clean_up_new_tables()
        # helpers.clean_up_schema(sql, schema=ms_schema)
