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

test_table = f'__testing_query_to_shp_{db.user}__'


class TestTableToShpPg:
    def test_table_to_shp_basic(self):
        schema = 'working'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {schema}.{test_table};
            CREATE TABLE {schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));
            INSERT INTO {schema}.{test_table}
                VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=schema)

        # table to shp
        db.table_to_shp(test_table, schema=schema, shp_name=shp, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_table_to_shp_basic_pth(self):
        schema = 'working'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {schema}.{test_table};
            CREATE TABLE {schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));
            INSERT INTO {schema}.{test_table}
                VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=schema)

        # table to shp
        db.table_to_shp(test_table, schema=schema, path=f'{fldr}\\{shp}', print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_table_to_shp_basic_pth_w_name(self):
        schema = 'working'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {schema}.{test_table};
            CREATE TABLE {schema}.{test_table} (id int, txt text, dte timestamp, geom geometry(Point));
            INSERT INTO {schema}.{test_table}
                VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=schema)

        # table to shp
        db.table_to_shp(test_table, schema=schema, shp_name=shp,  path=f'{fldr}\\err_{shp}', print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('shp', ext)))

    def test_table_to_shp_basic_quotes(self):
        schema = 'working'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = f'{test_table}.shp'

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {schema}.{test_table};
            CREATE TABLE {schema}.{test_table} (id int, "txt" text, dte timestamp, geom geometry(Point));
            INSERT INTO {schema}.{test_table}
                VALUES (1, 'test text', now(), st_setsrid(st_makepoint(1015329.1, 213793.1), 2263))
        """)
        assert db.table_exists(test_table, schema=schema)

        # table to shp
        db.table_to_shp(test_table, schema, path=fldr, shp_name=shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        db.drop_table(schema=schema, table=test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            os.remove(os.path.join(fldr, shp.replace('.shp', '.'+ext)))


class TestTableToShpMs:
    @classmethod
    def setup_class(cls):
        helpers.set_up_schema(sql)


    def test_table_to_shp_basic(self):
        schema = 'pytest'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        sql.drop_table(schema=schema, table=test_table)

        # create table
        sql.query(f"""
            CREATE TABLE {schema}.{test_table} (id int, txt text, dte datetime, geom geometry);
            INSERT INTO {schema}.{test_table} (id, txt, dte, geom)
                VALUES (1, 'test text', CURRENT_TIMESTAMP, geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=schema)

        # table to shp
        sql.table_to_shp(test_table, schema=schema, shp_name=shp, path=fldr, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_table_to_shp_basic_pth(self):
        schema = 'pytest'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'
        sql.drop_table(schema=schema, table=test_table)

        # create table
        sql.query(f"""
            CREATE TABLE {schema}.{test_table} (id int, txt text, dte datetime, geom geometry);
            INSERT INTO {schema}.{test_table} (id, txt, dte, geom)
                VALUES (1, 'test text', CURRENT_TIMESTAMP, geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=schema)

        # table to shp
        sql.table_to_shp(test_table, schema=schema, path=f'{fldr}\\{shp}', print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_table_to_shp_basic_pth_w_name(self):
        schema = 'pytest'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        sql.drop_table(schema=schema, table=test_table)
        # create table
        sql.query(f"""
            CREATE TABLE {schema}.{test_table} (id int, txt text, dte datetime, geom geometry);
            INSERT INTO {schema}.{test_table} (id, txt, dte, geom)
                VALUES (1, 'test text', CURRENT_TIMESTAMP, geometry::Point(1015329.1, 213793.1, 2263 ))
        """)
        assert sql.table_exists(test_table, schema=schema)

        # table to shp
        sql.table_to_shp(test_table, schema=schema, shp_name=shp ,path=f'{fldr}\\err_{shp}', print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    def test_table_to_shp_basic_brackets(self):
        schema = 'dbo'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = f'{test_table}.shp'
        sql.drop_table(table=test_table, schema=schema)

        # create table
        sql.query(f"""
                    CREATE TABLE {schema}.{test_table} (id int, [txt] text, dte datetime, geom geometry);
                    INSERT INTO {schema}.{test_table} (id, txt, dte, geom)
                        VALUES (1, 'test text', CURRENT_TIMESTAMP, geometry::Point(1015329.1, 213793.1, 2263))
                """)
        assert sql.table_exists(test_table, schema=schema)

        # table to shp
        sql.table_to_shp(test_table, schema=schema, path=fldr, shp_name=shp, print_cmd=True)

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, shp))

        # clean up
        sql.drop_table(schema, test_table)
        for ext in ('dbf', 'prj', 'shx', 'shp'):
            try:
                os.remove(os.path.join(fldr, shp.replace('shp', ext)))
            except:
                pass

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(sql, schema='pytest')
        sql.query(f"drop table pytest.{sql.log_table}")
        sql.clean_up_new_tables()
        helpers.clean_up_schema(sql)
