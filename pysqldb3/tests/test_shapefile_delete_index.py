import configparser
import os

from .. import pysqldb3 as pysqldb
from ..shapefile import Shapefile
from ..sql import *
from . import helpers

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                       server=config.get('PG_DB', 'SERVER'),
                       database=config.get('PG_DB', 'DB_NAME'),
                       user=config.get('PG_DB', 'DB_USER'),
                       password=config.get('PG_DB', 'DB_PASSWORD'))

if config.get('SQL_DB','LDAP').lower() == 'true':
    sql = pysqldb.DbConnect(type=config.get('SQL_DB','TYPE'),
                            server=config.get('SQL_DB','SERVER'),
                            database=config.get('SQL_DB','DB_NAME'),
                            ldap=True)
else:
    sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                            server=config.get('SQL_DB', 'SERVER'),
                            database=config.get('SQL_DB', 'DB_NAME'),
                            user=config.get('SQL_DB', 'DB_USER'),
                            password=config.get('SQL_DB', 'DB_PASSWORD'))

test_read_shp_table_name = 'test_read_shp_table_{}'.format(db.user)


class TestSHPDeleteIndexPG:
    @classmethod
    def setup_class(cls):
        # Setup; create sample file
        helpers.set_up_shapefile()

    def test_shp_delete_index_pg_basic(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        db.drop_table(schema='working', table=test_read_shp_table_name)

        # Assert no indexes to start
        indexes_df = db.dfquery(SHP_DEL_INDICES_QUERY_PG.format(s='working', t=test_read_shp_table_name))
        assert len(indexes_df) == 0

        # Read shp to new, test table
        s = Shapefile(dbo=db, path=fp, shp_name=shp_name, table=test_read_shp_table_name, schema='working')
        s.read_shp()

        # Assert two indexes were made; one for PK
        indexes_df = db.dfquery(SHP_DEL_INDICES_QUERY_PG.format(s='working', t=test_read_shp_table_name))
        assert len(indexes_df) == 2
        assert len(indexes_df[indexes_df['index_name'].str.contains('pkey')]) == 1

        # Call del_indexes
        s.del_indexes()

        # Assert one indexes left; contains pkey
        indexes_df = db.dfquery(SHP_DEL_INDICES_QUERY_PG.format(s='working', t=test_read_shp_table_name))
        assert len(indexes_df) == 1
        assert len(indexes_df[indexes_df['index_name'].str.contains('pkey')]) == 1

        # Cleanup
        db.drop_table(schema='working', table=test_read_shp_table_name)

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_shapefile()


class TestSHPDeleteIndexMS:
    @classmethod
    def setup_class(cls):
        # Setup; create sample file
        helpers.set_up_shapefile()

    def test_shp_delete_index_ms_basic(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        sql.drop_table(schema='dbo', table=test_read_shp_table_name)

        # Assert no indexes to start
        indexes_df = sql.dfquery(SHP_DEL_INDICES_QUERY_MS.format(s='dbo', t=test_read_shp_table_name))
        assert len(indexes_df) == 0

        # Read shp to new, test table
        s = Shapefile(dbo=sql, path=fp, shp_name=shp_name, table=test_read_shp_table_name, schema='dbo')
        s.read_shp()

        # Assert one index was made; one for PK
        indexes_df = sql.dfquery(SHP_DEL_INDICES_QUERY_MS.format(s='dbo', t=test_read_shp_table_name))
        assert len(indexes_df) == 1
        assert len(indexes_df[indexes_df['index_name'].str.contains('PK')]) == 1

        # Call del_indexes
        s.del_indexes()

        # Assert still one index left; contains PK
        indexes_df = sql.dfquery(SHP_DEL_INDICES_QUERY_MS.format(s='dbo', t=test_read_shp_table_name))
        assert len(indexes_df) == 1
        assert len(indexes_df[indexes_df['index_name'].str.contains('PK')]) == 1

        # Cleanup
        sql.drop_table(schema='dbo', table=test_read_shp_table_name)

    def test_shp_delete_index_ms_multiple(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        sql.drop_table(schema='dbo', table=test_read_shp_table_name)

        # Assert no indexes to start
        indexes_df = sql.dfquery(SHP_DEL_INDICES_QUERY_MS.format(s='dbo', t=test_read_shp_table_name))
        assert len(indexes_df) == 0

        # Read shp to new, test table
        s = Shapefile(dbo=sql, path=fp, shp_name=shp_name, table=test_read_shp_table_name, schema='dbo')
        s.read_shp()

        # Add one more
        sql.query("""
        CREATE INDEX IX_{} ON dbo.{} (ogr_fid)
        """.format(test_read_shp_table_name, test_read_shp_table_name))

        # Assert one index was made; one for PK
        indexes_df = sql.dfquery(SHP_DEL_INDICES_QUERY_MS.format(s='dbo', t=test_read_shp_table_name))
        assert len(indexes_df) == 2
        assert len(indexes_df[indexes_df['index_name'].str.contains('PK')]) == 1

        # Call del_indexes
        s.del_indexes()

        # Assert still one index left; contains PK
        indexes_df = sql.dfquery(SHP_DEL_INDICES_QUERY_MS.format(s='dbo', t=test_read_shp_table_name))
        assert len(indexes_df) == 1
        assert len(indexes_df[indexes_df['index_name'].str.contains('PK')]) == 1

        # Cleanup
        sql.drop_table(schema='dbo', table=test_read_shp_table_name)

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_shapefile()
