from .. import pysqldb3 as pysqldb
from ..pysqldb3 import *
from . import helpers

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

pg_dbconn = pysqldb.DbConnect(db_type=config.get('PG_DB', 'TYPE'),
                              host=config.get('PG_DB', 'SERVER'),
                              db_name=config.get('PG_DB', 'DB_NAME'),
                              username=config.get('PG_DB', 'DB_USER'),
                              password=config.get('PG_DB', 'DB_PASSWORD'))

ms_dbconn = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                              host=config.get('SQL_DB', 'SERVER'),
                              db_name=config.get('SQL_DB', 'DB_NAME'),
                              username=config.get('SQL_DB', 'DB_USER'),
                              password=config.get('SQL_DB', 'DB_PASSWORD'))

test_read_shp_table_name = 'test_read_shp_table_{user}'.format(user=pg_dbconn.username)


class TestSHPDeleteIndexPG:
    @classmethod
    def setup_class(cls):
        # Setup; create sample file
        helpers.set_up_shapefile()

    def test_shp_delete_index_pg_basic(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(dir)
        pg_dbconn.drop_table(schema_name='working', table_name=test_read_shp_table_name)

        # Assert no indexes to start
        indexes_df = pg_dbconn.dfquery(SHP_DEL_INDICES_QUERY_PG.format(schema='working', table=test_read_shp_table_name))
        assert len(indexes_df) == 0

        # Read shp to new, test table
        shpfile = Shapefile(pg_dbconn, path=dir, shpfile_name=shp_name, table_name=test_read_shp_table_name, schema_name='working')
        shpfile.read_shp()

        # Assert two indexes were made; one for PK
        indexes_df = pg_dbconn.dfquery(SHP_DEL_INDICES_QUERY_PG.format(schema='working', table=test_read_shp_table_name))
        assert len(indexes_df) == 2
        assert len(indexes_df[indexes_df['index_name'].str.contains('pkey')]) == 1

        # Call del_indexes
        shpfile.del_indexes()

        # Assert one indexes left; contains pkey
        indexes_df = pg_dbconn.dfquery(SHP_DEL_INDICES_QUERY_PG.format(schema='working', table=test_read_shp_table_name))
        assert len(indexes_df) == 1
        assert len(indexes_df[indexes_df['index_name'].str.contains('pkey')]) == 1

        # Cleanup
        pg_dbconn.drop_table(schema_name='working', table_name=test_read_shp_table_name)

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
        ms_dbconn.drop_table(schema_name='dbo', table_name=test_read_shp_table_name)

        # Assert no indexes to start
        indexes_df = ms_dbconn.dfquery(SHP_DEL_INDICES_QUERY_MS.format(schema='dbo', table=test_read_shp_table_name))
        assert len(indexes_df) == 0

        # Read shp to new, test table
        shpfile = Shapefile(ms_dbconn, path=fp, shpfile_name=shp_name, table_name=test_read_shp_table_name, schema_name='dbo')
        shpfile.read_shp()

        # Assert one index was made; one for PK
        indexes_df = ms_dbconn.dfquery(SHP_DEL_INDICES_QUERY_MS.format(schema='dbo', table=test_read_shp_table_name))
        assert len(indexes_df) == 1
        assert len(indexes_df[indexes_df['index_name'].str.contains('PK')]) == 1

        # Call del_indexes
        shpfile.del_indexes()

        # Assert still one index left; contains PK
        indexes_df = ms_dbconn.dfquery(SHP_DEL_INDICES_QUERY_MS.format(schema='dbo', table=test_read_shp_table_name))
        assert len(indexes_df) == 1
        assert len(indexes_df[indexes_df['index_name'].str.contains('PK')]) == 1

        # Cleanup
        ms_dbconn.drop_table(schema_name='dbo', table_name=test_read_shp_table_name)

    def test_shp_delete_index_ms_multiple(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        ms_dbconn.drop_table(schema_name='dbo', table_name=test_read_shp_table_name)

        # Assert no indexes to start
        indexes_df = ms_dbconn.dfquery(SHP_DEL_INDICES_QUERY_MS.format(schema='dbo', table=test_read_shp_table_name))
        assert len(indexes_df) == 0

        # Read shp to new, test table
        shpfile = Shapefile(ms_dbconn, path=fp, shpfile_name=shp_name, table_name=test_read_shp_table_name, schema_name='dbo')
        shpfile.read_shp()

        # Add one more
        ms_dbconn.query("""
        CREATE INDEX IX_{table} ON dbo.{table} (ogr_fid)
        """.format(table=test_read_shp_table_name))

        # Assert one index was made; one for PK
        indexes_df = ms_dbconn.dfquery(SHP_DEL_INDICES_QUERY_MS.format(schema='dbo', table=test_read_shp_table_name))
        assert len(indexes_df) == 2
        assert len(indexes_df[indexes_df['index_name'].str.contains('PK')]) == 1

        # Call del_indexes
        shpfile.del_indexes()

        # Assert still one index left; contains PK
        indexes_df = ms_dbconn.dfquery(SHP_DEL_INDICES_QUERY_MS.format(schema='dbo', table=test_read_shp_table_name))
        assert len(indexes_df) == 1
        assert len(indexes_df[indexes_df['index_name'].str.contains('PK')]) == 1

        # Cleanup
        ms_dbconn.drop_table(schema_name='dbo', table_name=test_read_shp_table_name)

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_shapefile()
