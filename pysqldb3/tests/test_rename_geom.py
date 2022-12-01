import os
import subprocess

import configparser
import pytest

from .. import util, pysqldb3 as pysqldb
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

table = 'test_feature_class_{user}'.format(user=pg_dbconn.username)


class TestRenamesGeomPg:
    @classmethod
    def setup_class(cls):
        helpers.set_up_feature_class()
        helpers.set_up_shapefile()

    @pytest.mark.order1
    def test_rename_geom_fc(self):
        fgdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data/lion/lion.gdb')
        fc = 'node'

        # import data without pysqldb
        assert pg_dbconn.table_exists(table_name=table, schema_name=pg_dbconn.default_schema) is False

        cmd = """
        ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs 
        "EPSG:{srid}" -f "PostgreSQL" PG:"host={host} user={username} dbname={dbname} 
        password={password}" "{gdb}" "{feature}" -nln {schema}.{table} -progress 
        """.format(gdal_data=util.GDAL_DATA_LOC,
                   srid=2263,
                   host=pg_dbconn.host,
                   dbname=pg_dbconn.db_name,
                   username=pg_dbconn.username,
                   password=pg_dbconn.password,
                   gdb=fgdb,
                   feature=fc,
                   schema=pg_dbconn.default_schema,
                   table=table
                   ).replace('\n', ' ')

        subprocess.call(cmd, shell=True)

        pg_dbconn.query("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name  = '{table}'
                    and column_name='geom'
                """.format(table=table))

        assert pg_dbconn.data == []
        pg_dbconn.drop_table(schema_name=pg_dbconn.default_schema,table_name=table)
        # clean up gdal import
        # import with pysqldb

        assert pg_dbconn.table_exists(table_name=table, schema_name=pg_dbconn.default_schema) is False
        pg_dbconn.feature_class_to_table(fgdb, table_name=table, schema_name=None, fc_name=fc)

        pg_dbconn.query("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name  = '{table}'
            and column_name='geom'
        """.format(table=table))

        assert pg_dbconn.data[0] == ('geom', 'USER-DEFINED')

        pg_dbconn.drop_table(schema_name=pg_dbconn.default_schema, table_name=table)

    @pytest.mark.order2
    def test_rename_geom_shp(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # import data without pysqldb
        pg_dbconn.drop_table(table_name=table, schema_name=pg_dbconn.default_schema)
        assert not pg_dbconn.table_exists(table_name=table, schema_name=pg_dbconn.default_schema)

        cmd = """
        ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs 
        "EPSG:{srid}" -progress -f "PostgreSQL" PG:"host={host} port={port} dbname={dbname} 
        user={username} password={password}" "{shp}" -nln {schema}.{table} 
        """.format(gdal_data=util.GDAL_DATA_LOC,
                   srid=2263,
                   host=pg_dbconn.host,
                   dbname=pg_dbconn.db_name,
                   username=pg_dbconn.username,
                   password=pg_dbconn.password,
                   shp=os.path.join(dir, shp),
                   schema=pg_dbconn.default_schema,
                   table=table,
                   port=5432
                   ).replace('\n', ' ')

        subprocess.call(cmd, shell=True)

        pg_dbconn.query("""
                            SELECT column_name, data_type
                            FROM information_schema.columns
                            WHERE table_name  = '{table}'
                            and column_name='geom'
                        """.format(table=table))
        assert pg_dbconn.data == []
        pg_dbconn.drop_table(pg_dbconn.default_schema, table)
        # clean up gdal import

        # import with pysqldb
        assert not pg_dbconn.table_exists(table_name=table, schema_name=pg_dbconn.default_schema)

        pg_dbconn.shp_to_table(path=dir, table_name=table, shpfile_name=shp)

        pg_dbconn.query("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name  = '{table}'
                    and column_name='geom'
                """.format(table=table))

        assert pg_dbconn.data[0] == ('geom', 'USER-DEFINED')

        pg_dbconn.drop_table(schema_name=pg_dbconn.default_schema, table_name=table)

    @classmethod
    def teardown_class(cls):
        # helpers.clean_up_feature_class()
        helpers.clean_up_shapefile()


class TestRenamesGeomMs:
    @classmethod
    def setup_class(cls):
        helpers.set_up_feature_class()
        helpers.set_up_shapefile()

    @pytest.mark.order3
    def test_rename_geom_fc(self):
        fgdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data/lion/lion.gdb')
        fc = 'node'

        # import data without pysqldb
        ms_dbconn.drop_table(table_name=table, schema_name=ms_dbconn.default_schema)
        assert ms_dbconn.table_exists(table_name=table, schema_name=ms_dbconn.default_schema) is False

        cmd = """
        ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs 
        "EPSG:{srid}" -f MSSQLSpatial "MSSQL:server={host};database={dbname};UID={username};PWD={password}"
         "{gdb}" "{feature}" -nln {schema}.{table} -progress --config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO
        """.format(gdal_data=util.GDAL_DATA_LOC,
                   srid=2263,
                   host=ms_dbconn.host,
                   dbname=ms_dbconn.db_name,
                   username=ms_dbconn.username,
                   password=ms_dbconn.password,
                   gdb=fgdb,
                   feature=fc,
                   schema=ms_dbconn.default_schema,
                   table=table
                   ).replace('\n', ' ')

        subprocess.call(cmd, shell=True)

        ms_dbconn.query("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name  = '{table}'
                    AND TABLE_SCHEMA='{schema}'
                    and column_name='geom'
                """.format(table=table, schema=ms_dbconn.default_schema))

        assert ms_dbconn.data == []
        ms_dbconn.drop_table(schema_name=ms_dbconn.default_schema, table_name=table)
        # clean up gdal import

        # import with pysqldb
        assert not ms_dbconn.table_exists(table_name=table, schema_name=ms_dbconn.default_schema)

        ms_dbconn.feature_class_to_table(fgdb, table_name=table, schema_name=None, fc_name=fc, skip_failures='-skip_failures')

        ms_dbconn.query("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name  = '{table}'
            and column_name='geom'
        """.format(table=table))

        assert ms_dbconn.data[0][0] == u'geom'
        assert ms_dbconn.data[0][1] == u'geometry'

        ms_dbconn.drop_table(table_name=table, schema_name=ms_dbconn.default_schema)

    @pytest.mark.order4
    def test_rename_geom_shp(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # import data without pysqldb
        ms_dbconn.drop_table(table_name=table, schema_name=ms_dbconn.default_schema)
        assert ms_dbconn.table_exists(table_name=table, schema_name=ms_dbconn.default_schema) is False

        cmd = """ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs
        "EPSG:{srid}" -progress -f MSSQLSpatial "MSSQL:server={host};database={dbname};UID={username};PWD={password}"
         "{shpfile}" -nln {schema}.{table} {precision} --config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO
        """.format(gdal_data=util.GDAL_DATA_LOC,
                   srid=2263,
                   host=ms_dbconn.host,
                   dbname=ms_dbconn.db_name,
                   username=ms_dbconn.username,
                   password=ms_dbconn.password,
                   shpfile=os.path.join(dir, shp),
                   schema=ms_dbconn.default_schema,
                   table=table,
                   precision='',
                   port=5432
                   ).replace('\n', ' ')

        subprocess.call(cmd, shell=True)

        ms_dbconn.query("""
                            SELECT column_name, data_type
                            FROM information_schema.columns
                            WHERE table_name  = '{table}'
                            and column_name='geom'
                        """.format(table=table))
        assert ms_dbconn.data == []
        ms_dbconn.drop_table(schema_name=pg_dbconn.default_schema, table_name=table)
        # clean up gdal import

        # import with pysqldb
        assert ms_dbconn.table_exists(table_name=table, schema_name=ms_dbconn.default_schema) is False

        ms_dbconn.shp_to_table(path=dir, table_name=table, shpfile_name=shp)

        ms_dbconn.query("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name  = '{t}'
                    and column_name='geom'
                """.format(t=table))

        assert ms_dbconn.data[0][0] == u'geom'
        assert ms_dbconn.data[0][1] == u'geometry'

        ms_dbconn.drop_table(ms_dbconn.default_schema, table)

    @classmethod
    def teardown_class(cls):
        # helpers.clean_up_feature_class()
        helpers.clean_up_shapefile()