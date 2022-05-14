import os
import subprocess

import configparser
import pytest

from .. import util, pysqldb3 as pysqldb
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

table = 'test_feature_class_{}'.format(db.user)


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
        assert db.table_exists(table, schema=db.default_schema) is False

        cmd = """
        ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs 
        "EPSG:{srid}" -f "PostgreSQL" PG:"host={host} user={user} dbname={dbname} 
        password={password}" "{gdb}" "{feature}" -nln {sch}.{tbl_name} -progress 
        """.format(gdal_data=util.GDAL_DATA_LOC,
                   srid=2263,
                   host=db.server,
                   dbname=db.database,
                   user=db.user,
                   password=db.password,
                   gdb=fgdb,
                   feature=fc,
                   sch=db.default_schema,
                   tbl_name=table
                   ).replace('\n', ' ')

        subprocess.call(cmd, shell=True)

        db.query("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name  = '{t}'
                    and column_name='geom'
                """.format(t=table))

        assert db.data == []
        db.drop_table(db.default_schema, table)
        # clean up gdal import
        # import with pysqldb

        assert db.table_exists(table, schema=db.default_schema) is False
        db.feature_class_to_table(fgdb, table, schema=None, shp_name=fc)

        db.query("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name  = '{t}'
            and column_name='geom'
        """.format(t=table))

        assert db.data[0] == ('geom', 'USER-DEFINED')

        db.drop_table(db.default_schema, table)

    @pytest.mark.order2
    def test_rename_geom_shp(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # import data without pysqldb
        db.drop_table(table=table, schema=db.default_schema)
        assert not db.table_exists(table, schema=db.default_schema)

        cmd = """
        ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs 
        "EPSG:{srid}" -progress -f "PostgreSQL" PG:"host={host} port={port} dbname={dbname} 
        user={user} password={password}" "{shp}" -nln {schema}.{tbl_name} 
        """.format(gdal_data=util.GDAL_DATA_LOC,
                   srid=2263,
                   host=db.server,
                   dbname=db.database,
                   user=db.user,
                   password=db.password,
                   shp=os.path.join(fldr, shp),
                   schema=db.default_schema,
                   tbl_name=table,
                   port=5432
                   ).replace('\n', ' ')

        subprocess.call(cmd, shell=True)

        db.query("""
                            SELECT column_name, data_type
                            FROM information_schema.columns
                            WHERE table_name  = '{t}'
                            and column_name='geom'
                        """.format(t=table))
        assert db.data == []
        db.drop_table(db.default_schema, table)
        # clean up gdal import

        # import with pysqldb
        assert not db.table_exists(table, schema=db.default_schema)

        db.shp_to_table(path=fldr, table=table, shp_name=shp)

        db.query("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name  = '{t}'
                    and column_name='geom'
                """.format(t=table))

        assert db.data[0] == ('geom', 'USER-DEFINED')

        db.drop_table(db.default_schema, table)

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
        sql.drop_table(table=table, schema=sql.default_schema)
        assert sql.table_exists(table, schema=sql.default_schema) is False

        cmd = """
        ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs 
        "EPSG:{srid}" -f MSSQLSpatial "MSSQL:server={host};database={dbname};UID={user};PWD={password}"
         "{gdb}" "{feature}" -nln {sch}.{tbl_name} -progress --config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO
        """.format(gdal_data=util.GDAL_DATA_LOC,
                   srid=2263,
                   host=sql.server,
                   dbname=sql.database,
                   user=sql.user,
                   password=sql.password,
                   gdb=fgdb,
                   feature=fc,
                   sch=sql.default_schema,
                   tbl_name=table
                   ).replace('\n', ' ')

        subprocess.call(cmd, shell=True)

        sql.query("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name  = '{t}'
                    AND TABLE_SCHEMA='{s}'
                    and column_name='geom'
                """.format(t=table, s=sql.default_schema))

        assert sql.data == []
        sql.drop_table(sql.default_schema, table)
        # clean up gdal import

        # import with pysqldb
        assert not sql.table_exists(table, schema=sql.default_schema)

        sql.feature_class_to_table(fgdb, table, schema=None, shp_name=fc, skip_failures='-skip_failures')

        sql.query("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name  = '{t}'
            and column_name='geom'
        """.format(t=table))

        assert sql.data[0][0] == u'geom'
        assert sql.data[0][1] == u'geometry'

        sql.drop_table(table=table, schema=sql.default_schema)

    @pytest.mark.order4
    def test_rename_geom_shp(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        shp = 'test.shp'

        # import data without pysqldb
        sql.drop_table(table=table, schema=sql.default_schema)
        assert sql.table_exists(table, schema=sql.default_schema) is False

        cmd = """ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs
        "EPSG:{srid}" -progress -f MSSQLSpatial "MSSQL:server={host};database={dbname};UID={user};PWD={password}"
         "{shp}" -nln {schema}.{tbl_name} {perc} --config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO
        """.format(gdal_data=util.GDAL_DATA_LOC,
                   srid=2263,
                   host=sql.server,
                   dbname=sql.database,
                   user=sql.user,
                   password=sql.password,
                   shp=os.path.join(fldr, shp),
                   schema=sql.default_schema,
                   tbl_name=table,
                   perc='',
                   port=5432
                   ).replace('\n', ' ')

        subprocess.call(cmd, shell=True)

        sql.query("""
                            SELECT column_name, data_type
                            FROM information_schema.columns
                            WHERE table_name  = '{t}'
                            and column_name='geom'
                        """.format(t=table))
        assert sql.data == []
        sql.drop_table(sql.default_schema, table)
        # clean up gdal import

        # import with pysqldb
        assert sql.table_exists(table, schema=sql.default_schema) is False

        sql.shp_to_table(path=fldr, table=table, shp_name=shp)

        sql.query("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name  = '{t}'
                    and column_name='geom'
                """.format(t=table))

        assert sql.data[0][0] == u'geom'
        assert sql.data[0][1] == u'geometry'

        sql.drop_table(sql.default_schema, table)

    @classmethod
    def teardown_class(cls):
        # helpers.clean_up_feature_class()
        helpers.clean_up_shapefile()