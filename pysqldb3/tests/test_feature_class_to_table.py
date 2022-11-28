import os

import configparser
import pytest

from . import helpers
from .. import pysqldb3 as pysqldb

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

pg_dbconn = pysqldb.DbConnect(db_type=config.get('PG_DB', 'TYPE'),
                              host=config.get('PG_DB', 'SERVER'),
                              db_name=config.get('PG_DB', 'DB_NAME'),
                              username=config.get('PG_DB', 'DB_USER'),
                              password=config.get('PG_DB', 'DB_PASSWORD'),
                              allow_temp_tables=True)

ms_dbconn = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                              host=config.get('SQL_DB', 'SERVER'),
                              db_name=config.get('SQL_DB', 'DB_NAME'),
                              username=config.get('SQL_DB', 'DB_USER'),
                              password=config.get('SQL_DB', 'DB_PASSWORD'),
                              allow_temp_tables=True)

fgdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data/lion/lion.gdb')
fc = 'node'
table = 'test_feature_class_{username}'.format(username=pg_dbconn.username)


class TestFeatureClassToTablePg:
    @classmethod
    def setup_class(cls):
        helpers.set_up_feature_class()

    @pytest.mark.order1
    def test_import_fc_basic(self):
        pg_dbconn.drop_table(table_name=table, schema_name=pg_dbconn.default_schema)
        assert not pg_dbconn.table_exists(table_name=table, schema_name=pg_dbconn.default_schema)

        pg_dbconn.feature_class_to_table(fgdb, table_name=table, schema_name=None, fc_name=fc)
        assert pg_dbconn.table_exists(table_name=table, schema_name=pg_dbconn.default_schema)

        pg_dbconn.drop_table(schema_name=pg_dbconn.default_schema, table_name=table)

    @pytest.mark.order2
    def test_import_fc_new_name(self):
        pg_dbconn.drop_table(table_name=table, schema_name=pg_dbconn.default_schema)
        assert not pg_dbconn.table_exists(table_name=table, schema=pg_dbconn.default_schema)

        pg_dbconn.feature_class_to_table(fgdb, table_name=table, schema_name=None, fc_name=fc)
        assert pg_dbconn.table_exists(table_name=table, schema_name=pg_dbconn.default_schema)

        pg_dbconn.drop_table(schema_name=pg_dbconn.default_schema, table_name=table)

    @pytest.mark.order3
    def test_import_fc_new_name_schema(self):
        schema_name = 'working'

        pg_dbconn.drop_table(table_name=table, schema_name=schema_name)
        assert not pg_dbconn.table_exists(table_name=table, schema_name=schema_name)

        pg_dbconn.feature_class_to_table(fgdb, table_name=table, fc_name=fc, schema_name=schema_name)
        assert pg_dbconn.table_exists(table_name=table, schema_name=schema_name)

        pg_dbconn.query("select * from {schema}.__temp_log_table_{user}__ where table_name = '{table}'".format(
            schema=schema_name, table=table, user=pg_dbconn.username))
        assert len(pg_dbconn.data) == 1

        pg_dbconn.drop_table(schema_name=schema_name, table_name=table)

    @pytest.mark.order4
    def test_import_fc_new_name_schema_srid(self):
        schema = 'working'

        pg_dbconn.drop_table(table_name=table, schema_name=schema)
        assert not pg_dbconn.table_exists(table, schema=schema)

        pg_dbconn.feature_class_to_table(fgdb, table_name=table, fc_name=fc, schema_name=schema, srid=4326)
        assert pg_dbconn.table_exists(table_name=table, schema_name=schema)

        pg_dbconn.query("select distinct st_srid(geom) from {schema}.{table}".format(schema=schema, table=table))
        assert pg_dbconn.data[0][0] == 4326

        pg_dbconn.drop_table(schema_name=schema, table_name=table)

    @pytest.mark.order5
    def test_import_fc_new_name_data_check(self):
        pg_dbconn.drop_table(table_name=table, schema_name=pg_dbconn.default_schema)
        assert not pg_dbconn.table_exists(table_name=table, schema_name=pg_dbconn.default_schema)

        pg_dbconn.feature_class_to_table(fgdb, table_name=table, schema_name=None, fc_name=fc)
        assert pg_dbconn.table_exists(table_name=table, schema_name=pg_dbconn.default_schema)

        pg_dbconn.query("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table}'
            AND table_schema = '{schema}'
        """.format(schema=pg_dbconn.default_schema, table=table))

        columns = {i[0] for i in pg_dbconn.data}
        types = {i[1] for i in pg_dbconn.data}

        assert columns == {'vintersect', 'objectid', 'geom', 'nodeid'}
        assert types == {'integer', 'integer', 'integer', 'character varying', 'USER-DEFINED'}

        # check non geom data
        pg_dbconn.query("""
                    select objectid, nodeid, vintersect from {schema}.{table} where nodeid in (88, 98, 100)
                """.format(
            schema=pg_dbconn.default_schema, table=table))

        row_values = [(87, 88, 'VirtualIntersection'),
                      (97, 98, ''),
                      (99, 100, 'VirtualIntersection')]

        # assert db.data == row_values
        for c in range(len(pg_dbconn.data)):
            for r in range(len(pg_dbconn.data[c])):
                assert row_values[c][r] == pg_dbconn.data[c][r]

        # check geom matches (less than 1 ft off
        pg_dbconn.query("""
            select st_distance(st_setsrid(ST_GeometryN(geom, 1), 2263),
                st_setsrid(st_makepoint(914145.1,126536.1, 2263),2263))
            from {schema}.{table}
            where nodeid=88
        """.format(schema=pg_dbconn.default_schema, table=table))
        assert pg_dbconn.data[0][0] < 1

        pg_dbconn.query("""
            select st_distance(st_setsrid(ST_GeometryN(geom, 1), 2263),
                st_setsrid(st_makepoint(920184.0, 138084.1, 2263),2263))
            from {schema}.{table}
            where nodeid=888
        """.format(schema=pg_dbconn.default_schema, table=table))

        assert pg_dbconn.data[0][0] < 1

        pg_dbconn.drop_table(schema_name=pg_dbconn.default_schema, table_name=table)

    @pytest.mark.order6
    def test_import_fc_new_name_schema_no_fc(self):
        schema = 'working'

        pg_dbconn.drop_table(table_name=table, schema_name=pg_dbconn.default_schema)
        assert not pg_dbconn.table_exists(table_name=table, schema_name=pg_dbconn.default_schema)

        try:
            pg_dbconn.feature_class_to_table(fgdb, table_name=table, fc_name=fc, schema_name=schema)
        except:
            assert not pg_dbconn.table_exists(table_name=table, schema_name=schema)

        pg_dbconn.drop_table(schema_name=schema, table_name=table)

    @pytest.mark.order7
    def test_import_fc_new_name_schema_private(self):
        private_table = table + '_priv'
        schema = 'working'

        pg_dbconn.drop_table(table_name=private_table, schema_name=schema)
        assert not pg_dbconn.table_exists(table_name=private_table, schema_name=schema)

        pg_dbconn.feature_class_to_table(fgdb, table_name=private_table, fc_name=fc, schema_name=schema, private=True)
        assert pg_dbconn.table_exists(table_name=private_table, schema_name=schema)

        pg_dbconn.query("""
            select distinct grantee from information_schema.table_privileges
            where table_name = '{table}'
            and table_schema='{schema}'
        """.format(schema=schema, table=private_table), strict=False)
        assert len(pg_dbconn.data) == 1

        pg_dbconn.drop_table(schema_name=schema, table_name=private_table)

    @pytest.mark.order8
    def test_import_fc_new_name_schema_tmp(self):
        not_temp_table = table + '_tmp'
        schema = 'working'
        pg_dbconn.drop_table(table_name=not_temp_table, schema_name=schema)
        assert not pg_dbconn.table_exists(table_name=not_temp_table, schema_name=schema)

        pg_dbconn.feature_class_to_table(fgdb, table_name=not_temp_table, fc_name=fc, schema_name=schema, temp=False)
        assert pg_dbconn.table_exists(table_name=not_temp_table, schema_name=schema)

        pg_dbconn.query("select * from {schema}.__temp_log_table_{user}__ where table_name = '{table}'".format(
            schema=schema, table=not_temp_table, user=pg_dbconn.username))
        assert len(pg_dbconn.data) == 0

        pg_dbconn.drop_table(schema_name=schema, table_name=not_temp_table)

    @classmethod
    def teardown_class(cls):
        # helpers.clean_up_feature_class()
        pg_dbconn.cleanup_new_tables()


class TestFeatureClassToTableMs:
    @classmethod
    def setup_class(cls):
        helpers.set_up_feature_class()

    @pytest.mark.order9
    def test_import_fc_basic(self):
        ms_dbconn.drop_table(table_name=table, schema_name=ms_dbconn.default_schema)
        assert not ms_dbconn.table_exists(table_name=table, schema_name=ms_dbconn.default_schema)

        ms_dbconn.feature_class_to_table(fgdb, table_name=table, schema_name=None, fc_name=fc, print_cmd=True, skip_failures='-skip_failures')
        assert ms_dbconn.table_exists(table_name=table, schema_name=ms_dbconn.default_schema)

        ms_dbconn.drop_table(schema_name=ms_dbconn.default_schema, table_name=table)

    @pytest.mark.order10
    def test_import_fc_new_name(self):
        ms_dbconn.drop_table(table_name=table, schema_name=ms_dbconn.default_schema)
        assert not ms_dbconn.table_exists(table_name=table, schema=ms_dbconn.default_schema)

        ms_dbconn.feature_class_to_table(fgdb, table_name=table, schema_name=None, fc_name=fc, skip_failures='-skip_failures')
        assert ms_dbconn.table_exists(table_name=table, schema_name=ms_dbconn.default_schema)

        ms_dbconn.drop_table(schema_name=ms_dbconn.default_schema, table_name=table)

    @pytest.mark.order11
    def test_import_fc_new_name_schema(self):
        schema = 'dbo'

        ms_dbconn.drop_table(table_name=table, schema_name=schema)
        assert not ms_dbconn.table_exists(table_name=table, schema_name=schema)

        ms_dbconn.feature_class_to_table(fgdb, table_name=table, fc_name=fc, schema_name=schema, skip_failures='-skip_failures')
        assert ms_dbconn.table_exists(table_name=table, schema_name=schema)

        ms_dbconn.drop_table(table_name=table, schema_name=schema)

    @pytest.mark.order12
    def test_import_fc_new_name_schema_srid(self):
        schema = 'dbo'

        ms_dbconn.drop_table(table_name=table, schema_name=schema)
        assert not ms_dbconn.table_exists(table_name=table, schema_name=schema)

        ms_dbconn.feature_class_to_table(fgdb, table_name=table, fc_name=fc, schema_name=schema, srid=4326, skip_failures='-skip_failures')
        assert ms_dbconn.table_exists(table_name=table, schema_name=schema)

        ms_dbconn.query("select distinct geom.STSrid from {schema}.{table}".format(schema=schema, table=table))
        assert ms_dbconn.data[0][0] == 4326

        ms_dbconn.drop_table(schema_name=schema, table_name=table)

    @pytest.mark.order13
    def test_import_fc_new_name_data_check(self):
        ms_dbconn.drop_table(table_name=table, schema_name=ms_dbconn.default_schema)

        assert not ms_dbconn.table_exists(table_name=table, schema=ms_dbconn.default_schema)
        ms_dbconn.feature_class_to_table(fgdb, table_name=table, schema_name=None, fc_name=fc, skip_failures='-skip_failures')

        assert ms_dbconn.table_exists(table_name=table, schema_name=ms_dbconn.default_schema)
        ms_dbconn.query("""
                select column_name, data_type
                from INFORMATION_SCHEMA.COLUMNS
                where table_name = '{table}'
                and table_schema='{schema}'
        """.format(schema=ms_dbconn.default_schema, table=table))

        columns = {i[0] for i in ms_dbconn.data}
        types = {i[1] for i in ms_dbconn.data}

        assert columns == {'objectid', 'geom', 'nodeid', 'vintersect'}
        assert types == {'int', 'geometry', 'int', 'nvarchar'}

        # check non geom data
        ms_dbconn.query("""
                            select objectid, nodeid, vintersect from {schema}.{table} where nodeid in (88, 98, 100)
                        """.format(
                             schema=ms_dbconn.default_schema, table=table))

        row_values = [(87, 88, 'VirtualIntersection'),
                   (97, 98, ''),
                   (99, 100, 'VirtualIntersection')]

        for c in range(len(ms_dbconn.data)):
            for r in range(len(ms_dbconn.data[c])):
                assert row_values[c][r] == ms_dbconn.data[c][r]

        # check geom matches (less than 1 ft off)
        ms_dbconn.query("""
            select geom.STGeometryN(1).STDistance(geometry::Point(914145.1,126536.1, 2263))
            from {schema}.{table}
            where nodeid=88
        """.format(schema=ms_dbconn.default_schema, table=table))
        assert ms_dbconn.data[0][0] < 1

        ms_dbconn.query("""
            select geom.STGeometryN(1).STDistance(geometry::Point(920184.0, 138084.1, 2263))
            from {schema}.{table}
            where nodeid=888
        """.format(schema=ms_dbconn.default_schema, table=table))
        assert ms_dbconn.data[0][0] < 1

        ms_dbconn.drop_table(schema_name=ms_dbconn.default_schema, table_name=table)

    @pytest.mark.order14
    def test_import_fc_new_name_schema_no_fc(self):
        fc = 'test_feature_class_no_table'
        schema = 'working'

        ms_dbconn.drop_table(table_name=table, schema_name=schema)
        assert not ms_dbconn.table_exists(table_name=table, schema_name=schema)

        try:
            ms_dbconn.feature_class_to_table(fgdb, table_name=table, fc_name=fc, schema_name=schema, skip_failures='-skip_failures')
        except:
            assert not ms_dbconn.table_exists(table_name=table, schema_name=schema)

        ms_dbconn.drop_table(schema_name=schema, table_name=table)

    @pytest.mark.order15
    def test_import_fc_new_name_schema_temp(self):
        schema = 'dbo'

        ms_dbconn.drop_table(table_name=table, schema_name=schema)
        assert not ms_dbconn.table_exists(table_name=table, schema_name=schema)

        ms_dbconn.feature_class_to_table(fgdb, table_name=table, fc_name=fc, schema_name=schema, temp=False,
                                         skip_failures='-skip_failures')
        assert ms_dbconn.table_exists(table_name=table, schema_name=schema)

        ms_dbconn.query("select * from {schema}.__temp_log_table_{user}__ where table_name = '{table}'".format(
            schema=schema, table=table, user=ms_dbconn.username))
        assert len(ms_dbconn.data) == 0

        ms_dbconn.drop_table(schema_name=schema, table_name=table)

    @pytest.mark.order16
    def test_import_fc_new_name_schema_private(self):
        schema = 'dbo'

        ms_dbconn.drop_table(table_name=table, schema_name=schema)
        assert not ms_dbconn.table_exists(table_name=table, schema_name=schema)

        ms_dbconn.feature_class_to_table(fgdb, table_name=table, fc_name=fc, schema_name=schema, private=True,
                                         skip_failures='-skip_failures')
        assert ms_dbconn.table_exists(table_name=table, schema_name=schema)

        ms_dbconn.query("""
            EXEC sp_table_privileges @table_name = '{table}';
            """.format(table=table))
        ms_dbconn.drop_table(schema_name=schema, table_name=table)

        # FAILING
        # assert df['GRANTEE'].nunique() == 1

    @classmethod
    def teardown_class(cls):
        # helpers.clean_up_feature_class()
        ms_dbconn.cleanup_new_tables()