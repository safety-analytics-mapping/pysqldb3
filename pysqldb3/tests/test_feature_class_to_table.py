import os

import configparser
import pytest

from . import helpers
from .. import pysqldb3 as pysqldb

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(db_type=config.get('PG_DB', 'TYPE'),
                       server=config.get('PG_DB', 'SERVER'),
                       db_name=config.get('PG_DB', 'DB_NAME'),
                       user=config.get('PG_DB', 'DB_USER'),
                       password=config.get('PG_DB', 'DB_PASSWORD'),
                       allow_temp_tables=True)

sql = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                        server=config.get('SQL_DB', 'SERVER'),
                        db_name=config.get('SQL_DB', 'DB_NAME'),
                        user=config.get('SQL_DB', 'DB_USER'),
                        password=config.get('SQL_DB', 'DB_PASSWORD'),
                        allow_temp_tables=True)

fgdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data/lion/lion.gdb')
fc = 'node'
table = 'test_feature_class_{}'.format(db.user)


class TestFeatureClassToTablePg:
    @classmethod
    def setup_class(cls):
        helpers.set_up_feature_class()

    @pytest.mark.order1
    def test_import_fc_basic(self):
        db.drop_table(table_name=table, schema_name=db.default_schema)
        assert not db.table_exists(table, schema=db.default_schema)

        db.feature_class_to_table(fgdb, table, schema_name=None, shp_name=fc)
        assert db.table_exists(table, schema=db.default_schema)

        db.drop_table(db.default_schema, table)

    @pytest.mark.order2
    def test_import_fc_new_name(self):
        db.drop_table(table_name=table, schema_name=db.default_schema)
        assert not db.table_exists(table, schema=db.default_schema)

        db.feature_class_to_table(fgdb, table, schema_name=None, shp_name=fc)
        assert db.table_exists(table, schema=db.default_schema)

        db.drop_table(db.default_schema, table)

    @pytest.mark.order3
    def test_import_fc_new_name_schema(self):
        schema = 'working'

        db.drop_table(table_name=table, schema_name=schema)
        assert not db.table_exists(table, schema=schema)

        db.feature_class_to_table(fgdb, table, shp_name=fc, schema_name=schema)
        assert db.table_exists(table, schema=schema)

        db.query("select * from {s}.__temp_log_table_{u}__ where table_name = '{t}'".format(
            s=schema, t=table, u=db.user))
        assert len(db.data) == 1

        db.drop_table(schema, table)

    @pytest.mark.order4
    def test_import_fc_new_name_schema_srid(self):
        schema = 'working'

        db.drop_table(table_name=table, schema_name=schema)
        assert not db.table_exists(table, schema=schema)

        db.feature_class_to_table(fgdb, table, shp_name=fc, schema_name=schema, srid=4326)
        assert db.table_exists(table, schema=schema)

        db.query("select distinct st_srid(geom) from {}.{}".format(schema, table))
        assert db.data[0][0] == 4326

        db.drop_table(schema, table)

    @pytest.mark.order5
    def test_import_fc_new_name_data_check(self):
        db.drop_table(table_name=table, schema_name=db.default_schema)
        assert not db.table_exists(table, schema=db.default_schema)

        db.feature_class_to_table(fgdb, table, schema_name=None, shp_name=fc)
        assert db.table_exists(table, schema=db.default_schema)

        db.query("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{t}'
            AND table_schema = '{s}'
        """.format(s=db.default_schema, t=table))

        columns = {i[0] for i in db.data}
        types = {i[1] for i in db.data}

        assert columns == {'vintersect', 'objectid', 'geom', 'nodeid'}
        assert types == {'integer', 'integer', 'integer', 'character varying', 'USER-DEFINED'}

        # check non geom data
        db.query("""
                    select objectid, nodeid, vintersect from {}.{} where nodeid in (88, 98, 100)
                """.format(
            db.default_schema, table))

        row_values = [(87, 88, 'VirtualIntersection'),
                      (97, 98, ''),
                      (99, 100, 'VirtualIntersection')]

        # assert db.data == row_values
        for c in range(len(db.data)):
            for r in range(len(db.data[c])):
                assert row_values[c][r] == db.data[c][r]

        # check geom matches (less than 1 ft off
        db.query("""
            select st_distance(st_setsrid(ST_GeometryN(geom, 1), 2263),
                st_setsrid(st_makepoint(914145.1,126536.1, 2263),2263))
            from {s}.{t}
            where nodeid=88
        """.format(s=db.default_schema, t=table))
        assert db.data[0][0] < 1

        db.query("""
            select st_distance(st_setsrid(ST_GeometryN(geom, 1), 2263),
                st_setsrid(st_makepoint(920184.0, 138084.1, 2263),2263))
            from {s}.{t}
            where nodeid=888
        """.format(s=db.default_schema, t=table))

        assert db.data[0][0] < 1

        db.drop_table(db.default_schema, table)

    @pytest.mark.order6
    def test_import_fc_new_name_schema_no_fc(self):
        schema = 'working'

        db.drop_table(table_name=table, schema_name=db.default_schema)
        assert not db.table_exists(table, schema=db.default_schema)

        try:
            db.feature_class_to_table(fgdb, table, shp_name=fc, schema_name=schema)
        except:
            assert not db.table_exists(table, schema=schema)

        db.drop_table(schema, table)

    @pytest.mark.order7
    def test_import_fc_new_name_schema_private(self):
        private_table = table + '_priv'
        schema = 'working'

        db.drop_table(table_name=private_table, schema_name=schema)
        assert not db.table_exists(private_table, schema=schema)

        db.feature_class_to_table(fgdb, private_table, shp_name=fc, schema_name=schema, private=True)
        assert db.table_exists(private_table, schema=schema)

        db.query("""
            select distinct grantee from information_schema.table_privileges
            where table_name = '{t}'
            and table_schema='{s}'
        """.format(s=schema, t=private_table), strict=False)
        assert len(db.data) == 1

        db.drop_table(schema, private_table)

    @pytest.mark.order8
    def test_import_fc_new_name_schema_tmp(self):
        not_temp_table = table + '_tmp'
        schema = 'working'
        db.drop_table(table_name=not_temp_table, schema_name=schema)
        assert not db.table_exists(not_temp_table, schema=schema)

        db.feature_class_to_table(fgdb, not_temp_table, shp_name=fc, schema_name=schema, temp=False)
        assert db.table_exists(not_temp_table, schema=schema)

        db.query("select * from {s}.__temp_log_table_{u}__ where table_name = '{t}'".format(
            s=schema, t=not_temp_table, u=db.user))
        assert len(db.data) == 0

        db.drop_table(schema, not_temp_table)

    @classmethod
    def teardown_class(cls):
        # helpers.clean_up_feature_class()
        db.cleanup_new_tables()


class TestFeatureClassToTableMs:
    @classmethod
    def setup_class(cls):
        helpers.set_up_feature_class()

    @pytest.mark.order9
    def test_import_fc_basic(self):
        sql.drop_table(table_name=table, schema_name=sql.default_schema)
        assert not sql.table_exists(table, schema=sql.default_schema)

        sql.feature_class_to_table(fgdb, table, schema_name=None, shp_name=fc, print_cmd=True,skip_failures='-skip_failures')
        assert sql.table_exists(table, schema=sql.default_schema)

        sql.drop_table(sql.default_schema, table)

    @pytest.mark.order10
    def test_import_fc_new_name(self):
        sql.drop_table(table_name=table, schema_name=sql.default_schema)
        assert not sql.table_exists(table, schema=sql.default_schema)

        sql.feature_class_to_table(fgdb, table, schema_name=None, shp_name=fc, skip_failures='-skip_failures')
        assert sql.table_exists(table, schema=sql.default_schema)

        sql.drop_table(sql.default_schema, table)

    @pytest.mark.order11
    def test_import_fc_new_name_schema(self):
        schema = 'dbo'

        sql.drop_table(table_name=table, schema_name=schema)
        assert not sql.table_exists(table, schema=schema)

        sql.feature_class_to_table(fgdb, table, shp_name=fc, schema_name=schema, skip_failures='-skip_failures')
        assert sql.table_exists(table, schema=schema)

        sql.drop_table(schema, table)

    @pytest.mark.order12
    def test_import_fc_new_name_schema_srid(self):
        schema = 'dbo'

        sql.drop_table(table_name=table, schema_name=schema)
        assert not sql.table_exists(table, schema=schema)

        sql.feature_class_to_table(fgdb, table, shp_name=fc, schema_name=schema, srid=4326, skip_failures='-skip_failures')
        assert sql.table_exists(table, schema=schema)

        sql.query("select distinct geom.STSrid from {}.{}".format(schema, table))
        assert sql.data[0][0] == 4326

        sql.drop_table(schema, table)

    @pytest.mark.order13
    def test_import_fc_new_name_data_check(self):
        sql.drop_table(table_name=table, schema_name=sql.default_schema)

        assert not sql.table_exists(table, schema=sql.default_schema)
        sql.feature_class_to_table(fgdb, table, schema_name=None, shp_name=fc, skip_failures='-skip_failures')

        assert sql.table_exists(table, schema=sql.default_schema)
        sql.query("""
                select column_name, data_type
                from INFORMATION_SCHEMA.COLUMNS
                where table_name = '{t}'
                and table_schema='{s}'
        """.format(s=sql.default_schema, t=table))

        columns = {i[0] for i in sql.data}
        types = {i[1] for i in sql.data}

        assert columns == {'objectid', 'geom', 'nodeid', 'vintersect'}
        assert types == {'int', 'geometry', 'int', 'nvarchar'}

        # check non geom data
        sql.query("""
                            select objectid, nodeid, vintersect from {}.{} where nodeid in (88, 98, 100)
                        """.format(
                             sql.default_schema, table))

        row_values = [(87, 88, 'VirtualIntersection'),
                   (97, 98, ''),
                   (99, 100, 'VirtualIntersection')]

        for c in range(len(sql.data)):
            for r in range(len(sql.data[c])):
                assert row_values[c][r] == sql.data[c][r]

        # check geom matches (less than 1 ft off)
        sql.query("""
            select geom.STGeometryN(1).STDistance(geometry::Point(914145.1,126536.1, 2263))
            from {s}.{t}
            where nodeid=88
        """.format(s=sql.default_schema, t=table))
        assert sql.data[0][0] < 1

        sql.query("""
            select geom.STGeometryN(1).STDistance(geometry::Point(920184.0, 138084.1, 2263))
            from {s}.{t}
            where nodeid=888
        """.format(s=sql.default_schema, t=table))
        assert sql.data[0][0] < 1

        sql.drop_table(sql.default_schema, table)

    @pytest.mark.order14
    def test_import_fc_new_name_schema_no_fc(self):
        fc = 'test_feature_class_no_table'
        schema = 'working'

        sql.drop_table(table_name=table, schema_name=schema)
        assert not sql.table_exists(table, schema=schema)

        try:
            sql.feature_class_to_table(fgdb, table, shp_name=fc, schema_name=schema, skip_failures='-skip_failures')
        except:
            assert not sql.table_exists(table, schema=schema)

        sql.drop_table(schema, table)

    @pytest.mark.order15
    def test_import_fc_new_name_schema_temp(self):
        schema = 'dbo'

        sql.drop_table(table_name=table, schema_name=schema)
        assert not sql.table_exists(table, schema=schema)

        sql.feature_class_to_table(fgdb, table, shp_name=fc, schema_name=schema, temp=False,
        skip_failures='-skip_failures')
        assert sql.table_exists(table, schema=schema)

        sql.query("select * from {s}.__temp_log_table_{u}__ where table_name = '{t}'".format(
            s=schema, t=table, u=sql.user))
        assert len(sql.data) == 0

        sql.drop_table(schema, table)

    @pytest.mark.order16
    def test_import_fc_new_name_schema_private(self):
        schema = 'dbo'

        sql.drop_table(table_name=table, schema_name=schema)
        assert not sql.table_exists(table, schema=schema)

        sql.feature_class_to_table(fgdb, table_name=table, shp_name=fc, schema_name=schema, private=True,
        skip_failures='-skip_failures')
        assert sql.table_exists(table=table, schema=schema)

        sql.query("""
            EXEC sp_table_privileges @table_name = '{t}';
            """.format(t=table))
        sql.drop_table(schema, table)

        # FAILING
        # assert df['GRANTEE'].nunique() == 1

    @classmethod
    def teardown_class(cls):
        # helpers.clean_up_feature_class()
        sql.cleanup_new_tables()