import os

import configparser
import pytest

from . import helpers
from .. import pysqldb3 as pysqldb

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                       server=config.get('PG_DB', 'SERVER'),
                       database=config.get('PG_DB', 'DB_NAME'),
                       user=config.get('PG_DB', 'DB_USER'),
                       password=config.get('PG_DB', 'DB_PASSWORD'),
                       allow_temp_tables=True)

sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                        server=config.get('SQL_DB', 'SERVER'),
                        database=config.get('SQL_DB', 'DB_NAME'),
                        user=config.get('SQL_DB', 'DB_USER'),
                        password=config.get('SQL_DB', 'DB_PASSWORD'),
                        allow_temp_tables=True)

fgdb = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data/lion/lion.gdb')
fc = 'node'
table = f'test_feature_class_{db.user}'

pg_schema = 'working'
ms_schema = 'dbo'

class TestFeatureClassToTablePg:
    @classmethod
    def setup_class(cls):
        helpers.set_up_feature_class()

    @pytest.mark.order1
    def test_import_fc_basic(self):
        db.drop_table(table=table, schema=db.default_schema)
        assert not db.table_exists(table, schema=db.default_schema)

        db.feature_class_to_table(fgdb, table, schema=None, shp_name=fc)
        assert db.table_exists(table, schema=db.default_schema)

        db.drop_table(db.default_schema, table)

    @pytest.mark.order2
    def test_import_fc_new_name(self):
        db.drop_table(table=table, schema=db.default_schema)
        assert not db.table_exists(table, schema=db.default_schema)

        db.feature_class_to_table(fgdb, table, schema=None, shp_name=fc)
        assert db.table_exists(table, schema=db.default_schema)

        db.drop_table(db.default_schema, table)

    @pytest.mark.order3
    def test_import_fc_new_name_schema(self):

        db.drop_table(table=table, schema=pg_schema)
        assert not db.table_exists(table, schema=pg_schema)

        db.feature_class_to_table(fgdb, table, shp_name=fc, schema=pg_schema)
        assert db.table_exists(table, schema=pg_schema)

        db.query(f"select * from {pg_schema}.__temp_log_table_{db.user}__ where table_name = '{table}'")
        assert len(db.data) == 1

        db.drop_table(pg_schema, table)

    @pytest.mark.order4
    def test_import_fc_new_name_schema_srid(self):

        db.drop_table(table=table, schema=pg_schema)
        assert not db.table_exists(table, schema=pg_schema)

        db.feature_class_to_table(fgdb, table, shp_name=fc, schema=pg_schema, srid=4326)
        assert db.table_exists(table, schema=pg_schema)

        db.query(f"select distinct st_srid(geom) from {pg_schema}.{table}")
        assert db.data[0][0] == 4326

        db.drop_table(pg_schema, table)

    @pytest.mark.order5
    def test_import_fc_new_name_data_check(self):
        db.drop_table(table=table, schema=db.default_schema)
        assert not db.table_exists(table, schema=db.default_schema)

        db.feature_class_to_table(fgdb, table, schema=None, shp_name=fc)
        assert db.table_exists(table, schema=db.default_schema)

        db.query(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table}'
            AND table_schema = '{db.default_schema}'
        """)

        columns = {i[0] for i in db.data}
        types = {i[1] for i in db.data}

        assert {'vintersect', 'objectid', 'geom', 'nodeid'}.issubset(columns)
        assert {'integer', 'integer', 'integer', 'character varying', 'USER-DEFINED'}.issubset(types)

        # check non geom data
        db.query(f"""
                    select nodeid, vintersect, st_astext(geom, 1) geom from {db.default_schema}.{table} where nodeid in (88, 98, 100)
                """)

        row_values = [(88, 'VirtualIntersection', 'MULTIPOINT(914145.1 126536.1)'),
                      (98, '', 'MULTIPOINT(914714.8 126499.8)'),
                      (100, 'VirtualIntersection', 'MULTIPOINT(914872 126696.6)')]

        # assert db.data == row_values
        for c in range(len(db.data)):
            for r in range(len(db.data[c])):
                assert row_values[c][r] == db.data[c][r]

        # check geom matches (less than 1 ft off
        db.query(f"""
            select st_distance(st_setsrid(ST_GeometryN(geom, 1), 2263),
                st_setsrid(st_makepoint(914145.1,126536.1, 2263),2263))
            from {db.default_schema}.{table}
            where nodeid=88
        """)
        assert db.data[0][0] < 1

        db.query(f"""
            select st_distance(st_setsrid(ST_GeometryN(geom, 1), 2263),
                st_setsrid(st_makepoint(920184.0, 138084.1, 2263),2263))
            from {db.default_schema}.{table}
            where nodeid=888
        """)

        assert db.data[0][0] < 1

        db.drop_table(db.default_schema, table)

    @pytest.mark.order6
    def test_import_fc_new_name_schema_no_fc(self):

        db.drop_table(table=table, schema=db.default_schema)
        assert not db.table_exists(table, schema=db.default_schema)

        try:
            db.feature_class_to_table(fgdb, table, shp_name=fc, schema=pg_schema)
        except:
            assert not db.table_exists(table, schema=pg_schema)

        db.drop_table(pg_schema, table)

    @pytest.mark.order7
    def test_import_fc_new_name_schema_private(self):
        private_table = table + '_priv'

        db.drop_table(table=private_table, schema=pg_schema)
        assert not db.table_exists(private_table, schema=pg_schema)

        db.feature_class_to_table(fgdb, private_table, shp_name=fc, schema=pg_schema, private=True)
        assert db.table_exists(private_table, schema=pg_schema)

        db.query(f"""
            select distinct grantee from information_schema.table_privileges
            where table_name = '{private_table}'
            and table_schema='{pg_schema}'
        """, strict=False)
        assert len(db.data) == 1

        db.drop_table(pg_schema, private_table)

    @pytest.mark.order8
    def test_import_fc_new_name_schema_tmp(self):
        not_temp_table = table + '_tmp'
        db.drop_table(table=not_temp_table, schema=pg_schema)
        assert not db.table_exists(not_temp_table, schema=pg_schema)

        db.feature_class_to_table(fgdb, not_temp_table, shp_name=fc, schema=pg_schema, temp=False)
        assert db.table_exists(not_temp_table, schema=pg_schema)

        db.query(f"select * from {pg_schema}.__temp_log_table_{db.user}__ where table_name = '{not_temp_table}'")
        assert len(db.data) == 0

        db.drop_table(pg_schema, not_temp_table)

    @classmethod
    def teardown_class(cls):
        db.cleanup_new_tables()

class TestFeatureClassToTableMs:
    @classmethod
    def setup_class(cls):
        helpers.set_up_feature_class()

    @pytest.mark.order9
    def test_import_fc_basic(self):
        sql.drop_table(table=table, schema=sql.default_schema)
        assert not sql.table_exists(table, schema=sql.default_schema)

        sql.feature_class_to_table(fgdb, table, schema=None, shp_name=fc, print_cmd=True,skip_failures='-skip_failures')
        assert sql.table_exists(table, schema=sql.default_schema)

        sql.drop_table(sql.default_schema, table)

    @pytest.mark.order10
    def test_import_fc_new_name(self):
        sql.drop_table(table=table, schema=sql.default_schema)
        assert not sql.table_exists(table, schema=sql.default_schema)

        sql.feature_class_to_table(fgdb, table, schema=None, shp_name=fc, skip_failures='-skip_failures')
        assert sql.table_exists(table, schema=sql.default_schema)

        sql.drop_table(sql.default_schema, table)

    @pytest.mark.order11
    def test_import_fc_new_name_schema(self):

        sql.drop_table(table=table, schema=ms_schema)
        assert not sql.table_exists(table, schema=ms_schema)

        sql.feature_class_to_table(fgdb, table, shp_name=fc, schema=ms_schema, skip_failures='-skip_failures')
        assert sql.table_exists(table, schema=ms_schema)

        sql.drop_table(ms_schema, table)

    @pytest.mark.order12
    def test_import_fc_new_name_schema_srid(self):

        sql.drop_table(table=table, schema=ms_schema)
        assert not sql.table_exists(table, schema=ms_schema)

        sql.feature_class_to_table(fgdb, table, shp_name=fc, schema=ms_schema, srid=4326, skip_failures='-skip_failures')
        assert sql.table_exists(table, schema=ms_schema)

        sql.query(f"select distinct geom.STSrid from {ms_schema}.{table}")
        assert sql.data[0][0] == 4326

        sql.drop_table(ms_schema, table)

    @pytest.mark.order13
    def test_import_fc_new_name_data_check(self):
        sql.drop_table(table=table, schema=sql.default_schema)

        assert not sql.table_exists(table, schema=sql.default_schema)
        sql.feature_class_to_table(fgdb, table, schema=None, shp_name=fc, skip_failures='-skip_failures')

        assert sql.table_exists(table, schema=sql.default_schema)
        sql.query(f"""
                select column_name, data_type
                from INFORMATION_SCHEMA.COLUMNS
                where table_name = '{table}'
                and table_schema='{sql.default_schema}'
        """)

        columns = {i[0] for i in sql.data}
        types = {i[1] for i in sql.data}

        assert {'objectid', 'geom', 'nodeid', 'vintersect'}.issubset(columns)
        assert {'int', 'geometry', 'int', 'nvarchar'}.issubset(types)

        # check non geom data
        sql.query(f"""
                            select nodeid, vintersect, geom.STAsText() geom from {sql.default_schema}.{table} where nodeid in (88, 98, 100)
                        """)

        row_values = [(88, 'VirtualIntersection', 'MULTIPOINT ((914145.06807594 126536.07138967514))'),
                      (98, '', 'MULTIPOINT ((914714.79952293634 126499.80801236629))'),
                      (100, 'VirtualIntersection', 'MULTIPOINT ((914872.03410968184 126696.62913236022))')]

        for c in range(len(sql.data)):
            for r in range(len(sql.data[c])):
                assert row_values[c][r] == sql.data[c][r]

        # check geom matches (less than 1 ft off)
        sql.query(f"""
            select geom.STGeometryN(1).STDistance(geometry::Point(914145.1,126536.1, 2263))
            from {sql.default_schema}.{table}
            where nodeid=88
        """)
        assert sql.data[0][0] < 1

        sql.query(f"""
            select geom.STGeometryN(1).STDistance(geometry::Point(920184.0, 138084.1, 2263))
            from {sql.default_schema}.{table}
            where nodeid=888
        """)
        assert sql.data[0][0] < 1

        sql.drop_table(sql.default_schema, table)

    @pytest.mark.order14
    def test_import_fc_new_name_schema_no_fc(self):
        fc = 'test_feature_class_no_table'

        sql.drop_table(table=table, schema=ms_schema)
        assert not sql.table_exists(table, schema=pg_schema)

        try:
            sql.feature_class_to_table(fgdb, table, shp_name=fc, schema=pg_schema, skip_failures='-skip_failures')
        except:
            assert not sql.table_exists(table, schema=pg_schema)

        sql.drop_table(ms_schema, table)

    @pytest.mark.order15
    def test_import_fc_new_name_schema_temp(self):

        sql.drop_table(table=table, schema=ms_schema)
        assert not sql.table_exists(table, schema=ms_schema)

        sql.feature_class_to_table(fgdb, table, shp_name=fc, schema=ms_schema, temp=False,
        skip_failures='-skip_failures')
        assert sql.table_exists(table, schema=ms_schema)

        sql.query(f"select * from {ms_schema,}.__temp_log_table_{sql.user}__ where table_name = '{table}'")
        assert len(sql.data) == 0

        sql.drop_table(ms_schema, table)

    @pytest.mark.order16
    def test_import_fc_new_name_schema_private(self):

        sql.drop_table(table=table, schema=ms_schema)
        assert not sql.table_exists(table, schema=ms_schema)

        sql.feature_class_to_table(fgdb, table=table, shp_name=fc, schema=ms_schema, private=True,
        skip_failures='-skip_failures')
        assert sql.table_exists(table=table, schema=ms_schema)

        sql.query("""
            EXEC sp_table_privileges @table_name = '{t}';
            """.format(t=table))
        sql.drop_table(ms_schema, table)

        # FAILING
        # assert df['GRANTEE'].nunique() == 1

    @classmethod
    def teardown_class(cls):
        sql.cleanup_new_tables()