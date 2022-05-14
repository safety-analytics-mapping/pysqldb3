# -*- coding: utf-8 -*-
import os
import configparser

from .. import pysqldb3 as pysqldb

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


class TestQueryToMapPg:
    # def test_query_to_map_builtin_precinct(self):
    #     try:
    #         db.query_to_map(query="select precinct, random()*100 as count from districts_police_precincts",
    #                         value_column='count')
    #     except Exception as e:
    #         print(e)
    #         assert False
    #
    #     assert True
    #
    # def test_query_to_map_builtin_nta(self):
    #     try:
    #         db.query_to_map(query="select ntacode, random()*100 as val from districts_nta",
    #                         value_column='val')
    #     except Exception as e:
    #         print(e)
    #         assert False
    #
    #     assert True
    #
    # def test_query_to_map_query_geom_1(self):
    #     try:
    #         db.query_to_map(query="select random()*100 count, objectid, geom from districts_community_boards",
    #                         value_column='count', geom_column='geom', id_column='objectid')
    #     except Exception as e:
    #         print(e)
    #         assert False
    #
    #     assert True
    #
    # def test_query_to_map_query_geom_2(self):
    #     try:
    #         db.query_to_map(query="select random()*100 count, assemdist, geom from districts_state_assembly",
    #                         value_column='count', geom_column='geom', id_column='assemdist')
    #     except Exception as e:
    #         print(e)
    #         assert False
    #
    #     assert True

    def test_query_to_map_error(self):
        try:
            db.query_to_map(query="select random()*100 count, blah_blah, geom from districts_state_assembly",
                            value_column='count', geom_column='geom')
        except Exception as e:
            print(e)
            assert True
            return

        assert False

    def test_query_to_map_geom_not_id(self):
        try:
            db.query_to_map(query="select random()*100 count, assemdist, geom from districts_state_assembly",
                            value_column='count', geom_column='geom')
        except Exception as e:
            print(e)
            assert True
            return

        assert False

    def test_query_to_map_id_not_geom(self):
        try:
            db.query_to_map(query="select random()*100 count, assemdist, geom from districts_state_assembly",
                            value_column='count', id_column='assemdist')
        except Exception as e:
            print(e)
            assert True
            return

        assert False
