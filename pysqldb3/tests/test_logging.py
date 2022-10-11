import os
import configparser
from .. import pysqldb3 as pysqldb
from . import helpers

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")


sql_dest = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                        server=config.get('SQL_DB', 'SERVER'),
                        database=config.get('SQL_DB', 'DB_NAME'),
                        user=config.get('SQL_DB', 'DB_USER'),
                        password=config.get('SQL_DB', 'DB_PASSWORD'))

sql_src = pysqldb.DbConnect(type=config.get('SECOND_SQL_DB', 'TYPE'),
                        server=config.get('SECOND_SQL_DB', 'SERVER'),
                        database=config.get('SECOND_SQL_DB', 'DB_NAME'),
                        user=config.get('SECOND_SQL_DB', 'DB_USER'),
                        password=config.get('SECOND_SQL_DB', 'DB_PASSWORD'))


sql_dest_src_user = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                        server=config.get('SQL_DB', 'SERVER'),
                        database=config.get('SQL_DB', 'DB_NAME'),
                        user=config.get('SECOND_SQL_DB', 'DB_USER'),
                        password=config.get('SECOND_SQL_DB', 'DB_PASSWORD'))


test_table = 'cross_db_test'
src_table = f'sql_test_table_{sql_src.user}'

class TestQueryCreatesTableLoggingSql:
    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_sql(sql_src)

    def test_create_table_basic(self):
        # make sure table doesnt exist
        sql_src.drop_table(sql_src.default_schema, test_table)
        # make table
        sql_src.query(f"""
            select top 1 *
                into {sql_src.default_schema}.{test_table}
                from dbo.{src_table}
        """)
        # make sure the table was added to the log
        sql_src.query(f"""
            select * from {sql_src.default_schema}.__temp_log_table_{sql_src.user}__ where table_name = '{test_table}'
        """)
        assert sql_src.data
        # clean up
        sql_src.drop_table(sql_src.default_schema, test_table)
        sql_src.query(f"""
                    select * from {sql_src.default_schema}.__temp_log_table_{sql_src.user}__ where table_name = '{test_table}'
                """)
        assert not sql_src.data

    def test_create_table_basic_w_db(self):
        # make sure table doesnt exist
        sql_src.drop_table(sql_src.default_schema, test_table)
        # make table
        sql_src.query(f"""
            select top 1 *
                into {sql_src.database}.{sql_src.default_schema}.{test_table}
                from dbo.{src_table}
        """)
        # make sure the table was added to the log
        sql_src.query(f"""
            select * from {sql_src.database}.{sql_src.default_schema}.__temp_log_table_{sql_src.user}__ where table_name = '{test_table}'
        """)
        assert sql_src.data
        # clean up
        sql_src.drop_table(sql_src.default_schema, test_table)
        sql_src.query(f"""
                    select * from {sql_src.database}.{sql_src.default_schema}.__temp_log_table_{sql_src.user}__ where table_name = '{test_table}'
                """)
        assert not sql_src.data

    def test_create_table_cross_db(self):
        helpers.set_up_test_table_sql(sql_src)

        # make sure table doesnt exist
        sql_dest.drop_table(sql_dest.default_schema, test_table)
        # make table
        sql_src.query(f"""
            select top 1 *
                into {sql_dest.database}.{sql_dest.default_schema}.{test_table}
                from {sql_src.database}.dbo.{src_table}
        """)
        # make sure the table was added to the correct log
        if sql_src.table_exists(f"__temp_log_table_{sql_src.user}__ ", schema='dbo'):
            sql_src.query(f"""
                select * from {sql_src.database}.dbo.__temp_log_table_{sql_src.user}__ where table_name = '{test_table}'
            """)
            assert not sql_src.data

        sql_dest.query(f"""
            select * from {sql_dest.database}.{sql_dest.default_schema}.__temp_log_table_{sql_src.user}__ where table_name = '{test_table}'
        """)
        assert sql_dest.data
        # clean up
        sql_dest.drop_table(sql_dest.default_schema, test_table)
        sql_dest.query(f"""
            select * from {sql_dest.database}.{sql_dest.default_schema}.__temp_log_table_{sql_dest.user}__ where table_name = '{test_table}'
        """)
        assert not sql_dest.data
#
    def test_rename_table_cross_db(self):
        # since SQL server doesnt allow this need ot conect to dest db with user from source and rename

        # make sure table doesnt exist
        sql_dest.drop_table(sql_dest.default_schema, test_table)
        # make table
        sql_src.query(f"""
                    select top 1 *
                        into {sql_dest.database}.{sql_dest.default_schema}.{test_table}
                        from {sql_src.database}.dbo.{src_table}
                    """)
        # make sure the table was added to the correct log

        # check not added to src log
        if sql_src.table_exists(f"__temp_log_table_{sql_src.user}__ ", schema='dbo'):
            sql_src.query(f"""
                select * from {sql_src.database}.dbo.__temp_log_table_{sql_src.user}__ where table_name = '{test_table}'
            """)
            assert not sql_src.data

        # check its in the dest log
        sql_dest.query(f"""
            select * from {sql_dest.database}.{sql_dest.default_schema}.__temp_log_table_{sql_src.user}__ where table_name = '{test_table}'
        """)
        assert sql_dest.data
        sql_dest.drop_table(sql_dest.default_schema, f'{test_table}_rename')

        # rename table
        sql_dest_src_user.query(f"""
            EXEC sp_rename '{sql_dest.database}.{sql_dest.default_schema}.{test_table}', '{test_table}_rename'
        """)

        # check dest log was updated
        sql_dest_src_user.query(f"""
            select * from {sql_dest_src_user.database}.{sql_dest.default_schema}.__temp_log_table_{sql_dest_src_user.user}__ 
                where table_name = '{test_table}_rename'
        """)
        assert sql_dest_src_user.data

        # check old name was removed from the log
        sql_dest_src_user.query(f"""
            select * from {sql_dest_src_user.database}.{sql_dest_src_user.default_schema}.__temp_log_table_{sql_dest_src_user.user}__ 
                where table_name = '{test_table}'
            """)
        assert not sql_dest_src_user.data

        # clean up
        sql_dest_src_user.drop_table(sql_dest_src_user.default_schema, f'{test_table}_rename')
        sql_dest_src_user.query(f"""
            select * from {sql_dest_src_user.database}.{sql_dest_src_user.default_schema}.__temp_log_table_{sql_dest_src_user.user}__ 
                where table_name = '{test_table}_rename'
            """)
        assert not sql_dest_src_user.data

        @classmethod
        def teardown_class(cls):
            helpers.clean_up_test_table_sql(sql_dest)

class TestQueryCreatesTableLoggingPgSql:
    # cross db queries are not currently permitted in our env
    pass