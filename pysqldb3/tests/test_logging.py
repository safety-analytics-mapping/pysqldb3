import os
import configparser
from .. import pysqldb3 as pysqldb
from . import helpers

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

dest_dbc = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                             host=config.get('SQL_DB', 'SERVER'),
                             db_name=config.get('SQL_DB', 'DB_NAME'),
                             username=config.get('SQL_DB', 'DB_USER'),
                             password=config.get('SQL_DB', 'DB_PASSWORD'))

src_dbc = pysqldb.DbConnect(db_type=config.get('SECOND_SQL_DB', 'TYPE'),
                            host=config.get('SECOND_SQL_DB', 'SERVER'),
                            db_name=config.get('SECOND_SQL_DB', 'DB_NAME'),
                            username=config.get('SECOND_SQL_DB', 'DB_USER'),
                            password=config.get('SECOND_SQL_DB', 'DB_PASSWORD'))

src_dest_dbc = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                                 host=config.get('SQL_DB', 'SERVER'),
                                 db_name=config.get('SQL_DB', 'DB_NAME'),
                                 username=config.get('SECOND_SQL_DB', 'DB_USER'),
                                 password=config.get('SECOND_SQL_DB', 'DB_PASSWORD'))

test_table = 'cross_db_test'
src_table = 'sql_test_table_{user}'.format(user=src_dbc.username)

class TestQueryCreatesTableLoggingSql:
    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_sql(src_dbc)

    def test_create_table_basic(self):
        # make sure table doesnt exist
        src_dbc.drop_table(schema_name=src_dbc.default_schema, table_name=test_table)
        # make table
        src_dbc.query("""
            select top 1 *
            into {schema}.{table}
            from dbo.{src_table}
        """.format(schema=src_dbc.default_schema, table=test_table, src_table=src_table))
        # make sure the table was added to the log
        src_dbc.query("""
            select * from {schema}.__temp_log_table_{user}__ where table_name = '{table}'
        """.format(schema=src_dbc.default_schema, user=src_dbc.username, table=test_table))
        assert src_dbc.data
        # clean up
        src_dbc.drop_table(src_dbc.default_schema, test_table)
        src_dbc.query("""
                    select * from {schema}.__temp_log_table_{user}__ where table_name = '{table}'
                """.format(schema=src_dbc.default_schema, user=src_dbc.username, table=test_table))
        assert not src_dbc.data

    def test_create_table_basic_w_db(self):
        # make sure table doesnt exist
        src_dbc.drop_table(src_dbc.default_schema, test_table)
        # make table
        src_dbc.query("""
            select top 1 *
            into {db}.{schema}.{table}
            from dbo.{src_table}
        """.format(schema=src_dbc.default_schema, table=test_table, db=src_dbc.db_name, src_table=src_table))
        # make sure the table was added to the log
        src_dbc.query("""
            select * from {db}.{schema}.__temp_log_table_{user}__ where table_name = '{table}'
        """.format(schema=src_dbc.default_schema, user=src_dbc.username, table=test_table, db=src_dbc.db_name))
        assert src_dbc.data
        # clean up
        src_dbc.drop_table(src_dbc.default_schema, test_table)
        src_dbc.query("""
                    select * from {db}.{schema}.__temp_log_table_{user}__ where table_name = '{table}'
                """.format(schema=src_dbc.default_schema, user=src_dbc.username, table=test_table, db=src_dbc.db_name))
        assert not src_dbc.data

    def test_create_table_cross_db(self):
        helpers.set_up_test_table_sql(src_dbc)

        # make sure table doesnt exist
        dest_dbc.drop_table(dest_dbc.default_schema, test_table)
        # make table
        src_dbc.query("""
            select top 1 *
            into {db}.{schema}.{table}
            from {sdb}.dbo.{src_table}
        """.format(schema=dest_dbc.default_schema, table=test_table, db=dest_dbc.db_name,
                   sdb=src_dbc.db_name, src_table=src_table))
        # make sure the table was added to the correct log
        if src_dbc.table_exists("__temp_log_table_%s__ " % src_dbc.username, schema='dbo'):
            src_dbc.query("""
                        select * from {db}.{schema}.__temp_log_table_{user}__ where table_name = '{table}'
                    """.format(schema='dbo', user=src_dbc.username, table=test_table, db=src_dbc.db_name))
            assert not src_dbc.data

        dest_dbc.query("""
            select * from {db}.{schema}.__temp_log_table_{user}__ where table_name = '{table}'
        """.format(schema=dest_dbc.default_schema, user=src_dbc.username, table=test_table, db=dest_dbc.db_name))
        assert dest_dbc.data
        # clean up
        dest_dbc.drop_table(dest_dbc.default_schema, test_table)
        dest_dbc.query("""
                    select * from {db}.{schema}.__temp_log_table_{user}__ where table_name = '{table}'
                """.format(schema=dest_dbc.default_schema, user=dest_dbc.username, table=test_table, db=dest_dbc.db_name))
        assert not dest_dbc.data
#
    def test_rename_table_cross_db(self):
        # since SQL server doesnt allow this need to connect to dest db with user from source and rename

        # make sure table doesnt exist
        dest_dbc.drop_table(schema_name=dest_dbc.default_schema, table_name=test_table)
        # make table
        src_dbc.query("""
                    select top 1 *
                    into {db}.{schema}.{test_table}
                    from {src_db}.dbo.{src_table}
                """.format(schema=dest_dbc.default_schema, test_table=test_table, db=dest_dbc.db_name,
                           src_db=src_dbc.db_name, src_table=src_table))
        # make sure the table was added to the correct log

        # check not added to src log
        if src_dbc.table_exists(table_name="__temp_log_table_{src_dbc.username}__", schema='dbo'):
            src_dbc.query("""
                select * from {db}.{schema}.__temp_log_table_{user}__ where table_name = '{table}'
            """.format(schema='dbo', user=src_dbc.username, table=test_table, db=dest_dbc.db_name))
            assert not src_dbc.data
        # check its in the dest log
        dest_dbc.query("""
            select * from {db}.{schema}.__temp_log_table_{user}__ where table_name = '{table}'
        """.format(schema=dest_dbc.default_schema, user=src_dbc.username, table=test_table, db=dest_dbc.db_name))
        assert dest_dbc.data
        dest_dbc.drop_table(schema_name=dest_dbc.default_schema, table_name=f'{test_table}_rename')
        # rename table
        src_dest_dbc.query("""
            EXEC sp_rename '{db}.{schema}.{table}', '{table}_rename'
        """.format(db=dest_dbc.db_name, schema=dest_dbc.default_schema, table=test_table))
        # check dest log was updated
        src_dest_dbc.query("""
            select * from {db}.{schema}.__temp_log_table_{user}__ where table_name = '{table}_rename'
        """.format(schema=dest_dbc.default_schema, user=src_dest_dbc.username, table=test_table, db=src_dest_dbc.db_name))
        assert src_dest_dbc.data
        # check old name was removed from the log
        src_dest_dbc.query("""
            select * from {db}.{schema}.__temp_log_table_{user}__ where table_name = '{table}'
        """.format(schema=src_dest_dbc.default_schema, user=src_dest_dbc.username, table=test_table, db=src_dest_dbc.db_name))
        assert not src_dest_dbc.data
        # clean up
        src_dest_dbc.drop_table(src_dest_dbc.default_schema, test_table + '_rename')
        src_dest_dbc.query("""
            select * from {db}.{schema}.__temp_log_table_{user}__ where table_name = '{table}_rename'
        """.format(schema=src_dest_dbc.default_schema, user=src_dest_dbc.username, table=test_table, db=src_dest_dbc.db_name))
        assert not src_dest_dbc.data

        @classmethod
        def teardown_class(cls):
            helpers.clean_up_test_table_sql(dest_dbc)

class TestQueryCreatesTableLoggingPgSql:
    # cross db queries are not currently permitted in our env
    pass