import os
import configparser
from .. import pysqldb3 as pysqldb
from . import helpers

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

sql_dest = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                             server=config.get('SQL_DB', 'SERVER'),
                             database=config.get('SQL_DB', 'DB_NAME'),
                             user=config.get('SQL_DB', 'DB_USER'),
                             password=config.get('SQL_DB', 'DB_PASSWORD'))

sql_src = pysqldb.DbConnect(db_type=config.get('SECOND_SQL_DB', 'TYPE'),
                            server=config.get('SECOND_SQL_DB', 'SERVER'),
                            database=config.get('SECOND_SQL_DB', 'DB_NAME'),
                            user=config.get('SECOND_SQL_DB', 'DB_USER'),
                            password=config.get('SECOND_SQL_DB', 'DB_PASSWORD'))

sql_dest_src_user = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                                      server=config.get('SQL_DB', 'SERVER'),
                                      database=config.get('SQL_DB', 'DB_NAME'),
                                      user=config.get('SECOND_SQL_DB', 'DB_USER'),
                                      password=config.get('SECOND_SQL_DB', 'DB_PASSWORD'))

test_table = 'cross_db_test'
src_table = 'sql_test_table_{}'.format(sql_src.user)

class TestQueryCreatesTableLoggingSql:
    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_sql(sql_src)

    def test_create_table_basic(self):
        # make sure table doesnt exist
        sql_src.drop_table(sql_src.default_schema, test_table)
        # make table
        sql_src.query("""
            select top 1 *
            into {s}.{t}
            from dbo.{src_tbl}
        """.format(s=sql_src.default_schema, t=test_table, src_tbl=src_table))
        # make sure the table was added to the log
        sql_src.query("""
            select * from {s}.__temp_log_table_{u}__ where table_name = '{t}'
        """.format(s=sql_src.default_schema, u=sql_src.user, t=test_table))
        assert sql_src.data
        # clean up
        sql_src.drop_table(sql_src.default_schema, test_table)
        sql_src.query("""
                    select * from {s}.__temp_log_table_{u}__ where table_name = '{t}'
                """.format(s=sql_src.default_schema, u=sql_src.user, t=test_table))
        assert not sql_src.data

    def test_create_table_basic_w_db(self):
        # make sure table doesnt exist
        sql_src.drop_table(sql_src.default_schema, test_table)
        # make table
        sql_src.query("""
            select top 1 *
            into {d}.{s}.{t}
            from dbo.{src_tbl}
        """.format(s=sql_src.default_schema, t=test_table, d=sql_src.database, src_tbl=src_table))
        # make sure the table was added to the log
        sql_src.query("""
            select * from {d}.{s}.__temp_log_table_{u}__ where table_name = '{t}'
        """.format(s=sql_src.default_schema, u=sql_src.user, t=test_table, d=sql_src.database))
        assert sql_src.data
        # clean up
        sql_src.drop_table(sql_src.default_schema, test_table)
        sql_src.query("""
                    select * from {d}.{s}.__temp_log_table_{u}__ where table_name = '{t}'
                """.format(s=sql_src.default_schema, u=sql_src.user, t=test_table, d=sql_src.database))
        assert not sql_src.data

    def test_create_table_cross_db(self):
        helpers.set_up_test_table_sql(sql_src)

        # make sure table doesnt exist
        sql_dest.drop_table(sql_dest.default_schema, test_table)
        # make table
        sql_src.query("""
            select top 1 *
            into {d}.{s}.{t}
            from {sd}.dbo.{src_tbl}
        """.format(s=sql_dest.default_schema, t=test_table, d=sql_dest.database,
                   sd=sql_src.database, src_tbl=src_table))
        # make sure the table was added to the correct log
        if sql_src.table_exists("__temp_log_table_%s__ " % sql_src.user, schema='dbo'):
            sql_src.query("""
                        select * from {d}.{s}.__temp_log_table_{u}__ where table_name = '{t}'
                    """.format(s='dbo', u=sql_src.user, t=test_table, d=sql_src.database))
            assert not sql_src.data

        sql_dest.query("""
            select * from {d}.{s}.__temp_log_table_{u}__ where table_name = '{t}'
        """.format(s=sql_dest.default_schema, u=sql_src.user, t=test_table, d=sql_dest.database))
        assert sql_dest.data
        # clean up
        sql_dest.drop_table(sql_dest.default_schema, test_table)
        sql_dest.query("""
                    select * from {d}.{s}.__temp_log_table_{u}__ where table_name = '{t}'
                """.format(s=sql_dest.default_schema, u=sql_dest.user, t=test_table, d=sql_dest.database))
        assert not sql_dest.data
#
    def test_rename_table_cross_db(self):
        # since SQL server doesnt allow this need ot conect to dest db with user from source and rename

        # make sure table doesnt exist
        sql_dest.drop_table(sql_dest.default_schema, test_table)
        # make table
        sql_src.query("""
                    select top 1 *
                    into {d}.{s}.{t}
                    from {sd}.dbo.{tt}
                """.format(s=sql_dest.default_schema, t=test_table, d=sql_dest.database,
                           sd=sql_src.database, tt=src_table))
        # make sure the table was added to the correct log

        # check not added to src log
        if sql_src.table_exists("__temp_log_table_%s__ " % sql_src.user, schema='dbo'):
            sql_src.query("""
                select * from {d}.{s}.__temp_log_table_{u}__ where table_name = '{t}'
            """.format(s='dbo', u=sql_src.user, t=test_table, d=sql_src.database))
            assert not sql_src.data
        # check its in the dest log
        sql_dest.query("""
            select * from {d}.{s}.__temp_log_table_{u}__ where table_name = '{t}'
        """.format(s=sql_dest.default_schema, u=sql_src.user, t=test_table, d=sql_dest.database))
        assert sql_dest.data
        sql_dest.drop_table(sql_dest.default_schema, test_table+'_rename')
        # rename table
        sql_dest_src_user.query("""
            EXEC sp_rename '{db}.{sch}.{t}', '{t}_rename'
        """.format(db=sql_dest.database, sch=sql_dest.default_schema, t=test_table))
        # check dest log was updated
        sql_dest_src_user.query("""
            select * from {d}.{s}.__temp_log_table_{u}__ where table_name = '{t}_rename'
        """.format(s=sql_dest.default_schema, u=sql_dest_src_user.user, t=test_table, d=sql_dest_src_user.database))
        assert sql_dest_src_user.data
        # check old name was removed from the log
        sql_dest_src_user.query("""
            select * from {d}.{s}.__temp_log_table_{u}__ where table_name = '{t}'
        """.format(s=sql_dest_src_user.default_schema, u=sql_dest_src_user.user, t=test_table, d=sql_dest_src_user.database))
        assert not sql_dest_src_user.data
        # clean up
        sql_dest_src_user.drop_table(sql_dest_src_user.default_schema, test_table+'_rename')
        sql_dest_src_user.query("""
            select * from {d}.{s}.__temp_log_table_{u}__ where table_name = '{t}_rename'
        """.format(s=sql_dest_src_user.default_schema, u=sql_dest_src_user.user, t=test_table, d=sql_dest_src_user.database))
        assert not sql_dest_src_user.data

        @classmethod
        def teardown_class(cls):
            helpers.clean_up_test_table_sql(sql_dest)

class TestQueryCreatesTableLoggingPgSql:
    # cross db queries are not currently permitted in our env
    pass