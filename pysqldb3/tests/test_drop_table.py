import os

import configparser

from .. import pysqldb3 as pysqldb

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

test_for_drop_table = 'test_for_drop_table_func_{username}'.format(username=pg_dbconn.username)


class TestDropTablePG:
    def test_drop_table_basic(self):
        schema = 'working'
        query_string = """
        
        DROP TABLE IF EXISTS working.{table};
        CREATE TABLE working.{table} AS 
    
        SELECT 'a' as col1, 'b' as col2;
    
        """.format(table=test_for_drop_table)

        pg_dbconn.query(query_string)
        pg_dbconn.drop_table(schema_name=schema, table_name=test_for_drop_table)
        assert not pg_dbconn.table_exists(table_name=test_for_drop_table, schema_name=schema)

    def test_remove_table_from_log(self):
        query_string = """

        DROP TABLE IF EXISTS working.{table};
        CREATE TABLE working.{table} AS 
        
        SELECT 'a' as col1, 'b' as col2;
        
        INSERT INTO working.__temp_log_table_{username}__ (tbl_id, table_owner,table_schema,table_name,created_on,expires) 
            VALUES (0, '{username}', 'working','{username}','09/18/2020',null);
        """.format(table=test_for_drop_table, username=pg_dbconn.username)

        pg_dbconn.query(query_string)
        pg_dbconn.drop_table(schema_name='working', table_name=test_for_drop_table)

        log_check_query_string = """
        
            SELECT tbl_id, table_owner, table_schema, table_name, created_on, expires
            FROM working.__temp_log_table_{username}__
            WHERE table_name = '{table}'
            
        """.format(username=pg_dbconn.username, table=test_for_drop_table)

        assert len(pg_dbconn.dfquery(log_check_query_string)) == 0

    def test_table_not_in_log(self):
        query_string = """
            DROP TABLE IF EXISTS working.{table};
            CREATE TABLE working.{table} AS 
            
            SELECT 'a' as col1, 'b' as col2;
            """.format(table=test_for_drop_table)

        pg_dbconn.query(query_string)
        pg_dbconn.drop_table(schema_name='working', table_name=test_for_drop_table)

        log_check_query_string = """
        
            SELECT tbl_id, table_owner, table_schema, table_name, created_on, expires
            FROM working.__temp_log_table_{username}__
            WHERE table_name = '{table}'
        
        """.format(username=pg_dbconn.username, table=test_for_drop_table)

        assert len(pg_dbconn.dfquery(log_check_query_string)) == 0

    def test_table_DNE_but_in_log(self):
        query_string = """

        INSERT INTO working.__temp_log_table_{username}__ (tbl_id, table_owner,table_schema,table_name,created_on,expires) 
            VALUES (0, '{table}', 'working','{table}','09/18/2020',null);
            """.format(username=pg_dbconn.username, table=test_for_drop_table)

        pg_dbconn.query(query_string)
        pg_dbconn.drop_table('working', table_name=test_for_drop_table)

        log_check_query_string = """
            SELECT tbl_id, table_owner, table_schema, table_name, created_on, expires
            FROM working.__temp_log_table_{username}__
            WHERE table_name = '{table}'
        """.format(username=pg_dbconn.username, table=test_for_drop_table)

        assert len(pg_dbconn.dfquery(log_check_query_string)) == 0

    def test_table_does_not_exist(self):
        assert pg_dbconn.drop_table('working', test_for_drop_table) is None


class TestDropTableMS:

    def test_drop_table_basic(self):
        table_name = 'sql_test_table_{username}'.format(username=ms_dbconn.username)  # from helper.py

        schema = 'dbo'
        query_string = """
        IF OBJECT_ID('dbo.{table}', 'u') IS NOT NULL
        DROP TABLE dbo.{table};
        
        SELECT 'a' as col1, 'b' as col2
        INTO dbo.{table};
        """.format(table=test_for_drop_table)

        ms_dbconn.query(query_string)
        ms_dbconn.drop_table(schema_name=schema, table_name=test_for_drop_table)

        assert not ms_dbconn.table_exists(test_for_drop_table, schema=schema)

    def test_remove_table_from_log(self):
        table_name = 'sql_test_table_{table}'.format(table=ms_dbconn.username)  # from helper.py
        query_string = """

        IF OBJECT_ID('dbo.{table}', 'u') IS NOT NULL
        DROP TABLE dbo.{table};

        SELECT 'a' as col1, 'b' as col2
        INTO dbo.{table};

        INSERT INTO [dbo].[__temp_log_table_{username}__] (table_owner,table_schema,table_name,created_on,expires) 
        VALUES ('{username}', 'dbo','{table_name}','09/18/2020',null);

        """.format(table=test_for_drop_table, table_name=table_name, username=ms_dbconn.username)

        ms_dbconn.query(query_string)
        ms_dbconn.drop_table(schema_name='dbo', table_name=test_for_drop_table)

        log_check_query_string = """
            SELECT tbl_id, table_owner, table_schema, table_name, created_on, expires
            FROM [dbo].[__temp_log_table_{username}__]
            WHERE table_name = '{table}'
        """.format(username=ms_dbconn.username, table=test_for_drop_table)

        assert len(ms_dbconn.dfquery(log_check_query_string)) == 0

    def test_table_not_in_log(self):
        table_name = 'sql_test_table_{username}'.format(username=ms_dbconn.username)  # from helper.py
        query_string = """

        IF OBJECT_ID('dbo.{table}', 'u') IS NOT NULL
        DROP TABLE dbo.{table};
        SELECT 'a' as col1, 'b' as col2
        INTO dbo.{table};
        """.format(table=test_for_drop_table)

        ms_dbconn.query(query_string)
        ms_dbconn.drop_table(schema_name='dbo', table_name=test_for_drop_table)

        log_check_query_string = """
            SELECT tbl_id, table_owner, table_schema, table_name, created_on, expires
            FROM [dbo].[__temp_log_table_{username}__]
            WHERE table_name = '{table}'
        """.format(username=ms_dbconn.username, table=test_for_drop_table)

        assert len(ms_dbconn.dfquery(log_check_query_string)) == 0

    def test_table_DNE_but_in_log(self):
        query_string = """
        INSERT INTO [dbo].[__temp_log_table_{username}__] (table_owner,table_schema,table_name,created_on,expires) 
        VALUES ('{username}', 'dbo','{table}','09/18/2020',null);
        """.format(username=ms_dbconn.username, table=test_for_drop_table)

        ms_dbconn.query(query_string)
        ms_dbconn.drop_table(schema_name='dbo', table_name=test_for_drop_table)

        log_check_query_string = """
            SELECT tbl_id, table_owner, table_schema, table_name, created_on, expires
            FROM [dbo].[__temp_log_table_{username}__]
            WHERE table_name = '{table}'
        """.format(username=ms_dbconn.username, table=test_for_drop_table)

        assert len(ms_dbconn.dfquery(log_check_query_string)) == 0

    def test_table_does_not_exist(self):
        assert ms_dbconn.drop_table('dbo', table_name=test_for_drop_table) is None
