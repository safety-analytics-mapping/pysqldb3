import os

import configparser

from .. import pysqldb3 as pysqldb

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                       server=config.get('PG_DB', 'SERVER'),
                       database=config.get('PG_DB', 'DB_NAME'),
                       user=config.get('PG_DB', 'DB_USER'),
                       password=config.get('PG_DB', 'DB_PASSWORD'))

sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                        server=config.get('SQL_DB', 'SERVER'),
                        database=config.get('SQL_DB', 'DB_NAME'),
                        user=config.get('SQL_DB', 'DB_USER'),
                        password=config.get('SQL_DB', 'DB_PASSWORD'))

test_for_drop_table = f'test_for_drop_table_func_{db.user}'
pg_schema = 'working'
ms_schema = 'dbo'

class TestDropTablePG:
    def test_drop_table_basic(self):

        query_string = f"""
        
        DROP TABLE IF EXISTS {pg_schema}.{test_for_drop_table};
        CREATE TABLE {pg_schema}.{test_for_drop_table} AS 
    
        SELECT 'a' as col1, 'b' as col2;
    
        """

        db.query(query_string)
        db.drop_table(pg_schema, test_for_drop_table)
        assert not db.table_exists(test_for_drop_table, schema=pg_schema)

    def test_remove_table_from_log(self):
        query_string = f"""

        DROP TABLE IF EXISTS {pg_schema}.{test_for_drop_table};
        CREATE TABLE {pg_schema}.{test_for_drop_table} AS 

        SELECT 'a' as col1, 'b' as col2;

        INSERT INTO {pg_schema}.__temp_log_table_{db.user}__ (tbl_id, table_owner,table_schema,table_name,created_on,expires) VALUES (0, '{db.user}', 'working','{test_for_drop_table}','09/18/2020',null);

        """

        db.query(query_string)
        db.drop_table('working', test_for_drop_table)

        log_check_query_string = f"""
        
            SELECT tbl_id, table_owner, table_schema, table_name, created_on, expires
            FROM {pg_schema}.__temp_log_table_{db.user}__
            WHERE table_name = '{test_for_drop_table}'
            
        """

        assert len(db.dfquery(log_check_query_string)) == 0

    def test_table_not_in_log(self):
        query_string = f"""
    
            DROP TABLE IF EXISTS {pg_schema}.{test_for_drop_table};
            CREATE TABLE {pg_schema}.{test_for_drop_table} AS 
    
            SELECT 'a' as col1, 'b' as col2;
            """

        db.query(query_string)
        db.drop_table(schema=pg_schema, table=test_for_drop_table)

        log_check_query_string = f"""
        
            SELECT tbl_id, table_owner, table_schema, table_name, created_on, expires
            FROM {pg_schema}.__temp_log_table_{db.user}__
            WHERE table_name = '{test_for_drop_table}'
        
        """

        assert len(db.dfquery(log_check_query_string)) == 0

    def test_table_DNE_but_in_log(self):
        query_string = f"""

        INSERT INTO working.__temp_log_table_{db.user}__ (tbl_id, table_owner,table_schema,table_name,created_on,expires) VALUES (0, '{db.user}', 'working','{test_for_drop_table}','09/18/2020',null);

        """

        db.query(query_string)
        db.drop_table(pg_schema, table=test_for_drop_table)

        log_check_query_string = f"""
            SELECT tbl_id, table_owner, table_schema, table_name, created_on, expires
            FROM {pg_schema}.__temp_log_table_{db.user}__
            WHERE table_name = '{test_for_drop_table}'
        """

        assert len(db.dfquery(log_check_query_string)) == 0

    def test_table_does_not_exist(self):
        assert db.drop_table(pg_schema, test_for_drop_table) is None


class TestDropTableMS:

    def test_drop_table_basic(self):

        query_string = f"""

        IF OBJECT_ID('{ms_schema}.{test_for_drop_table}', 'u') IS NOT NULL
        DROP TABLE {ms_schema}.{test_for_drop_table};

        SELECT 'a' as col1, 'b' as col2
        INTO {ms_schema}.{test_for_drop_table};

        """
        sql.query(query_string)
        sql.drop_table(ms_schema, table=test_for_drop_table)

        assert not sql.table_exists(test_for_drop_table, schema=ms_schema)

    def test_remove_table_from_log(self):
        query_string = f"""

        IF OBJECT_ID('{ms_schema}.{test_for_drop_table}', 'u') IS NOT NULL
        DROP TABLE {ms_schema}.{test_for_drop_table};

        SELECT 'a' as col1, 'b' as col2
        INTO {ms_schema}.{test_for_drop_table};

        INSERT INTO [{ms_schema}].[__temp_log_table_{sql.user}__] (table_owner,table_schema,table_name,created_on,expires) 
        VALUES ('{sql.user}', '{ms_schema}','{test_for_drop_table}','09/18/2020',null);

        """

        sql.query(query_string)
        sql.drop_table(schema = ms_schema, table=test_for_drop_table)

        log_check_query_string = f"""
            SELECT tbl_id, table_owner, table_schema, table_name, created_on, expires
            FROM [{ms_schema}].[__temp_log_table_{sql.user}__]
            WHERE table_name = '{test_for_drop_table}'
        """

        assert len(sql.dfquery(log_check_query_string)) == 0

    def test_table_not_in_log(self):
        query_string = f"""

        IF OBJECT_ID('{ms_schema}.{test_for_drop_table}', 'u') IS NOT NULL
        DROP TABLE {ms_schema}.{test_for_drop_table};


        SELECT 'a' as col1, 'b' as col2
        INTO {ms_schema}.{test_for_drop_table};

        """

        sql.query(query_string)
        sql.drop_table(ms_schema, table=test_for_drop_table)

        log_check_query_string = f"""
            SELECT tbl_id, table_owner, table_schema, table_name, created_on, expires
            FROM [{ms_schema}].[__temp_log_table_{sql.user}__]
            WHERE table_name = '{test_for_drop_table}'
        """

        assert len(sql.dfquery(log_check_query_string)) == 0

    def test_table_DNE_but_in_log(self):
        query_string = f"""

        INSERT INTO [dbo].[__temp_log_table_{sql.user}__] (table_owner,table_schema,table_name,created_on,expires) 
        VALUES ('{sql.user}', 'dbo','{test_for_drop_table}','09/18/2020',null);

        """

        sql.query(query_string)
        sql.drop_table(ms_schema, table=test_for_drop_table)

        log_check_query_string = f"""
            SELECT tbl_id, table_owner, table_schema, table_name, created_on, expires
            FROM [{ms_schema}].[__temp_log_table_{sql.user}__]
            WHERE table_name = '{test_for_drop_table}'
        """
        assert len(sql.dfquery(log_check_query_string)) == 0

    def test_table_does_not_exist(self):
        assert sql.drop_table(ms_schema, table=test_for_drop_table) is None
