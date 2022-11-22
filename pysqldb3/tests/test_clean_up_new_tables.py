import configparser
import os

from .. import pysqldb3 as pysqldb
from ..data_io import *

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

test_pg_to_pg_cleanup_table = 'test_pg_to_pg_cleanup_{}'.format(pg_dbconn.username)
test_pg_to_sql_cleanup_table = 'test_pg_to_pg_cleanup_{}'.format(pg_dbconn.username)
test_sql_to_pg_cleanup_table = 'test_sql_to_pg_cleanup_{}'.format(pg_dbconn.username)
test_clean_up_new_table = 'test_new_table_testing_{}'.format(pg_dbconn.username)
test_clean_up_new_table2 = 'test_new_table_testing_{}_2'.format(pg_dbconn.username)
test_sql_to_pg_qry_cleanup_table = 'test_sql_to_pg_qry_cleanup_{}'.format(pg_dbconn.username)


ms_schema = 'dbo'
pg_schema = 'working'


class TestCleanUpNewTablesPg:
    def test_clean_up_new_tables_basic(self):
        pg_dbconn.drop_table(table_name=test_clean_up_new_table, schema_name=pg_schema)

        # csv_to_table
        pg_dbconn.query("""
            CREATE TABLE {schema}.{table} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(schema=pg_schema, table=test_clean_up_new_table))
        assert pg_dbconn.table_exists(table_name=test_clean_up_new_table, schema_name=pg_schema)

        pg_dbconn.cleanup_new_tables()
        assert not pg_dbconn.table_exists(table_name=test_clean_up_new_table, schema_name=pg_schema)

    def test_clean_up_new_tables_schema(self):
        pg_dbconn.drop_table(table_name=test_clean_up_new_table, schema_name=pg_schema)

        # csv_to_table
        pg_dbconn.query("""
            CREATE TABLE {schema}.{table} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(schema=pg_schema, table=test_clean_up_new_table))

        assert pg_dbconn.table_exists(table_name=test_clean_up_new_table, schema_name=pg_schema)

        pg_dbconn.cleanup_new_tables()
        assert not pg_dbconn.table_exists(table_name=test_clean_up_new_table, schema_name=pg_schema)

    def test_clean_up_new_tables_rename(self):
        # csv_to_table
        table_name = 'test_new_table_92820_testing_{user}'.format(user=pg_dbconn.username)
        pg_dbconn.drop_table(table_name=table_name, schema_name=pg_schema)
        pg_dbconn.drop_table(table_name=f"{table_name}_rename", schema_name=pg_schema)

        pg_dbconn.query("""
            CREATE TABLE {schema}.{table} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(schema=pg_schema, table=table_name))
        assert pg_dbconn.table_exists(table_name=table_name, schema_name=pg_schema)

        pg_dbconn.query("alter table {schema}.{table} rename to {table}_rename".format(schema=pg_schema, table=table_name))
        assert pg_dbconn.table_exists(table_name=table_name, schema_name=pg_schema) == False
        assert pg_dbconn.table_exists(table_name=f'{table_name}_rename', schema_name=pg_schema)

        pg_dbconn.cleanup_new_tables()
        assert pg_dbconn.table_exists(table_name=table_name, schema_name=pg_schema) == False
        assert pg_dbconn.table_exists(table_name=f'{table_name}_rename', schema_name=pg_schema) == False

    def test_clean_up_new_tables_temp(self):
        table_name = 'test_new_table_92820_testing'
        pg_dbconn.query("""
            CREATE TEMPORARY TABLE {table} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(table=table_name))

        pg_dbconn.query("""
            INSERT INTO {table}
             VALUES (1, 'test', now()), (2, 'test', now())
        """.format(table=table_name))

        pg_dbconn.query(f"select * from {table_name}")

        assert len(pg_dbconn.data) == 2
        assert len(pg_dbconn.tables_created) == 0

        pg_dbconn.cleanup_new_tables()

    def test_clean_up_new_tables_already_dropped(self):
        pg_dbconn.drop_table(schema_name=pg_schema, table_name=test_clean_up_new_table)
        pg_dbconn.drop_table(schema_name=pg_schema, table_name=f'{test_clean_up_new_table}_2')

        pg_dbconn.query("""
            CREATE TABLE {schema}.{table} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(schema=pg_schema, table=test_clean_up_new_table))

        pg_dbconn.query("""
                    CREATE TABLE {schema}.{table} (
                        id int,
                        column2 text,
                        column3 timestamp
                    );
        """.format(schema=pg_schema, table=f'{test_clean_up_new_table}_2'))

        pg_dbconn.drop_table(pg_schema, test_clean_up_new_table)
        assert not pg_dbconn.table_exists(table_name=test_clean_up_new_table, schema_name=pg_schema)
        assert pg_dbconn.table_exists(table_name=f'{test_clean_up_new_table}_2', schema_name=pg_schema)

        pg_dbconn.cleanup_new_tables()

        assert not pg_dbconn.table_exists(table_name=f'{test_clean_up_new_table}_2', schema_name=pg_schema)


class TestCleanUpNewTablesMs:
    def test_clean_up_new_tables_basic(self):
        ms_dbconn.drop_table(table_name=test_clean_up_new_table, schema_name=ms_dbconn.default_schema)
        ms_dbconn.query("""
            CREATE TABLE {table} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(table=test_clean_up_new_table))
        assert ms_dbconn.table_exists(table_name=test_clean_up_new_table, schema_name=ms_dbconn.default_schema)

        ms_dbconn.cleanup_new_tables()
        assert not ms_dbconn.table_exists(table_name=test_clean_up_new_table, schema_name=ms_dbconn.default_schema)

    def test_clean_up_new_tables_schema(self):
        ms_dbconn.drop_table(table_name=test_clean_up_new_table, schema_name=ms_schema)

        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(schema=ms_schema, table=test_clean_up_new_table))
        assert ms_dbconn.table_exists(table_name=test_clean_up_new_table, schema_name=ms_schema)

        ms_dbconn.cleanup_new_tables()
        assert not ms_dbconn.table_exists(test_clean_up_new_table, schema_name=ms_schema)

    def test_clean_up_new_tables_rename(self):
        # csv_to_table
        table_name = 'test_new_table_92820_testing'
        ms_dbconn.drop_table(table_name=table_name, schema_name=ms_schema)
        ms_dbconn.drop_table(table_name=f'{table_name}_rename', schema_name=ms_schema)

        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(schema=ms_schema, table=table_name))
        assert ms_dbconn.table_exists(table_name=table_name, schema_name=ms_schema)

        ms_dbconn.query("EXEC sp_rename '{schema}.{table}', '{table}_rename';".format(schema=ms_schema, table=table_name))
        assert ms_dbconn.table_exists(table_name=table_name, schema_name=ms_schema) == False
        assert ms_dbconn.table_exists(table_name=f'{table_name}_rename', schema_name=ms_schema)

        ms_dbconn.cleanup_new_tables()
        assert ms_dbconn.table_exists(table_name=table_name, schema_name=ms_schema) == False
        assert ms_dbconn.table_exists(table_name=f'{table_name}_rename', schema_name=ms_schema) == False

    def test_clean_up_new_tables_temp(self):
        table_name = 'test_new_table_92820_testing'
        ms_dbconn.tables_created = []

        ms_dbconn.query("""
            CREATE TABLE #{} (
                id int,
                column2 text,
                column3 datetime
            );
        """.format(table_name))

        ms_dbconn.query("""
                    INSERT INTO #{}
                    VALUES (1, 'test', CURRENT_TIMESTAMP), (2, 'test', CURRENT_TIMESTAMP)
         """.format(table_name))

        ms_dbconn.query("select * from #%s" % table_name)
        assert len(ms_dbconn.data) == 2
        assert len(ms_dbconn.tables_created) == 0

        ms_dbconn.cleanup_new_tables()

    def test_clean_up_new_tables_already_dropped(self):
        ms_dbconn.drop_table(table_name=test_clean_up_new_table, schema_name=ms_dbconn.default_schema)
        ms_dbconn.drop_table(table_name=test_clean_up_new_table2, schema_name=ms_dbconn.default_schema)

        ms_dbconn.query("""
            CREATE TABLE {} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(test_clean_up_new_table))

        ms_dbconn.query("""
                    CREATE TABLE {} (
                        id int,
                        column2 text,
                        column3 timestamp
                    );
                """.format(test_clean_up_new_table2))

        ms_dbconn.drop_table(ms_dbconn.default_schema, test_clean_up_new_table)
        assert not ms_dbconn.table_exists(test_clean_up_new_table, schema=ms_dbconn.default_schema)
        assert ms_dbconn.table_exists(test_clean_up_new_table2, schema=ms_dbconn.default_schema)

        ms_dbconn.cleanup_new_tables()
        assert not ms_dbconn.table_exists(test_clean_up_new_table2, schema='test')


class TestCleanUpNewTablesIO:
    def test_pg_to_pg(self):
        ris = pysqldb.DbConnect(db_type=config.get('SECOND_PG_DB', 'TYPE'),
                                server=config.get('SECOND_PG_DB', 'SERVER'),
                                db_name=config.get('SECOND_PG_DB', 'DB_NAME'),
                                user=config.get('SECOND_PG_DB', 'DB_USER'),
                                password=config.get('SECOND_PG_DB', 'DB_PASSWORD'))

        # Setup
        ris.tables_created = []
        pg_dbconn.drop_table(schema_name=pg_schema, table_name=test_pg_to_pg_cleanup_table)

        pg_dbconn.query("""
        create table {0}.{1}(col1 int, col2 int);

        insert into {0}.{1} values (1, 2);
        """.format(pg_schema, test_pg_to_pg_cleanup_table))

        assert len(ris.tables_created) == 0
        assert not ris.table_exists(schema=pg_schema, table=test_pg_to_pg_cleanup_table)

        pg_to_pg(from_pg=pg_dbconn, to_pg=ris, org_schema=pg_schema, org_table_name=test_pg_to_pg_cleanup_table,
                 dest_schema=pg_schema)

        assert ris.table_exists(schema=pg_schema, table=test_pg_to_pg_cleanup_table)
        assert len(ris.tables_created) == 1

        ris.cleanup_new_tables()

        assert not ris.table_exists(schema=pg_schema, table=test_pg_to_pg_cleanup_table)
        assert len(ris.tables_created) == 0

        pg_dbconn.drop_table(schema_name=pg_schema, table_name=test_pg_to_pg_cleanup_table)

    def test_pg_to_sql(self):
        # Setup
        ms_dbconn.tables_created = []
        pg_dbconn.drop_table(schema_name=pg_schema, table_name=test_pg_to_sql_cleanup_table)
        ms_dbconn.drop_table(schema_name=ms_schema, table_name=test_pg_to_sql_cleanup_table)
        pg_dbconn.query("""
        create table {0}.{1}(col1 int, col2 int);

        insert into {0}.{1} values (1, 2);
        """.format(pg_schema, test_pg_to_sql_cleanup_table))

        assert len(ms_dbconn.tables_created) == 0
        assert not ms_dbconn.table_exists(schema=ms_schema, table=test_pg_to_sql_cleanup_table)

        pg_to_sql(pg=pg_dbconn, ms=ms_dbconn, org_schema=pg_schema, org_table=test_pg_to_sql_cleanup_table, dest_schema=ms_schema)

        assert ms_dbconn.table_exists(schema=ms_schema, table=test_pg_to_sql_cleanup_table)
        assert len(ms_dbconn.tables_created) == 1

        ms_dbconn.cleanup_new_tables()

        assert not ms_dbconn.table_exists(schema=ms_schema, table=test_pg_to_sql_cleanup_table)
        assert len(ms_dbconn.tables_created) == 0

        pg_dbconn.drop_table(schema_name=pg_schema, table_name=test_pg_to_sql_cleanup_table)

    def test_sql_to_pg(self):
        # Setup
        pg_dbconn.tables_created =[]
        ms_dbconn.drop_table(schema_name=ms_schema, table_name=test_sql_to_pg_cleanup_table)

        # Create
        ms_dbconn.query("""
        create table {0}.{1} (col1 int, col2 int);
        insert into {0}.{1} values (1, 2);
        """.format(ms_schema, test_sql_to_pg_cleanup_table))

        assert len(pg_dbconn.tables_created) == 0
        assert not pg_dbconn.table_exists(schema=pg_schema, table=test_sql_to_pg_cleanup_table)

        sql_to_pg(ms=ms_dbconn, pg=pg_dbconn, org_schema=ms_schema, org_table=test_sql_to_pg_cleanup_table, dest_schema=pg_schema)

        assert pg_dbconn.table_exists(schema=pg_schema, table=test_sql_to_pg_cleanup_table)
        assert len(pg_dbconn.tables_created) == 1

        pg_dbconn.cleanup_new_tables()

        assert not pg_dbconn.table_exists(schema=pg_schema, table=test_sql_to_pg_cleanup_table)
        assert len(pg_dbconn.tables_created) == 0

        ms_dbconn.drop_table(schema_name=ms_schema, table_name=test_sql_to_pg_cleanup_table)

    def test_sql_to_pg_qry(self):
        # Setup
        pg_dbconn.tables_created = []
        sql_query = "select 1 as col1, 2 as col2"

        assert len(pg_dbconn.tables_created) == 0
        assert not pg_dbconn.table_exists(schema=pg_schema, table=test_sql_to_pg_qry_cleanup_table)

        sql_to_pg_qry(ms=ms_dbconn, pg=pg_dbconn, query=sql_query, dest_table=test_sql_to_pg_qry_cleanup_table,
                      dest_schema=pg_schema)

        assert pg_dbconn.table_exists(schema=pg_schema, table=test_sql_to_pg_qry_cleanup_table)
        assert len(pg_dbconn.tables_created) == 1

        pg_dbconn.cleanup_new_tables()

        assert not pg_dbconn.table_exists(schema=pg_schema, table=test_sql_to_pg_qry_cleanup_table)
        assert len(pg_dbconn.tables_created) == 0
