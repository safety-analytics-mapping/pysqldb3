import configparser
import os

from .. import pysqldb3 as pysqldb
from ..data_io import *

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

test_pg_to_pg_cleanup_table = 'test_pg_to_pg_cleanup_{}'.format(db.user)
test_pg_to_sql_cleanup_table = 'test_pg_to_pg_cleanup_{}'.format(db.user)
test_sql_to_pg_cleanup_table = 'test_sql_to_pg_cleanup_{}'.format(db.user)
test_clean_up_new_table = 'test_new_table_testing_{}'.format(db.user)
test_clean_up_new_table2 = 'test_new_table_testing_{}_2'.format(db.user)
test_sql_to_pg_qry_cleanup_table = 'test_sql_to_pg_qry_cleanup_{}'.format(db.user)


ms_schema = 'dbo'
pg_schema = 'working'


class TestCleanUpNewTablesPg:
    def test_clean_up_new_tables_basic(self):
        db.drop_table(table_name=test_clean_up_new_table, schema_name=pg_schema)

        # csv_to_table
        db.query("""
            CREATE TABLE {}.{} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(pg_schema, test_clean_up_new_table))
        assert db.table_exists(test_clean_up_new_table, schema=pg_schema)

        db.cleanup_new_tables()
        assert not db.table_exists(test_clean_up_new_table, schema=pg_schema)

    def test_clean_up_new_tables_schema(self):
        db.drop_table(table_name=test_clean_up_new_table, schema_name=pg_schema)

        # csv_to_table
        db.query("""
            CREATE TABLE {}.{} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(pg_schema, test_clean_up_new_table))

        assert db.table_exists(test_clean_up_new_table, schema=pg_schema)

        db.cleanup_new_tables()
        assert not db.table_exists(test_clean_up_new_table, schema=pg_schema)

    def test_clean_up_new_tables_rename(self):
        # csv_to_table
        table_name = 'test_new_table_92820_testing_{}'.format(db.user)
        db.drop_table(table_name=table_name, schema_name=pg_schema)
        db.drop_table(table_name=table_name + "_rename", schema_name=pg_schema)

        db.query("""
            CREATE TABLE {}.{} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(pg_schema, table_name))
        assert db.table_exists(table_name, schema=pg_schema)

        db.query("alter table {s}.{t} rename to {t}_rename".format(s=pg_schema, t=table_name))
        assert db.table_exists(table_name, schema=pg_schema) == False
        assert db.table_exists(table_name + '_rename', schema=pg_schema)

        db.cleanup_new_tables()
        assert db.table_exists(table_name, schema=pg_schema) == False
        assert db.table_exists(table_name + '_rename', schema=pg_schema) == False

    def test_clean_up_new_tables_temp(self):
        table_name = 'test_new_table_92820_testing'
        db.query("""
            CREATE TEMPORARY TABLE {} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(table_name))

        db.query("""
            INSERT INTO {}
             VALUES (1, 'test', now()), (2, 'test', now())
        """.format(table_name))

        db.query("select * from %s" % table_name)

        assert len(db.data) == 2
        assert len(db.tables_created) == 0

        db.cleanup_new_tables()

    def test_clean_up_new_tables_already_dropped(self):
        db.drop_table(schema_name=pg_schema, table_name=test_clean_up_new_table)
        db.drop_table(schema_name=pg_schema, table_name=test_clean_up_new_table + '_2')

        db.query("""
            CREATE TABLE {}.{} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(pg_schema, test_clean_up_new_table))

        db.query("""
                    CREATE TABLE {}.{} (
                        id int,
                        column2 text,
                        column3 timestamp
                    );
        """.format(pg_schema,test_clean_up_new_table + '_2'))

        db.drop_table(pg_schema, test_clean_up_new_table)
        assert not db.table_exists(test_clean_up_new_table, schema=pg_schema)
        assert db.table_exists(test_clean_up_new_table + '_2', schema=pg_schema)

        db.cleanup_new_tables()

        assert not db.table_exists(test_clean_up_new_table + '_2', schema=pg_schema)


class TestCleanUpNewTablesMs:
    def test_clean_up_new_tables_basic(self):
        sql.drop_table(table_name=test_clean_up_new_table, schema_name=sql.default_schema)
        sql.query("""
            CREATE TABLE {} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(test_clean_up_new_table))
        assert sql.table_exists(test_clean_up_new_table, schema=sql.default_schema)

        sql.cleanup_new_tables()
        assert not sql.table_exists(test_clean_up_new_table, schema=sql.default_schema)

    def test_clean_up_new_tables_schema(self):
        sql.drop_table(table_name=test_clean_up_new_table, schema_name=ms_schema)

        sql.query("""
            CREATE TABLE {}.{} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(ms_schema, test_clean_up_new_table))
        assert sql.table_exists(test_clean_up_new_table, schema=ms_schema)

        sql.cleanup_new_tables()
        assert not sql.table_exists(test_clean_up_new_table, schema=ms_schema)

    def test_clean_up_new_tables_rename(self):
        # csv_to_table
        table_name = 'test_new_table_92820_testing'
        sql.drop_table(table_name=table_name, schema_name=ms_schema)
        sql.drop_table(table_name=table_name + '_rename', schema_name=ms_schema)

        sql.query("""
            CREATE TABLE {}.{} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(ms_schema, table_name))
        assert sql.table_exists(table_name, schema=ms_schema)

        sql.query("EXEC sp_rename '{s}.{t}', '{t}_rename';".format(s=ms_schema, t=table_name))
        assert sql.table_exists(table_name, schema=ms_schema) == False
        assert sql.table_exists(table_name + '_rename', schema=ms_schema)

        sql.cleanup_new_tables()
        assert sql.table_exists(table_name, schema=ms_schema) == False
        assert sql.table_exists(table_name + '_rename', schema=ms_schema) == False

    def test_clean_up_new_tables_temp(self):
        table_name = 'test_new_table_92820_testing'
        sql.tables_created = []

        sql.query("""
            CREATE TABLE #{} (
                id int,
                column2 text,
                column3 datetime
            );
        """.format(table_name))

        sql.query("""
                    INSERT INTO #{}
                    VALUES (1, 'test', CURRENT_TIMESTAMP), (2, 'test', CURRENT_TIMESTAMP)
         """.format(table_name))

        sql.query("select * from #%s" % table_name)
        assert len(sql.data) == 2
        assert len(sql.tables_created) == 0

        sql.cleanup_new_tables()

    def test_clean_up_new_tables_already_dropped(self):
        sql.drop_table(table_name=test_clean_up_new_table, schema_name=sql.default_schema)
        sql.drop_table(table_name=test_clean_up_new_table2, schema_name=sql.default_schema)

        sql.query("""
            CREATE TABLE {} (
                id int,
                column2 text,
                column3 timestamp
            );
        """.format(test_clean_up_new_table))

        sql.query("""
                    CREATE TABLE {} (
                        id int,
                        column2 text,
                        column3 timestamp
                    );
                """.format(test_clean_up_new_table2))

        sql.drop_table(sql.default_schema, test_clean_up_new_table)
        assert not sql.table_exists(test_clean_up_new_table, schema=sql.default_schema)
        assert sql.table_exists(test_clean_up_new_table2, schema=sql.default_schema)

        sql.cleanup_new_tables()
        assert not sql.table_exists(test_clean_up_new_table2, schema='test')


class TestCleanUpNewTablesIO:
    def test_pg_to_pg(self):
        ris = pysqldb.DbConnect(db_type=config.get('SECOND_PG_DB', 'TYPE'),
                                server=config.get('SECOND_PG_DB', 'SERVER'),
                                db_name=config.get('SECOND_PG_DB', 'DB_NAME'),
                                user=config.get('SECOND_PG_DB', 'DB_USER'),
                                password=config.get('SECOND_PG_DB', 'DB_PASSWORD'))

        # Setup
        ris.tables_created = []
        db.drop_table(schema_name=pg_schema, table_name=test_pg_to_pg_cleanup_table)

        db.query("""
        create table {0}.{1}(col1 int, col2 int);

        insert into {0}.{1} values (1, 2);
        """.format(pg_schema, test_pg_to_pg_cleanup_table))

        assert len(ris.tables_created) == 0
        assert not ris.table_exists(schema=pg_schema, table=test_pg_to_pg_cleanup_table)

        pg_to_pg(from_pg=db, to_pg=ris, org_schema=pg_schema, org_table_name=test_pg_to_pg_cleanup_table,
                 dest_schema=pg_schema)

        assert ris.table_exists(schema=pg_schema, table=test_pg_to_pg_cleanup_table)
        assert len(ris.tables_created) == 1

        ris.cleanup_new_tables()

        assert not ris.table_exists(schema=pg_schema, table=test_pg_to_pg_cleanup_table)
        assert len(ris.tables_created) == 0

        db.drop_table(schema_name=pg_schema, table_name=test_pg_to_pg_cleanup_table)

    def test_pg_to_sql(self):
        # Setup
        sql.tables_created = []
        db.drop_table(schema_name=pg_schema, table_name=test_pg_to_sql_cleanup_table)
        sql.drop_table(schema_name=ms_schema, table_name=test_pg_to_sql_cleanup_table)
        db.query("""
        create table {0}.{1}(col1 int, col2 int);

        insert into {0}.{1} values (1, 2);
        """.format(pg_schema, test_pg_to_sql_cleanup_table))

        assert len(sql.tables_created) == 0
        assert not sql.table_exists(schema=ms_schema, table=test_pg_to_sql_cleanup_table)

        pg_to_sql(pg=db, ms=sql, org_schema=pg_schema, org_table=test_pg_to_sql_cleanup_table, dest_schema=ms_schema)

        assert sql.table_exists(schema=ms_schema, table=test_pg_to_sql_cleanup_table)
        assert len(sql.tables_created) == 1

        sql.cleanup_new_tables()

        assert not sql.table_exists(schema=ms_schema, table=test_pg_to_sql_cleanup_table)
        assert len(sql.tables_created) == 0

        db.drop_table(schema_name=pg_schema, table_name=test_pg_to_sql_cleanup_table)

    def test_sql_to_pg(self):
        # Setup
        db.tables_created =[]
        sql.drop_table(schema_name=ms_schema, table_name=test_sql_to_pg_cleanup_table)

        # Create
        sql.query("""
        create table {0}.{1} (col1 int, col2 int);
        insert into {0}.{1} values (1, 2);
        """.format(ms_schema, test_sql_to_pg_cleanup_table))

        assert len(db.tables_created) == 0
        assert not db.table_exists(schema=pg_schema, table=test_sql_to_pg_cleanup_table)

        sql_to_pg(ms=sql, pg=db, org_schema=ms_schema, org_table=test_sql_to_pg_cleanup_table, dest_schema=pg_schema)

        assert db.table_exists(schema=pg_schema, table=test_sql_to_pg_cleanup_table)
        assert len(db.tables_created) == 1

        db.cleanup_new_tables()

        assert not db.table_exists(schema=pg_schema, table=test_sql_to_pg_cleanup_table)
        assert len(db.tables_created) == 0

        sql.drop_table(schema_name=ms_schema, table_name=test_sql_to_pg_cleanup_table)

    def test_sql_to_pg_qry(self):
        # Setup
        db.tables_created = []
        sql_query = "select 1 as col1, 2 as col2"

        assert len(db.tables_created) == 0
        assert not db.table_exists(schema=pg_schema, table=test_sql_to_pg_qry_cleanup_table)

        sql_to_pg_qry(ms=sql, pg=db, query=sql_query, dest_table=test_sql_to_pg_qry_cleanup_table,
                      dest_schema=pg_schema)

        assert db.table_exists(schema=pg_schema, table=test_sql_to_pg_qry_cleanup_table)
        assert len(db.tables_created) == 1

        db.cleanup_new_tables()

        assert not db.table_exists(schema=pg_schema, table=test_sql_to_pg_qry_cleanup_table)
        assert len(db.tables_created) == 0
