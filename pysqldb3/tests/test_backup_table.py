import configparser
import os

from .. import pysqldb3 as pysqldb
from ..data_io import *

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

test_data_dir = os.path.dirname(os.path.abspath(__file__))+'/test_data'


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

test_pg_to_backup = 'test_pg_to_backup_{}'.format(db.user)
test_pg_from_backup = 'test_pg_from_backup_{}'.format(db.user)
test_sql_to_backup = 'test_sql_to_backup_{}'.format(db.user)
test_sql_from_backup = 'test_sql_from_backup_{}'.format(db.user)

ms_schema = 'dbo'
pg_schema = 'working'

test_back_file = test_data_dir+'/backup.sql'


# org_table = 'daylighting_old_turn_calming_20240626'
# org_schema= 'minireports'

class TestBackupTablesPg:
    def test_backup_tables_basic(self):
        db.drop_table(table=test_pg_to_backup, schema=pg_schema)

        # table schema
        db.query(f"""
            CREATE TABLE {pg_schema}.{test_pg_to_backup} (
                id int,
                column2 text,
                column3 timestamp,
                "1 test messy column" text
            );
        """)
        # populate table
        for i in range(10):
            db.query(f"""
                INSERT INTO {pg_schema}.{test_pg_to_backup}
                    (id, column2, column3, "1 test messy column")
                values ({i}, '{i}', '{'2022-10-14 10:12:40-04'}', '{'test '*i}')
            """)
        # validate table created
        assert db.table_exists(test_pg_to_backup, schema=pg_schema)
        db.query(f"select count(*) cnt from {pg_schema}.{test_pg_to_backup}")
        assert db.data[0][0] == 10

        if os.path.isfile(test_back_file):
            os.remove(test_back_file)
        assert not os.path.isfile(test_back_file)

        db.backup_table(pg_schema, test_pg_to_backup, test_back_file, pg_schema, test_pg_from_backup)
        assert os.path.isfile(test_back_file)

        # run backup
        db.create_table_from_backup(test_back_file)

        # validate table exists
        assert db.table_exists(test_pg_from_backup, schema=pg_schema)
        db.query(f"select count(*) cnt from {pg_schema}.{test_pg_from_backup}")
        assert db.data[0][0] == 10

        # Validate schema matches
        _to = db.get_table_columns(test_pg_to_backup, schema=pg_schema)
        _from = db.get_table_columns(test_pg_from_backup, schema=pg_schema)
        assert _to == _from

        # clean up
        db.cleanup_new_tables()
        assert not db.table_exists(test_pg_to_backup, schema=pg_schema)
        assert not db.table_exists(test_pg_from_backup, schema=pg_schema)
        if os.path.isfile(test_back_file):
            os.remove(test_back_file)
        assert not os.path.isfile(test_back_file)

    def test_backup_tables_idx_basic(self):
        db.drop_table(table=test_pg_to_backup, schema=pg_schema)

        # table schema
        db.query(f"""
            CREATE TABLE {pg_schema}.{test_pg_to_backup} (
                id int,
                column2 text,
                column3 timestamp,
                "1 test messy column" text
            );
            
            CREATE INDEX idx_test_backup_{test_pg_to_backup} on 
            {pg_schema}.{test_pg_to_backup} USING btree (id);
        """)
        # populate table
        for i in range(10):
            db.query(f"""
                INSERT INTO {pg_schema}.{test_pg_to_backup}
                    (id, column2, column3, "1 test messy column")
                values ({i}, '{i}', '{'2022-10-14 10:12:40-04'}', '{'test '*i}')
            """)
        # validate table created
        assert db.table_exists(test_pg_to_backup, schema=pg_schema)
        db.query(f"select count(*) cnt from {pg_schema}.{test_pg_to_backup}")
        assert db.data[0][0] == 10

        if os.path.isfile(test_back_file):
            os.remove(test_back_file)
        assert not os.path.isfile(test_back_file)

        db.drop_table(pg_schema, test_pg_from_backup)
        db.backup_table(pg_schema, test_pg_to_backup, test_back_file, pg_schema, test_pg_from_backup)
        assert os.path.isfile(test_back_file)

        # run backup
        db.create_table_from_backup(test_back_file)

        # validate table exists
        assert db.table_exists(test_pg_from_backup, schema=pg_schema)
        db.query(f"select count(*) cnt from {pg_schema}.{test_pg_from_backup}")
        assert db.data[0][0] == 10

        # Validate schema matches
        _to = db.get_table_columns(test_pg_to_backup, schema=pg_schema)
        _from = db.get_table_columns(test_pg_from_backup, schema=pg_schema)
        assert _to == _from

        # validate indexes are the same
        db.query(f"SELECT indexname, indexdef FROM pg_indexes WHERE schemaname='{pg_schema}' AND tablename='{test_pg_to_backup}';")
        to_data = db.data
        to_data = [_[1].replace(_[0], _[0] + '_backup').replace(f'ON {pg_schema}.{test_pg_to_backup}', f'ON {pg_schema}.{test_pg_from_backup}') for _ in to_data]
        db.query(f"SELECT indexdef FROM pg_indexes WHERE schemaname='{pg_schema}' AND tablename='{test_pg_from_backup}';")
        from_data = [_[0] for _ in db.data]

        assert to_data == from_data

        # clean up
        db.cleanup_new_tables()
        assert not db.table_exists(test_pg_to_backup, schema=pg_schema)
        assert not db.table_exists(test_pg_from_backup, schema=pg_schema)
        if os.path.isfile(test_back_file):
            os.remove(test_back_file)
        assert not os.path.isfile(test_back_file)


class TestBackupTablesMs:
    def test_backup_tables_basic(self):
        sql.drop_table(table=test_sql_to_backup, schema=ms_schema)

        # table schema
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_sql_to_backup} (
                id int,
                column2 text,
                column3 datetime,
                "1 test messy column" text
            );
        """)
        # populate table
        for i in range(10):
            sql.query(f"""
                INSERT INTO {ms_schema}.{test_sql_to_backup}
                    (id, column2, column3, "1 test messy column")
                values ({i}, '{i}', '{'2022-10-14 10:12:40'}', '{'test '*i}')
            """)
        # validate table created
        assert sql.table_exists(test_sql_to_backup, schema=ms_schema)
        sql.query(f"select count(*) cnt from {ms_schema}.{test_sql_to_backup}")
        assert sql.data[0][0] == 10

        if os.path.isfile(test_back_file):
            os.remove(test_back_file)
        assert not os.path.isfile(test_back_file)

        sql.backup_table(ms_schema, test_sql_to_backup, test_back_file, ms_schema, test_sql_from_backup)
        assert os.path.isfile(test_back_file)

        # run backup
        sql.create_table_from_backup(test_back_file)

        # validate table exists
        assert sql.table_exists(test_sql_from_backup, schema=ms_schema)
        sql.query(f"select count(*) cnt from {ms_schema}.{test_sql_from_backup}")
        assert sql.data[0][0] == 10

        # Validate schema matches
        _to = sql.get_table_columns(test_sql_to_backup, schema=ms_schema)
        _from = sql.get_table_columns(test_sql_from_backup, schema=ms_schema)
        assert _to == _from

        # clean up
        sql.cleanup_new_tables()
        assert not sql.table_exists(test_sql_to_backup, schema=ms_schema)
        assert not sql.table_exists(test_sql_from_backup, schema=ms_schema)
        if os.path.isfile(test_back_file):
            os.remove(test_back_file)
        assert not os.path.isfile(test_back_file)
