import configparser
import os

from .. import pysqldb3 as pysqldb
from ..data_io import *
from .. sql import GET_MS_INDEX_QUERY

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
        db.drop_table(pg_schema, test_pg_from_backup)
        schema_table_name = db.create_table_from_backup(test_back_file)
        assert re.findall(r'[\-["\w"\]]*\.[-\["\w"\]]*', schema_table_name)

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

    def test_backup_tables_idx_rename(self):
        new_backup_name = '1-'+test_pg_from_backup + '_new_name'
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
        db.query(f'select count(*) cnt from {pg_schema}."{test_pg_to_backup}"')
        assert db.data[0][0] == 10

        if os.path.isfile(test_back_file):
            os.remove(test_back_file)
        assert not os.path.isfile(test_back_file)

        db.drop_table(pg_schema, test_pg_from_backup)
        db.backup_table(pg_schema, test_pg_to_backup, test_back_file, pg_schema, test_pg_from_backup)
        assert os.path.isfile(test_back_file)

        # run backup
        db.drop_table(pg_schema, new_backup_name)
        db.create_table_from_backup(test_back_file, overwrite_schema=pg_schema, overwrite_name=new_backup_name)

        # validate table exists
        assert db.table_exists(new_backup_name, schema=pg_schema)
        db.query(f'select count(*) cnt from {pg_schema}."{new_backup_name}"')
        assert db.data[0][0] == 10

        # Validate schema matches
        _to = db.get_table_columns(test_pg_to_backup, schema=pg_schema)
        _from = db.get_table_columns(new_backup_name, schema=pg_schema)
        assert _to == _from

        # validate indexes are the same
        db.query(f"SELECT indexname, indexdef FROM pg_indexes WHERE schemaname='{pg_schema}' AND tablename='{test_pg_to_backup}';")
        to_data = db.data
        to_data = [_[1].replace(_[0], _[0] + '_backup').replace(f'ON {pg_schema}.{test_pg_to_backup}', f'ON {pg_schema}."{new_backup_name}"') for _ in to_data]
        db.query(f"SELECT indexdef FROM pg_indexes WHERE schemaname='{pg_schema}' AND tablename='{new_backup_name}';")
        from_data = [_[0] for _ in db.data]

        assert to_data == from_data

        # clean up
        db.cleanup_new_tables()
        # assert not db.table_exists(test_pg_to_backup, schema=pg_schema)
        # db.query(f"""
        # SELECT EXISTS (
        # SELECT 1
        # FROM pg_catalog.pg_tables
        # WHERE schemaname = '{pg_schema}'
        # AND tablename = '{new_backup_name}'
        # """)
        assert not db.table_exists(new_backup_name, schema=pg_schema)
        if os.path.isfile(test_back_file):
            os.remove(test_back_file)
        assert not os.path.isfile(test_back_file)

    def test_messy_table(self):
        # base messy case
        # schema = 'minireports'
        # table_name = 'daylighting_old_turn_calming_20240626'


        db.drop_table(pg_schema, "4_table name")
        # table schema
        db.query(f"""
                   CREATE TABLE {pg_schema}."4_table name" (
                       id int,
                       column2 text,
                       column3 timestamp,
                    "1 test messy column" text,
                    "2 test messy column" numeric
                   );
               """)
        # populate table
        for i in range(10):
            db.query(f"""
                   INSERT INTO {pg_schema}."4_table name"
                       (id, column2, column3, "1 test messy column", "2 test messy column")
                   values ({i}, '{i}', '{'2022-10-14 10:12:40-04'}', '{'{1 test messy column}'}', NULL)
               """)

        db.backup_table(pg_schema, "4_table name", test_back_file, pg_schema, '4_table name_backup')

        db.create_table_from_backup(test_back_file)

        # validate table exists
        assert db.table_exists('4_table name_backup', schema=pg_schema)
        db.query(f'select count(*) cnt from {pg_schema}."4_table name_backup"')
        assert db.data[0][0] == 10

        # Validate schema matches
        _to = db.get_table_columns( '4_table name', schema=pg_schema)
        _from = db.get_table_columns( '4_table name_backup', schema=pg_schema)
        assert _to == _from

        # clean up
        db.cleanup_new_tables()
        assert not db.table_exists( '4_table name', schema=pg_schema)
        assert not db.table_exists( '4_table name_backup', schema=pg_schema)
        if os.path.isfile(test_back_file):
            os.remove(test_back_file)
        assert not os.path.isfile(test_back_file)
        db.cleanup_new_tables()

    def test_backup_tables_citext(self):
        db.drop_table(table=test_pg_to_backup, schema=pg_schema)

        # table schema
        db.query(f"""
            CREATE TABLE {pg_schema}.{test_pg_to_backup} (
                id int,
                column2 citext,
                column3 citext,
                "1 test messy column" text
            );
        """)
        # populate table
        for i in range(10):
            db.query(f"""
                INSERT INTO {pg_schema}.{test_pg_to_backup}
                    (id, column2, column3, "1 test messy column")
                values ({i}, '{'SUV'}', '{'testing@! citext col2'}', '{'test '*i}')
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
        db.drop_table(pg_schema, test_pg_from_backup)
        schema_table_name = db.create_table_from_backup(test_back_file)
        assert re.findall(r'[\-["\w"\]]*\.[-\["\w"\]]*', schema_table_name)

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

    def test_table_with_array(self):

        db.drop_table(pg_schema, "table_with_array")
        db.drop_table(pg_schema, "table_with_array_backup")
        # table schema
        db.query(f"""
                   CREATE TABLE {pg_schema}.table_with_array (
                       id int,
                       column2 integer ARRAY[1],
                       column3 text ARRAY[2]
                   );
               """)
        # populate table
        for i in range(10):
            db.query(f"""
                   INSERT INTO {pg_schema}.table_with_array
                       (id, column2, column3)
                   values ({i}, '{{4321}}', '{{"cool", "kool"}}')
               """)

        db.backup_table(pg_schema, "table_with_array", test_back_file, pg_schema, 'table_with_array_backup')

        db.create_table_from_backup(test_back_file)

        # validate table exists
        assert db.table_exists('table_with_array_backup', schema=pg_schema)
        db.query(f'select count(*) cnt from {pg_schema}."table_with_array_backup"')


        # Validate schema matches
        _to = db.get_table_columns( 'table_with_array', schema=pg_schema)
        _from = db.get_table_columns( 'table_with_array_backup', schema=pg_schema)
        assert _to == _from
        assert db.data[0][0] == 10

        # clean up
        db.cleanup_new_tables()
        assert not db.table_exists( 'table_with_array', schema=pg_schema)
        assert not db.table_exists( 'table_with_array_backup', schema=pg_schema)
        if os.path.isfile(test_back_file):
            os.remove(test_back_file)
        assert not os.path.isfile(test_back_file)

    def test_table_with_array_contents(self):

        db.drop_table(pg_schema, "table_with_array_contents")
        db.drop_table(pg_schema, "table_with_array_contents_backup")
        # table schema
        db.query(f"""
                   CREATE TABLE {pg_schema}.table_with_array_contents (
                        column1 text ARRAY[3],
                        column2 integer ARRAY[1],
                        column3 text ARRAY[2]
                   );
               """)
        # populate table
        for i in range(10):
            db.query(f"""
                   INSERT INTO {pg_schema}.table_with_array_contents
                       (column1, column2, column3)
                   values ('{{"!!??", "!test/@", ";"}}', '{{4321}}', '{{"cool", "kool"}}')
               """)

        # back up and create table
        db.backup_table(pg_schema, "table_with_array_contents", test_back_file, pg_schema, 'table_with_array_contents_backup')
        db.create_table_from_backup(test_back_file)

        # validate table exists
        assert db.table_exists('table_with_array_contents_backup', schema=pg_schema)

        # Validate that the tables are the exact same
        orig_table = db.dfquery(f"select * from {pg_schema}.table_with_array_contents;")
        backup_table = db.dfquery(f"select * from {pg_schema}.table_with_array_contents_backup;")
        pd.testing.assert_frame_equal(orig_table, backup_table,
                                      check_dtype=True,
                                      check_exact=True)

        # unnest arrays and make sure they match
        db.query(f'select unnest(column1) from {pg_schema}.table_with_array_contents limit 1')
        assert db.data[0][0] == '!!??'
        db.query(f'select unnest(column1) from {pg_schema}.table_with_array_contents_backup limit 1')
        assert db.data[0][0] == '!!??'

        # check that both of these tables show 4321
        db.query(f'select unnest(column2) from {pg_schema}.table_with_array_contents limit 1')
        assert db.data[0][0] == 4321

        db.query(f'select unnest(column2) from {pg_schema}.table_with_array_contents_backup limit 1')
        assert db.data[0][0] == 4321

        # clean up
        db.cleanup_new_tables()
        assert not db.table_exists( 'table_with_array_contents', schema=pg_schema)
        assert not db.table_exists( 'table_with_array_contents_backup', schema=pg_schema)
        if os.path.isfile(test_back_file):
            os.remove(test_back_file)
        assert not os.path.isfile(test_back_file)


class TestBackupTablesMs:
    def test_backup_tables_basic(self):
        sql.drop_table(table=test_sql_to_backup, schema=ms_schema)

        # table schema
        sql.drop_table(ms_schema, test_sql_to_backup)
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
        sql.drop_table(ms_schema, test_sql_from_backup)
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

    def test_backup_tables_idx_basic(self):
        sql.drop_table(table=test_sql_to_backup, schema=ms_schema)

        # table schema
        sql.drop_table(ms_schema, test_sql_to_backup)
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_sql_to_backup} (
                id int,
                column2 text,
                column3 datetime,
                "1 test messy column" text
            );
            
            CREATE NONCLUSTERED INDEX [idx_test_backup_{test_sql_to_backup}] ON {ms_schema}.{test_sql_to_backup} (id);
        """)
        # populate table
        for i in range(10):
            sql.query(f"""
                INSERT INTO {ms_schema}.{test_sql_to_backup}
                    (id, column2, column3, "1 test messy column")
                values ({i}, '{i}', '{'2022-10-14 10:12:40'}', '{'test ' * i}')
            """)
        # validate table created
        assert sql.table_exists(test_sql_to_backup, schema=ms_schema)
        sql.query(f"select count(*) cnt from {ms_schema}.{test_sql_to_backup}")
        assert sql.data[0][0] == 10

        if os.path.isfile(test_back_file):
            os.remove(test_back_file)
        assert not os.path.isfile(test_back_file)

        sql.drop_table(ms_schema, test_sql_from_backup)
        sql.backup_table(ms_schema, test_sql_to_backup, test_back_file, ms_schema, test_sql_from_backup)
        assert os.path.isfile(test_back_file)

        # run backup
        sql.drop_table(ms_schema, test_sql_from_backup)
        sql.create_table_from_backup(test_back_file)

        # validate table exists
        assert sql.table_exists(test_sql_from_backup, schema=ms_schema)
        sql.query(f"select count(*) cnt from {ms_schema}.{test_sql_from_backup}")
        assert sql.data[0][0] == 10

        # Validate schema matches
        _to = sql.get_table_columns(test_sql_to_backup, schema=ms_schema)
        _from = sql.get_table_columns(test_sql_from_backup, schema=ms_schema)
        assert _to == _from

        # validate indexes are the same
        sql.query(GET_MS_INDEX_QUERY.format(schema=ms_schema, table=test_sql_to_backup))
        to_data = sql.data
        to_data = [_[-2:] for _ in to_data]

        sql.query(GET_MS_INDEX_QUERY.format(schema=ms_schema, table=test_sql_from_backup))
        from_data = sql.data
        from_data = [_[-2:] for _ in from_data]

        assert to_data == from_data

        # clean up
        sql.cleanup_new_tables()
        assert not sql.table_exists(test_sql_to_backup, schema=ms_schema)
        assert not sql.table_exists(test_sql_from_backup, schema=ms_schema)
        if os.path.isfile(test_back_file):
            os.remove(test_back_file)
        assert not os.path.isfile(test_back_file)


    def test_backup_tables_idx_rename(self):
        new_backup_name = test_sql_from_backup+'_new_name'
        sql.drop_table(table=test_sql_to_backup, schema=ms_schema)

        # table schema
        sql.drop_table(ms_schema, test_sql_to_backup)
        sql.query(f"""
            CREATE TABLE {ms_schema}.{test_sql_to_backup} (
                id int,
                column2 text,
                column3 datetime,
                "1 test messy column" text
            );
            
            CREATE NONCLUSTERED INDEX [idx_test_backup_{test_sql_to_backup}] ON {ms_schema}.{test_sql_to_backup} (id);
        """)
        # populate table
        for i in range(10):
            sql.query(f"""
                INSERT INTO {ms_schema}.{test_sql_to_backup}
                    (id, column2, column3, "1 test messy column")
                values ({i}, '{i}', '{'2022-10-14 10:12:40'}', '{'test ' * i}')
            """)
        # validate table created
        assert sql.table_exists(test_sql_to_backup, schema=ms_schema)
        sql.query(f"select count(*) cnt from {ms_schema}.{test_sql_to_backup}")
        assert sql.data[0][0] == 10

        if os.path.isfile(test_back_file):
            os.remove(test_back_file)
        assert not os.path.isfile(test_back_file)

        sql.drop_table(ms_schema, test_sql_from_backup)
        sql.backup_table(ms_schema, test_sql_to_backup, test_back_file, ms_schema, test_sql_from_backup)
        assert os.path.isfile(test_back_file)

        # run backup
        sql.drop_table(ms_schema, new_backup_name)
        sql.create_table_from_backup(test_back_file, overwrite_name=new_backup_name, overwrite_schema=ms_schema)

        # validate table exists
        assert sql.table_exists(new_backup_name, schema=ms_schema)
        sql.query(f"select count(*) cnt from {ms_schema}.{new_backup_name}")
        assert sql.data[0][0] == 10

        # Validate schema matches
        _to = sql.get_table_columns(test_sql_to_backup, schema=ms_schema)
        _from = sql.get_table_columns(new_backup_name, schema=ms_schema)
        assert _to == _from

        # validate indexes are the same
        sql.query(GET_MS_INDEX_QUERY.format(schema=ms_schema, table=test_sql_to_backup))
        to_data = sql.data
        to_data = [_[-2:] for _ in to_data]

        sql.query(GET_MS_INDEX_QUERY.format(schema=ms_schema, table=new_backup_name))
        from_data = sql.data
        from_data = [_[-2:] for _ in from_data]

        assert to_data == from_data

        # clean up
        sql.cleanup_new_tables()
        assert not sql.table_exists(test_sql_to_backup, schema=ms_schema)
        assert not sql.table_exists(test_sql_from_backup, schema=ms_schema)
        if os.path.isfile(test_back_file):
            os.remove(test_back_file)
        assert not os.path.isfile(test_back_file)

    def test_messy_table(self):
        # base messy case
        # schema = 'minireports'
        # table_name = 'daylighting_old_turn_calming_20240626'

        sql.drop_table(ms_schema, "4_table name")
        # table schema
        sql.query(f"""
                   CREATE TABLE {ms_schema}."4_table name" (
                       id int,
                       column2 text,
                       column3 datetime,
                    "1 test messy column" text,
                    "2 test messy column" numeric
                   );
               """)
        # populate table
        for i in range(10):
            sql.query(f"""
                   INSERT INTO {ms_schema}."4_table name"
                       (id, column2, column3, "1 test messy column", "2 test messy column")
                   values ({i}, '{i}', '{'2022-10-14 10:12:40'}', '{'{1 test messy column}'}', NULL)
               """)

        sql.backup_table(ms_schema, "4_table name", test_back_file, ms_schema, '4_table name_backup')

        sql.create_table_from_backup(test_back_file)

        # validate table exists
        assert sql.table_exists('4_table name_backup', schema=ms_schema)
        sql.query(f'select count(*) cnt from {ms_schema}."4_table name_backup"')
        assert sql.data[0][0] == 10

        # Validate schema matches
        _to = sql.get_table_columns('4_table name', schema=ms_schema)
        _from = sql.get_table_columns('4_table name_backup', schema=ms_schema)
        assert _to == _from

        # clean up
        sql.cleanup_new_tables()
        assert not sql.table_exists('4_table name', schema=ms_schema)
        assert not sql.table_exists('4_table name_backup', schema=ms_schema)
        if os.path.isfile(test_back_file):
            os.remove(test_back_file)
        assert not os.path.isfile(test_back_file)

        sql.cleanup_new_tables()

