import datetime
import os

import configparser
import pandas as pd

from . import helpers
from .. import pysqldb3 as pysqldb

test_config = configparser.ConfigParser()
test_config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(default=True, password=test_config.get('PG_DB', 'DB_PASSWORD'),
                       user=test_config.get('PG_DB', 'DB_USER'))

sql = pysqldb.DbConnect(type=test_config.get('SQL_DB', 'TYPE'),
                        server=test_config.get('SQL_DB', 'SERVER'),
                        database=test_config.get('SQL_DB', 'DB_NAME'),
                        user=test_config.get('SQL_DB', 'DB_USER'),
                        password=test_config.get('SQL_DB', 'DB_PASSWORD'))

pg_table_name = f'pg_test_table_{db.user}'
sql_table_name = f'sql_test_table_{sql.user}'
table_for_testing = f'table_for_testing_{db.user}'
table_for_testing_logging = f'testing_logging_table_{db.user}'

class TestMisc:
    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_pg(db)
        helpers.set_up_test_table_sql(sql, sql.default_schema)

    def test_get_schemas_pg(self):
        schemas = db.get_schemas()

        query_schema_df = db.dfquery("""
        select schema_name
        from information_schema.schemata;
        """)

        # Assert same number of schemas
        assert len(schemas) == len(query_schema_df)

        # Assert same values
        assert set(schemas) == set(query_schema_df['schema_name'])

    def test_get_schemas_ms(self):
        schemas = sql.get_schemas()

        query_schema_df = sql.dfquery("""
        select s.name as schema_name,
            s.schema_id,
            u.name as schema_owner
        from sys.schemas s
            inner join sys.sysusers u
                on u.uid = s.principal_id
        order by s.name
        """)

        # Assert same number of schemas
        assert len(schemas) == len(query_schema_df)

        # Assert same values
        assert set(schemas) == set(query_schema_df['schema_name'])

    def test_get_columns_pg(self):
        db.drop_table('working', table_for_testing)
        db.query(f"create table working.{table_for_testing} (test_int int, test_str varchar(10))")
        columns = db.get_table_columns(table_for_testing, schema='working')

        # Assert correct columns
        # cast to string to avoid issue with odbcs' return types
        assert str(columns) == str([('test_int', 'integer'), ('test_str', 'character varying')])
        db.drop_table('working', table_for_testing)

    def test_get_columns_ms(self):
        sql.drop_table(schema=sql.default_schema, table=table_for_testing)
        sql.query(f"create table {sql.default_schema}.{table_for_testing} (test_int int, test_str varchar(10))")
        columns = sql.get_table_columns(table_for_testing)

        # Assert correct columns
        # cast to string to avoid issue with odbcs' return types
        assert str(columns) == str([('test_int', 'int'), ('test_str', 'varchar')])
        sql.drop_table(schema=sql.default_schema, table=table_for_testing)

    def test_my_tables_pg_basic(self):
        db.drop_table(schema='working', table=table_for_testing)
        my_tables_df = db.my_tables(schema='working')
        number_of_my_tables = len(my_tables_df)

        db.query(f'create table working.{table_for_testing} as select * from working.{pg_table_name} limit 10')

        new_my_tables_df = db.my_tables(schema='working')
        new_number_of_my_tables = len(new_my_tables_df)

        # Assert new table is in my tables
        assert number_of_my_tables == new_number_of_my_tables - 1

        db.drop_table(table=table_for_testing, schema='working')

    def test_my_tables_pg_multiple(self):
        my_tables_df = db.my_tables(schema='working')
        number_of_my_tables = len(my_tables_df)
        another_table_for_testing = table_for_testing + '2'

        db.query(f'create table working.{table_for_testing} as select * from working.{pg_table_name} limit 10')
        db.query(f'create table working.{another_table_for_testing} as select * from working.{pg_table_name} limit 10')

        new_my_tables_df = db.my_tables(schema='working')
        new_number_of_my_tables = len(new_my_tables_df)

        # Assert new table is in my tables
        assert number_of_my_tables == new_number_of_my_tables - 2

        db.drop_table(table=table_for_testing, schema='working')
        db.drop_table(table=another_table_for_testing, schema='working')

    def test_my_tables_pg_drop(self):
        my_tables_df = db.my_tables(schema='working')
        number_of_my_tables = len(my_tables_df)

        db.query(f'create table working.{table_for_testing} as select * from working.{pg_table_name} limit 10')

        new_my_tables_df = db.my_tables(schema='working')
        new_number_of_my_tables = len(new_my_tables_df)

        # Assert new table is in my tables
        assert number_of_my_tables == new_number_of_my_tables - 1

        db.drop_table(table=table_for_testing, schema='working')
        drop_my_tables_df = db.my_tables(schema='working')

        drop_number_of_my_tables = len(drop_my_tables_df)
        assert drop_number_of_my_tables == number_of_my_tables

    def test_my_tables_pg_confirm(self):
        # Public schema my tables (PG)
        my_tables_df = db.my_tables()

        query_owner_df = db.dfquery(f"""
        SELECT *
        FROM pg_catalog.pg_tables
        WHERE schemaname = 'public'
        AND tableowner='{db.user}'
        """)

        # Assert same number returned
        assert len(my_tables_df) == len(query_owner_df)

        # Assert same values returned
        assert set(my_tables_df['tablename']) == set(query_owner_df['tablename'])

    def test_my_tables_pg_schema(self):
        # Public schema my tables (PG)
        my_tables_df = db.my_tables(schema='working')
        number_of_my_tables = len(my_tables_df)

        db.query(f'create table working.{table_for_testing} as select * from working.{pg_table_name} limit 10')

        new_my_tables_df = db.my_tables(schema='working')
        new_number_of_my_tables = len(new_my_tables_df)

        # Assert new table is in my tables
        assert number_of_my_tables == new_number_of_my_tables - 1

        db.drop_table(table=table_for_testing, schema='working')

    def test_my_tables_ms(self):
        # My_tables does not do anything for Sql Server - should return nothing and print an error statement
        returned = sql.my_tables()
        assert returned is None

    def test_rename_column_pg(self):
        db.query(f'create table working.{table_for_testing} as select * from working.{pg_table_name} limit 10')

        og_columns = list(db.dfquery(f'select * from working.{table_for_testing}'))
        original_column = og_columns[0]

        # Rename columns
        db.rename_column(schema='working', table=table_for_testing, old_column=original_column, new_column='new_col_name')

        # Assert columns have changed accordingly
        assert 'new_col_name' in set(db.dfquery(f'select * from working.{table_for_testing}'))
        assert original_column not in set(db.dfquery(f'select * from working.{table_for_testing}'))

        db.drop_table(table=table_for_testing, schema='working')

    def test_rename_column_ms(self):

        # TODO: this is failing with geom column - seems to be an issue with ODBC driver and geom...???

        sql.drop_table(table=table_for_testing, schema='dbo')
        sql.query(f'select top 10 test_col1, test_col2 into dbo.{table_for_testing} from {sql.default_schema}.{sql_table_name}')

        og_columns = list(sql.dfquery(f'select test_col1, test_col2 from dbo.{table_for_testing}'))
        original_column = og_columns[0]

        # Rename columns
        sql.rename_column(schema='dbo', table=table_for_testing, old_column=original_column, new_column='new_col_name')

        # Assert columns hasve changed accordingly
        assert 'new_col_name' in set(sql.dfquery(f'select * from dbo.{table_for_testing}'))
        assert original_column not in set(sql.dfquery(f'select * from dbo.{table_for_testing}'))

        sql.drop_table(table=table_for_testing, schema='dbo')

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(db)
        helpers.clean_up_test_table_sql(sql)


class TestCheckLog:
    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_pg(db)
        helpers.set_up_test_table_sql(sql)

    def test_check_log_pg(self):
        logs_df = db.check_logs()
        query_df = db.dfquery(f"select * from {db.default_schema}.{db.log_table}")
        pd.testing.assert_frame_equal(logs_df, query_df)

    def test_check_log_ms(self):
        logs_df = sql.check_logs()
        query_df = sql.dfquery(f"select * from {sql.default_schema}.{sql.log_table}")
        pd.testing.assert_frame_equal(logs_df, query_df)

    def test_check_log_pg_working(self):
        logs_df = db.check_logs(schema='working')
        query_df = db.dfquery(f"select * from working.{db.log_table}")

        pd.testing.assert_frame_equal(logs_df, query_df)

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(db)
        helpers.clean_up_test_table_sql(sql)


class TestLogging:
    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_pg(db)

    def test_query_temp_logging(self):
        db.query(f"""
            DROP TABLE IF EXISTS working.{table_for_testing_logging};
            CREATE TABLE working.{table_for_testing_logging} as
            SELECT *
            FROM working.{pg_table_name}
            LIMIT 10
        """)

        assert db.table_exists(table=table_for_testing_logging, schema='working')

        before_log_df = db.dfquery(f"""
            SELECT *
            FROM working.__temp_log_table_{db.user}__
            where table_name='{table_for_testing_logging}'
        """)

        before_drop_working_log_length = len(before_log_df)
        assert before_drop_working_log_length == 1

        db.query(f"""
        DROP TABLE IF EXISTS working.{table_for_testing_logging};
        """)

        after_log_df = db.dfquery(f"""
            SELECT *
            FROM working.__temp_log_table_{db.user}__
            where table_name='{table_for_testing_logging}'
        """)

        after_drop_working_log_length = len(after_log_df)
        assert after_drop_working_log_length == 0

    def test_query_rename_logging(self):
        return

    def test_drop_table_logging(self):
        db.query(f"""
                    DROP TABLE IF EXISTS working.{table_for_testing_logging};
                    CREATE TABLE working.{table_for_testing_logging} as
                    SELECT *
                    FROM working.{pg_table_name}
                    LIMIT 10
        """)

        assert db.table_exists(table=table_for_testing_logging, schema='working')

        before_log_df = db.dfquery(f"""
                    SELECT *
                    FROM working.__temp_log_table_{db.user}__
                    where table_name='{table_for_testing_logging}'
                """)

        before_drop_working_log_length = len(before_log_df)

        assert before_drop_working_log_length == 1

        db.drop_table(table=table_for_testing_logging, schema='working')

        after_log_df = db.dfquery(f"""
                    SELECT *
                    FROM working.__temp_log_table_{db.user}__
                    where table_name='{table_for_testing_logging}'
                """)

        after_drop_working_log_length = len(after_log_df)
        assert after_drop_working_log_length == 0

    def test_correct_logging_expiration_deletion(self):
        db.query(f"""
            drop table if exists working.{table_for_testing_logging};
            create table working.{table_for_testing_logging} as
            select * from working.{pg_table_name}
            limit 1
        """)

        # assert initial exp date is one week from now
        initial_exp_date = list(db.dfquery(f"""
            SELECT expires
            FROM working.__temp_log_table_{db.user}__
            WHERE table_name='{table_for_testing_logging}';""")['expires'])[0]
        assert initial_exp_date == (datetime.datetime.now().date() + datetime.timedelta(7))

        # Re-test with a new date
        db.query(f"""
        UPDATE working.__temp_log_table_{db.user}__
        SET expires=now()::date - interval '1 day'
        WHERE table_name='{table_for_testing_logging}';
        """)

        updated_exp_date = list(db.dfquery(f"""
        SELECT expires
        FROM working.__temp_log_table_{db.user}__
        WHERE table_name='{table_for_testing_logging}';
        """)['expires'])[0]

        # assert updated exp date is one day from now
        assert updated_exp_date == (datetime.datetime.now().date() - datetime.timedelta(1))

        # assert log of newly-created dbc is empty
        reconnect_db = pysqldb.DbConnect(type=db.type,
                                         server=db.server,
                                         database=db.database,
                                         user=db.user,
                                         password=db.password)

        new_log_tbl_df = reconnect_db.dfquery(f"""
        SELECT expires
        FROM working.__temp_log_table_{db.user}__
        WHERE table_name='{table_for_testing_logging}';
        """)

        assert len(new_log_tbl_df) == 0

    def test_custom_logging_expiration_date(self):
        db.query(f"""
            drop table if exists working.{table_for_testing_logging};
            create table working.{table_for_testing_logging} as
            select * from working.{pg_table_name}
            limit 1
        """, days=10)

        # assert a custom exp date of 10 days from now
        initial_exp_date = list(db.dfquery(f"""
            SELECT expires
            FROM working.__temp_log_table_{db.user}__
            WHERE table_name='{table_for_testing_logging}';
        """)['expires'])[0]

        assert initial_exp_date == (datetime.datetime.now().date() + datetime.timedelta(10))
        db.drop_table(schema='working', table=table_for_testing_logging)

    def test_custom_logging_expiration_date_2(self):
        db.query(f"""
            drop table if exists working.{table_for_testing_logging};
            create table working.{table_for_testing_logging} as
            select * from working.{pg_table_name}
            limit 1
        """, days=1)

        # 1 day
        initial_exp_date = list(db.dfquery(f"""
            SELECT expires
            FROM working.__temp_log_table_{db.user}__
            WHERE table_name='{table_for_testing_logging}';
        """)['expires'])[0]

        assert initial_exp_date == (datetime.datetime.now().date() + datetime.timedelta(1))
        db.drop_table(schema='working', table=table_for_testing_logging)

    def test_csv_to_table_logging(self):
        fp = os.path.dirname(os.path.abspath(__file__)) + '/test_data/test_csv.csv'

        before_log_df = db.dfquery(f"""
                    SELECT *
                    FROM working.__temp_log_table_{db.user}__
                    where table_name='{table_for_testing_logging}'
                """)

        before_drop_working_log_length = len(before_log_df)

        assert before_drop_working_log_length == 0

        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df.to_csv(fp)
        db.csv_to_table(input_file=fp, schema='working', table=table_for_testing_logging)

        after_log_df = db.dfquery(f"""
                    SELECT *
                    FROM working.__temp_log_table_{db.user}__
                    where table_name='{table_for_testing_logging}'
                """)

        after_drop_working_log_length = len(after_log_df)
        assert after_drop_working_log_length == 1

        db.drop_table(table=table_for_testing_logging, schema='working')

    def test_excel_to_table_logging(self):
        helpers.set_up_xls()

        fp = os.path.dirname(os.path.abspath(__file__)) + '/test_data/test_xls.xls'

        before_log_df = db.dfquery(f"""
                    SELECT *
                    FROM working.__temp_log_table_{db.user}__
                    where table_name='{table_for_testing_logging}'
                """)

        before_drop_working_log_length = len(before_log_df)
        assert before_drop_working_log_length == 0

        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df.to_excel(fp)
        db.xls_to_table(input_file=fp, schema='working', table=table_for_testing_logging)

        after_log_df = db.dfquery(f"""
                    SELECT *
                    FROM working.__temp_log_table_{db.user}__
                    where table_name='{table_for_testing_logging}'
                """)

        after_drop_working_log_length = len(after_log_df)
        assert after_drop_working_log_length == 1

        db.drop_table(table=table_for_testing_logging, schema='working')

    def test_dataframe_to_table_logging(self):
        before_log_df = db.dfquery(f"""
                    SELECT *
                    FROM working.__temp_log_table_{db.user}__
                    where table_name='{table_for_testing_logging}'
                """)

        before_drop_working_log_length = len(before_log_df)
        assert before_drop_working_log_length == 0

        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        db.dataframe_to_table(df=df, schema='working', table=table_for_testing_logging)

        after_log_df = db.dfquery(f"""
                    SELECT *
                    FROM working.__temp_log_table_{db.user}__
                    where table_name='{table_for_testing_logging}'
                """)

        after_drop_working_log_length = len(after_log_df)
        assert after_drop_working_log_length == 1

        db.drop_table(table=table_for_testing_logging, schema='working')

    def test_shp_to_table_logging(self):
        helpers.set_up_shapefile()

        before_log_df = db.dfquery(f"""
                    SELECT *
                    FROM working.__temp_log_table_{db.user}__
                    where table_name='{table_for_testing_logging}'
                """)

        before_log_length = len(before_log_df)
        assert before_log_length == 0

        db.shp_to_table(path=os.path.join(os.path.dirname(os.path.abspath(__file__)))+'\\test_data',
                        shp_name='test.shp',
                        schema='working',
                        table=table_for_testing_logging)

        after_log_df = db.dfquery(f"""
                    SELECT *
                    FROM working.__temp_log_table_{db.user}__
                    where table_name='{table_for_testing_logging}'
                """)

        after_log_length = len(after_log_df)
        assert after_log_length == 1

        # Cleanup
        db.drop_table(table=table_for_testing_logging, schema='working')
        helpers.clean_up_shapefile()

    def test_bulk_csv_to_table_logging(self):
        fp = os.path.dirname(os.path.abspath(__file__)) + '/test_data/test_bulk.csv'
        db.drop_table('working', table_for_testing_logging)
        before_log_df = db.dfquery(f"""
                    SELECT *
                    FROM working.__temp_log_table_{db.user}__
                    where table_name='{table_for_testing_logging}'
                """)

        before_drop_working_log_length = len(before_log_df)
        assert before_drop_working_log_length == 0

        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df.to_csv(fp)

        input_schema = db.dataframe_to_table_schema(df, table=table_for_testing_logging,
                                                    schema='working', temp=True, overwrite=True)

        db._bulk_csv_to_table(input_file=fp, schema='working', table=table_for_testing_logging,
                              table_schema=input_schema)

        after_log_df = db.dfquery(f"""
                    SELECT *
                    FROM working.__temp_log_table_{db.user}__
                    where table_name='{table_for_testing_logging}'
                """)

        after_drop_working_log_length = len(after_log_df)
        assert after_drop_working_log_length == 1

        db.drop_table(table=table_for_testing_logging, schema='working')

    def test_table_to_csv_check_file_quote_name(self):
        schema = 'working'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # Create table
        db.query(f"""
            DROP TABLE IF EXISTS  "{schema}"."{table_for_testing_logging}";
            CREATE TABLE
                    "{schema}"."{table_for_testing_logging}"
            AS SELECT
                id, test_col1, test_col2, geom
            FROM
                working.{pg_table_name}
            LIMIT 10
        """)
        assert db.table_exists(table_for_testing_logging, schema=schema)

        # table to csv
        db.table_to_csv(table_for_testing_logging,
                        schema=schema,
                        output_file=os.path.join(fldr, f'{table_for_testing_logging}.csv'))

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, f'{table_for_testing_logging}.csv'))

        # clean up
        db.drop_table(schema, table_for_testing_logging)
        os.remove(os.path.join(fldr, f'{table_for_testing_logging}.csv'))

    def test_pg_capitals(self):
        # Assert no test table
        assert len(db.dfquery(f"""
        DROP TABLE IF EXISTS {table_for_testing_logging.capitalize()};
        DROP TABLE IF EXISTS working.{table_for_testing_logging.capitalize()};

        SELECT table_schema, table_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_name='{table_for_testing_logging}'
        """)) == 0

        # Assert doesn't exist in log either
        assert len(db.dfquery(f"""
        SELECT * FROM working.__temp_log_table_{db.user}__ WHERE table_name='{table_for_testing_logging}'
        """)) == 0

        # Add capitalized test table
        db.query(f"""
        create table working.{table_for_testing_logging.capitalize()} as
        select *
        from working.{pg_table_name}
        limit 10
        """)

        # Assert pg stores as non-capitalized
        assert len(db.dfquery(f"""
        SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name='{table_for_testing_logging}'
        """)) == 1

        # Assert log stores as non-capitalized
        assert len(db.dfquery(f"""
        SELECT * FROM working.__temp_log_table_{db.user}__ WHERE table_name='{table_for_testing_logging}'
        """.format(db.user, table_for_testing_logging))) == 1

        # Cleanup
        db.drop_table(schema='working', table=table_for_testing_logging)

    def test_pg_quotes_no_capitals(self):
        # Assert no table test_table
        assert len(db.dfquery(f"""
        DROP TABLE IF EXISTS {table_for_testing_logging};
        DROP TABLE IF EXISTS working.{table_for_testing_logging};

        SELECT table_schema, table_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_name='{table_for_testing_logging}'
        """)) == 0

        # Assert doesn't exist in log either
        assert len(db.dfquery(f"""
        SELECT * FROM working.__temp_log_table_{db.user}__ WHERE table_name='{table_for_testing_logging}'
        """)) == 0

        # Add table in (quotes)
        db.query(f"""
        create table working."{table_for_testing_logging}" as
        select *
        from working.{pg_table_name}
        limit 10
        """)

        # Assert pg stores without quotes
        assert len(db.dfquery(f"""
        SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name='{table_for_testing_logging}'
        """)) == 1

        # Assert log stores without quotes
        assert len(db.dfquery(f"""
        SELECT * FROM working.__temp_log_table_{db.user}__ WHERE table_name='{table_for_testing_logging}'
        """)) == 1

        # Cleanup
        db.query(f"DROP TABLE IF EXISTS working.{table_for_testing_logging}")

    def test_pg_quotes_with_capital(self):
        # Assert no test table
        assert len(db.dfquery(f"""
        DROP TABLE IF EXISTS "{table_for_testing_logging.capitalize()}";
        DROP TABLE IF EXISTS working."{table_for_testing_logging.capitalize()}";

        SELECT table_schema, table_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_name='{table_for_testing_logging.capitalize()}'
        """)) == 0

        # Assert doesn't exist in log either
        assert len(db.dfquery(f"""
        SELECT * FROM working.__temp_log_table_{db.user}__ WHERE table_name='{table_for_testing_logging.capitalize()}'
        """)) == 0

        # Add capitalized test table (in quotes)
        db.query(f"""
        create table working."{table_for_testing_logging.capitalize()}" as
        select *
        from working.{pg_table_name}
        limit 10
        """)

        # Assert pg stores with capitalization
        assert len(db.dfquery(f"""
        SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name='{table_for_testing_logging.capitalize()}'
        """)) == 1

        # Assert log stores with capitalization
        assert len(db.dfquery(f"""
        SELECT * FROM working.__temp_log_table_{db.user}__ WHERE table_name='{table_for_testing_logging.capitalize()}'
        """)) == 1

        # Cleanup
        db.dfquery(f'DROP TABLE IF EXISTS working."{db.user}"'.format(table_for_testing_logging.capitalize()))

    def test_ms_capitals(self):
        sql.drop_table(schema='dbo', table=table_for_testing_logging)

        # Assert no test table
        assert len(sql.dfquery(f"""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name='dbo' and t.name = '{table_for_testing_logging}'
        """)) == 0

        # Assert doesn't exist in log either
        assert len(sql.dfquery(f"""
        SELECT * FROM dbo.__temp_log_table_{sql.user}__ WHERE table_name='{table_for_testing_logging}'
        """)) == 0

        # Add test table (capitalized)
        sql.query(f"""
        create table dbo.{table_for_testing_logging.capitalize()} (test_col1 int, test_col2 int);
        insert into dbo.{table_for_testing_logging.capitalize()} VALUES(1, 2);
        insert into dbo.{table_for_testing_logging.capitalize()} VALUES(3, 4);
        """)

        # Assert sql stores without capitalization
        assert len(sql.dfquery(f"""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' and t.name = '{table_for_testing_logging}'
        """)) == 1

        # Assert log stores without capitaliztion
        assert len(sql.dfquery(f"""
        SELECT * FROM dbo.__temp_log_table_{sql.user}__ WHERE table_name='{table_for_testing_logging}'
        """)) == 1

        # Cleanup
        sql.query(f'DROP TABLE dbo.{table_for_testing_logging}')

    def test_ms_quotes(self):
        # Assert no test table
        assert len(sql.dfquery(f"""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name='dbo' and t.name = '{table_for_testing_logging}'
        """)) == 0

        # Assert doesn't exist in log either
        assert len(sql.dfquery(f"""
        SELECT * FROM dbo.__temp_log_table_{sql.user}__ WHERE table_name='{table_for_testing_logging}'
        """)) == 0

        # Add test table with quotes
        sql.query(f"""
        create table dbo."{table_for_testing_logging}"(test_col1 int, test_col2 int);
        insert into dbo."{table_for_testing_logging}" VALUES(1, 2);
        insert into dbo."{table_for_testing_logging}" VALUES(3, 4);
        """)

        # Assert sql stores without quotes
        assert len(sql.dfquery(f"""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' and t.name = '{table_for_testing_logging}'
        """)) == 1

        # Assert log stores without quotes
        assert len(sql.dfquery(f"""
        SELECT * FROM dbo.__temp_log_table_{sql.user}__ WHERE table_name='{table_for_testing_logging}'
        """)) == 1

        # Cleanup
        sql.query(f'DROP TABLE dbo."{table_for_testing_logging}"')

    def test_ms_brackets(self):
        # Assert no test table
        assert len(sql.dfquery(f"""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name='dbo' and t.name = '{table_for_testing_logging}'
        """)) == 0

        # Assert doesn't exist in log either
        assert len(sql.dfquery(f"""
        SELECT * FROM dbo.__temp_log_table_{sql.user}__ WHERE table_name='{table_for_testing_logging}'
        """)) == 0

        # Add test table with brackets
        sql.query(f"""
        create table dbo.[{table_for_testing_logging}](test_col1 int, test_col2 int);
        insert into dbo.[{table_for_testing_logging}] VALUES(1, 2);
        insert into dbo.[{table_for_testing_logging}] VALUES(3, 4);
        """)

        # Assert sql stores without brackets
        assert len(sql.dfquery(f"""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' and t.name = '{table_for_testing_logging}'
        """)) == 1

        # Assert log stores without brackets
        assert len(sql.dfquery(f"""
        SELECT * FROM dbo.__temp_log_table_{sql.user}__ WHERE table_name='{table_for_testing_logging}'
        """)) == 1

        # Cleanup
        sql.query(f'DROP TABLE dbo.[{table_for_testing_logging}]')

    def test_ms_brackets_caps(self):
        # Assert no test table
        assert len(sql.dfquery(f"""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name='dbo' and t.name = '{table_for_testing_logging}'
        """)) == 0

        # Assert doesn't exist in log either
        assert len(sql.dfquery(f"""
        SELECT * FROM dbo.__temp_log_table_{sql.user}__ WHERE table_name='{table_for_testing_logging}'
        """)) == 0

        # Add test table in brackets, capitalized
        sql.query(f"""
        create table dbo.[{table_for_testing_logging.capitalize()}](test_col1 int, test_col2 int);
        insert into dbo.[{table_for_testing_logging.capitalize()}] VALUES(1, 2);
        insert into dbo.[{table_for_testing_logging.capitalize()}] VALUES(3, 4);
        """)

        # Assert sql stores lowercase, without bracket
        assert len(sql.dfquery(f"""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' and t.name = '{table_for_testing_logging}'
        """)) == 1

        # Assert log stores lowercase, without brackets
        assert len(sql.dfquery(f"""
        SELECT * FROM dbo.__temp_log_table_{sql.user}__ WHERE table_name='{table_for_testing_logging}'
        """)) == 1

        # Cleanup
        sql.query(f'DROP TABLE dbo.[{table_for_testing_logging.capitalize()}]')

    def test_ms_quotes_brackets(self):
        sql.drop_table(schema='dbo', table=f'["{table_for_testing_logging}"]')

        # Assert no test table in quotes
        assert len(sql.dfquery(f"""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name='dbo' and t.name = '"{table_for_testing_logging}"'
        """)) == 0

        # Assert doesn't exist in log either
        assert len(sql.dfquery(f"""
        SELECT * FROM dbo.__temp_log_table_{sql.user}__ WHERE table_name='"{table_for_testing_logging}"'
        """)) == 0

        # Add test table in quotes and brackets
        sql.query(f"""
        create table dbo.["{table_for_testing_logging}"] (test_col1 int, test_col2 int);
        insert into dbo.["{table_for_testing_logging}"] VALUES(1, 2);
        insert into dbo.["{table_for_testing_logging}"] VALUES(3, 4);
        """)

        # Assert sql stores in quotes, no brackets
        assert len(sql.dfquery(f"""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' and t.name = '"{table_for_testing_logging}"'
        """)) == 1

        # Assert log stores in quotes, no brackets
        assert len(sql.dfquery(f"""
        SELECT * FROM dbo.__temp_log_table_{sql.user}__ WHERE table_name='"{table_for_testing_logging}"'
        """)) == 1

        # Cleanup
        sql.query(f'DROP TABLE dbo.["{table_for_testing_logging}"]')

    def test_ms_quotes_brackets_caps(self):
        sql.drop_table(schema='dbo', table=f'["{table_for_testing_logging}"]')

        # Assert no test table
        assert len(sql.dfquery(f"""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name='dbo' and t.name = '"{table_for_testing_logging}"'
        """)) == 0

        # Assert doesn't exist in log either
        assert len(sql.dfquery(f"""
        SELECT * FROM dbo.__temp_log_table_{sql.user}__ WHERE table_name='"{table_for_testing_logging}"'
        """)) == 0

        # Add test table in brackets, quotes, and caps
        sql.query(f"""
        create table dbo.["{table_for_testing_logging.capitalize()}"](test_col1 int, test_col2 int);
        insert into dbo.["{table_for_testing_logging.capitalize()}"] VALUES(1, 2);
        insert into dbo.["{table_for_testing_logging.capitalize()}"] VALUES(3, 4);
        """)

        # Assert sql stores in quotes, not capitalized
        assert len(sql.dfquery(f"""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' and t.name = '"{table_for_testing_logging}"'
        """)) == 1

        # Assert log stores in quotes, not capitalized
        assert len(sql.dfquery(f"""
        SELECT * FROM dbo.__temp_log_table_{sql.user}__ WHERE table_name='"{table_for_testing_logging}"'
        """)) == 1

        # Cleanup
        sql.query(f'DROP TABLE dbo.["{table_for_testing_logging}"]')

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(db)
