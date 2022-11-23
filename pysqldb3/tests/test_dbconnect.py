import datetime
import os

import configparser
import pandas as pd

from . import helpers
from .. import pysqldb3 as pysqldb

test_config = configparser.ConfigParser()
test_config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

pg_dbconn = pysqldb.DbConnect(default=True, password=test_config.get('PG_DB', 'DB_PASSWORD'),
                              username=test_config.get('PG_DB', 'DB_USER'))
ms_dbconn = pysqldb.DbConnect(db_type=test_config.get('SQL_DB', 'TYPE'),
                              host=test_config.get('SQL_DB', 'SERVER'),
                              db_name=test_config.get('SQL_DB', 'DB_NAME'),
                              username=test_config.get('SQL_DB', 'DB_USER'),
                              password=test_config.get('SQL_DB', 'DB_PASSWORD'))

pg_table_name = 'pg_test_table_{username}'.format(username=pg_dbconn.username)
sql_table_name = 'sql_test_table_{username}'.format(username=ms_dbconn.username)
table_for_testing = 'table_for_testing_{username}'.format(username=pg_dbconn.username)
table_for_testing_logging = 'testing_logging_table_{username}'.format(username=pg_table_name)


class TestMisc:
    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_pg(pg_dbconn)
        helpers.set_up_test_table_sql(ms_dbconn, ms_dbconn.default_schema)

    def test_get_schemas_pg(self):
        schemas = pg_dbconn.get_schemas()

        query_schema_df = pg_dbconn.dfquery("""
        select schema_name
        from information_schema.schemata;
        """)

        # Assert same number of schemas
        assert len(schemas) == len(query_schema_df)

        # Assert same values
        assert set(schemas) == set(query_schema_df['schema_name'])

    def test_get_schemas_ms(self):
        schemas = ms_dbconn.get_schemas()

        query_schema_df = ms_dbconn.dfquery("""
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

    def test_my_tables_pg_basic(self):
        pg_dbconn.drop_table(schema_name='working', table_name=table_for_testing)
        my_tables_df = pg_dbconn.my_tables(schema_name='working')
        number_of_my_tables = len(my_tables_df)

        pg_dbconn.query('create table working.{test_table} as select * from working.{table} limit 10'.format(test_table=table_for_testing, table=pg_table_name))

        new_my_tables_df = pg_dbconn.my_tables(schema_name='working')
        new_number_of_my_tables = len(new_my_tables_df)

        # Assert new table is in my tables
        assert number_of_my_tables == new_number_of_my_tables - 1

        pg_dbconn.drop_table(table_name=table_for_testing, schema_name='working')

    def test_my_tables_pg_multiple(self):
        my_tables_df = pg_dbconn.my_tables(schema_name='working')
        number_of_my_tables = len(my_tables_df)
        another_table_for_testing = table_for_testing + '2'

        pg_dbconn.query('create table working.{test_table} as select * from working.{table} limit 10'.format(test_table=table_for_testing, table=pg_table_name))
        pg_dbconn.query('create table working.{test_table2} as select * from working.{table} limit 10'.format(test_table2=another_table_for_testing, table=pg_table_name))

        new_my_tables_df = pg_dbconn.my_tables(schema_name='working')
        new_number_of_my_tables = len(new_my_tables_df)

        # Assert new table is in my tables
        assert number_of_my_tables == new_number_of_my_tables - 2

        pg_dbconn.drop_table(table_name=table_for_testing, schema_name='working')
        pg_dbconn.drop_table(table_name=another_table_for_testing, schema_name='working')

    def test_my_tables_pg_drop(self):
        my_tables_df = pg_dbconn.my_tables(schema_name='working')
        number_of_my_tables = len(my_tables_df)

        pg_dbconn.query('create table working.{test_table} as select * from working.{table} limit 10'.format(test_table=table_for_testing, table=pg_table_name))

        new_my_tables_df = pg_dbconn.my_tables(schema_name='working')
        new_number_of_my_tables = len(new_my_tables_df)

        # Assert new table is in my tables
        assert number_of_my_tables == new_number_of_my_tables - 1

        pg_dbconn.drop_table(table_name=table_for_testing, schema_name='working')
        drop_my_tables_df = pg_dbconn.my_tables(schema_name='working')

        drop_number_of_my_tables = len(drop_my_tables_df)
        assert drop_number_of_my_tables == number_of_my_tables

    def test_my_tables_pg_confirm(self):
        # Public schema my tables (PG)
        my_tables_df = pg_dbconn.my_tables()

        query_owner_df = pg_dbconn.dfquery("""
        SELECT *
        FROM pg_catalog.pg_tables
        WHERE schemaname = 'public'
        AND tableowner='{username}'
        """.format(username=pg_dbconn.username))

        # Assert same number returned
        assert len(my_tables_df) == len(query_owner_df)

        # Assert same values returned
        assert set(my_tables_df['tablename']) == set(query_owner_df['tablename'])

    def test_my_tables_pg_schema(self):
        # Public schema my tables (PG)
        my_tables_df = pg_dbconn.my_tables(schema_name='working')
        number_of_my_tables = len(my_tables_df)

        pg_dbconn.query('create table working.{test_table} as select * from working.{table} limit 10'.format(
            test_table=table_for_testing, table=pg_table_name))

        new_my_tables_df = pg_dbconn.my_tables(schema_name='working')
        new_number_of_my_tables = len(new_my_tables_df)

        # Assert new table is in my tables
        assert number_of_my_tables == new_number_of_my_tables - 1

        pg_dbconn.drop_table(table_name=table_for_testing, schema_name='working')

    def test_my_tables_ms(self):
        # My_tables does not do anything for Sql Server - should return nothing and print an error statement
        returned = ms_dbconn.my_tables()
        assert returned is None

    def test_rename_column_pg(self):
        pg_dbconn.query('create table working.{test_table} as select * from working.{table} limit 10'.format(
            test_table=table_for_testing, table=pg_table_name))

        og_columns = list(pg_dbconn.dfquery('select * from working.{table}'.format(table=table_for_testing)))
        original_column = og_columns[0]

        # Rename columns
        pg_dbconn.rename_column(schema_name='working', table_name=table_for_testing, old_column=original_column, new_column='new_col_name')

        # Assert columns have changed accordingly
        assert 'new_col_name' in set(pg_dbconn.dfquery('select * from working.{table}'.format(table=table_for_testing)))
        assert original_column not in set(pg_dbconn.dfquery('select * from working.{table}'.format(table=table_for_testing)))

        pg_dbconn.drop_table(table_name=table_for_testing, schema_name='working')

    def test_rename_column_ms(self):

        # TODO: this is failing with geom column - seems to be an issue with ODBC driver and geom...???

        ms_dbconn.drop_table(table_name=table_for_testing, schema_name='dbo')

        ms_dbconn.query('select top 10 test_col1, test_col2 into dbo.{test_table} from {schema}.{table}'.format(
            test_table=table_for_testing, schema=ms_dbconn.default_schema, table=sql_table_name))

        og_columns = list(ms_dbconn.dfquery('select test_col1, test_col2 from dbo.{table}'.format(table=table_for_testing)))
        original_column = og_columns[0]

        # Rename columns
        ms_dbconn.rename_column(schema_name='dbo', table_name=table_for_testing, old_column=original_column, new_column='new_col_name')

        # Assert columns hasve changed accordingly
        assert 'new_col_name' in set(ms_dbconn.dfquery('select * from dbo.{test_table}'.format(test_table=table_for_testing)))
        assert original_column not in set(ms_dbconn.dfquery('select * from dbo.{test_table}'.format(test_table=table_for_testing)))

        ms_dbconn.drop_table(table_name=table_for_testing, schema_name='dbo')

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(pg_dbconn)
        helpers.clean_up_test_table_sql(ms_dbconn)


class TestCheckLog:
    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_pg(pg_dbconn)
        helpers.set_up_test_table_sql(ms_dbconn)

    def test_check_log_pg(self):
        logs_df = pg_dbconn.check_logs()
        query_df = pg_dbconn.dfquery("select * from {schema}.{table}".format(schema=pg_dbconn.default_schema, table=pg_dbconn.log_table))
        pd.testing.assert_frame_equal(logs_df, query_df)

    def test_check_log_ms(self):
        logs_df = ms_dbconn.check_logs()
        query_df = ms_dbconn.dfquery("select * from {schema}.{table}".format(schema=ms_dbconn.default_schema, table=ms_dbconn.log_table))
        pd.testing.assert_frame_equal(logs_df, query_df)

    def test_check_log_pg_working(self):
        logs_df = pg_dbconn.check_logs(schema_name='working')
        query_df = pg_dbconn.dfquery("select * from {schema}.{table}".format(schema='working', table=pg_dbconn.log_table))

        pd.testing.assert_frame_equal(logs_df, query_df)

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(pg_dbconn)
        helpers.clean_up_test_table_sql(ms_dbconn)


class TestLogging:
    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_pg(pg_dbconn)

    def test_query_temp_logging(self):
        pg_dbconn.query("""
            DROP TABLE IF EXISTS working.{test_table};
            CREATE TABLE working.{test_table} as
            SELECT *
            FROM working.{table}
            LIMIT 10
        """.format(test_table=table_for_testing_logging, table=pg_table_name))

        assert pg_dbconn.table_exists(table_name=table_for_testing_logging, schema='working')

        before_log_df = pg_dbconn.dfquery("""
            SELECT *
            FROM working.__temp_log_table_{username}__
            where table_name='{table}'
        """.format(username=pg_dbconn.username, table=table_for_testing_logging))

        before_drop_working_log_length = len(before_log_df)
        assert before_drop_working_log_length == 1

        pg_dbconn.query("""
        DROP TABLE IF EXISTS working.{table};
        """.format(table=table_for_testing_logging))

        after_log_df = pg_dbconn.dfquery("""
            SELECT *
            FROM working.__temp_log_table_{username}__
            where table_name='{table}'
        """.format(username=pg_dbconn.username, table=table_for_testing_logging))

        after_drop_working_log_length = len(after_log_df)
        assert after_drop_working_log_length == 0

    def test_query_rename_logging(self):
        return

    def test_drop_table_logging(self):
        pg_dbconn.query("""
                    DROP TABLE IF EXISTS working.{test_table};
                    CREATE TABLE working.{test_table} as
                    SELECT *
                    FROM working.{table}
                    LIMIT 10
        """.format(test_table=table_for_testing_logging, table=pg_table_name))

        assert pg_dbconn.table_exists(table_name=table_for_testing_logging, schema_name='working')

        before_log_df = pg_dbconn.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{username}__
                    where table_name='{table}'
                """.format(username=pg_dbconn.username, table=table_for_testing_logging))

        before_drop_working_log_length = len(before_log_df)

        assert before_drop_working_log_length == 1

        pg_dbconn.drop_table(table_name=table_for_testing_logging, schema_name='working')

        after_log_df = pg_dbconn.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{username}__
                    where table_name='{table}'
                """.format(username=pg_dbconn.username, table=table_for_testing_logging))

        after_drop_working_log_length = len(after_log_df)
        assert after_drop_working_log_length == 0

    def test_correct_logging_expiration_deletion(self):
        pg_dbconn.query("""
            drop table if exists working.{test_table};
            create table working.{test_table} as

            select * from working.{table}
            limit 1
        """.format(test_table=table_for_testing_logging, table=pg_table_name))

        initial_exp_date = list(pg_dbconn.dfquery("""
            SELECT expires
            FROM working.__temp_log_table_{username}__
            WHERE table_name='{table}';
        """.format(username=pg_dbconn.username, table=table_for_testing_logging))['expires'])[0]

        assert initial_exp_date == (datetime.datetime.now().date() + datetime.timedelta(7))

        pg_dbconn.query("""
        UPDATE working.__temp_log_table_{username}__
        SET expires=now()::date - interval '1 day'
        WHERE table_name='{table}';
        """.format(username=pg_dbconn.username, table=table_for_testing_logging))

        updated_exp_date = list(pg_dbconn.dfquery("""
        SELECT expires
        FROM working.__temp_log_table_{username}__
        WHERE table_name='{table}';
        """.format(username=pg_dbconn.username, table=table_for_testing_logging))['expires'])[0]

        assert updated_exp_date == (datetime.datetime.now().date() - datetime.timedelta(1))

        reconnect_db = pysqldb.DbConnect(db_type=pg_dbconn.type,
                                         host=pg_dbconn.host,
                                         db_name=pg_dbconn.db_name,
                                         username=pg_dbconn.username,
                                         password=pg_dbconn.password)

        new_log_tbl_df = reconnect_db.dfquery("""
        SELECT expires
        FROM working.__temp_log_table_{username}__
        WHERE table_name='{table}';
        """.format(username=pg_dbconn.username, table=table_for_testing_logging))

        assert len(new_log_tbl_df) == 0

    def test_custom_logging_expiration_date(self):
        pg_dbconn.query("""
            drop table if exists working.{test_table};
            create table working.{test_table} as
            select * from working.{table}
            limit 1
        """.format(test_table=table_for_testing_logging, table=pg_table_name), days=10)

        initial_exp_date = list(pg_dbconn.dfquery("""
            SELECT expires
            FROM working.__temp_log_table_{username}__
            WHERE table_name='{table}';
        """.format(username=pg_dbconn.username, table=table_for_testing_logging))['expires'])[0]

        assert initial_exp_date == (datetime.datetime.now().date() + datetime.timedelta(10))
        pg_dbconn.drop_table(schema_name='working', table_name=table_for_testing_logging)

    def test_custom_logging_expiration_date_2(self):
        pg_dbconn.query("""
            drop table if exists working.{test_table};
            create table working.{test_table} as

            select * from working.{table}
            limit 1
        """.format(test_table=table_for_testing_logging, table=pg_table_name), days=1)

        initial_exp_date = list(pg_dbconn.dfquery("""
            SELECT expires
            FROM working.__temp_log_table_{username}__
            WHERE table_name='{table}';
        """.format(username=pg_dbconn.username, table=table_for_testing_logging))['expires'])[0]

        assert initial_exp_date == (datetime.datetime.now().date() + datetime.timedelta(1))
        pg_dbconn.drop_table(schema_name='working', table_name=table_for_testing_logging)

    def test_csv_to_table_logging(self):
        fp = os.path.dirname(os.path.abspath(__file__)) + '/test_data/test_csv.csv'

        before_log_df = pg_dbconn.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{username}__
                    where table_name='{table}'
                """.format(username=pg_dbconn.username, table=table_for_testing_logging))

        before_drop_working_log_length = len(before_log_df)

        assert before_drop_working_log_length == 0

        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df.to_csv(fp)
        pg_dbconn.csv_to_table(input_file=fp, schema_name='working', table_name=table_for_testing_logging)

        after_log_df = pg_dbconn.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{username}__
                    where table_name='{table}'
                """.format(username=pg_dbconn.username, table=table_for_testing_logging))

        after_drop_working_log_length = len(after_log_df)
        assert after_drop_working_log_length == 1

        pg_dbconn.drop_table(table_name=table_for_testing_logging, schema_name='working')

    def test_excel_to_table_logging(self):
        helpers.set_up_xls()

        fp = os.path.dirname(os.path.abspath(__file__)) + '/test_data/test_xls.xls'

        before_log_df = pg_dbconn.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{username}__
                    where table_name='{table}'
                """.format(username=pg_dbconn.username, table=table_for_testing_logging))

        before_drop_working_log_length = len(before_log_df)
        assert before_drop_working_log_length == 0

        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df.to_excel(fp)
        pg_dbconn.xls_to_table(input_file=fp, schema_name='working', table_name=table_for_testing_logging)

        after_log_df = pg_dbconn.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{username}__
                    where table_name='{table}'
                """.format(username=pg_dbconn.username, table=table_for_testing_logging))

        after_drop_working_log_length = len(after_log_df)
        assert after_drop_working_log_length == 1

        pg_dbconn.drop_table(table_name=table_for_testing_logging, schema_name='working')

    def test_dataframe_to_table_logging(self):
        before_log_df = pg_dbconn.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{username}__
                    where table_name='{table}'
                """.format(username=pg_dbconn.username, table=table_for_testing_logging))

        before_drop_working_log_length = len(before_log_df)
        assert before_drop_working_log_length == 0

        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        pg_dbconn.dataframe_to_table(df=df, schema_name='working', table_name=table_for_testing_logging)

        after_log_df = pg_dbconn.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{username}__
                    where table_name='{table}'
                """.format(username=pg_dbconn.username, table=table_for_testing_logging))

        after_drop_working_log_length = len(after_log_df)
        assert after_drop_working_log_length == 1

        pg_dbconn.drop_table(table_name=table_for_testing_logging, schema_name='working')

    def test_shp_to_table_logging(self):
        helpers.set_up_shapefile()

        before_log_df = pg_dbconn.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{username}__
                    where table_name='{table}'
                """.format(username=pg_dbconn.username, table=table_for_testing_logging))

        before_log_length = len(before_log_df)
        assert before_log_length == 0

        pg_dbconn.shp_to_table(path=os.path.join(os.path.dirname(os.path.abspath(__file__))) + '\\test_data',
                               shp_name='test.shp',
                               schema_name='working',
                               table_name=table_for_testing_logging)

        after_log_df = pg_dbconn.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{username}__
                    where table_name='{table}'
                """.format(username=pg_dbconn.username, table=table_for_testing_logging))

        after_log_length = len(after_log_df)
        assert after_log_length == 1

        # Cleanup
        pg_dbconn.drop_table(table_name=table_for_testing_logging, schema_name='working')
        helpers.clean_up_shapefile()

    def test_bulk_csv_to_table_logging(self):
        fp = os.path.dirname(os.path.abspath(__file__)) + '/test_data/test_bulk.csv'
        pg_dbconn.drop_table('working', table_for_testing_logging)
        before_log_df = pg_dbconn.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{username}__
                    where table_name='{table}'
                """.format(username=pg_dbconn.username, table=table_for_testing_logging))

        before_drop_working_log_length = len(before_log_df)
        assert before_drop_working_log_length == 0

        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df.to_csv(fp)

        input_schema = pg_dbconn.dataframe_to_table_schema(df, table_name=table_for_testing_logging,
                                                           schema_name='working', temp=True, overwrite=True)

        pg_dbconn._bulk_csv_to_table(input_file=fp, schema_name='working', table_name=table_for_testing_logging,
                                     table_schema=input_schema)

        after_log_df = pg_dbconn.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{username}__
                    where table_name='{table}'
                """.format(username=pg_dbconn.username, table=table_for_testing_logging))

        after_drop_working_log_length = len(after_log_df)
        assert after_drop_working_log_length == 1

        pg_dbconn.drop_table(table_name=table_for_testing_logging, schema_name='working')

    def test_table_to_csv_check_file_quote_name(self):
        schema_name = 'working'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # Create table
        pg_dbconn.query("""
            DROP TABLE IF EXISTS  "{schema}"."{table}";
            CREATE TABLE
                    "{schema}"."{table}"
            AS SELECT
                id, test_col1, test_col2, geom
            FROM
                working.{pg}
            LIMIT 10
        """.format(schema=schema_name, table=table_for_testing_logging, pg=pg_table_name))
        assert pg_dbconn.table_exists(table_for_testing_logging, schema=schema_name)

        # table to csv
        pg_dbconn.table_to_csv(table_for_testing_logging,
                               schema_name=schema_name,
                               output_file=os.path.join(fldr, table_for_testing_logging + '.csv'))

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, table_for_testing_logging + '.csv'))

        # clean up
        pg_dbconn.drop_table(schema_name, table_for_testing_logging)
        os.remove(os.path.join(fldr, table_for_testing_logging + '.csv'))

    def test_pg_capitals(self):
        # Assert no test table
        assert len(pg_dbconn.dfquery("""
        DROP TABLE IF EXISTS {cap_table};
        DROP TABLE IF EXISTS working.{cap_table};

        SELECT table_schema, table_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_name='{table}'
        """.format(cap_table=table_for_testing_logging.capitalize(), table=table_for_testing_logging))) == 0

        # Assert doesn't exist in log either
        assert len(pg_dbconn.dfquery("""
        SELECT * FROM working.__temp_log_table_{username}__ WHERE table_name='{table}'
        """.format(username=pg_dbconn.username, table=table_for_testing_logging))) == 0

        # Add capitalized test table
        pg_dbconn.query("""
        create table working.{cap_table} as
        select *
        from working.{table}
        limit 10
        """.format(cap_table=table_for_testing_logging.capitalize(), table=pg_table_name))

        # Assert pg stores as non-capitalized
        assert len(pg_dbconn.dfquery("""
        SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name='{table}'
        """.format(table=table_for_testing_logging))) == 1

        # Assert log stores as non-capitalized
        assert len(pg_dbconn.dfquery("""
        SELECT * FROM working.__temp_log_table_{username}__ WHERE table_name='{table}'
        """.format(username=pg_dbconn.username, table=table_for_testing_logging))) == 1

        # Cleanup
        pg_dbconn.drop_table(schema_name='working', table_name=table_for_testing_logging)

    def test_pg_quotes_no_capitals(self):
        # Assert no table test_table
        assert len(pg_dbconn.dfquery("""
        DROP TABLE IF EXISTS {table};
        DROP TABLE IF EXISTS working.{table};

        SELECT table_schema, table_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_name='{table}'
        """.format(table=table_for_testing_logging) == 0))

        # Assert doesn't exist in log either
        assert len(pg_dbconn.dfquery("""
        SELECT * FROM working.__temp_log_table_{username}__ WHERE table_name='{table}'
        """.format(username=pg_dbconn.username, table=table_for_testing_logging))) == 0

        # Add table in (quotes)
        pg_dbconn.query("""
        create table working."{test_table}" as
        select *
        from working.{table}
        limit 10
        """.format(test_table=table_for_testing_logging, table=pg_table_name))

        # Assert pg stores without quotes
        assert len(pg_dbconn.dfquery("""
        SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name='{}'
        """.format(table_for_testing_logging))) == 1

        # Assert log stores without quotes
        assert len(pg_dbconn.dfquery("""
        SELECT * FROM working.__temp_log_table_{username}__ WHERE table_name='{table}'
        """.format(username=pg_dbconn.username, table=table_for_testing_logging))) == 1

        # Cleanup
        pg_dbconn.query("DROP TABLE IF EXISTS working.{table}".format(table=table_for_testing_logging))

    def test_pg_quotes_with_capital(self):
        # Assert no test table
        assert len(pg_dbconn.dfquery("""
        DROP TABLE IF EXISTS "{table}";
        DROP TABLE IF EXISTS working."{table}";

        SELECT table_schema, table_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_name='{table}'
        """.format(table=table_for_testing_logging.capitalize()))) == 0

        # Assert doesn't exist in log either
        assert len(pg_dbconn.dfquery("""
        SELECT * FROM working.__temp_log_table_{username}__ WHERE table_name='{table}'
        """.format(username=pg_dbconn.username, table=table_for_testing_logging.capitalize()))) == 0

        # Add capitalized test table (in quotes)
        pg_dbconn.query("""
        create table working."{test_table}" as
        select *
        from working.{table}
        limit 10
        """.format(test_table=table_for_testing_logging.capitalize(), table=pg_table_name))

        # Assert pg stores with capitalization
        assert len(pg_dbconn.dfquery("""
        SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name='{table}'
        """.format(table=table_for_testing_logging.capitalize()))) == 1

        # Assert log stores with capitalization
        assert len(pg_dbconn.dfquery("""
        SELECT * FROM working.__temp_log_table_{username}__ WHERE table_name='{table}'
        """.format(username=pg_dbconn.username, table=table_for_testing_logging.capitalize()))) == 1

        # Cleanup
        pg_dbconn.dfquery('DROP TABLE IF EXISTS working."{table}"'.format(table=table_for_testing_logging.capitalize()))

    def test_ms_capitals(self):
        ms_dbconn.drop_table(schema_name='dbo', table_name=table_for_testing_logging)

        # Assert no test table
        assert len(ms_dbconn.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name='dbo' and t.name = '{table}'
        """.format(table=table_for_testing_logging))) == 0

        # Assert doesn't exist in log either
        assert len(ms_dbconn.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{username}__ WHERE table_name='{table}'
        """.format(username=ms_dbconn.username, table=table_for_testing_logging))) == 0

        # Add test table (capitalized)
        ms_dbconn.query("""
        create table dbo.{table} (test_col1 int, test_col2 int);
        insert into dbo.{table} VALUES(1, 2);
        insert into dbo.{table} VALUES(3, 4);
        """.format(table=table_for_testing_logging.capitalize()))

        # Assert sql stores without capitalization
        assert len(ms_dbconn.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' and t.name = '{table}'
        """.format(table=table_for_testing_logging))) == 1

        # Assert log stores without capitaliztion
        assert len(ms_dbconn.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{username}__ WHERE table_name='{table}'
        """.format(username=ms_dbconn.username, table=table_for_testing_logging))) == 1

        # Cleanup
        ms_dbconn.query('DROP TABLE dbo.{table}'.format(table=table_for_testing_logging))

    def test_ms_quotes(self):
        # Assert no test table
        assert len(ms_dbconn.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name='dbo' and t.name = '{table}'
        """.format(table=table_for_testing_logging))) == 0

        # Assert doesn't exist in log either
        assert len(ms_dbconn.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{username}__ WHERE table_name='{table}'
        """.format(username=ms_dbconn.username, table=table_for_testing_logging))) == 0

        # Add test table with quotes
        ms_dbconn.query("""
        create table dbo."{table}"(test_col1 int, test_col2 int);
        insert into dbo."{table}" VALUES(1, 2);
        insert into dbo."{table}" VALUES(3, 4);
        """.format(table=table_for_testing_logging))

        # Assert sql stores without quotes
        assert len(ms_dbconn.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' and t.name = '{table}'
        """.format(table=table_for_testing_logging))) == 1

        # Assert log stores without quotes
        assert len(ms_dbconn.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{username}__ WHERE table_name='{table}'
        """.format(username=ms_dbconn.username, table=table_for_testing_logging))) == 1

        # Cleanup
        ms_dbconn.query('DROP TABLE dbo."{table}"'.format(table=table_for_testing_logging))

    def test_ms_brackets(self):
        # Assert no test table
        assert len(ms_dbconn.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name='dbo' and t.name = '{table}'
        """.format(table=table_for_testing_logging))) == 0

        # Assert doesn't exist in log either
        assert len(ms_dbconn.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{username}__ WHERE table_name='{table}'
        """.format(username=ms_dbconn.username, table=table_for_testing_logging))) == 0

        # Add test table with brackets
        ms_dbconn.query("""
        create table dbo.[{table}](test_col1 int, test_col2 int);
        insert into dbo.[{table}] VALUES(1, 2);
        insert into dbo.[{table}] VALUES(3, 4);
        """.format(table=table_for_testing_logging))

        # Assert sql stores without brackets
        assert len(ms_dbconn.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' and t.name = '{table}'
        """.format(table=table_for_testing_logging))) == 1

        # Assert log stores without brackets
        assert len(ms_dbconn.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{username}__ WHERE table_name='{table}'
        """.format(username=ms_dbconn.username, table=table_for_testing_logging))) == 1

        # Cleanup
        ms_dbconn.query('DROP TABLE dbo.[{table}]'.format(table=table_for_testing_logging))

    def test_ms_brackets_caps(self):
        # Assert no test table
        assert len(ms_dbconn.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name='dbo' and t.name = '{table}'
        """.format(table=table_for_testing_logging))) == 0

        # Assert doesn't exist in log either
        assert len(ms_dbconn.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{username}__ WHERE table_name='{table}'
        """.format(username=ms_dbconn.username, table=table_for_testing_logging))) == 0

        # Add test table in brackets, capitalized
        ms_dbconn.query("""
        create table dbo.[{table}](test_col1 int, test_col2 int);
        insert into dbo.[{table}] VALUES(1, 2);
        insert into dbo.[{table}] VALUES(3, 4);
        """.format(table=table_for_testing_logging.capitalize()))

        # Assert sql stores lowercase, without bracket
        assert len(ms_dbconn.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' and t.name = '{table}'
        """.format(table=table_for_testing_logging))) == 1

        # Assert log stores lowercase, without brackets
        assert len(ms_dbconn.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{username}__ WHERE table_name='{table}'
        """.format(username=ms_dbconn.username, table=table_for_testing_logging))) == 1

        # Cleanup
        ms_dbconn.query('DROP TABLE dbo.[{table}]'.format(table=table_for_testing_logging.capitalize()))

    def test_ms_quotes_brackets(self):
        ms_dbconn.drop_table(schema_name='dbo', table_name='["{table}"]'.format(table=table_for_testing_logging))

        # Assert no test table in quotes
        assert len(ms_dbconn.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name='dbo' and t.name = '"{table}"'
        """.format(table=table_for_testing_logging))) == 0

        # Assert doesn't exist in log either
        assert len(ms_dbconn.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{username}__ WHERE table_name='"{table}"'
        """.format(username=ms_dbconn.username, table=table_for_testing_logging))) == 0

        # Add test table in quotes and brackets
        ms_dbconn.query("""
        create table dbo.["{table}"](test_col1 int, test_col2 int);
        insert into dbo.["{table}"] VALUES(1, 2);
        insert into dbo.["{table}"] VALUES(3, 4);
        """.format(table=table_for_testing_logging))

        # Assert sql stores in quotes, no brackets
        assert len(ms_dbconn.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' and t.name = '"{table}"'
        """.format(table=table_for_testing_logging))) == 1

        # Assert log stores in quotes, no brackets
        assert len(ms_dbconn.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{username}__ WHERE table_name='"{table}"'
        """.format(username=ms_dbconn.username, table=table_for_testing_logging))) == 1

        # Cleanup
        ms_dbconn.query('DROP TABLE dbo.["{table}"]'.format(table=table_for_testing_logging))

    def test_ms_quotes_brackets_caps(self):
        ms_dbconn.drop_table(schema_name='dbo', table_name='["{table}"]'.format(table=table_for_testing_logging))

        # Assert no test table
        assert len(ms_dbconn.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name='dbo' and t.name = '"{}"'
        """.format(table_for_testing_logging))) == 0

        # Assert doesn't exist in log either
        assert len(ms_dbconn.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{username}__ WHERE table_name='"{table}"'
        """.format(username=ms_dbconn.username, table=table_for_testing_logging))) == 0

        # Add test table in brackets, quotes, and caps
        ms_dbconn.query("""
        create table dbo.["{table}"](test_col1 int, test_col2 int);
        insert into dbo.["{table}"] VALUES(1, 2);
        insert into dbo.["{table}"] VALUES(3, 4);
        """.format(table=table_for_testing_logging.capitalize()))

        # Assert sql stores in quotes, not capitalized
        assert len(ms_dbconn.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' and t.name = '"{table}"'
        """.format(table=table_for_testing_logging))) == 1

        # Assert log stores in quotes, not capitalized
        assert len(ms_dbconn.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{username}__ WHERE table_name='"{table}"'
        """.format(username=ms_dbconn.username, table=table_for_testing_logging))) == 1

        # Cleanup
        ms_dbconn.query('DROP TABLE dbo.["{table}"]'.format(table=table_for_testing_logging))

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(pg_dbconn)
