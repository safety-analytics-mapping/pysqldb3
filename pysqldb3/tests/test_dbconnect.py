import datetime
import os

import configparser
import pandas as pd

from . import helpers
from .. import pysqldb3 as pysqldb

test_config = configparser.ConfigParser()
test_config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")



sql = pysqldb.DbConnect(type=test_config.get('SQL_DB', 'TYPE'),
                        server=test_config.get('SQL_DB', 'SERVER'),
                        database=test_config.get('SQL_DB', 'DB_NAME'),
                        user=test_config.get('SQL_DB', 'DB_USER'),
                        password=test_config.get('SQL_DB', 'DB_PASSWORD'))

db = pysqldb.DbConnect(default=True, password=test_config.get('PG_DB', 'DB_PASSWORD'),
                       user=test_config.get('PG_DB', 'DB_USER'))

azure = pysqldb.DbConnect(type=test_config.get('AZ_DB', 'TYPE'),
                          server=test_config.get('AZ_DB', 'SERVER'),
                          database=test_config.get('AZ_DB', 'DB_NAME'),
                          user=test_config.get('AZ_DB', 'DB_USER')
                          )

pg_table_name = 'pg_test_table_{}'.format(db.user)
sql_table_name = 'sql_test_table_{}'.format(sql.user)
table_for_testing = 'table_for_testing_{}'.format(db.user)
table_for_testing_logging = 'testing_logging_table_{}'.format(db.user)


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

    def test_get_schemas_az(self):
        # not possible in azure
        assert True

    def test_my_tables_pg_basic(self):
        db.drop_table(schema='working', table=table_for_testing)
        my_tables_df = db.my_tables(schema='working')
        number_of_my_tables = len(my_tables_df)

        db.query('create table working.{} as select * from working.{} limit 10'.format(table_for_testing, pg_table_name))

        new_my_tables_df = db.my_tables(schema='working')
        new_number_of_my_tables = len(new_my_tables_df)

        # Assert new table is in my tables
        assert number_of_my_tables == new_number_of_my_tables - 1

        db.drop_table(table=table_for_testing, schema='working')

    def test_schema_tables_pg_basic(self):
        db.drop_table(schema='working', table=table_for_testing)
        schema_tables_df = db.schema_tables(schema='working')
        number_of_my_tables = len(schema_tables_df)

        assert schema_tables_df.tableowner.nunique() > 1

        db.query('create table working.{} as select * from working.{} limit 10'.format(table_for_testing, pg_table_name))

        new_schema_tables_df = db.schema_tables(schema='working')
        new_number_of_schema_tables = len(new_schema_tables_df)

        # Assert new table is in my tables
        assert number_of_my_tables == new_number_of_schema_tables - 1

        db.drop_table(table=table_for_testing, schema='working')

    def test_my_tables_pg_multiple(self):
        my_tables_df = db.my_tables(schema='working')
        number_of_my_tables = len(my_tables_df)
        another_table_for_testing = table_for_testing + '2'

        db.query('create table working.{} as select * from working.{} limit 10'.format(table_for_testing, pg_table_name))
        db.query('create table working.{} as select * from working.{} limit 10'.format(another_table_for_testing, pg_table_name))

        new_my_tables_df = db.my_tables(schema='working')
        new_number_of_my_tables = len(new_my_tables_df)

        # Assert new table is in my tables
        assert number_of_my_tables == new_number_of_my_tables - 2

        db.drop_table(table=table_for_testing, schema='working')
        db.drop_table(table=another_table_for_testing, schema='working')

    def test_my_tables_pg_drop(self):
        my_tables_df = db.my_tables(schema='working')
        number_of_my_tables = len(my_tables_df)

        db.query('create table working.{} as select * from working.{} limit 10'.format(table_for_testing, pg_table_name))

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

        query_owner_df = db.dfquery("""
        SELECT *
        FROM pg_catalog.pg_tables
        WHERE schemaname = 'public'
        AND tableowner='{}'
        """.format(db.user))

        # Assert same number returned
        assert len(my_tables_df) == len(query_owner_df)

        # Assert same values returned
        assert set(my_tables_df['tablename']) == set(query_owner_df['tablename'])

    def test_my_tables_pg_schema(self):
        # Public schema my tables (PG)
        my_tables_df = db.my_tables(schema='working')
        number_of_my_tables = len(my_tables_df)

        db.query('create table working.{} as select * from working.{} limit 10'.format(table_for_testing, pg_table_name))

        new_my_tables_df = db.my_tables(schema='working')
        new_number_of_my_tables = len(new_my_tables_df)

        # Assert new table is in my tables
        assert number_of_my_tables == new_number_of_my_tables - 1

        db.drop_table(table=table_for_testing, schema='working')

    def test_my_tables_ms(self):
        # My_tables does not do anything for Sql Server - should return nothing and print an error statement
        returned = sql.my_tables()
        assert returned is None

    def test_my_tables_az(self):
        # My_tables does not do anything for Sql Server - should return nothing and print an error statement
        returned = azure.my_tables()
        assert returned is None

    def test_rename_column_pg(self):
        db.query('create table working.{} as select * from working.{} limit 10'.format(table_for_testing, pg_table_name))

        og_columns = list(db.dfquery('select * from working.{}'.format(table_for_testing)))
        original_column = og_columns[0]

        # Rename columns
        db.rename_column(schema='working', table=table_for_testing, old_column=original_column, new_column='new_col_name')

        # Assert columns have changed accordingly
        assert 'new_col_name' in set(db.dfquery('select * from working.{}'.format(table_for_testing)))
        assert original_column not in set(db.dfquery('select * from working.{}'.format(table_for_testing)))

        db.drop_table(table=table_for_testing, schema='working')

    def test_rename_column_ms(self):

        # TODO: this is failing with geom column - seems to be an issue with ODBC driver and geom...???

        sql.drop_table(table=table_for_testing, schema='dbo')

        sql.query('select top 10 test_col1, test_col2 into dbo.{} from {}.{}'.format(table_for_testing,
                                                                                     sql.default_schema,
                                                                                     sql_table_name))

        og_columns = list(sql.dfquery('select test_col1, test_col2 from dbo.{}'.format(table_for_testing)))
        original_column = og_columns[0]

        # Rename columns
        sql.rename_column(schema='dbo', table=table_for_testing, old_column=original_column, new_column='new_col_name')

        # Assert columns hasve changed accordingly
        assert 'new_col_name' in set(sql.dfquery('select * from dbo.{}'.format(table_for_testing)))
        assert original_column not in set(sql.dfquery('select * from dbo.{}'.format(table_for_testing)))

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
        query_df = db.dfquery("select * from {}.{}".format(db.default_schema, db.log_table))
        pd.testing.assert_frame_equal(logs_df, query_df)

    def test_check_log_ms(self):
        logs_df = sql.check_logs()
        query_df = sql.dfquery("select * from {}.{}".format(sql.default_schema, sql.log_table))
        pd.testing.assert_frame_equal(logs_df, query_df)

    def test_check_log_pg_working(self):
        logs_df = db.check_logs(schema='working')
        query_df = db.dfquery("select * from {}.{}".format('working', db.log_table))

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
        db.query("""
            DROP TABLE IF EXISTS working.{};
            CREATE TABLE working.{} as
            SELECT *
            FROM working.{}
            LIMIT 10
        """.format(table_for_testing_logging, table_for_testing_logging, pg_table_name))

        assert db.table_exists(table=table_for_testing_logging, schema='working')

        before_log_df = db.dfquery("""
            SELECT *
            FROM working.__temp_log_table_{}__
            where table_name='{}'
        """.format(db.user, table_for_testing_logging))

        before_drop_working_log_length = len(before_log_df)
        assert before_drop_working_log_length == 1

        db.query("""
        DROP TABLE IF EXISTS working.{};
        """.format(table_for_testing_logging))

        after_log_df = db.dfquery("""
            SELECT *
            FROM working.__temp_log_table_{}__
            where table_name='{}'
        """.format(db.user, table_for_testing_logging))

        after_drop_working_log_length = len(after_log_df)
        assert after_drop_working_log_length == 0

    def test_query_rename_logging(self):
        return

    def test_drop_table_logging(self):
        db.query("""
                    DROP TABLE IF EXISTS working.{};
                    CREATE TABLE working.{} as
                    SELECT *
                    FROM working.{}
                    LIMIT 10
        """.format(table_for_testing_logging, table_for_testing_logging, pg_table_name))

        assert db.table_exists(table=table_for_testing_logging, schema='working')

        before_log_df = db.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{}__
                    where table_name='{}'
                """.format(db.user, table_for_testing_logging))

        before_drop_working_log_length = len(before_log_df)

        assert before_drop_working_log_length == 1

        db.drop_table(table=table_for_testing_logging, schema='working')

        after_log_df = db.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{}__
                    where table_name='{}'
                """.format(db.user, table_for_testing_logging))

        after_drop_working_log_length = len(after_log_df)
        assert after_drop_working_log_length == 0

    def test_correct_logging_expiration_deletion(self):
        db.query("""
            drop table if exists working.{};
            create table working.{} as

            select * from working.{}
            limit 1
        """.format(table_for_testing_logging, table_for_testing_logging, pg_table_name))

        initial_exp_date = list(db.dfquery("""
            SELECT expires
            FROM working.__temp_log_table_{}__
            WHERE table_name='{}';
        """.format(db.user, table_for_testing_logging))['expires'])[0]

        assert initial_exp_date == (datetime.datetime.now().date() + datetime.timedelta(7))

        db.query("""
        UPDATE working.__temp_log_table_{}__
        SET expires=now()::date - interval '1 day'
        WHERE table_name='{}';
        """.format(db.user, table_for_testing_logging))

        updated_exp_date = list(db.dfquery("""
        SELECT expires
        FROM working.__temp_log_table_{}__
        WHERE table_name='{}';
        """.format(db.user, table_for_testing_logging))['expires'])[0]

        assert updated_exp_date == (datetime.datetime.now().date() - datetime.timedelta(1))

        reconnect_db = pysqldb.DbConnect(type=db.type,
                                         server=db.server,
                                         database=db.database,
                                         user=db.user,
                                         password=db.password)

        new_log_tbl_df = reconnect_db.dfquery("""
        SELECT expires
        FROM working.__temp_log_table_{}__
        WHERE table_name='{}';
        """.format(db.user, table_for_testing_logging))

        assert len(new_log_tbl_df) == 0

    def test_custom_logging_expiration_date(self):
        db.query("""
            drop table if exists working.{};
            create table working.{} as

            select * from working.{}
            limit 1
        """.format(table_for_testing_logging, table_for_testing_logging, pg_table_name), days=10)

        initial_exp_date = list(db.dfquery("""
            SELECT expires
            FROM working.__temp_log_table_{}__
            WHERE table_name='{}';
        """.format(db.user, table_for_testing_logging))['expires'])[0]

        assert initial_exp_date == (datetime.datetime.now().date() + datetime.timedelta(10))
        db.drop_table(schema='working', table=table_for_testing_logging)

    def test_custom_logging_expiration_date_2(self):
        db.query("""
            drop table if exists working.{};
            create table working.{} as

            select * from working.{}
            limit 1
        """.format(table_for_testing_logging, table_for_testing_logging, pg_table_name), days=1)

        initial_exp_date = list(db.dfquery("""
            SELECT expires
            FROM working.__temp_log_table_{}__
            WHERE table_name='{}';
        """.format(db.user, table_for_testing_logging))['expires'])[0]

        assert initial_exp_date == (datetime.datetime.now().date() + datetime.timedelta(1))
        db.drop_table(schema='working', table=table_for_testing_logging)

    def test_csv_to_table_logging(self):
        fp = os.path.dirname(os.path.abspath(__file__)) + '/test_data/test_csv.csv'

        before_log_df = db.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{}__
                    where table_name='{}'
                """.format(db.user, table_for_testing_logging))

        before_drop_working_log_length = len(before_log_df)

        assert before_drop_working_log_length == 0

        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df.to_csv(fp)
        db.csv_to_table(input_file=fp, schema='working', table=table_for_testing_logging)

        after_log_df = db.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{}__
                    where table_name='{}'
                """.format(db.user, table_for_testing_logging))

        after_drop_working_log_length = len(after_log_df)
        assert after_drop_working_log_length == 1

        db.drop_table(table=table_for_testing_logging, schema='working')

    def test_excel_to_table_logging(self):
        helpers.set_up_xls()

        fp = os.path.dirname(os.path.abspath(__file__)) + '/test_data/test_xls.xls'

        before_log_df = db.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{}__
                    where table_name='{}'
                """.format(db.user, table_for_testing_logging))

        before_drop_working_log_length = len(before_log_df)
        assert before_drop_working_log_length == 0

        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df.to_excel(fp)
        db.xls_to_table(input_file=fp, schema='working', table=table_for_testing_logging)

        after_log_df = db.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{}__
                    where table_name='{}'
                """.format(db.user, table_for_testing_logging))

        after_drop_working_log_length = len(after_log_df)
        assert after_drop_working_log_length == 1

        db.drop_table(table=table_for_testing_logging, schema='working')

    def test_dataframe_to_table_logging(self):
        before_log_df = db.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{}__
                    where table_name='{}'
                """.format(db.user, table_for_testing_logging))

        before_drop_working_log_length = len(before_log_df)
        assert before_drop_working_log_length == 0

        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        db.dataframe_to_table(df=df, schema='working', table=table_for_testing_logging)

        after_log_df = db.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{}__
                    where table_name='{}'
                """.format(db.user, table_for_testing_logging))

        after_drop_working_log_length = len(after_log_df)
        assert after_drop_working_log_length == 1

        db.drop_table(table=table_for_testing_logging, schema='working')

    def test_shp_to_table_logging(self):
        helpers.set_up_shapefile()

        before_log_df = db.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{}__
                    where table_name='{}'
                """.format(db.user, table_for_testing_logging))

        before_log_length = len(before_log_df)
        assert before_log_length == 0
        # make sure table doesnt exists
        db.drop_table(table=table_for_testing_logging, schema='working')
        # import table
        db.shp_to_table(path=os.path.join(os.path.dirname(os.path.abspath(__file__)))+'\\test_data',
                        shp_name='test.shp',
                        schema='working',
                        table=table_for_testing_logging)

        after_log_df = db.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{}__
                    where table_name='{}'
                """.format(db.user, table_for_testing_logging))

        after_log_length = len(after_log_df)
        assert after_log_length == 1

        # Cleanup
        db.drop_table(table=table_for_testing_logging, schema='working')
        helpers.clean_up_shapefile()

    def test_bulk_csv_to_table_logging(self):
        fp = os.path.dirname(os.path.abspath(__file__)) + '/test_data/test_bulk.csv'
        db.drop_table('working', table_for_testing_logging)
        before_log_df = db.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{}__
                    where table_name='{}'
                """.format(db.user, table_for_testing_logging))

        before_drop_working_log_length = len(before_log_df)
        assert before_drop_working_log_length == 0

        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df.to_csv(fp)

        input_schema = db.dataframe_to_table_schema(df, table=table_for_testing_logging,
                                                    schema='working', temp=True, overwrite=True)

        db._bulk_csv_to_table(input_file=fp, schema='working', table=table_for_testing_logging,
                              table_schema=input_schema)

        after_log_df = db.dfquery("""
                    SELECT *
                    FROM working.__temp_log_table_{}__
                    where table_name='{}'
                """.format(db.user, table_for_testing_logging))

        after_drop_working_log_length = len(after_log_df)
        assert after_drop_working_log_length == 1

        db.drop_table(table=table_for_testing_logging, schema='working')

    def test_table_to_csv_check_file_quote_name(self):
        schema = 'working'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # Create table
        db.query("""
            DROP TABLE IF EXISTS  "{s}"."{t}";
            CREATE TABLE
                    "{s}"."{t}"
            AS SELECT
                id, test_col1, test_col2, geom
            FROM
                working.{pg}
            LIMIT 10
        """.format(s=schema, t=table_for_testing_logging, pg=pg_table_name))
        assert db.table_exists(table_for_testing_logging, schema=schema)

        # table to csv
        db.table_to_csv(table_for_testing_logging,
                        schema=schema,
                        output_file=os.path.join(fldr, table_for_testing_logging + '.csv'))

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, table_for_testing_logging + '.csv'))

        # clean up
        db.drop_table(schema, table_for_testing_logging)
        os.remove(os.path.join(fldr, table_for_testing_logging + '.csv'))

    def test_pg_capitals(self):
        # Assert no test table
        assert len(db.dfquery("""
        DROP TABLE IF EXISTS {};
        DROP TABLE IF EXISTS working.{};

        SELECT table_schema, table_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_name='{}'
        """.format(table_for_testing_logging.capitalize(),
                   table_for_testing_logging.capitalize(),
                   table_for_testing_logging))) == 0

        # Assert doesn't exist in log either
        assert len(db.dfquery("""
        SELECT * FROM working.__temp_log_table_{}__ WHERE table_name='{}'
        """.format(db.user, table_for_testing_logging))) == 0

        # Add capitalized test table
        db.query("""
        create table working.{} as
        select *
        from working.{}
        limit 10
        """.format(table_for_testing_logging.capitalize(), pg_table_name))

        # Assert pg stores as non-capitalized
        assert len(db.dfquery("""
        SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name='{}'
        """.format(table_for_testing_logging))) == 1

        # Assert log stores as non-capitalized
        assert len(db.dfquery("""
        SELECT * FROM working.__temp_log_table_{}__ WHERE table_name='{}'
        """.format(db.user, table_for_testing_logging))) == 1

        # Cleanup
        db.drop_table(schema='working', table=table_for_testing_logging)

    def test_pg_quotes_no_capitals(self):
        # Assert no table test_table
        assert len(db.dfquery("""
        DROP TABLE IF EXISTS {};
        DROP TABLE IF EXISTS working.{};

        SELECT table_schema, table_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_name='{}'
        """.format(table_for_testing_logging, table_for_testing_logging, table_for_testing_logging))) == 0

        # Assert doesn't exist in log either
        assert len(db.dfquery("""
        SELECT * FROM working.__temp_log_table_{}__ WHERE table_name='{}'
        """.format(db.user, table_for_testing_logging))) == 0

        # Add table in (quotes)
        db.query("""
        create table working."{}" as
        select *
        from working.{}
        limit 10
        """.format(table_for_testing_logging, pg_table_name))

        # Assert pg stores without quotes
        assert len(db.dfquery("""
        SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name='{}'
        """.format(table_for_testing_logging))) == 1

        # Assert log stores without quotes
        assert len(db.dfquery("""
        SELECT * FROM working.__temp_log_table_{}__ WHERE table_name='{}'
        """.format(db.user, table_for_testing_logging))) == 1

        # Cleanup
        db.query("DROP TABLE IF EXISTS working.{}".format(table_for_testing_logging))

    def test_pg_quotes_with_capital(self):
        # Assert no test table
        assert len(db.dfquery("""
        DROP TABLE IF EXISTS "{}";
        DROP TABLE IF EXISTS working."{}";

        SELECT table_schema, table_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_name='{}'
        """.format(table_for_testing_logging.capitalize(),
                   table_for_testing_logging.capitalize(),
                   table_for_testing_logging.capitalize()))) == 0

        # Assert doesn't exist in log either
        assert len(db.dfquery("""
        SELECT * FROM working.__temp_log_table_{}__ WHERE table_name='{}'
        """.format(db.user, table_for_testing_logging.capitalize()))) == 0

        # Add capitalized test table (in quotes)
        db.query("""
        create table working."{}" as
        select *
        from working.{}
        limit 10
        """.format(table_for_testing_logging.capitalize(), pg_table_name))

        # Assert pg stores with capitalization
        assert len(db.dfquery("""
        SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name='{}'
        """.format(table_for_testing_logging.capitalize()))) == 1

        # Assert log stores with capitalization
        assert len(db.dfquery("""
        SELECT * FROM working.__temp_log_table_{}__ WHERE table_name='{}'
        """.format(db.user, table_for_testing_logging.capitalize()))) == 1

        # Cleanup
        db.dfquery('DROP TABLE IF EXISTS working."{}"'.format(table_for_testing_logging.capitalize()))

    def test_ms_capitals(self):
        sql.drop_table(schema='dbo', table=table_for_testing_logging)

        # Assert no test table
        assert len(sql.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name='dbo' and t.name = '{}'
        """.format(table_for_testing_logging))) == 0

        # Assert doesn't exist in log either
        assert len(sql.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{}__ WHERE table_name='{}'
        """.format(sql.user, table_for_testing_logging))) == 0

        # Add test table (capitalized)
        sql.query("""
        create table dbo.{} (test_col1 int, test_col2 int);
        insert into dbo.{} VALUES(1, 2);
        insert into dbo.{} VALUES(3, 4);
        """.format(table_for_testing_logging.capitalize(),
                   table_for_testing_logging.capitalize(),
                   table_for_testing_logging.capitalize()))

        # Assert sql stores without capitalization
        assert len(sql.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' and t.name = '{}'
        """.format(table_for_testing_logging))) == 1

        # Assert log stores without capitaliztion
        assert len(sql.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{}__ WHERE table_name='{}'
        """.format(sql.user, table_for_testing_logging))) == 1

        # Cleanup
        sql.query('DROP TABLE dbo.{}'.format(table_for_testing_logging))

    def test_ms_quotes(self):
        sql.drop_table('dbo', table_for_testing_logging)
        # Assert no test table
        assert len(sql.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name='dbo' and t.name = '{}'
        """.format(table_for_testing_logging))) == 0

        # Assert doesn't exist in log either
        assert len(sql.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{}__ WHERE table_name='{}'
        """.format(sql.user, table_for_testing_logging))) == 0

        # Add test table with quotes
        sql.query("""
        create table dbo."{}"(test_col1 int, test_col2 int);
        insert into dbo."{}" VALUES(1, 2);
        insert into dbo."{}" VALUES(3, 4);
        """.format(table_for_testing_logging, table_for_testing_logging, table_for_testing_logging))

        # Assert sql stores without quotes
        assert len(sql.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' and t.name = '{}'
        """.format(table_for_testing_logging))) == 1

        # Assert log stores without quotes
        assert len(sql.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{}__ WHERE table_name='{}'
        """.format(sql.user, table_for_testing_logging))) == 1

        # Cleanup
        sql.drop_table('dbo', table_for_testing_logging)

    def test_ms_brackets(self):
        # Assert no test table
        sql.drop_table('dbo', table_for_testing_logging)
        assert len(sql.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name='dbo' and t.name = '{}'
        """.format(table_for_testing_logging))) == 0

        # Assert doesn't exist in log either
        assert len(sql.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{}__ WHERE table_name='{}'
        """.format(sql.user, table_for_testing_logging))) == 0

        # Add test table with brackets
        sql.query("""
        create table dbo.[{}](test_col1 int, test_col2 int);
        insert into dbo.[{}] VALUES(1, 2);
        insert into dbo.[{}] VALUES(3, 4);
        """.format(table_for_testing_logging, table_for_testing_logging, table_for_testing_logging))

        # Assert sql stores without brackets
        assert len(sql.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' and t.name = '{}'
        """.format(table_for_testing_logging))) == 1

        # Assert log stores without brackets
        assert len(sql.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{}__ WHERE table_name='{}'
        """.format(sql.user, table_for_testing_logging))) == 1

        # Cleanup
        sql.query('DROP TABLE dbo.[{}]'.format(table_for_testing_logging))

    def test_ms_brackets_caps(self):
        sql.drop_table('dbo', table_for_testing_logging)
        # Assert no test table
        assert len(sql.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name='dbo' and t.name = '{}'
        """.format(table_for_testing_logging))) == 0

        # Assert doesn't exist in log either
        assert len(sql.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{}__ WHERE table_name='{}'
        """.format(sql.user, table_for_testing_logging))) == 0

        # Add test table in brackets, capitalized
        sql.query("""
        create table dbo.[{}](test_col1 int, test_col2 int);
        insert into dbo.[{}] VALUES(1, 2);
        insert into dbo.[{}] VALUES(3, 4);
        """.format(table_for_testing_logging.capitalize(),
                   table_for_testing_logging.capitalize(),
                   table_for_testing_logging.capitalize()))

        # Assert sql stores lowercase, without bracket
        assert len(sql.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' and t.name = '{}'
        """.format(table_for_testing_logging))) == 1

        # Assert log stores lowercase, without brackets
        assert len(sql.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{}__ WHERE table_name='{}'
        """.format(sql.user, table_for_testing_logging))) == 1

        # Cleanup
        sql.query('DROP TABLE dbo.[{}]'.format(table_for_testing_logging.capitalize()))

    def test_ms_quotes_brackets(self):
        sql.drop_table(schema='dbo', table='["{}"]'.format(table_for_testing_logging))

        # Assert no test table in quotes
        assert len(sql.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name='dbo' and t.name = '"{}"'
        """.format(table_for_testing_logging))) == 0

        # Assert doesn't exist in log either
        assert len(sql.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{}__ WHERE table_name='"{}"'
        """.format(sql.user, table_for_testing_logging))) == 0

        # Add test table in quotes and brackets
        sql.query("""
        create table dbo.["{}"](test_col1 int, test_col2 int);
        insert into dbo.["{}"] VALUES(1, 2);
        insert into dbo.["{}"] VALUES(3, 4);
        """.format(table_for_testing_logging, table_for_testing_logging, table_for_testing_logging))

        # Assert sql stores in quotes, no brackets
        assert len(sql.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' and t.name = '"{}"'
        """.format(table_for_testing_logging))) == 1

        # Assert log stores in quotes, no brackets
        assert len(sql.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{}__ WHERE table_name='"{}"'
        """.format(sql.user, table_for_testing_logging))) == 1

        # Cleanup
        sql.query('DROP TABLE dbo.["{}"]'.format(table_for_testing_logging)) # todo not removing from log????
        print('here')

    def test_ms_quotes_brackets_caps(self):
        sql.query('DROP TABLE dbo.["{}"]'.format(table_for_testing_logging), strict=False)

        # Assert no test table
        assert len(sql.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name='dbo' and t.name = '"{}"'
        """.format(table_for_testing_logging))) == 0

        # Assert doesn't exist in log either
        assert len(sql.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{}__ WHERE table_name='"{}"'
        """.format(sql.user, table_for_testing_logging))) == 0

        # Add test table in brackets, quotes, and caps
        sql.query("""
        create table dbo.["{}"](test_col1 int, test_col2 int);
        insert into dbo.["{}"] VALUES(1, 2);
        insert into dbo.["{}"] VALUES(3, 4);
        """.format(table_for_testing_logging.capitalize(),
                   table_for_testing_logging.capitalize(),
                   table_for_testing_logging.capitalize()))

        # Assert sql stores in quotes, not capitalized
        assert len(sql.dfquery("""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
        ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' and t.name = '"{}"'
        """.format(table_for_testing_logging))) == 1

        # Assert log stores in quotes, not capitalized
        assert len(sql.dfquery("""
        SELECT * FROM dbo.__temp_log_table_{}__ WHERE table_name='"{}"'
        """.format(sql.user, table_for_testing_logging))) == 1

        # Cleanup
        sql.query('DROP TABLE dbo.["{}"]'.format(table_for_testing_logging), strict=False)

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(db)

class TestInheritence:
    def test_pg_connect_all_but_db(self):
        second_db = test_config.get('SECOND_PG_DB', 'DB_NAME')
        db2 = pysqldb.DbConnect(inherits_from=db, database=second_db)
        assert db2.database==second_db
        assert db.database != second_db
    def test_pg_connect_all_but_user(self):
        # cant really test, user input requirement doesnt work in pytest
        # db2 = pysqldb.DbConnect(inherits_from=db, database='ris_dev', user='risadmin')
        # assert db2.database=='ris_dev'
        # assert db2.user=='risadmin'
        # assert db.database != 'ris_dev'
        # assert db.user !='risadmin'

        assert True

    def test_pg_connect_all_but_type(self):
        # this connection would fail
        # third_pwd = test_config.get('THIRD_SQL_DB', 'DB_PASSWORD')
        # db3 = pysqldb.DbConnect(inherits_from=db, password=third_pwd)
        # assert db3.database==db.database

        assert True

    def test_pg_connect_all_but_pwd(self):
        # this connection would fail
        # third_type = test_config.get('THIRD_SQL_DB', 'TYPE')
        # db3 = pysqldb.DbConnect(inherits_from=db, type=third_pwd)
        # assert db3.database==db.database

        assert True

    def test_sql_connect_all_but_user_pwd(self):
        second_user = test_config.get('SECOND_SQL_DB', 'DB_USER')
        second_pwd = test_config.get('SECOND_SQL_DB', 'DB_PASSWORD')
        db2 = pysqldb.DbConnect(inherits_from=sql, user=second_user, password=second_pwd)
        assert db2.database==sql.database
        db2.user!=sql.user


