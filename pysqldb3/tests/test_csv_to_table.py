import os

import configparser
import pandas as pd

from .. import pysqldb3 as pysqldb
from . import helpers

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

pg_dbconn = pysqldb.DbConnect(db_type=config.get('PG_DB', 'TYPE'),
                              host=config.get('PG_DB', 'SERVER'),
                              db_name=config.get('PG_DB', 'DB_NAME'),
                              username=config.get('PG_DB', 'DB_USER'),
                              password=config.get('PG_DB', 'DB_PASSWORD'))

sql = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                        host=config.get('SQL_DB', 'SERVER'),
                        db_name=config.get('SQL_DB', 'DB_NAME'),
                        username=config.get('SQL_DB', 'DB_USER'),
                        password=config.get('SQL_DB', 'DB_PASSWORD'))

pg_table_name = 'pg_test_table_{}'.format(pg_dbconn.username)
create_table_name = 'sample_acs_test_csv_to_table_{}'.format(pg_dbconn.username)


class TestCsvToTablePG:
    @classmethod
    def setup_class(cls):
        # helpers.set_up_test_table_pg(db)
        helpers.set_up_test_csv()

    def test_csv_to_table_basic(self):
        # csv_to_table
        pg_dbconn.query('drop table if exists working.{table}'.format(table=create_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test.csv"
        pg_dbconn.csv_to_table(input_file=fp, table_name=create_table_name, schema_name='working')

        # Check to see if table is in database
        assert pg_dbconn.table_exists(table_name=create_table_name, schema_name='working')
        db_df = pg_dbconn.dfquery("select * from working.{table}".format(table=create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        pg_dbconn.drop_table(schema_name='working', table_name=create_table_name)

    def test_csv_to_table_column_override(self):
        pg_dbconn.drop_table(schema_name='working', table_name=create_table_name)
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\csv_override_ex.csv"
        test_df = pd.DataFrame([{'a': 1, 'b': 2, 'c': 3, 'd': 'text'}, {'a': 4, 'b': 5, 'c': 6, 'd': 'another'}])
        test_df.to_csv(fp)
        pg_dbconn.csv_to_table(input_file=fp, table_name=create_table_name, schema_name='working',
                               column_type_overrides={'a': 'varchar'})

        # Check to see if table is in database
        assert pg_dbconn.table_exists(table_name=create_table_name, schema_name='working')
        db_df = pg_dbconn.dfquery("select * from working.{table}".format(table=create_table_name))

        # Modify to make column types altered
        altered_column_type_df = pd.DataFrame([{'a': '1.0', 'b': 2, 'c': 3, 'd': 'text'},
                                               {'a': '4.0', 'b': 5, 'c': 6, 'd': 'another'}])

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df[['a', 'b', 'c', 'd']], altered_column_type_df)

        # Assert df column types match override
        pd.testing.assert_frame_equal(pd.DataFrame([{"column_name": 'a', "data_type": 'character varying'},
                                                    {"column_name": 'b', "data_type": 'bigint'},
                                                    {"column_name": 'c', "data_type": 'bigint'},
                                                    {"column_name": 'd', "data_type": 'character varying'}]),
                                      pg_dbconn.dfquery("""
                                            select distinct column_name, data_type
                                            from information_schema.columns
                                            where table_name = '{}' and lower(column_name) not like '%unnamed%';
                                      """.format(create_table_name)))

        # Cleanup
        pg_dbconn.drop_table(schema_name='working', table_name=create_table_name)
        os.remove(fp)

    def test_csv_to_table_separator(self):
        # csv_to_table
        pg_dbconn.query('drop table if exists working.{table}'.format(table=create_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test2.csv"
        pg_dbconn.csv_to_table(
            input_file=fp,
            table_name=create_table_name, schema_name='working',
            sep="|"
        )

        # Check to see if table is in database
        assert pg_dbconn.table_exists(table_name=create_table_name, schema_name='working')
        db_df = pg_dbconn.dfquery("select * from working.{table}".format(table=create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp, sep='|')
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        pg_dbconn.drop_table(schema_name='working', table_name=create_table_name)

    # what if the table has no header?
    def test_csv_to_table_no_header(self):
        # csv_to_table
        pg_dbconn.query('drop table if exists working.{table}'.format(table=create_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test3.csv"
        pg_dbconn.csv_to_table(input_file=fp, table_name=create_table_name, schema_name='working', sep="|")

        # did it enter the db correctly?
        assert pg_dbconn.table_exists(table_name=create_table_name, schema_name='working')
        db_df = pg_dbconn.dfquery("select * from working.{table}".format(table=create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp, sep='|')
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference
        csv_df.columns = [c.replace('.', '') for c in list(csv_df.columns)]
        csv_df.columns = [c.lower() for c in list(csv_df.columns)]

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        pg_dbconn.drop_table(schema_name='working', table_name=create_table_name)

    def test_csv_to_table_overwrite(self):
        # Make a table to fill the eventual table location and confirm it exists
        pg_dbconn.query('create table working.{table} as select 10'.format(table=create_table_name))
        assert pg_dbconn.table_exists(table_name=create_table_name, schema_name='working')

        # csv_to_table
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test.csv"
        pg_dbconn.csv_to_table(input_file=fp, table_name=create_table_name, schema_name='working', overwrite=True)

        # Check to see if table is in database
        assert pg_dbconn.table_exists(table_name=create_table_name, schema_name='working')
        db_df = pg_dbconn.dfquery("select * from working.{table}".format(table=create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        pg_dbconn.drop_table(schema_name='working', table_name=create_table_name)

    def test_csv_to_table_long_column(self):
        # csv_to_table
        pg_dbconn.query('drop table if exists working.{table}'.format(table=create_table_name))

        base_string = 'text'*150
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\varchar.csv"
        pg_dbconn.dfquery("select '{bs}' as long_col".format(bs=base_string)).to_csv(fp, index=False)
        pg_dbconn.csv_to_table(input_file=fp, table_name=create_table_name, schema_name='working', long_varchar_check=True)

        # Check to see if table is in database
        assert pg_dbconn.table_exists(table_name=create_table_name, schema_name='working')
        db_df = pg_dbconn.dfquery("select * from working.{table}".format(table=create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Confirm long columns are not truncated and are equal with table from long_varchar_check=True
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        pg_dbconn.drop_table(schema_name='working', table_name=create_table_name)

    # Temp test is in logging tests

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(pg_dbconn)
        pg_dbconn.cleanup_new_tables()


class TestBulkCSVToTablePG:
    @classmethod
    def setup_class(cls):
        return

    def test_bulk_csv_to_table_basic(self):
        # bulk_csv_to_table
        pg_dbconn.query('drop table if exists working.{table}'.format(table=create_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test.csv"
        input_schema = pg_dbconn.dataframe_to_table_schema(df=pd.read_csv(fp), table_name=create_table_name, schema_name='working')
        pg_dbconn._bulk_csv_to_table(input_file=fp, table_name=create_table_name, schema_name='working', table_schema=input_schema)

        # Check to see if table is in database
        assert pg_dbconn.table_exists(table_name=create_table_name, schema_name='working')
        db_df = pg_dbconn.dfquery("select * from working.{table}".format(table=create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        pg_dbconn.drop_table(schema_name='working', table_name=create_table_name)

    def test_bulk_csv_to_table_default_schema(self):
        # bulk_csv_to_table
        pg_dbconn.query('drop table if exists {table}'.format(table=create_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test.csv"
        input_schema = pg_dbconn.dataframe_to_table_schema(df=pd.read_csv(fp), table_name=create_table_name)
        pg_dbconn._bulk_csv_to_table(input_file=fp, table_name=create_table_name, table_schema=input_schema)

        # Check to see if table is in database
        assert pg_dbconn.table_exists(table_name=create_table_name)
        db_df = pg_dbconn.dfquery("select * from {}".format(create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        pg_dbconn.drop_table(schema_name=pg_dbconn.default_schema, table_name=create_table_name)

    def test_bulk_csv_to_table_long_column(self):
        # csv_to_table
        if sql.table_exists(schema_name='dbo', table_name=create_table_name):
            sql.drop_table(schema_name='dbo', table_name=create_table_name)

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\varchar.csv"
        pd.DataFrame(['text'*150]*10000, columns=['long_column']).to_csv(fp)
        sql.csv_to_table(input_file=fp, table_name=create_table_name, schema_name='dbo', long_varchar_check=True)

        # Check to see if table is in database
        assert sql.table_exists(table_name=create_table_name, schema_name='dbo')
        sql_df = sql.dfquery("select * from dbo.{table}".format(table=create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Confirm long columns are not truncated and are equal with table from long_varchar_check=True
        pd.testing.assert_frame_equal(sql_df[['long_column']], csv_df[['long_column']])

        # Cleanup
        sql.drop_table(schema_name='dbo', table_name=create_table_name)

    def test_bulk_csv_to_table_input_schema(self):
        # Test input schema
        return

    # Temp test is in logging tests

    @classmethod
    def teardown_class(cls):
        pg_dbconn.cleanup_new_tables()


class TestCsvToTableMS:
    @classmethod
    def setup_class(cls):
        return

    def test_csv_to_table_basic(self):
        # csv_to_table
        if sql.table_exists(schema_name='dbo', table_name=create_table_name):
            sql.query('drop table dbo.{table}'.format(table=create_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test.csv"
        sql.csv_to_table(input_file=fp, table_name=create_table_name, schema_name='dbo')

        # Check to see if table is in database
        assert sql.table_exists(table_name=create_table_name, schema_name='dbo')
        sql_df = sql.dfquery("select * from dbo.{table}".format(table=create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df, csv_df)

        # Cleanup
        sql.drop_table(schema_name='dbo', table_name=create_table_name)

    def test_csv_to_table_column_override(self):
        sql.drop_table(schema_name='dbo', table_name=create_table_name)

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\csv_override_ex.csv"
        test_df = pd.DataFrame([{'a': 1, 'b': 2, 'c': 3, 'd': 'text'}, {'a': 4, 'b': 5, 'c': 6, 'd': 'another'}])
        test_df.to_csv(fp)
        sql.csv_to_table(input_file=fp, table_name=create_table_name, schema_name='dbo',
                         column_type_overrides={'a': 'numeric'})

        # Check to see if table is in database
        assert sql.table_exists(table_name=create_table_name, schema_name='dbo')

        # Assert df column types match override
        pd.testing.assert_frame_equal(pd.DataFrame(
            [{"column_name": 'a', "data_type": 'numeric'}, {"column_name": 'b', "data_type": 'bigint'},
             {"column_name": 'c', "data_type": 'bigint'}, {"column_name": 'd', "data_type": 'varchar'}]),
                                      sql.dfquery("""
                                            select distinct column_name, data_type
                                            from information_schema.columns
                                            where table_name = '{table}' and lower(column_name) not like '%unnamed%';
                                      """.format(table=create_table_name))
                                      )

        # Cleanup
        sql.drop_table(schema_name='dbo', table_name=create_table_name)
        os.remove(fp)

    def test_csv_to_table_separator(self):
        # csv_to_table
        if sql.table_exists(schema_name='dbo', table_name=create_table_name):
            sql.query('drop table dbo.{table}'.format(table=create_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test2.csv"
        sql.csv_to_table(
            input_file=fp,
            table_name=create_table_name, schema_name='dbo',
            sep="|"
        )

        # Check to see if table is in database
        assert sql.table_exists(table_name=create_table_name, schema_name='dbo')
        sql_df = sql.dfquery("select * from dbo.{table}".format(table=create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp, sep='|')
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df, csv_df)

        # Cleanup
        sql.drop_table(schema_name='dbo', table_name=create_table_name)

    # what if the table has no header?
    def test_csv_to_table_no_header(self):
        # csv_to_table
        if sql.table_exists(schema_name='dbo', table_name=create_table_name):
            sql.query('drop table dbo.{table}'.format(table=create_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test3.csv"
        sql.csv_to_table(input_file=fp, table_name=create_table_name, schema_name='dbo', sep="|")

        # did it enter the db correctly?
        assert sql.table_exists(table_name=create_table_name, schema_name='dbo')
        sql_df = sql.dfquery("select * from dbo.{table}".format(table=create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp, sep='|')
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference
        csv_df.columns = [c.replace('.', '') for c in list(csv_df.columns)]
        csv_df.columns = [c.lower() for c in list(csv_df.columns)]

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df, csv_df)

        # Cleanup
        sql.drop_table(schema_name='dbo', table_name=create_table_name)

    def test_csv_to_table_overwrite(self):
        if sql.table_exists(schema_name='dbo', table_name=create_table_name):
            sql.query('drop table dbo.{table}'.format(table=create_table_name))

        # Make a table to fill the eventual table location and confirm it exists
        # Add test_table
        sql.query("""
        create table dbo.{table} (test_col1 int, test_col2 int, geom geometry);
        insert into dbo.{table} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
        insert into dbo.{table} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
        """.format(table=create_table_name))
        assert sql.table_exists(table_name=create_table_name, schema_name='dbo')

        # csv_to_table
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test.csv"
        sql.csv_to_table(input_file=fp, table_name=create_table_name, schema_name='dbo', overwrite=True)

        # Check to see if table is in database
        assert sql.table_exists(table_name=create_table_name, schema_name='dbo')
        sql_df = sql.dfquery("select * from dbo.{table}".format(table=create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference
        csv_df.columns = [c.replace('.', '') for c in list(csv_df.columns)]
        csv_df.columns = [c.lower() for c in list(csv_df.columns)]

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df, csv_df)

        # Cleanup
        sql.drop_table(schema_name='dbo', table_name=create_table_name)

    # Temp test is in logging tests

    def test_csv_to_table_long_column(self):
        # csv_to_table
        if sql.table_exists(schema_name='dbo', table_name=create_table_name):
            sql.drop_table(schema_name='dbo', table_name=create_table_name)

        base_string = 'text'*150
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\varchar.csv"
        sql.dfquery("select '{bs}' as long_col".format(bs=base_string)).to_csv(fp, index=False)
        sql.csv_to_table(input_file=fp, table_name=create_table_name, schema_name='dbo', long_varchar_check=True)

        # Check to see if table is in database
        assert sql.table_exists(table_name=create_table_name, schema_name='dbo')
        sql_df = sql.dfquery("select * from dbo.{table}".format(table=create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Confirm long columns are not truncated and are equal with table from long_varchar_check=True
        pd.testing.assert_frame_equal(sql_df, csv_df)

        # Cleanup
        sql.drop_table(schema_name='dbo', table_name=create_table_name)

    @classmethod
    def teardown_class(cls):
        sql.cleanup_new_tables()


class TestBulkCSVToTableMS:
    @classmethod
    def setup_class(cls):
        return

    def test_bulk_csv_to_table_basic(self):
        # bulk_csv_to_table
        if sql.drop_table(schema_name='dbo', table_name=create_table_name):
            sql.query('drop table dbo.{table}'.format(table=create_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test.csv"
        input_schema = sql.dataframe_to_table_schema(df=pd.read_csv(fp), table_name=create_table_name, schema_name='dbo')
        sql._bulk_csv_to_table(input_file=fp, table_name=create_table_name, schema_name='dbo', table_schema=input_schema)

        # Check to see if table is in database
        assert sql.table_exists(table_name=create_table_name, schema_name='dbo')
        sql_df = sql.dfquery("select * from dbo.{table}".format(table=create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df, csv_df)

        # Cleanup
        sql.drop_table(schema_name='dbo', table_name=create_table_name)

    def test_bulk_csv_to_table_default_schema(self):
        # bulk_csv_to_table
        if sql.table_exists(table_name=create_table_name):
            sql.query('drop table {table}'.format(table=create_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test.csv"
        input_schema = sql.dataframe_to_table_schema(df=pd.read_csv(fp), table_name=create_table_name)
        sql._bulk_csv_to_table(input_file=fp, table_name=create_table_name, table_schema=input_schema)

        # Check to see if table is in database
        # This example is linked to the mssql default server bug
        assert sql.table_exists(table_name=create_table_name)
        sql_df = sql.dfquery("select * from {table}".format(table=create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df, csv_df)

        # Cleanup
        sql.drop_table(schema_name=sql.default_schema, table_name=create_table_name)

    def test_bulk_csv_to_table_long_column(self):
        # csv_to_table
        if sql.table_exists(schema_name='dbo', table_name=create_table_name):
            sql.drop_table(schema_name='dbo', table_name=create_table_name)

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\varchar.csv"
        pd.DataFrame(['text'*150]*10000, columns=['long_column']).to_csv(fp)
        sql.csv_to_table(input_file=fp, table_name=create_table_name, schema_name='dbo', long_varchar_check=True)

        # Check to see if table is in database
        assert sql.table_exists(table_name=create_table_name, schema_name='dbo')
        sql_df = sql.dfquery("select * from dbo.{table}".format(table=create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Confirm long columns are not truncated and are equal with table from long_varchar_check=True
        pd.testing.assert_frame_equal(sql_df[['long_column']], csv_df[['long_column']])

        # Cleanup
        sql.drop_table(schema_name='dbo', table_name=create_table_name)

    def test_bulk_csv_to_table_input_schema(self):
        # Test input schema
        return

    # Temp test is in logging tests

    @classmethod
    def teardown_class(cls):
        sql.cleanup_new_tables()