# -*- coding: utf-8 -*-

import csv
import datetime
import os

import configparser
import pandas as pd

from .. import pysqldb3 as pysqldb

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

pg_dbconn = pysqldb.DbConnect(db_type=config.get('PG_DB', 'TYPE'),
                              host=config.get('PG_DB', 'SERVER'),
                              db_name=config.get('PG_DB', 'DB_NAME'),
                              username=config.get('PG_DB', 'DB_USER'),
                              password=config.get('PG_DB', 'DB_PASSWORD'),
                              allow_temp_tables=True)

ris_dbconn = pysqldb.DbConnect(db_type=config.get('SECOND_PG_DB', 'TYPE'),
                               host=config.get('SECOND_PG_DB', 'SERVER'),
                               db_name=config.get('SECOND_PG_DB', 'DB_NAME'),
                               username=config.get('SECOND_PG_DB', 'DB_USER'),
                               password=config.get('SECOND_PG_DB', 'DB_PASSWORD'),
                               allow_temp_tables=True)

ms_dbconn = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                              host=config.get('SQL_DB', 'SERVER'),
                              db_name=config.get('SQL_DB', 'DB_NAME'),
                              username=config.get('SQL_DB', 'DB_USER'),
                              password=config.get('SQL_DB', 'DB_PASSWORD'),
                              allow_temp_tables=True)

test_csv_name = 'test_csv_name_table_{user}'.format(user=pg_dbconn.username)


class TestQueryToCSVPG:
    def test_query_to_csv_basic(self):
        output = os.path.dirname(os.path.abspath(__file__)) + 'test_query_to_csv.csv'
        pg_dbconn.drop_table(schema_name='working', table_name=test_csv_name)

        # Setup table
        pg_dbconn.query("""
            create table working.{table} (col1 varchar, col2 varchar, col3 varchar);
            insert into working.{table} values ('a', 'b', 'c');
        """.format(table=test_csv_name))

        # Query_to_csv
        pg_dbconn.query_to_csv(query='select * from working.{table}'.format(table=test_csv_name),
                               output_file=output)

        # Get result from query_to_csv as df
        result_df = pd.read_csv(output)

        # Get result from dfquery
        query_df = pg_dbconn.dfquery('select * from working.{table}'.format(table=test_csv_name))

        # Assert equality
        pd.testing.assert_frame_equal(result_df, query_df)

        # Cleanup
        pg_dbconn.drop_table(schema_name='working', table_name=test_csv_name)
        os.remove(output)

    def test_query_to_csv_special_char(self):
        output = os.path.dirname(os.path.abspath(__file__)) + 'test_query_to_csv.csv'
        pg_dbconn.drop_table(schema_name='working', table_name=test_csv_name)

        # Setup table
        pg_dbconn.query("""
            create table working.{table} (col1 varchar, col2 varchar, col3 varchar);

            insert into working.{table} values ('á', 'b', 'c');
        """.format(table=test_csv_name))

        # Query_to_csv
        pg_dbconn.query_to_csv(query='select * from working.{table}'.format(table=test_csv_name),
                               output_file=output)

    def test_query_to_csv_query_first(self):
        """
        Testing a DB connection issue where it wasn't possible to query and then query_to_csv without a connection
        closed issue.
        """
        db2 = pysqldb.DbConnect(db_type=config.get('PG_DB', 'TYPE'),
                                host=config.get('PG_DB', 'SERVER'),
                                db_name=config.get('PG_DB', 'DB_NAME'),
                                username=config.get('PG_DB', 'DB_USER'),
                                password=config.get('PG_DB', 'DB_PASSWORD'))

        output = os.path.dirname(os.path.abspath(__file__)) + 'test_query_to_csv.csv'
        db2.drop_table(schema_name='working', table_name=test_csv_name)

        # Setup table
        db2.query("""
            create table working.{table} (col1 varchar, col2 varchar, col3 varchar);
            insert into working.{table} values ('a', 'b', 'c');
        """.format(table=test_csv_name))

        # Random query
        db2.dfquery("select * from working.{table}".format(table=test_csv_name))

        # Query_to_csv
        db2.query_to_csv(query='select * from working.{table}'.format(table=test_csv_name),
                         output_file=output)

        # Get result from query_to_csv as df
        result_df = pd.read_csv(output)

        # Get result from dfquery
        query_df = db2.dfquery('select * from working.{table}'.format(table=test_csv_name))

        # Assert equality
        pd.testing.assert_frame_equal(result_df, query_df)

        # Cleanup
        db2.drop_table(schema_name='working', table_name=test_csv_name)

        os.remove(output)

    def test_query_to_csv_no_output(self):
        pg_dbconn.drop_table(schema_name='working', table_name=test_csv_name)

        # Setup table
        pg_dbconn.query("""
            create table working.{table} (col1 varchar, col2 varchar, col3 varchar);

            insert into working.{table} values ('a', 'b', 'c');
        """.format(table=test_csv_name))

        # Query_to_csv
        pg_dbconn.query_to_csv(query='select * from working.{table}'.format(table=test_csv_name))

        # Get result from query_to_csv as df
        output = os.path.join(os.getcwd(), 'data_{dt}.csv'.format(dt=datetime.datetime.now().strftime('%Y%m%d%H%M')))
        result_df = pd.read_csv(output)

        # Get result from dfquery
        query_df = pg_dbconn.dfquery('select * from working.{table}'.format(table=test_csv_name))

        # Assert equality
        pd.testing.assert_frame_equal(result_df, query_df)

        # Cleanup
        pg_dbconn.drop_table(schema_name='working', table_name=test_csv_name)
        os.remove(output)

    def test_query_to_csv_strict(self):
        try:
            pg_dbconn.query_to_csv(query='select * from junktable', strict=True)
        except SystemExit:
            return

        """
        Must fail with SystemExit and return in the except clause 
        """
        assert False

    def test_query_to_csv_sep(self):
        output = os.path.dirname(os.path.abspath(__file__)) + 'test_query_to_csv.csv'
        pg_dbconn.drop_table(schema_name='working', table_name=test_csv_name)

        # Setup table
        pg_dbconn.query("""
            create table working.{table} (col1 varchar, col2 varchar, col3 varchar);
            
            insert into working.{table} values ('a', 'b', 'c');
        """.format(table=test_csv_name))

        # Query_to_csv
        pg_dbconn.query_to_csv(query='select * from working.{table}'.format(table=test_csv_name),
                               output_file=output,
                               sep=';')

        # Get result from query_to_csv as df
        result_df = pd.read_csv(output, sep=';')

        # Get result from dfquery
        query_df = pg_dbconn.dfquery('select * from working.{table}'.format(table=test_csv_name))

        # Assert equality
        pd.testing.assert_frame_equal(result_df, query_df)

        # Cleanup
        pg_dbconn.drop_table(schema_name='working', table_name=test_csv_name)
        os.remove(output)

    def test_query_to_csv_quote_strings(self):
        output = os.path.dirname(os.path.abspath(__file__)) + 'test_query_to_csv.csv'
        pg_dbconn.drop_table(schema_name='working', table_name=test_csv_name)

        # Setup table
        pg_dbconn.query("""
            create table working.{table} (col1 varchar, col2 varchar, col3 varchar);

            insert into working.{table} values ('a,', 'b', 'c');
        """.format(table=test_csv_name))

        # Query_to_csv
        pg_dbconn.query_to_csv(query='select * from working.{table}'.format(table=test_csv_name),
                               output_file=output,
                               quote_strings=False)

        # Get result from query_to_csv as df
        result_df = pd.read_csv(output)

        # Get result from dfquery
        query_df = pg_dbconn.dfquery('select * from working.{table}'.format(table=test_csv_name))

        # Assert equality
        pd.testing.assert_frame_equal(result_df, query_df)

        # Assert the first column contains quotes as it was quoted in QUOTE_MINIMAL due to the comma, while the others do not raw_csv_with_quotes
        raw_csv_with_quotes = pd.read_csv(output, quoting=csv.QUOTE_NONE)
        assert '"' in raw_csv_with_quotes.iloc[0]["col1"]
        assert '"' not in raw_csv_with_quotes.iloc[0]["col2"]
        assert '"' not in raw_csv_with_quotes.iloc[0]["col3"]

        # Cleanup
        pg_dbconn.drop_table(schema_name='working', table_name=test_csv_name)
        os.remove(output)


class TestQueryToCSVMS:
    def test_query_to_csv_basic(self):
        output = os.path.dirname(os.path.abspath(__file__)) + 'test_query_to_csv.csv'
        ms_dbconn.drop_table(schema_name='dbo', table_name=test_csv_name)

        # Setup table
        ms_dbconn.query("""
            create table dbo.{table} (col1 varchar, col2 varchar, col3 varchar);

            insert into dbo.{table} values ('a', 'b', 'c');
        """.format(table=test_csv_name))

        # Query_to_csv
        ms_dbconn.query_to_csv(query='select * from dbo.{table}'.format(table=test_csv_name),
                               output_file=output)

        # Get result from query_to_csv as df
        result_df = pd.read_csv(output)

        # Get result from dfquery
        query_df = ms_dbconn.dfquery('select * from dbo.{table}'.format(table=test_csv_name))

        # Assert equality
        pd.testing.assert_frame_equal(result_df, query_df)

        # Cleanup
        ms_dbconn.drop_table(schema_name='dbo', table_name=test_csv_name)
        os.remove(output)

    def test_query_to_csv_no_output(self):
        ms_dbconn.drop_table(schema_name='dbo', table_name=test_csv_name)

        # Setup table
        ms_dbconn.query("""
            create table dbo.{table} (col1 varchar, col2 varchar, col3 varchar);

            insert into dbo.{table} values ('a', 'b', 'c');
        """.format(table=test_csv_name))

        # Query_to_csv
        ms_dbconn.query_to_csv(query='select * from dbo.{table}'.format(table=test_csv_name))

        # Get result from query_to_csv as df
        output = os.path.join(os.getcwd(), 'data_{dt}.csv'.format(dt=datetime.datetime.now().strftime('%Y%m%d%H%M')))
        result_df = pd.read_csv(output)

        # Get result from dfquery
        query_df = ms_dbconn.dfquery('select * from dbo.{table}'.format(table=test_csv_name))

        # Assert equality
        pd.testing.assert_frame_equal(result_df, query_df)

        # Cleanup
        ms_dbconn.drop_table(schema_name='dbo', table_name=test_csv_name)
        os.remove(output)

    def test_query_to_csv_strict(self):
        try:
            ms_dbconn.query_to_csv(query='select * from junktable', strict=True)
        except SystemExit:
            return

        """
        Must fail with SystemExit and return in the except clause 
        """
        assert False

    def test_query_to_csv_sep(self):
        output = os.path.dirname(os.path.abspath(__file__)) + 'test_query_to_csv.csv'
        ms_dbconn.drop_table(schema_name='dbo', table_name=test_csv_name)

        # Setup table
        ms_dbconn.query("""
            create table dbo.{table} (col1 varchar, col2 varchar, col3 varchar);

            insert into dbo.{table} values ('a', 'b', 'c');
        """.format(table=test_csv_name))

        # Query_to_csv
        ms_dbconn.query_to_csv(query='select * from dbo.{table}'.format(table=test_csv_name),
                               output_file=output,
                               sep=';')

        # Get result from query_to_csv as df
        result_df = pd.read_csv(output, sep=';')

        # Get result from dfquery
        query_df = ms_dbconn.dfquery('select * from dbo.{table}'.format(table=test_csv_name))

        # Assert equality
        pd.testing.assert_frame_equal(result_df, query_df)

        # Cleanup
        ms_dbconn.drop_table(schema_name='dbo', table_name=test_csv_name)
        os.remove(output)

    def test_query_to_csv_quote_strings(self):
        output = os.path.dirname(os.path.abspath(__file__)) + 'test_query_to_csv.csv'
        ms_dbconn.drop_table(schema_name='dbo', table_name=test_csv_name)

        # Setup table
        ms_dbconn.query("""
            create table dbo.{table} (col1 text, col2 varchar, col3 varchar);

            insert into dbo.{table} values ('a, ', 'b', 'c');
        """.format(table=test_csv_name))

        # Query_to_csv
        ms_dbconn.query_to_csv(query='select * from dbo.{table}'.format(table=test_csv_name),
                               output_file=output,
                               quote_strings=False)

        # Get result from query_to_csv as df
        result_df = pd.read_csv(output)

        # Get result from dfquery
        query_df = ms_dbconn.dfquery('select * from dbo.{table}'.format(table=test_csv_name))

        # Assert equality
        pd.testing.assert_frame_equal(result_df, query_df)

        # Assert the first column contains quotes as it was quoted in QUOTE_MINIMAL due to the comma, while the others do not raw_csv_with_quotes
        raw_csv_with_quotes = pd.read_csv(output, quoting=csv.QUOTE_NONE)
        assert '"' in raw_csv_with_quotes.iloc[0]["col1"]
        assert '"' not in raw_csv_with_quotes.iloc[0]["col2"]
        assert '"' not in raw_csv_with_quotes.iloc[0]["col3"]

        # Cleanup
        ms_dbconn.drop_table(schema_name='dbo', table_name=test_csv_name)
        os.remove(output)
