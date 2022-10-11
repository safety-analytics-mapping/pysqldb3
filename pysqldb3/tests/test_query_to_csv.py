# -*- coding: utf-8 -*-

import csv
import datetime
import os
from datetime import datetime
import configparser
import pandas as pd

from .. import pysqldb3 as pysqldb

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                       server=config.get('PG_DB', 'SERVER'),
                       database=config.get('PG_DB', 'DB_NAME'),
                       user=config.get('PG_DB', 'DB_USER'),
                       password=config.get('PG_DB', 'DB_PASSWORD'),
                       allow_temp_tables=True)

ris_db = pysqldb.DbConnect(type=config.get('SECOND_PG_DB', 'TYPE'),
                           server=config.get('SECOND_PG_DB', 'SERVER'),
                           database=config.get('SECOND_PG_DB', 'DB_NAME'),
                           user=config.get('SECOND_PG_DB', 'DB_USER'),
                           password=config.get('SECOND_PG_DB', 'DB_PASSWORD'),
                           allow_temp_tables=True)

sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                        server=config.get('SQL_DB', 'SERVER'),
                        database=config.get('SQL_DB', 'DB_NAME'),
                        user=config.get('SQL_DB', 'DB_USER'),
                        password=config.get('SQL_DB', 'DB_PASSWORD'),
                        allow_temp_tables=True)

test_csv_name = f'test_csv_name_table_{db.user}'


class TestQueryToCSVPG:
    def test_query_to_csv_basic(self):
        output = os.path.dirname(os.path.abspath(__file__)) + 'test_query_to_csv.csv'
        db.drop_table(schema='working', table=test_csv_name)

        # Setup table
        db.query(f"""
            create table working.{test_csv_name} (col1 varchar, col2 varchar, col3 varchar);
            insert into working.{test_csv_name} values ('a', 'b', 'c');
        """)

        # Query_to_csv
        db.query_to_csv(query=f'select * from working.{test_csv_name}', output_file=output)

        # Get result from query_to_csv as df
        result_df = pd.read_csv(output)

        # Get result from dfquery
        query_df = db.dfquery(f'select * from working.{test_csv_name}')

        # Assert equality
        pd.testing.assert_frame_equal(result_df, query_df)

        # Cleanup
        db.drop_table(schema='working', table=test_csv_name)
        os.remove(output)

    def test_query_to_csv_special_char(self):
        output = os.path.dirname(os.path.abspath(__file__)) + 'test_query_to_csv.csv'
        db.drop_table(schema='working', table=test_csv_name)

        # Setup table
        db.query(f"""
            create table working.{test_csv_name} (col1 varchar, col2 varchar, col3 varchar);
            insert into working.{test_csv_name} values ('á', 'b', 'c');
        """)

        # Query_to_csv
        db.query_to_csv(query='select * from working.{test_csv_name}', output_file=output)

    def test_query_to_csv_query_first(self):
        """
        Testing a DB connection issue where it wasn't possible to query and then query_to_csv without a connection
        closed issue.
        """
        db2 = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                                server=config.get('PG_DB', 'SERVER'),
                                database=config.get('PG_DB', 'DB_NAME'),
                                user=config.get('PG_DB', 'DB_USER'),
                                password=config.get('PG_DB', 'DB_PASSWORD'))

        output = os.path.dirname(os.path.abspath(__file__)) + 'test_query_to_csv.csv'
        db2.drop_table(schema='working', table=test_csv_name)

        # Setup table
        db2.query(f"""
            create table working.{test_csv_name} (col1 varchar, col2 varchar, col3 varchar);
            insert into working.{test_csv_name} values ('a', 'b', 'c');
        """)

        # Random query
        db2.dfquery(f"select * from working.{test_csv_name}")

        # Query_to_csv
        db2.query_to_csv(query=f'select * from working.{test_csv_name}', output_file=output)

        # Get result from query_to_csv as df
        result_df = pd.read_csv(output)

        # Get result from dfquery
        query_df = db2.dfquery(f'select * from working.{test_csv_name}')

        # Assert equality
        pd.testing.assert_frame_equal(result_df, query_df)

        # Cleanup
        db2.drop_table(schema='working', table=test_csv_name)

        os.remove(output)

    def test_query_to_csv_no_output(self):
        db.drop_table(schema='working', table=test_csv_name)

        # Setup table
        db.query(f"""
            create table working.{test_csv_name} (col1 varchar, col2 varchar, col3 varchar);
            insert into working.{test_csv_name} values ('a', 'b', 'c');
        """)

        # Query_to_csv
        db.query_to_csv(query=f'select * from working.{test_csv_name}')

        # Get result from query_to_csv as df
        output = os.path.join(os.getcwd(), f"data_{datetime.now().strftime('%Y%m%d%H%M')}.csv")
        result_df = pd.read_csv(output)

        # Get result from dfquery
        query_df = db.dfquery(f'select * from working.{test_csv_name}')

        # Assert equality
        pd.testing.assert_frame_equal(result_df, query_df)

        # Cleanup
        db.drop_table(schema='working', table=test_csv_name)
        os.remove(output)

    def test_query_to_csv_strict(self):
        try:
            db.query_to_csv(query='select * from junktable', strict=True)
        except SystemExit:
            return

        """
        Must fail with SystemExit and return in the except clause 
        """
        assert False

    def test_query_to_csv_sep(self):
        output = os.path.dirname(os.path.abspath(__file__)) + 'test_query_to_csv.csv'
        db.drop_table(schema='working', table=test_csv_name)

        # Setup table
        db.query(f"""
            create table working.{test_csv_name} (col1 varchar, col2 varchar, col3 varchar);
            insert into working.{test_csv_name} values ('a', 'b', 'c');
        """)

        # Query_to_csv
        db.query_to_csv(query=f'select * from working.{test_csv_name}', output_file=output, sep=';')

        # Get result from query_to_csv as df
        result_df = pd.read_csv(output, sep=';')

        # Get result from dfquery
        query_df = db.dfquery(f'select * from working.{test_csv_name}')

        # Assert equality
        pd.testing.assert_frame_equal(result_df, query_df)

        # Cleanup
        db.drop_table(schema='working', table=test_csv_name)
        os.remove(output)

    def test_query_to_csv_quote_strings(self):
        output = os.path.dirname(os.path.abspath(__file__)) + 'test_query_to_csv.csv'
        db.drop_table(schema='working', table=test_csv_name)

        # Setup table
        db.query(f"""
            create table working.{test_csv_name} (col1 varchar, col2 varchar, col3 varchar);
            insert into working.{test_csv_name} values ('a,', 'b', 'c');
        """)

        # Query_to_csv
        db.query_to_csv(query=f'select * from working.{test_csv_name}', output_file=output, quote_strings=False)

        # Get result from query_to_csv as df
        result_df = pd.read_csv(output)

        # Get result from dfquery
        query_df = db.dfquery(f'select * from working.{test_csv_name}')

        # Assert equality
        pd.testing.assert_frame_equal(result_df, query_df)

        # Assert the first column contains quotes as it was quoted in QUOTE_MINIMAL due to the comma, while the others do not raw_csv_with_quotes
        raw_csv_with_quotes = pd.read_csv(output, quoting=csv.QUOTE_NONE)
        assert '"' in raw_csv_with_quotes.iloc[0]["col1"]
        assert '"' not in raw_csv_with_quotes.iloc[0]["col2"]
        assert '"' not in raw_csv_with_quotes.iloc[0]["col3"]

        # Cleanup
        db.drop_table(schema='working', table=test_csv_name)
        os.remove(output)


class TestQueryToCSVMS:
    def test_query_to_csv_basic(self):
        output = os.path.dirname(os.path.abspath(__file__)) + 'test_query_to_csv.csv'
        sql.drop_table(schema='dbo', table=test_csv_name)

        # Setup table
        sql.query(f"""
            create table dbo.{test_csv_name} (col1 varchar, col2 varchar, col3 varchar);
            insert into dbo.{test_csv_name} values ('a', 'b', 'c');
        """)

        # Query_to_csv
        sql.query_to_csv(query=f'select * from dbo.{test_csv_name}', output_file=output)

        # Get result from query_to_csv as df
        result_df = pd.read_csv(output)

        # Get result from dfquery
        query_df = sql.dfquery(f'select * from dbo.{test_csv_name}')

        # Assert equality
        pd.testing.assert_frame_equal(result_df, query_df)

        # Cleanup
        sql.drop_table(schema='dbo', table=test_csv_name)
        os.remove(output)

    def test_query_to_csv_no_output(self):
        sql.drop_table(schema='dbo', table=test_csv_name)

        # Setup table
        sql.query(f"""
            create table dbo.{test_csv_name} (col1 varchar, col2 varchar, col3 varchar);
            insert into dbo.{test_csv_name} values ('a', 'b', 'c');
        """)

        # Query_to_csv
        sql.query_to_csv(query=f'select * from dbo.{test_csv_name}')

        # Get result from query_to_csv as df
        output = os.path.join(os.getcwd(), f"data_{datetime.now().strftime('%Y%m%d%H%M')}.csv")
        result_df = pd.read_csv(output)

        # Get result from dfquery
        query_df = sql.dfquery(f'select * from dbo.{test_csv_name}')

        # Assert equality
        pd.testing.assert_frame_equal(result_df, query_df)

        # Cleanup
        sql.drop_table(schema='dbo', table=test_csv_name)
        os.remove(output)

    def test_query_to_csv_strict(self):
        try:
            sql.query_to_csv(query='select * from junktable', strict=True)
        except SystemExit:
            return

        """
        Must fail with SystemExit and return in the except clause 
        """
        assert False

    def test_query_to_csv_sep(self):
        output = os.path.dirname(os.path.abspath(__file__)) + 'test_query_to_csv.csv'
        sql.drop_table(schema='dbo', table=test_csv_name)

        # Setup table
        sql.query(f"""
            create table dbo.{test_csv_name} (col1 varchar, col2 varchar, col3 varchar);
            insert into dbo.{test_csv_name} values ('a', 'b', 'c');
        """)

        # Query_to_csv
        sql.query_to_csv(query=f'select * from dbo.{test_csv_name}', output_file=output, sep=';')

        # Get result from query_to_csv as df
        result_df = pd.read_csv(output, sep=';')

        # Get result from dfquery
        query_df = sql.dfquery(f'select * from dbo.{test_csv_name}')

        # Assert equality
        pd.testing.assert_frame_equal(result_df, query_df)

        # Cleanup
        sql.drop_table(schema='dbo', table=test_csv_name)
        os.remove(output)

    def test_query_to_csv_quote_strings(self):
        output = os.path.dirname(os.path.abspath(__file__)) + 'test_query_to_csv.csv'
        sql.drop_table(schema='dbo', table=test_csv_name)

        # Setup table
        sql.query(f"""
            create table dbo.{test_csv_name} (col1 text, col2 varchar, col3 varchar);
            insert into dbo.{test_csv_name} values ('a, ', 'b', 'c');
        """)

        # Query_to_csv
        sql.query_to_csv(query=f'select * from dbo.{test_csv_name}', output_file=output, quote_strings=False)

        # Get result from query_to_csv as df
        result_df = pd.read_csv(output)

        # Get result from dfquery
        query_df = sql.dfquery(f'select * from dbo.{test_csv_name}')

        # Assert equality
        pd.testing.assert_frame_equal(result_df, query_df)

        # Assert the first column contains quotes as it was quoted in QUOTE_MINIMAL due to the comma, while the others do not raw_csv_with_quotes
        raw_csv_with_quotes = pd.read_csv(output, quoting=csv.QUOTE_NONE)
        assert '"' in raw_csv_with_quotes.iloc[0]["col1"]
        assert '"' not in raw_csv_with_quotes.iloc[0]["col2"]
        assert '"' not in raw_csv_with_quotes.iloc[0]["col3"]

        # Cleanup
        sql.drop_table(schema='dbo', table=test_csv_name)
        os.remove(output)
