import os

import configparser
import pandas as pd
import datetime
import urllib

from .. import pysqldb3 as pysqldb
from . import helpers

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                       server=config.get('PG_DB', 'SERVER'),
                       database=config.get('PG_DB', 'DB_NAME'),
                       user=config.get('PG_DB', 'DB_USER'),
                       password=config.get('PG_DB', 'DB_PASSWORD'))

sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                        server=config.get('SQL_DB', 'SERVER'),
                        database=config.get('SQL_DB', 'DB_NAME'),
                        user=config.get('SQL_DB', 'DB_USER'),
                        password=config.get('SQL_DB', 'DB_PASSWORD'))

table_name = 'test_sharepoint_to_table_{}'.format(db.user)

pg_table_name = 'pg_test_table_{}'.format(db.user)
sql_table_name = 'sql_test_table_{}'.format(db.user)
create_table_name = '_testing_table_to_csv_{}_{}_'.format(datetime.datetime.now().strftime('%Y-%m-%d').replace('-', '_'), db.user)

ms_schema = 'dbo'
pg_schema = 'working'





class Test_SharePoint_To_Table_PG:

    @classmethod
    def setup_class(cls):
        """
        PostgreSQL:
        1. Create a simple source table.
        2. Export it to SharePoint (table_to_sharepoint).
        3. Construct the SharePoint URL directly.
        """

        cls.schema = pg_schema
        cls.src_table = pg_table_name
        cls.dest_table = f"{create_table_name}_pg_from_sp"

        # Create clean source table
        db.query(f"""
            drop table if exists {cls.schema}.{cls.src_table};
            create table {cls.schema}.{cls.src_table}(
                id int,
                val1 int,
                val2 text
            );
        """)

        # Insert known deterministic rows
        db.query(f"""
            insert into {cls.schema}.{cls.src_table} values
            (1, 10, 'a'),
            (2, 20, 'b'),
            (3, 30, 'c');
        """)

        # Export to SharePoint
        cls.subfolder = "upload test"
        cls.filename = "pg_test_upload.xlsx"

        db.table_to_sharepoint(
            table=cls.src_table,
            schema=cls.schema,
            target_subfolder=cls.subfolder,
            output_filename=cls.filename
        )

        # Construct the SharePoint URL (no helper function)
        user_lower = db.user.lower()

        subfolder_encoded = urllib.parse.quote(cls.subfolder)
        filename_encoded  = urllib.parse.quote(cls.filename)

        cls.file_url = (
            f"https://nycdot-my.sharepoint.com/personal/"
            f"{user_lower}_dot_nyc_gov/Documents/"
            f"{subfolder_encoded}/{filename_encoded}"
        )

    def test_sharepoint_to_pg_table(self):
        """
        Read the exported SharePoint Excel back into a new PG table.
        Compare the new table with the original source table.
        """

        db.sharepoint_to_table(
            df=None,
            table=self.dest_table,
            file_url=self.file_url,
            sheet_name=0,
            schema=self.schema,
            overwrite=True,
            temp=False
        )

        # Read both tables
        df_src = db.dfquery(f"""
            select * from {self.schema}.{self.src_table} order by id
        """)

        df_dest = db.dfquery(f"""
            select * from {self.schema}.{self.dest_table} order by id
        """)

        pd.testing.assert_frame_equal(
            df_src.sort_index(axis=1),
            df_dest.sort_index(axis=1),
            check_dtype=False
        )

    @classmethod
    def teardown_class(cls):
        """
        Drop both PG tables after the test finishes.
        """
        try:
            db.drop_table(table=cls.src_table, schema=cls.schema)
        except Exception:
            pass

        try:
            db.drop_table(table=cls.dest_table, schema=cls.schema)
        except Exception:
            pass


class Test_SharePoint_To_Table_SQL:

    @classmethod
    def setup_class(cls):
        """
        SQL Server:
        1. Create deterministic source table.
        2. Export to SharePoint.
        3. Construct SharePoint URL with sql.user.
        """

        cls.schema = ms_schema
        cls.src_table = sql_table_name
        cls.dest_table = f"{create_table_name}_sql_from_sp"

        sql.drop_table(schema=cls.schema, table=cls.src_table)
        sql.query(f"""
            create table {cls.schema}.{cls.src_table}(
                id int,
                val1 int,
                val2 varchar(100)
            );
            insert into {cls.schema}.{cls.src_table} values
            (1, 100, 'apple'),
            (2, 200, 'banana'),
            (3, 300, 'citrus');
        """)

        cls.subfolder = "upload test"
        cls.filename = "sql_test_upload.xlsx"

        sql.table_to_sharepoint(
            table=cls.src_table,
            schema=cls.schema,
            target_subfolder=cls.subfolder,
            output_filename=cls.filename
        )

        user_lower = sql.user.lower()

        subfolder_encoded = urllib.parse.quote(cls.subfolder)
        filename_encoded  = urllib.parse.quote(cls.filename)

        cls.file_url = (
            f"https://nycdot-my.sharepoint.com/personal/"
            f"{user_lower}_dot_nyc_gov/Documents/"
            f"{subfolder_encoded}/{filename_encoded}"
        )

    def test_sharepoint_to_sql_table(self):
        """
        Import SharePoint Excel into a new SQL Server table.
        Validate that data matches the source table.
        """

        sql.sharepoint_to_table(
            df=None,
            table=self.dest_table,
            file_url=self.file_url,
            sheet_name=0,
            schema=self.schema,
            overwrite=True,
            temp=False
        )

        df_src = sql.dfquery(f"""
            select * from {self.schema}.{self.src_table} order by id
        """)

        df_dest = sql.dfquery(f"""
            select * from {self.schema}.{self.dest_table} order by id
        """)

        pd.testing.assert_frame_equal(
            df_src.sort_index(axis=1),
            df_dest.sort_index(axis=1),
            check_dtype=False
        )

    @classmethod
    def teardown_class(cls):
        """
        Drop SQL Server tables after test completes.
        """
        try:
            sql.drop_table(table=cls.src_table, schema=cls.schema)
        except Exception:
            pass

        try:
            sql.drop_table(table=cls.dest_table, schema=cls.schema)
        except Exception:
            pass


