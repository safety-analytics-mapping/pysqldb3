import datetime
import os

import configparser
import pandas as pd
from pytest import raises

from .. import pysqldb3 as pysqldb
from . import helpers

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                       server=config.get('PG_DB', 'SERVER'),
                       database=config.get('PG_DB', 'DB_NAME'),
                       user=config.get('PG_DB', 'DB_USER'),
                       password=config.get('PG_DB', 'DB_PASSWORD'),
                       allow_temp_tables=True
                       )

sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                        server=config.get('SQL_DB', 'SERVER'),
                        database=config.get('SQL_DB', 'DB_NAME'),
                        user=config.get('SQL_DB', 'DB_USER'),
                        password=config.get('SQL_DB', 'DB_PASSWORD'),
                        allow_temp_tables=True)

pg_table_name = 'pg_test_table_{}'.format(db.user)
sql_table_name = 'sql_test_table_{}'.format(db.user)
create_table_name = '_testing_table_to_csv_{}_{}_'.format(datetime.datetime.now().strftime('%Y-%m-%d').replace('-', '_'), db.user)

ms_schema = 'testing'
pg_schema = 'working'
sharepoint_user = config.get('PG_DB', 'DB_USER')


class Test_Table_To_SharePoint_PG:

    @classmethod
    def setup_class(cls):
        cls.schema = "working"

        raw_user = db.user
        user = raw_user[0].upper() + raw_user[1:]
        cls.onedrive_path = f"C:/Users/{user}/OneDrive - NYCDOT/TableTest/PG"
        os.makedirs(cls.onedrive_path, exist_ok=True)

        # mixed data types
        cls.table_mixed = "pg_test_mixed_types"
        cls.xlsx_mixed = "pg_test_mixed_types.xlsx"
        cls.path_mixed = os.path.join(cls.onedrive_path, cls.xlsx_mixed)

        db.query(f"""
            drop table if exists {cls.schema}.{cls.table_mixed};
            create table {cls.schema}.{cls.table_mixed}(
                id int,
                int_col int,
                float_col float,
                text_col text,
                bool_col boolean
            );
        """)

        db.query(f"""
            insert into {cls.schema}.{cls.table_mixed} values
            (1, 10, 1.5, 'a', true),
            (2, 20, 2.5, 'b', false),
            (3, null, null, null, null);
        """)

        # large volume
        cls.table_large = "pg_test_large"
        cls.xlsx_large = "pg_test_large.xlsx"
        cls.path_large = os.path.join(cls.onedrive_path, cls.xlsx_large)

        db.query(f"""
            drop table if exists {cls.schema}.{cls.table_large};
            create table {cls.schema}.{cls.table_large}(
                id int,
                val int
            );
        """)

        db.query(f"""
            insert into {cls.schema}.{cls.table_large}
            select
                generate_series(1, 100000) as id,
                generate_series(1, 100000) as val;
        """)

    def test_table_to_sharepoint_mixed_types(self):
        db.table_to_sharepoint(
            table=self.table_mixed,
            schema=self.schema,
            target_subfolder="TableTest/PG",
            output_filename=self.xlsx_mixed
        )

        assert os.path.exists(self.path_mixed)

        df_xlsx = pd.read_excel(self.path_mixed)
        df_db = db.dfquery(f"""
            select * from {self.schema}.{self.table_mixed} order by id
        """)

        pd.testing.assert_frame_equal(
            df_xlsx.sort_index(axis=1),
            df_db.sort_index(axis=1),
            check_dtype=False
        )

    def test_table_to_sharepoint_large_volume(self):
        db.table_to_sharepoint(
            table=self.table_large,
            schema=self.schema,
            target_subfolder="TableTest/PG",
            output_filename=self.xlsx_large
        )

        assert os.path.exists(self.path_large)

        df_xlsx = pd.read_excel(self.path_large)

        # only validate row count
        assert len(df_xlsx) == 100000

    @classmethod
    def teardown_class(cls):
        """
        Clean up local files and drop test tables.
        """

        # remove exported files
        for path in [
            cls.path_mixed,
            cls.path_large,
        ]:
            if os.path.exists(path):
                os.remove(path)

        # drop test tables
        for table in [
            cls.table_mixed,
            cls.table_large,
        ]:
            db.query(f"""
                drop table if exists {cls.schema}.{table};
            """)


class Test_Table_To_SharePoint_SQL:

    @classmethod
    def setup_class(cls):
        """
        1. Mixed data types table
        2. Large volume table
        """

        cls.schema = "dbo"

        raw_user = sharepoint_user.lower()
        user = raw_user[0].upper() + raw_user[1:]
        cls.onedrive_path = f"C:/Users/{user}/OneDrive - NYCDOT/TableTest/SQL"
        os.makedirs(cls.onedrive_path, exist_ok=True)

        cls.table_mixed = f"{sql_table_name}_mixed"
        cls.xlsx_mixed = "sql_test_mixed_export.xlsx"
        cls.path_mixed = os.path.join(cls.onedrive_path, cls.xlsx_mixed)

        sql.drop_table(schema=cls.schema, table=cls.table_mixed)

        sql.query(f"""
            create table {cls.schema}.{cls.table_mixed}(
                id int,
                int_col int,
                float_col float,
                text_col varchar(100),
                bool_col bit
            );
            insert into {cls.schema}.{cls.table_mixed} values
            (1, 10, 1.5, 'apple', 1),
            (2, 20, 2.5, 'banana', 0),
            (3, null, null, null, null);
        """)

        cls.table_large = f"{sql_table_name}_large"
        cls.xlsx_large = "sql_test_large_export.xlsx"
        cls.path_large = os.path.join(cls.onedrive_path, cls.xlsx_large)

        sql.drop_table(schema=cls.schema, table=cls.table_large)

        sql.query(f"""
            create table {cls.schema}.{cls.table_large}(
                id int,
                val int
            );

            ;with nums as (
                select top (5000)
                    row_number() over (order by (select null)) as n
                from sys.objects
            )
            insert into {cls.schema}.{cls.table_large}
            select n, n from nums;
        """)


    def test_table_to_sharepoint_sql_mixed_types(self):
        """
        Validate correctness for mixed data types.
        """

        sql.table_to_sharepoint(
            table=self.table_mixed,
            schema=self.schema,
            sharepoint_user=sharepoint_user,
            target_subfolder="TableTest/SQL",
            output_filename=self.xlsx_mixed
        )

        assert os.path.exists(self.path_mixed)

        df_xlsx = pd.read_excel(self.path_mixed)

        df_db = sql.dfquery(f"""
            select * from {self.schema}.{self.table_mixed} order by id
        """)

        pd.testing.assert_frame_equal(
            df_xlsx.sort_index(axis=1),
            df_db.sort_index(axis=1),
            check_dtype=False
        )

    def test_table_to_sharepoint_sql_large_volume(self):
        """
        Validate stability for larger datasets (row count only).
        """

        sql.table_to_sharepoint(
            table=self.table_large,
            schema=self.schema,
            sharepoint_user=sharepoint_user,
            target_subfolder="TableTest/SQL",
            output_filename=self.xlsx_large
        )

        assert os.path.exists(self.path_large)

        df_xlsx = pd.read_excel(self.path_large)

        assert len(df_xlsx) == 5000

    @classmethod
    def teardown_class(cls):
        """
        Clean up local files and drop SQL Server test tables.
        """

        for path in [
            cls.path_mixed,
            cls.path_large,
        ]:
            if os.path.exists(path):
                os.remove(path)

        for table in [
            cls.table_mixed,
            cls.table_large,
        ]:
            try:
                sql.drop_table(schema=cls.schema, table=table)
            except Exception:
                pass
