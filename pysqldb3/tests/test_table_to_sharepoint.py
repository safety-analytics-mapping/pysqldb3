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
                bool_col boolean,
                dt_col timestamp
            );
        """)

        db.query(f"""
            insert into {cls.schema}.{cls.table_mixed} values
            (1, 10, 1.5, 'a', true,  '2024-01-01 10:30:00'),
            (2, 20, 2.5, 'b', false, '2024-01-02 15:45:00'),
            (3, null, null, null, null, null);
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

        # mostly numeric but real string
        cls.table_mostly_numeric = "pg_test_mostly_numeric"
        cls.xlsx_mostly_numeric = "pg_test_mostly_numeric.xlsx"
        cls.path_mostly_numeric = os.path.join(
            cls.onedrive_path, cls.xlsx_mostly_numeric
        )

        db.query(f"""
            drop table if exists {cls.schema}.{cls.table_mostly_numeric};
            create table {cls.schema}.{cls.table_mostly_numeric}(
                id int,
                code text
            );
        """)

        db.query(f"""
            insert into {cls.schema}.{cls.table_mostly_numeric} values
            (1, '100010'),
            (2, '100020'),
            (3, '100030'),
            (4, '10001A'),
            (5, '100040'),
            (6, null);
        """)

    def test_table_to_sharepoint_mixed_types(self):
        db.table_to_sharepoint(
            table=self.table_mixed,
            schema=self.schema,
            target_subfolder="TableTest/PG",
            output_filename=self.xlsx_mixed,
            sharepoint_user=sharepoint_user,
            overwrite=True
        )

        assert os.path.exists(self.path_mixed)

        df_xlsx = pd.read_excel(self.path_mixed)
        df_db = db.dfquery(f"""
            select * from {self.schema}.{self.table_mixed} order by id
        """)

        df_xlsx['dt_col'] = pd.to_datetime(df_xlsx['dt_col'], errors="coerce")
        df_db['dt_col'] = pd.to_datetime(df_db['dt_col'], errors="coerce")

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
            output_filename=self.xlsx_large,
            sharepoint_user=sharepoint_user,
            overwrite=True
        )

        assert os.path.exists(self.path_large)

        df_xlsx = pd.read_excel(self.path_large)

        # only validate row count
        assert len(df_xlsx) == 100000

    def test_table_to_sharepoint_pg_mostly_numeric(self):
        """
        Validate correctness for mostly-numeric-but-string columns.
        """

        db.table_to_sharepoint(
            table=self.table_mostly_numeric,
            schema=self.schema,
            target_subfolder="TableTest/PG",
            output_filename=self.xlsx_mostly_numeric,
            sharepoint_user=sharepoint_user,
            overwrite=True
        )

        assert os.path.exists(self.path_mostly_numeric)

        df_xlsx = pd.read_excel(self.path_mostly_numeric)

        # must remain string-like
        assert df_xlsx["code"].dtype == object

        # real string must survive
        assert (df_xlsx["code"] == "10001A").any()

        # numeric coercion behavior
        num = pd.to_numeric(df_xlsx["code"], errors="coerce")
        assert num.isna().sum() >= 2  # '10001A' + null

    @classmethod
    def teardown_class(cls):
        """
        Clean up local files and drop test tables.
        """

        # remove exported files
        for path in [
            cls.path_mixed,
            cls.path_large,
            cls.path_mostly_numeric,
        ]:
            if os.path.exists(path):
                os.remove(path)

        # drop test tables
        for table in [
            cls.table_mixed,
            cls.table_large,
            cls.table_mostly_numeric,
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
                bool_col bit,
                dt_col datetime2(0)
            );
            insert into {cls.schema}.{cls.table_mixed} values
            (1, 10, 1.5, 'apple', 1, '2024-01-01 10:30:00'),
            (2, 20, 2.5, 'banana', 0, '2024-01-02 15:45:00'),
            (3, null, null, null, null, null);
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
                select top (100000)
                    row_number() over (order by (select null)) as n
                from sys.objects a
                cross join sys.objects b
            )
            insert into {cls.schema}.{cls.table_large}
            select n, n from nums;
        """)

        cls.table_mostly_numeric = f"{sql_table_name}_mostly_numeric"
        cls.xlsx_mostly_numeric = "sql_test_mostly_numeric_export.xlsx"
        cls.path_mostly_numeric = os.path.join(
            cls.onedrive_path, cls.xlsx_mostly_numeric
        )

        sql.drop_table(schema=cls.schema, table=cls.table_mostly_numeric)

        sql.query(f"""
            create table {cls.schema}.{cls.table_mostly_numeric}(
                id int,
                code nvarchar(20)
            );
            insert into {cls.schema}.{cls.table_mostly_numeric} values
            (1, '100010'),
            (2, '100020'),
            (3, '100030'),
            (4, '10001A'),
            (5, '100040'),
            (6, null);
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
            output_filename=self.xlsx_mixed,
            overwrite=True
        )

        assert os.path.exists(self.path_mixed)

        df_xlsx = pd.read_excel(self.path_mixed)


        df_db = sql.dfquery(f"""
            select * from {self.schema}.{self.table_mixed} order by id
        """)

        df_xlsx['dt_col'] = pd.to_datetime(df_xlsx['dt_col'], errors="coerce")
        df_db['dt_col'] = pd.to_datetime(df_db['dt_col'], errors="coerce")

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
            output_filename=self.xlsx_large,
            overwrite=True
        )

        assert os.path.exists(self.path_large)

        df_xlsx = pd.read_excel(self.path_large)

        assert len(df_xlsx) == 100000

    def test_table_to_sharepoint_sql_mostly_numeric(self):
        """
        Validate correctness for mostly-numeric-but-string columns.
        """

        sql.table_to_sharepoint(
            table=self.table_mostly_numeric,
            schema=self.schema,
            sharepoint_user=sharepoint_user,
            target_subfolder="TableTest/SQL",
            output_filename=self.xlsx_mostly_numeric,
            overwrite=True
        )

        assert os.path.exists(self.path_mostly_numeric)

        df_xlsx = pd.read_excel(self.path_mostly_numeric)

        # must stay string-like
        assert df_xlsx["code"].dtype == object

        # real string must survive
        assert (df_xlsx["code"] == "10001A").any()

        # numeric coercion check
        num = pd.to_numeric(df_xlsx["code"], errors="coerce")
        assert num.isna().sum() >= 2

    @classmethod
    def teardown_class(cls):
        """
        Clean up local files and drop SQL Server test tables.
        """

        for path in [
            cls.path_mixed,
            cls.path_large,
            cls.path_mostly_numeric,
        ]:
            if os.path.exists(path):
                os.remove(path)

        for table in [
            cls.table_mixed,
            cls.table_large,
            cls.path_mostly_numeric,
        ]:
            try:
                sql.drop_table(schema=cls.schema, table=table)
            except Exception:
                pass
