import os

import configparser
import pandas as pd
import datetime
import urllib

from io import StringIO
from office365.sharepoint.files.file import File

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

ms_schema = 'testing'
pg_schema = 'working'
sharepoint_user = config.get('PG_DB', 'DB_USER')
sharepoint_password = password=config.get('PG_DB', 'DB_PASSWORD')





class Test_SharePoint_To_Table_PG:

    @classmethod
    def setup_class(cls):
        """
        1. Create multiple source tables (mixed types + large volume)
        2. Export each to SharePoint
        3. Construct SharePoint file URLs explicitly
        """

        # Build OneDrive path
        raw_user = sharepoint_user.lower()
        user = raw_user[0].upper() + raw_user[1:]  # hshi → HShi,
        onedrive_root = f"C:/Users/{user}/OneDrive - NYCDOT"

        cls.schema = pg_schema
        cls.subfolder = "upload test"
        onedrive_folder = os.path.join(onedrive_root, cls.subfolder)

        # mixed data types
        cls.dest_mixed = f"{create_table_name}_mixed_from_sp"
        cls.file_mixed = "pg_test_mixed_upload.xlsx"

        df_mixed = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "int_col": [10, 20, None],
                "float_col": [1.5, 2.5, None],
                "text_col": ["a", "b", None],
                "bool_col": [True, False, None],
                "dt_col": [
                    "2024-01-01 10:30:00",
                    "2024-01-02 15:45:00",
                    None
                ]
            }
        )

        # ensure datetime column behaves correctly
        df_mixed["dt_col"] = pd.to_datetime(df_mixed["dt_col"])

        # Create folder if missing
        os.makedirs(onedrive_folder, exist_ok=True)
        final_dest = os.path.join(onedrive_folder, cls.file_mixed)

        # Copy to OneDrive folder
        df_mixed.to_excel(final_dest, index=False, engine="openpyxl")

        # large volume
        cls.dest_large = f"{create_table_name}_large_from_sp"
        cls.file_large = "pg_test_large_upload.xlsx"

        df_large = pd.DataFrame(
            {
                "id": range(1, 100001),
                "val": range(1, 100001),
            }
        )

        final_dest_large = os.path.join(onedrive_folder, cls.file_large)
        df_large.to_excel(final_dest_large, index=False, engine="openpyxl")

        # mostly numeric but real string
        cls.dest_mostly_numeric = f"{create_table_name}_mostly_numeric_from_sp"
        cls.file_mostly_numeric = "pg_test_mostly_numeric_upload.xlsx"

        df_mostly_numeric = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5, 6],
                "code": [
                    "100010",
                    "100020",
                    "100030",
                    "10001A",  # real string
                    "100040",
                    None
                ],
            }
        )

        final_dest_mostly_numeric = os.path.join(
            onedrive_folder, cls.file_mostly_numeric
        )
        df_mostly_numeric.to_excel(
            final_dest_mostly_numeric,
            index=False,
            engine="openpyxl"
        )

        # construct SharePoint URLs
        user_lower = sharepoint_user.lower()
        subfolder_encoded = urllib.parse.quote(cls.subfolder)

        cls.file_url_mixed = (
            f"https://nycdot-my.sharepoint.com/personal/"
            f"{user_lower}_dot_nyc_gov/Documents/"
            f"{subfolder_encoded}/{urllib.parse.quote(cls.file_mixed)}"
        )

        cls.file_url_large = (
            f"https://nycdot-my.sharepoint.com/personal/"
            f"{user_lower}_dot_nyc_gov/Documents/"
            f"{subfolder_encoded}/{urllib.parse.quote(cls.file_large)}"
        )

        cls.file_url_mostly_numeric = (
            f"https://nycdot-my.sharepoint.com/personal/"
            f"{user_lower}_dot_nyc_gov/Documents/"
            f"{subfolder_encoded}/{urllib.parse.quote(cls.file_mostly_numeric)}"
        )

    def test_sharepoint_to_pg_mixed_types(self):
        """
        Validate correctness for mixed data types.
        """

        db.sharepoint_to_table(
            sharepoint_user=sharepoint_user,
            sharepoint_password=sharepoint_password,
            table=self.dest_mixed,
            file_url=self.file_url_mixed,
            sheet_name=0,
            schema=self.schema,
            overwrite=True,
            temp=False
        )

        df_src = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "int_col": [10, 20, None],
                "float_col": [1.5, 2.5, None],
                "text_col": ["a", "b", None],
                "bool_col": [True, False, None],
                "dt_col": [
                    "2024-01-01 10:30:00",
                    "2024-01-02 15:45:00",
                    None
                ]
            }
        )

        df_dest = db.dfquery(f"""
                select * from {self.schema}.{self.dest_mixed} order by id
            """)

        df_src['dt_col'] = pd.to_datetime(df_src['dt_col'], errors="coerce")
        df_dest['dt_col'] = pd.to_datetime(df_dest['dt_col'], errors="coerce")

        pd.testing.assert_frame_equal(
            df_src.sort_index(axis=1),
            df_dest.sort_index(axis=1),
            check_dtype=False
        )

    def test_sharepoint_to_pg_large_volume(self):
        """
        Validate stability for large datasets (row count only).
        """

        db.sharepoint_to_table(
            table=self.dest_large,
            sharepoint_user=sharepoint_user,
            sharepoint_password=sharepoint_password,
            file_url=self.file_url_large,
            sheet_name=0,
            schema=self.schema,
            overwrite=True,
            temp=False
        )

        df_dest = db.dfquery(f"""
                select count(*) as cnt
                from {self.schema}.{self.dest_large}
            """)

        assert df_dest.loc[0, "cnt"] == 100000

    def test_sharepoint_to_pg_mostly_numeric(self):
        """
        Validate semantic correctness for mostly-numeric-but-string columns.
        """

        db.sharepoint_to_table(
            sharepoint_user=sharepoint_user,
            sharepoint_password=sharepoint_password,
            table=self.dest_mostly_numeric,
            file_url=self.file_url_mostly_numeric,
            sheet_name=0,
            schema=self.schema,
            overwrite=True,
            temp=False
        )

        # row count
        df_cnt = db.dfquery(f"""
            select count(*) as cnt
            from {self.schema}.{self.dest_mostly_numeric}
        """)
        assert df_cnt.loc[0, "cnt"] == 6

        # content check
        df = db.dfquery(f"""
            select *
            from {self.schema}.{self.dest_mostly_numeric}
            order by id
        """)

        # schema-level expectation
        assert df["code"].dtype == object

        # real string must survive
        assert (df["code"] == "10001A").any()

        # numeric coercion behavior
        num = pd.to_numeric(df["code"], errors="coerce")
        assert num.isna().sum() >= 2  # '10001A' + null

    @classmethod
    def teardown_class(cls):
        """
        Drop all test tables created by this test.
        """

        for table in [
            cls.dest_mixed,
            cls.dest_large,
            cls.dest_mostly_numeric,
        ]:
            try:
                db.drop_table(table=table, schema=cls.schema)
            except Exception:
                pass


class Test_SharePoint_To_Table_SQL:

    @classmethod
    def setup_class(cls):
        """
        SQL Server:
        Prepare SharePoint files directly from Python (no DB source),
        then validate SharePoint → SQL ingestion.
        """

        cls.schema = ms_schema
        cls.subfolder = "upload test"

        # ------------------------------------------------------------
        # Build OneDrive local sync path
        # ------------------------------------------------------------
        raw_user = sharepoint_user.lower()
        user = raw_user[0].upper() + raw_user[1:]
        onedrive_root = f"C:/Users/{user}/OneDrive - NYCDOT"
        onedrive_folder = os.path.join(onedrive_root, cls.subfolder)
        os.makedirs(onedrive_folder, exist_ok=True)

        # mixed data types
        cls.dest_mixed = f"{create_table_name}_sql_mixed_from_sp"
        cls.file_mixed = "sql_test_mixed_upload.xlsx"

        df_mixed = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "int_col": [10, 20, None],
                "float_col": [1.5, 2.5, None],
                "text_col": ["apple", "banana", None],
                "bool_col": [True, False, None],
                "dt_col": [
                    "2024-01-01 10:30:00",
                    "2024-01-02 15:45:00",
                    None
                ],
            }
        )
        df_mixed["dt_col"] = pd.to_datetime(df_mixed["dt_col"])

        df_mixed.to_excel(
            os.path.join(onedrive_folder, cls.file_mixed),
            index=False,
            engine="openpyxl"
        )

        # large volume
        cls.dest_large = f"{create_table_name}_sql_large_from_sp"
        cls.file_large = "sql_test_large_upload.xlsx"

        df_large = pd.DataFrame(
            {
                "id": range(1, 100001),
                "val": range(1, 100001),
            }
        )

        df_large.to_excel(
            os.path.join(onedrive_folder, cls.file_large),
            index=False,
            engine="openpyxl"
        )

        # mostly numeric but with real strings
        cls.dest_mostly_numeric = f"{create_table_name}_sql_mostly_numeric_from_sp"
        cls.file_mostly_numeric = "sql_test_mostly_numeric_upload.xlsx"

        df_mostly_numeric = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5, 6],
                "code": [
                    "100010",
                    "100020",
                    "100030",
                    "10001A",  # real string
                    "100040",
                    None,
                ],
            }
        )

        df_mostly_numeric.to_excel(
            os.path.join(onedrive_folder, cls.file_mostly_numeric),
            index=False,
            engine="openpyxl"
        )

        # ------------------------------------------------------------
        # Construct SharePoint URLs
        # ------------------------------------------------------------
        user_lower = sharepoint_user.lower()
        subfolder_encoded = urllib.parse.quote(cls.subfolder)

        cls.file_url_mixed = (
            f"https://nycdot-my.sharepoint.com/personal/"
            f"{user_lower}_dot_nyc_gov/Documents/"
            f"{subfolder_encoded}/{urllib.parse.quote(cls.file_mixed)}"
        )

        cls.file_url_large = (
            f"https://nycdot-my.sharepoint.com/personal/"
            f"{user_lower}_dot_nyc_gov/Documents/"
            f"{subfolder_encoded}/{urllib.parse.quote(cls.file_large)}"
        )

        cls.file_url_mostly_numeric = (
            f"https://nycdot-my.sharepoint.com/personal/"
            f"{user_lower}_dot_nyc_gov/Documents/"
            f"{subfolder_encoded}/{urllib.parse.quote(cls.file_mostly_numeric)}"
        )

    def test_sharepoint_to_sql_mixed_types(self):
        """
        Validate correctness for mixed data types.
        """

        sql.sharepoint_to_table(
            table=self.dest_mixed,
            sharepoint_user=sharepoint_user,
            sharepoint_password=sharepoint_password,
            file_url=self.file_url_mixed,
            sheet_name=0,
            schema=self.schema,
            overwrite=True,
            temp=False
        )

        df_src = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "int_col": [10, 20, None],
                "float_col": [1.5, 2.5, None],
                "text_col": ["apple", "banana", None],
                "bool_col": [True, False, None],
                "dt_col": [
                    "2024-01-01 10:30:00",
                    "2024-01-02 15:45:00",
                    None
                ],
            }
        )

        df_dest = sql.dfquery(f"""
            select * from {self.schema}.{self.dest_mixed} order by id
        """)

        df_src['dt_col'] = pd.to_datetime(df_src['dt_col'], errors="coerce")
        df_dest['dt_col'] = pd.to_datetime(df_dest['dt_col'], errors="coerce")

        pd.testing.assert_frame_equal(
            df_src.sort_index(axis=1),
            df_dest.sort_index(axis=1),
            check_dtype=False
        )

    def test_sharepoint_to_sql_large_volume(self):
        """
        Validate stability for larger datasets (row count only).
        """

        sql.sharepoint_to_table(
            table=self.dest_large,
            sharepoint_user=sharepoint_user,
            sharepoint_password=sharepoint_password,
            file_url=self.file_url_large,
            sheet_name=0,
            schema=self.schema,
            overwrite=True,
            temp=False
        )

        df_cnt = sql.dfquery(f"""
            select count(*) as cnt
            from {self.schema}.{self.dest_large}
        """)

        assert df_cnt.loc[0, "cnt"] == 100000

    def test_sharepoint_to_sql_mostly_numeric(self):
        """
        Validate stability and semantic correctness for
        mostly-numeric-but-string columns.
        """

        sql.sharepoint_to_table(
            table=self.dest_mostly_numeric,
            sharepoint_user=sharepoint_user,
            sharepoint_password=sharepoint_password,
            file_url=self.file_url_mostly_numeric,
            sheet_name=0,
            schema=self.schema,
            overwrite=True,
            temp=False
        )

        # 1. row count check
        df_cnt = sql.dfquery(f"""
            select count(*) as cnt
            from {self.schema}.{self.dest_mostly_numeric}
        """)
        assert df_cnt.loc[0, "cnt"] == 6

        # 2. semantic correctness check
        df = sql.dfquery(f"""
            select *
            from {self.schema}.{self.dest_mostly_numeric}
            order by id
        """)

        # column must be string-like in pandas
        assert df["code"].dtype == object

        # must preserve real string content
        assert (df["code"] == "10001A").any()

        # numeric coercion behaves as expected
        num = pd.to_numeric(df["code"], errors="coerce")
        assert num.isna().sum() >= 2  # '10001A' + null

    @classmethod
    def teardown_class(cls):
        """
        Drop all SQL Server tables created by this test.
        """

        for table in [
            cls.dest_mixed,
            cls.dest_large,
            cls.dest_mostly_numeric,
        ]:
            try:
                sql.drop_table(schema=cls.schema, table=table)
            except Exception:
                pass


