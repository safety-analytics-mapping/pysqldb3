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

        cls.schema = pg_schema
        cls.subfolder = "upload test"

        # mixed data types
        cls.src_mixed = f"{pg_table_name}_mixed"
        cls.dest_mixed = f"{create_table_name}_mixed_from_sp"
        cls.file_mixed = "pg_test_mixed_upload.xlsx"

        db.query(f"""
                drop table if exists {cls.schema}.{cls.src_mixed};
                create table {cls.schema}.{cls.src_mixed}(
                    id int,
                    int_col int,
                    float_col float,
                    text_col text,
                    bool_col boolean
                );
            """)

        db.query(f"""
                insert into {cls.schema}.{cls.src_mixed} values
                (1, 10, 1.5, 'a', true),
                (2, 20, 2.5, 'b', false),
                (3, null, null, null, null);
            """)

        db.table_to_sharepoint(
            table=cls.src_mixed,
            schema=cls.schema,
            sharepoint_user=sharepoint_user,
            target_subfolder=cls.subfolder,
            output_filename=cls.file_mixed,
            overwrite=True
        )

        # large volume
        cls.src_large = f"{pg_table_name}_large"
        cls.dest_large = f"{create_table_name}_large_from_sp"
        cls.file_large = "pg_test_large_upload.xlsx"

        db.query(f"""
                drop table if exists {cls.schema}.{cls.src_large};
                create table {cls.schema}.{cls.src_large}(
                    id int,
                    val int
                );
            """)

        db.query(f"""
                insert into {cls.schema}.{cls.src_large}
                select
                    generate_series(1, 5000) as id,
                    generate_series(1, 5000) as val;
            """)

        db.table_to_sharepoint(
            table=cls.src_large,
            schema=cls.schema,
            sharepoint_user=sharepoint_user,
            target_subfolder=cls.subfolder,
            output_filename=cls.file_large,
            overwrite=True
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


    def test_sharepoint_to_pg_mixed_types(self):
        """
        Validate correctness for mixed data types.
        """

        db.sharepoint_to_table(
            df=None,
            sharepoint_user=sharepoint_user,
            sharepoint_password=sharepoint_password,
            table=self.dest_mixed,
            file_url=self.file_url_mixed,
            sheet_name=0,
            schema=self.schema,
            overwrite=True,
            temp=False
        )

        df_src = db.dfquery(f"""
                select * from {self.schema}.{self.src_mixed} order by id
            """)

        df_dest = db.dfquery(f"""
                select * from {self.schema}.{self.dest_mixed} order by id
            """)

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
            df=None,
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

        assert df_dest.loc[0, "cnt"] == 5000


    @classmethod
    def teardown_class(cls):
        """
        Drop all test tables created by this test.
        """

        for table in [
            cls.src_mixed,
            cls.dest_mixed,
            cls.src_large,
            cls.dest_large,
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
        1. Create source tables (mixed types + large volume).
        2. Export each to SharePoint.
        3. Construct SharePoint URLs explicitly.
        """

        cls.schema = ms_schema
        cls.subfolder = "upload test"

        # mixed data types
        cls.src_mixed = f"{sql_table_name}_mixed"
        cls.dest_mixed = f"{create_table_name}_sql_mixed_from_sp"
        cls.file_mixed = "sql_test_mixed_upload.xlsx"

        sql.drop_table(schema=cls.schema, table=cls.src_mixed)
        sql.query(f"""
                create table {cls.schema}.{cls.src_mixed}(
                    id int,
                    int_col int,
                    float_col float,
                    text_col varchar(100),
                    bool_col bit
                );
                insert into {cls.schema}.{cls.src_mixed} values
                (1, 10, 1.5, 'apple', 1),
                (2, 20, 2.5, 'banana', 0),
                (3, null, null, null, null);
            """)

        # large volume
        cls.src_large = f"{sql_table_name}_large"
        cls.dest_large = f"{create_table_name}_sql_large_from_sp"
        cls.file_large = "sql_test_large_upload.xlsx"

        sql.drop_table(schema=cls.schema, table=cls.src_large)
        sql.query(f"""
                create table {cls.schema}.{cls.src_large}(
                    id int,
                    val int
                );

                ;with nums as (
                    select top (5000)
                        row_number() over (order by (select null)) as n
                    from sys.objects
                )
                insert into {cls.schema}.{cls.src_large}
                select n, n from nums;
            """)

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


        # export tables to SharePoint
        sql.table_to_sharepoint(
            table=cls.src_mixed,
            schema=cls.schema,
            sharepoint_user=sharepoint_user,
            target_subfolder=cls.subfolder,
            output_filename=cls.file_mixed,
            overwrite=True
        )

        sql.table_to_sharepoint(
            table=cls.src_large,
            schema=cls.schema,
            sharepoint_user=sharepoint_user,
            target_subfolder=cls.subfolder,
            output_filename=cls.file_large,
            overwrite=True
        )

    def test_sharepoint_to_sql_mixed_types(self):
        """
        Validate correctness for mixed data types.
        """

        sql.sharepoint_to_table(
            df=None,
            table=self.dest_mixed,
            sharepoint_user=sharepoint_user,
            sharepoint_password=sharepoint_password,
            file_url=self.file_url_mixed,
            sheet_name=0,
            schema=self.schema,
            overwrite=True,
            temp=False
        )

        df_src = sql.dfquery(f"""
            select * from {self.schema}.{self.src_mixed} order by id
        """)

        df_dest = sql.dfquery(f"""
            select * from {self.schema}.{self.dest_mixed} order by id
        """)

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
            df=None,
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

        assert df_cnt.loc[0, "cnt"] == 5000


    @classmethod
    def teardown_class(cls):
        """
        Drop all SQL Server tables created by this test.
        """

        for table in [
            cls.src_mixed,
            cls.dest_mixed,
            cls.src_large,
            cls.dest_large,
        ]:
            try:
                sql.drop_table(schema=cls.schema, table=table)
            except Exception:
                pass


