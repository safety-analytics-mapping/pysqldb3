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

ms_schema = 'dbo'
pg_schema = 'working'


class Test_Table_To_SharePoint_PG:

    @classmethod
    def setup_class(cls):

        cls.schema = "working"

        # Create test table
        cls.table = pg_table_name

        # If table doesn't exist, create it

        db.query(f"""
                drop table if exists {cls.schema}.{cls.table};
                create table {cls.schema}.{cls.table}(
                    id int,
                    val1 int,
                    val2 text
                );
            """)

        db.query(f"""
                insert into {cls.schema}.{cls.table} values
                (1, 10, 'a'),
                (2, 20, 'b'),
                (3, 30, 'c');
            """)

        # Prepare OneDrive destination
        raw_user = db.user
        user = raw_user[0].upper() + raw_user[1:]    # hshi → HShi,
        cls.onedrive_path = f"C:/Users/{user}/OneDrive - NYCDOT/TableTest/PG"

        os.makedirs(cls.onedrive_path, exist_ok=True)

        cls.xlsx_filename = "pg_test_export.xlsx"
        cls.full_dest_path = os.path.join(cls.onedrive_path, cls.xlsx_filename)

    def test_table_to_sharepoint_pg(self):

        # Export Postgres table to SharePoint/OneDrive
        db.table_to_sharepoint(
            table=pg_table_name,
            schema=pg_schema,
            target_subfolder="TableTest/PG",
            output_filename=self.xlsx_filename
        )

        # Local file exists?
        assert os.path.exists(self.full_dest_path), \
            f"Exported xlsx does not exist at {self.full_dest_path}"

        # Read xlsx back
        df_xlsx = pd.read_excel(self.full_dest_path)

        # Read DB table
        df_db = db.dfquery(f"""
            select * from {self.schema}.{self.table} order by id
        """)

        # Compare dataframes
        pd.testing.assert_frame_equal(
            df_xlsx.sort_index(axis=1),
            df_db.sort_index(axis=1),
            check_dtype=False
        )

    @classmethod
    def teardown_class(cls):
        """
        Remove local CSV and drop test table.
        """
        if os.path.exists(cls.full_dest_path):
            os.remove(cls.full_dest_path)

        helpers.clean_up_test_table_pg(db)

class Test_Table_To_SharePoint_SQL:

    @classmethod
    def setup_class(cls):
        """
        Create SQL Server test table for export testing.
        """

        cls.schema = "dbo"
        cls.table = sql_table_name

        # Create table if missing
        sql.drop_table(schema=cls.schema, table=sql_table_name)
        sql.query(f"""
                create table {cls.schema}.{cls.table}(
                    id int,
                    val1 int,
                    val2 varchar(100)
                );
                insert into {cls.schema}.{cls.table} values
                (1, 100, 'apple'),
                (2, 200, 'banana'),
                (3, 300, 'citrus');
            """)

        raw_user = db.user
        user = raw_user[0].upper() + raw_user[1:]  # hshi → HShi,
        cls.onedrive_path = f"C:/Users/{user}/OneDrive - NYCDOT/TableTest/SQL"

        os.makedirs(cls.onedrive_path, exist_ok=True)

        cls.xlsx_filename = "sql_test_export.xlsx"
        cls.full_dest_path = os.path.join(cls.onedrive_path, cls.xlsx_filename)

    def test_table_to_sharepoint_sql(self):

        sql.table_to_sharepoint(
            table=sql_table_name,
            schema='dbo',
            target_subfolder="TableTest/SQL",
            output_filename=self.xlsx_filename
        )

        assert os.path.exists(self.full_dest_path)

        df_xlsx = pd.read_excel(self.full_dest_path)

        df_db = sql.dfquery(f"""
            select * from {self.schema}.{self.table} order by id
        """)

        pd.testing.assert_frame_equal(
            df_xlsx.sort_index(axis=1),
            df_db.sort_index(axis=1),
            check_dtype=False
        )

    @classmethod
    def teardown_class(cls):
        if os.path.exists(cls.full_dest_path):
            os.remove(cls.full_dest_path)

        helpers.clean_up_test_table_sql(sql, schema=ms_schema)
