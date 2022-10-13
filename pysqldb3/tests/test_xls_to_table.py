import os

import configparser
import pandas as pd
import time

from .. import pysqldb3 as pysqldb

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

xls_table_name = f'sample_test_xls_to_table_{db.user}'


class TestXlsToTablePG:
    def test_xls_to_table_basic(self):
        # xls_to_table
        db.query(f'drop table if exists working.{xls_table_name}')
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls.xls"

        db.xls_to_table(
            input_file=fp,
            table=xls_table_name,
            schema='working'
        )

        # Check to see if table is in database
        assert db.table_exists(table=xls_table_name, schema='working')
        db_df = db.dfquery(f"select * from working.{xls_table_name}")

        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp)# .rename(columns={"Unnamed: 0": "unnamed: 0"})
        xls_df.columns = [c.lower().strip().replace(' ', '_').replace('.', '_').replace(':', '_') for c in xls_df.columns]

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, xls_df)

        # Cleanup
        db.drop_table(schema='working', table=xls_table_name)

    def test_xls_to_table_override(self):
        # xls_to_table
        db.query(f'drop table if exists working.{xls_table_name}')
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls.xls"

        # Try first without column override; confirm will cast as bigint
        db.xls_to_table(
            input_file=fp,
            table=xls_table_name,
            schema='working'
        )

        # Check to see if table is in database
        assert db.table_exists(table=xls_table_name, schema='working')

        # Assert df column types match without override
        pd.testing.assert_frame_equal(
            pd.DataFrame(
                [{"column_name": 'a', "data_type": 'bigint'}, {"column_name": 'b', "data_type": 'bigint'}]),

            db.dfquery(f"""
                        select distinct column_name, data_type
                        from information_schema.columns
                        where table_name = '{xls_table_name}' 
                        and table_schema = 'working'
                        and lower(column_name) not like '%unnamed%' 
                        and lower(column_name) not like '%ogc_fid%';
                    """)
        )

        # Now test with override
        db.xls_to_table(
            input_file=fp,
            table=xls_table_name,
            schema='working',
            column_type_overrides={'b': 'varchar'},
            overwrite=True
        )

        # Check to see if table is in database
        assert db.table_exists(table=xls_table_name, schema='working')

        # Assert df column types match override
        pd.testing.assert_frame_equal(pd.DataFrame(
            [{"column_name": 'a', "data_type": 'bigint'}, {"column_name": 'b', "data_type": 'character varying'}]),

            db.dfquery(f"""
                select distinct column_name, data_type
                from information_schema.columns
                where table_name = '{xls_table_name}'
                and table_schema = 'working'
                and lower(column_name) not like '%unnamed%' 
                and lower(column_name) not like '%ogc_fid%';
            """)
        )

        # Cleanup
        db.drop_table(schema='working', table=xls_table_name)

    def test_xls_to_table_sheet(self):
        # xls_to_table
        db.query(f'drop table if exists working.{xls_table_name}')

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls_with_sheet.xls"

        db.xls_to_table(
            input_file=fp,
            table=xls_table_name,
            schema='working',
            sheet_name='AnotherSheet'
        )

        # Check to see if table is in database
        assert db.table_exists(table=xls_table_name, schema='working')
        db_df = db.dfquery(f"select * from working.{xls_table_name}")

        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp, sheet_name='AnotherSheet').rename(columns={"Unnamed: 0": "unnamed__0"})

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, xls_df)

        # Cleanup
        db.drop_table(schema='working', table=xls_table_name)

    def test_xls_to_table_sheet_int(self):
        # xls_to_table
        db.query(f'drop table if exists working.{xls_table_name}')

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls_with_sheet.xls"

        db.xls_to_table(
            input_file=fp,
            table=xls_table_name,
            schema='working',
            sheet_name=1
        )

        # Check to see if table is in database
        assert db.table_exists(table=xls_table_name, schema='working')
        db_df = db.dfquery(f"select * from working.{xls_table_name}")

        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp, sheet_name='AnotherSheet').rename(columns={"Unnamed: 0": "unnamed__0"})

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, xls_df)

        # Cleanup
        db.drop_table(schema='working', table=xls_table_name)

    def test_xls_to_table_schema(self):
        return

    def test_xls_to_table_overwrite(self):
        return

    # Temp test is in logging tests


class TestBulkXLSToTablePG:
    @classmethod
    def setup_class(cls):
        return

    def test_bulk_xls_to_table_basic(self):
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\Test.xlsx"

        # bulk_xls_to_table
        if db.table_exists(schema='working', table=xls_table_name):
            db.query(f'drop table working.{xls_table_name}')

        # Make large XLSX file
        data = []
        for i in range(0, 100000):
            data.append((i, i + 1))

        pd.DataFrame(data, columns=['ogr_ex_col_1', 'ogr_ex_col_2']).to_excel(fp, index=False)

        # Try via bulk loader
        db._bulk_xlsx_to_table(input_file=fp, table=xls_table_name, schema='working')

        # Check to see if table is in database
        # This example is linked to the mssql default server bug
        assert db.table_exists(schema='working', table=xls_table_name)
        sql_df = db.dfquery(f"select * from working.{xls_table_name}")

        # Raw df from data above
        raw_df = pd.DataFrame(data, columns=['ogr_ex_col_1', 'ogr_ex_col_2'])

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df.drop(['ogc_fid'], axis=1), raw_df, check_column_type=False)

        # Cleanup
        db.drop_table(schema='working', table=xls_table_name)
        os.remove(fp)

    def test_bulk_xls_to_table_default_schema(self):
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\Test.xlsx"

        # bulk_xls_to_table
        if db.table_exists(table=xls_table_name):
            db.query(f'drop table {xls_table_name}')

        # Make large XLSX file
        data = []
        for i in range(0, 100000):
            data.append((i, i + 1))

        pd.DataFrame(data, columns=['ogr_ex_col_1', 'ogr_ex_col_2']).to_excel(fp, index=False)

        # Try via bulk loader
        db._bulk_xlsx_to_table(input_file=fp, table=xls_table_name)

        # Check to see if table is in database
        assert db.table_exists(table=xls_table_name)
        sql_df = db.dfquery(f"select * from {xls_table_name}")

        # Get raw df via data above
        raw_df = pd.DataFrame(data, columns=['ogr_ex_col_1', 'ogr_ex_col_2'])

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df.drop(['ogc_fid'], axis=1), raw_df, check_column_type=False)

        # Cleanup
        db.drop_table(schema=db.default_schema, table=xls_table_name)
        os.remove(fp)

    def test_bulk_xls_to_table_multisheet(self):
        fp_xlsx = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\Test.xlsx"
        writer = pd.ExcelWriter(fp_xlsx)

        db.query(f'drop table if exists {db.default_schema}.{xls_table_name}')

        # Save multi-sheet xlsx
        pd.DataFrame([1, 2], columns=["sheet1"]).to_excel(writer, 'Sheet1', index=False)
        pd.DataFrame([3, 4], columns=["sheet2"]).to_excel(writer, 'Sheet2', index=False)
        writer.save()

        # Try via bulk loader
        init_count = len(db.my_tables())
        db.xls_to_table(input_file=fp_xlsx, table=xls_table_name, sheet_name="Sheet2")
        post_count = len(db.my_tables())

        # Check to see if table is in database (and only one table added)
        assert db.table_exists(schema=db.default_schema, table=xls_table_name)
        assert init_count + 1 == post_count

        # Df Equality
        df1 = db.dfquery(f"select * from {xls_table_name}")
        df2 = pd.DataFrame([3, 4], columns=["sheet2"])
        pd.testing.assert_frame_equal(df1, df2)

        # Cleanup
        db.drop_table(schema=db.default_schema, table=xls_table_name)
        # os.remove(fp_xlsx)  # TODO: this is failing and i have no idea why...

    def test_bulk_xls_to_table_input_schema(self):
        # Test input schema
        return

    # Temp test is in logging tests

    @classmethod
    def teardown_class(cls):
        sql.clean_up_new_tables()


class TestXlsToTableMS:
    def test_xls_to_table_basic(self):
        # Define table name and cleanup
        if sql.table_exists(table=xls_table_name, schema='dbo'):
            sql.query(f'drop table dbo.{xls_table_name}')

        if sql.table_exists(table='stg_' + xls_table_name, schema='dbo'):
            sql.query(f'drop table dbo.stg_{xls_table_name}')

        # xls_to_table
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls.xls"
        sql.xls_to_table(
            input_file=fp,
            table=xls_table_name, schema='dbo'
        )

        # Check to see if table is in database
        assert sql.table_exists(table=xls_table_name, schema='dbo')
        sql_df = sql.dfquery(f"select * from dbo.{xls_table_name}")

        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp).rename(columns={"Unnamed: 0": "unnamed__0"})

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df, xls_df)

        # Cleanup
        sql.drop_table(schema='dbo', table=xls_table_name)

    def test_xls_to_table_override(self):
        # xls_to_table
        sql.drop_table(schema='dbo', table=xls_table_name)
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls.xls"

        # Try first without column override; confirm will cast as bigint
        sql.xls_to_table(
            input_file=fp,
            table=xls_table_name,
            schema='dbo'
        )

        # Check to see if table is in database
        assert sql.table_exists(table=xls_table_name, schema='dbo')

        # Assert df column types match without override
        pd.testing.assert_frame_equal(pd.DataFrame(
            [{"column_name": 'a', "data_type": 'bigint'}, {"column_name": 'b', "data_type": 'bigint'}]),

            sql.dfquery(f"""
                        select distinct column_name, data_type
                        from information_schema.columns
                        where table_name = '{xls_table_name}' 
                        and lower(column_name) not like '%unnamed%' 
                        and lower(column_name) not like '%ogr_fid%';
                    """)
        )

        # Now test with override
        sql.xls_to_table(
            input_file=fp,
            table=xls_table_name,
            schema='dbo',
            column_type_overrides={'b': 'varchar'},
            overwrite=True
        )

        # Check to see if table is in database
        assert sql.table_exists(table=xls_table_name, schema='dbo')

        # Assert df column types match override
        pd.testing.assert_frame_equal(pd.DataFrame(
            [{"column_name": 'a', "data_type": 'bigint'}, {"column_name": 'b', "data_type": 'varchar'}]),

            sql.dfquery(f"""
                select distinct column_name, data_type
                from information_schema.columns
                where table_name = '{xls_table_name}' and lower(column_name) not like '%unnamed%' 
                and lower(column_name) not like '%ogr_fid%';
            """)
        )

        # Cleanup
        sql.drop_table(schema='dbo', table=xls_table_name)

    def test_xls_to_table_zsheet(self):
        # xls_to_table
        sql.drop_table(schema='dbo', table=xls_table_name)

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls_with_sheet.xls"

        sql.xls_to_table(
            input_file=fp,
            table=xls_table_name, schema='dbo',
            sheet_name='AnotherSheet'
        )

        # Check to see if table is in database
        assert sql.table_exists(table=xls_table_name, schema='dbo')
        db_df = sql.dfquery(f"select * from dbo.{xls_table_name}")

        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp, sheet_name='AnotherSheet').rename(columns={"Unnamed: 0": "unnamed__0"})

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, xls_df)

        # Cleanup
        sql.drop_table(schema='dbo', table=xls_table_name)

    def test_xls_to_table_sheet_int(self):
        # xls_to_table
        sql.drop_table(schema='dbo', table=xls_table_name)

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls_with_sheet.xls"

        sql.xls_to_table(
            input_file=fp,
            table=xls_table_name, schema='dbo',
            sheet_name=1
        )

        # Check to see if table is in database
        assert sql.table_exists(table=xls_table_name, schema='dbo')
        db_df = sql.dfquery(f"select * from dbo.{xls_table_name}")

        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp, sheet_name='AnotherSheet').rename(columns={"Unnamed: 0": "unnamed__0"})

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, xls_df)

        # Cleanup
        sql.drop_table(schema='dbo', table=xls_table_name)

    def test_xls_to_table_schema(self):
        return

    def test_xls_to_table_overwrite(self):
        return

    # Temp test is in logging tests


class TestBulkXLSToTableMS:
    @classmethod
    def setup_class(cls):
        return

    def test_bulk_xls_to_table_basic(self):
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\Test.xlsx"

        if sql.table_exists(schema='dbo', table=xls_table_name):
            sql.query(f'drop table dbo.{xls_table_name}')

        # Make large XLSX file
        data = []
        for i in range(0, 100000):
            data.append((i, i + 1))

        pd.DataFrame(data, columns=['ogr_ex_col_1', 'ogr_ex_col_2']).to_excel(fp, index=False)

        # Try via bulk loader
        sql._bulk_xlsx_to_table(input_file=fp, table=xls_table_name, schema='dbo')

        # Check to see if table is in database
        # This example is linked to the mssql default server bug
        assert sql.table_exists(schema='dbo', table=xls_table_name)
        sql_df = sql.dfquery(f"select * from dbo.{xls_table_name}")

        # Get raw df from above
        raw_df = pd.DataFrame(data, columns=['ogr_ex_col_1', 'ogr_ex_col_2'])

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df.drop(['ogr_fid'], axis=1), raw_df, check_column_type=False)

        # Cleanup
        sql.drop_table(schema='dbo', table=xls_table_name)
        os.remove(fp)

    def test_bulk_xls_to_table_default_schema(self):
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\Test.xlsx"

        # bulk_xls_to_table
        if sql.table_exists(table=xls_table_name):
            sql.query(f'drop table {xls_table_name}')

        # Make large XLSX file
        data = []
        for i in range(0, 100000):
            data.append((i, i + 1))

        pd.DataFrame(data, columns=['ogr_ex_col_1', 'ogr_ex_col_2']).to_excel(fp, index=False)

        # Try via bulk loader
        sql._bulk_xlsx_to_table(input_file=fp, table=xls_table_name)

        # Check to see if table is in database
        # This example is linked to the mssql default server bug
        assert sql.table_exists(table=xls_table_name)
        sql_df = sql.dfquery(f"select * from {xls_table_name}")

        # Get excel df via pd.read_excel
        raw_df = pd.DataFrame(data, columns=['ogr_ex_col_1', 'ogr_ex_col_2'])

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df.drop(['ogr_fid'], axis=1), raw_df, check_column_type=False)

        # Cleanup
        sql.drop_table(schema=sql.default_schema, table=xls_table_name)
        os.remove(fp)

    def test_bulk_xls_to_table_correct_functionality(self):
        sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                                server=config.get('SQL_DB', 'SERVER'),
                                database=config.get('SQL_DB', 'DB_NAME'),
                                user=config.get('SQL_DB', 'DB_USER'),
                                password=config.get('SQL_DB', 'DB_PASSWORD'))

        fp_xlsx = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\Test.xlsx"
        fp_xls = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\Test2.xls"

        if sql.table_exists(schema=sql.default_schema, table=xls_table_name):
            sql.query(f'drop table {sql.default_schema}.{xls_table_name}')

        if sql.table_exists(schema=sql.default_schema, table=f"{xls_table_name}_2"):
            sql.query(f'drop table {sql.default_schema}.{xls_table_name}_2')

        # Make large XLSX file
        data = []
        for i in range(0, 20000):
            data.append((j for j in range(0, 20)))

        cols = [f'ogr_ex_col_{i}' for i in range(0, 20)]

        sample_df = pd.DataFrame(data, columns=cols)
        sample_df.to_excel(fp_xlsx)
        sample_df.to_excel(fp_xls)

        # Try via bulk loader
        start_time = time.time()
        sql.xls_to_table(input_file=fp_xlsx, table=xls_table_name)
        end_xlsx_time = time.time()

        sql.xls_to_table(input_file=fp_xls, table=f"{xls_table_name}_2")
        end_xls_time = time.time()

        xlsx_time = (end_xlsx_time - start_time)/60.0
        xls_time = (end_xls_time - end_xlsx_time)/60.0

        # This is an approximation to ensure xls and xlsx both went via ogr (roughly the same time
        assert ((xls_time*1.0)/xlsx_time < 2) and ((xls_time*1.0)/xlsx_time > 0)

        # Check to see if table is in database
        assert sql.table_exists(table=xls_table_name)
        assert sql.table_exists(table=f"{xls_table_name}_2")

        # Df Equality
        df1 = sql.dfquery(f"select * from {xls_table_name}")
        df2 = sql.dfquery(f"select * from {xls_table_name}_2")
        commons_cols = set(df1.columns) - (set(df1.columns) - set(df2.columns))
        pd.testing.assert_frame_equal(df1[commons_cols], df2[commons_cols])

        # Cleanup
        sql.drop_table(schema=sql.default_schema, table=xls_table_name)
        sql.drop_table(schema=sql.default_schema, table=f"{xls_table_name}_2")
        os.remove(fp_xlsx)
        os.remove(fp_xls)

    def test_bulk_xls_to_table_multisheet(self):
        sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                                server=config.get('SQL_DB', 'SERVER'),
                                database=config.get('SQL_DB', 'DB_NAME'),
                                user=config.get('SQL_DB', 'DB_USER'),
                                password=config.get('SQL_DB', 'DB_PASSWORD'))

        fp_xlsx = os.path.dirname(os.path.abspath(__file__)) + "\\Test.xlsx"
        writer = pd.ExcelWriter(fp_xlsx)

        if sql.table_exists(schema=sql.default_schema, table=xls_table_name):
            sql.query(f'drop table {sql.default_schema}.{xls_table_name}')

        # Save multi-sheet xlsx
        pd.DataFrame([1, 2], columns=["sheet1"]).to_excel(writer, 'Sheet1', index=False)
        pd.DataFrame([3, 4], columns=["sheet2"]).to_excel(writer, 'Sheet2', index=False)
        writer.save()

        # Try via bulk loader
        sql.xls_to_table(input_file=fp_xlsx, table=xls_table_name, sheet_name="Sheet2")

        # Check to see if table is in database (and only one table added)
        assert sql.table_exists(table=xls_table_name)

        # Df Equality
        df1 = sql.dfquery(f"select * from {xls_table_name}")
        df2 = pd.DataFrame([3, 4], columns=["sheet2"])
        pd.testing.assert_frame_equal(df1, df2)

        # Cleanup
        sql.drop_table(schema=sql.default_schema, table=xls_table_name)
        # os.remove(fp_xlsx)  # TODO: this fails for some reason

    # Temp test is in logging tests

    @classmethod
    def teardown_class(cls):
        sql.clean_up_new_tables()
