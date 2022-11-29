import os

import configparser
import pandas as pd
import time

from .. import pysqldb3 as pysqldb

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

pg_dbconn = pysqldb.DbConnect(db_type=config.get('PG_DB', 'TYPE'),
                              host=config.get('PG_DB', 'SERVER'),
                              db_name=config.get('PG_DB', 'DB_NAME'),
                              username=config.get('PG_DB', 'DB_USER'),
                              password=config.get('PG_DB', 'DB_PASSWORD'))

ms_dbconn = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                              host=config.get('SQL_DB', 'SERVER'),
                              db_name=config.get('SQL_DB', 'DB_NAME'),
                              username=config.get('SQL_DB', 'DB_USER'),
                              password=config.get('SQL_DB', 'DB_PASSWORD'))

xls_table_name = 'sample_test_xls_to_table_{user}'.format(user=pg_dbconn.username)


class TestXlsToTablePG:
    def test_xls_to_table_basic(self):
        # xls_to_table
        pg_dbconn.query('drop table if exists working.{table}'.format(table=xls_table_name))
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls.xls"

        pg_dbconn.xls_to_table(
            input_file=fp,
            table_name=xls_table_name,
            schema_name='working'
        )

        # Check to see if table is in database
        assert pg_dbconn.table_exists(table_name=xls_table_name, schema='working')
        pg_dbconn.query("alter table working.{table} drop column if exists ogc_fid".format(table=xls_table_name))
        db_df = pg_dbconn.dfquery("select * from working.{table}".format(table=xls_table_name))

        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp).rename(columns={"Unnamed: 0": "unnamed__0"})
        xls_df.columns = [c.lower().strip().replace(' ', '_').replace('.', '_').replace(':', '_') for c in xls_df.columns]

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, xls_df)

        # Cleanup
        pg_dbconn.drop_table(schema_name='working', table_name=xls_table_name)

    def test_xls_to_table_override(self):
        # xls_to_table
        pg_dbconn.query('drop table if exists working.{table}'.format(table=xls_table_name))
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls.xls"

        # Try first without column override; confirm will cast as bigint
        pg_dbconn.xls_to_table(
            input_file=fp,
            table_name=xls_table_name,
            schema_name='working'
        )

        # Check to see if table is in database
        assert pg_dbconn.table_exists(table_name=xls_table_name, schema='working')

        # Assert df column types match without override
        pd.testing.assert_frame_equal(
            pd.DataFrame(
                [{"column_name": 'a', "data_type": 'integer'}, {"column_name": 'b', "data_type": 'integer'}]),

            pg_dbconn.dfquery("""

                        select distinct column_name, data_type
                        from information_schema.columns
                        where table_name = '{table}' 
                        and table_schema = '{schema}'
                        and lower(column_name) not like '%unnamed%' 
                        and lower(column_name) not like '%ogc_fid%';

                    """.format(table=xls_table_name, schema='working'))
        )

        # Now test with override
        pg_dbconn.xls_to_table(
            input_file=fp,
            table_name=xls_table_name,
            schema_name='working',
            column_type_overrides={'b': 'varchar'},
            overwrite=True
        )

        # Check to see if table is in database
        assert pg_dbconn.table_exists(table_name=xls_table_name, schema='working')

        # Assert df column types match override
        pd.testing.assert_frame_equal(pd.DataFrame(
            [{"column_name": 'a', "data_type": 'bigint'}, {"column_name": 'b', "data_type": 'character varying'}]),

            pg_dbconn.dfquery("""

                select distinct column_name, data_type
                from information_schema.columns
                where table_name = '{table}'
                and table_schema = '{schema}'
                and lower(column_name) not like '%unnamed%' 
                and lower(column_name) not like '%ogc_fid%';

            """.format(table=xls_table_name, schema='working'))
        )

        # Cleanup
        pg_dbconn.drop_table(schema_name='working', table_name=xls_table_name)

    def test_xls_to_table_sheet(self):
        # xls_to_table
        pg_dbconn.query('drop table if exists working.{}'.format(xls_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls_with_sheet.xls"

        pg_dbconn.xls_to_table(
            input_file=fp,
            table_name=xls_table_name,
            schema_name='working',
            sheet_name='AnotherSheet'
        )

        # Check to see if table is in database
        assert pg_dbconn.table_exists(table_name=xls_table_name, schema='working')
        pg_dbconn.query("alter table working.{table} drop column if exists ogc_fid".format(table=xls_table_name))
        db_df = pg_dbconn.dfquery("select * from working.{table}".format(table=xls_table_name))

        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp, sheet_name='AnotherSheet').rename(columns={"Unnamed: 0": "unnamed__0",
                                                                              "Unnamed: 0.1": "unnamed__0_1"})

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, xls_df)

        # Cleanup
        pg_dbconn.drop_table(schema_name='working', table_name=xls_table_name)

    def test_xls_to_table_sheet_int(self):
        # xls_to_table
        pg_dbconn.query('drop table if exists working.{}'.format(xls_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls_with_sheet.xls"

        pg_dbconn.xls_to_table(
            input_file=fp,
            table_name=xls_table_name,
            schema_name='working',
            sheet_name=1
        )

        # Check to see if table is in database
        assert pg_dbconn.table_exists(table_name=xls_table_name, schema='working')
        pg_dbconn.query("alter table working.{table} drop column if exists ogc_fid".format(table=xls_table_name))
        db_df = pg_dbconn.dfquery("select * from working.{table}".format(table=xls_table_name))


        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp, sheet_name='AnotherSheet').rename(columns={"Unnamed: 0": "unnamed__0",
                                                                              "Unnamed: 0.1": "unnamed__0_1"})

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, xls_df)

        # Cleanup
        pg_dbconn.drop_table(schema_name='working', table_name=xls_table_name)

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
        if pg_dbconn.table_exists(schema='working', table_name=xls_table_name):
            pg_dbconn.query('drop table working.{table}'.format(table=xls_table_name))

        # Make large XLSX file
        data = []
        for i in range(0, 100000):
            data.append((i, i + 1))

        pd.DataFrame(data, columns=['ogr_ex_col_1', 'ogr_ex_col_2']).to_excel(fp, index=False)

        # Try via bulk loader
        pg_dbconn._bulk_xlsx_to_table(input_file=fp, table_name=xls_table_name, schema_name='working')

        # Check to see if table is in database
        # This example is linked to the mssql default server bug
        assert pg_dbconn.table_exists(schema='working', table_name=xls_table_name)
        sql_df = pg_dbconn.dfquery("select * from working.{table}".format(table=xls_table_name))

        # Raw df from data above
        raw_df = pd.DataFrame(data, columns=['ogr_ex_col_1', 'ogr_ex_col_2'])

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df.drop(['ogc_fid'], axis=1), raw_df, check_column_type=False)

        # Cleanup
        pg_dbconn.drop_table(schema_name='working', table_name=xls_table_name)
        os.remove(fp)

    def test_bulk_xls_to_table_default_schema(self):
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\Test.xlsx"

        # bulk_xls_to_table
        if pg_dbconn.table_exists(table_name=xls_table_name):
            pg_dbconn.query('drop table {table}'.format(table=xls_table_name))

        # Make large XLSX file
        data = []
        for i in range(0, 100000):
            data.append((i, i + 1))

        pd.DataFrame(data, columns=['ogr_ex_col_1', 'ogr_ex_col_2']).to_excel(fp, index=False)

        # Try via bulk loader
        pg_dbconn._bulk_xlsx_to_table(input_file=fp, table_name=xls_table_name)

        # Check to see if table is in database
        assert pg_dbconn.table_exists(table_name=xls_table_name)
        sql_df = pg_dbconn.dfquery("select * from {table}".format(table=xls_table_name))

        # Get raw df via data above
        raw_df = pd.DataFrame(data, columns=['ogr_ex_col_1', 'ogr_ex_col_2'])

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df.drop(['ogc_fid'], axis=1), raw_df, check_column_type=False)

        # Cleanup
        pg_dbconn.drop_table(schema_name=pg_dbconn.default_schema, table_name=xls_table_name)
        os.remove(fp)

    def test_bulk_xls_to_table_multisheet(self):
        fp_xlsx = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\Test.xlsx"
        writer = pd.ExcelWriter(fp_xlsx)

        pg_dbconn.query('drop table if exists {schema}.{table}'.format(schema=pg_dbconn.default_schema, table=xls_table_name))

        # Save multi-sheet xlsx
        pd.DataFrame([1, 2], columns=["sheet1"]).to_excel(writer, 'Sheet1', index=False)
        pd.DataFrame([3, 4], columns=["sheet2"]).to_excel(writer, 'Sheet2', index=False)
        writer.save()

        # Try via bulk loader
        init_count = len(pg_dbconn.my_tables())
        pg_dbconn.xls_to_table(input_file=fp_xlsx, table_name=xls_table_name, sheet_name="Sheet2")
        post_count = len(pg_dbconn.my_tables())

        # Check to see if table is in database (and only one table added)
        assert pg_dbconn.table_exists(schema_name=pg_dbconn.default_schema, table_name=xls_table_name)
        assert init_count + 1 == post_count

        # Df Equality
        pg_dbconn.query("alter table {table} drop column if exists ogc_fid".format(table=xls_table_name))
        df1 = pg_dbconn.dfquery("select * from {table}".format(table=xls_table_name))
        df2 = pd.DataFrame([3, 4], columns=["sheet2"])
        pd.testing.assert_frame_equal(df1, df2)

        # Cleanup
        pg_dbconn.drop_table(schema_name=pg_dbconn.default_schema, table_name=xls_table_name)
        # os.remove(fp_xlsx)  # TODO: this is failing and i have no idea why...

    def test_bulk_xls_to_table_input_schema(self):
        # Test input schema
        return

    # Temp test is in logging tests

    @classmethod
    def teardown_class(cls):
        ms_dbconn.cleanup_new_tables()


class TestXlsToTableMS:
    def test_xls_to_table_basic(self):
        # Define table name and cleanup
        if ms_dbconn.table_exists(table_name=xls_table_name, schema_name='dbo'):
            ms_dbconn.query('drop table dbo.{table}'.format(table=xls_table_name))

        if ms_dbconn.table_exists(table_name=f'stg_{xls_table_name}', schema_name='dbo'):
            ms_dbconn.query('drop table dbo.stg_{table}'.format(table=xls_table_name))

        # xls_to_table
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls.xls"
        ms_dbconn.xls_to_table(
            input_file=fp,
            table_name=xls_table_name, schema_name='dbo'
        )

        # Check to see if table is in database
        assert ms_dbconn.table_exists(table_name=xls_table_name, schema_name='dbo')
        sql_df = ms_dbconn.dfquery("select * from dbo.{table}".format(table=xls_table_name))
        if 'ogr_fid' in sql_df.columns:
            sql_df = sql_df.drop(columns=['ogr_fid'])

        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp).rename(columns={"Unnamed: 0": "unnamed__0"})

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df, xls_df)

        # Cleanup
        ms_dbconn.drop_table(schema_name='dbo', table_name=xls_table_name)

    def test_xls_to_table_override(self):
        # xls_to_table
        ms_dbconn.drop_table(schema_name='dbo', table_name=xls_table_name)
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls.xls"

        # Try first without column override; confirm will cast as bigint
        ms_dbconn.xls_to_table(
            input_file=fp,
            table_name=xls_table_name,
            schema_name='dbo'
        )

        # Check to see if table is in database
        assert ms_dbconn.table_exists(table_name=xls_table_name, schema_name='dbo')

        # Assert df column types match without override
        pd.testing.assert_frame_equal(pd.DataFrame(
            [{"column_name": 'a', "data_type": 'int'}, {"column_name": 'b', "data_type": 'int'}]),

            ms_dbconn.dfquery("""
                        select distinct column_name, data_type
                        from information_schema.columns
                        where table_name = '{table}' 
                        and lower(column_name) not like '%unnamed%' 
                        and lower(column_name) not like '%ogr_fid%';
                    """.format(table=xls_table_name))
            )

        # Now test with override
        ms_dbconn.xls_to_table(
            input_file=fp,
            table_name=xls_table_name,
            schema_name='dbo',
            column_type_overrides={'b': 'varchar'},
            overwrite=True
        )

        # Check to see if table is in database
        assert ms_dbconn.table_exists(table_name=xls_table_name, schema_name='dbo')

        # Assert df column types match override
        pd.testing.assert_frame_equal(pd.DataFrame(
            [{"column_name": 'a', "data_type": 'bigint'}, {"column_name": 'b', "data_type": 'varchar'}]),

            ms_dbconn.dfquery("""
                select distinct column_name, data_type
                from information_schema.columns
                where table_name = '{table}' and lower(column_name) not like '%unnamed%' 
                and lower(column_name) not like '%ogr_fid%';
            """.format(table=xls_table_name))
        )

        # Cleanup
        ms_dbconn.drop_table(schema_name='dbo', table_name=xls_table_name)

    def test_xls_to_table_zsheet(self):
        # xls_to_table
        ms_dbconn.drop_table(schema_name='dbo', table_name=xls_table_name)

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls_with_sheet.xls"

        ms_dbconn.xls_to_table(
            input_file=fp,
            table_name=xls_table_name,
            schema_name='dbo',
            sheet_name='AnotherSheet'
        )

        # Check to see if table is in database
        assert ms_dbconn.table_exists(table_name=xls_table_name, schema_name='dbo')
        db_df = ms_dbconn.dfquery("select * from dbo.{table}".format(table=xls_table_name))
        if 'ogr_fid' in db_df.columns:
            db_df = db_df.drop(columns=['ogr_fid'])

        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp, sheet_name='AnotherSheet').rename(columns={"Unnamed: 0": "unnamed__0",
                                                                              "Unnamed: 0.1": "unnamed__0_1"})

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, xls_df)

        # Cleanup
        ms_dbconn.drop_table(schema_name='dbo', table_name=xls_table_name)

    def test_xls_to_table_sheet_int(self):
        # xls_to_table
        ms_dbconn.drop_table(schema_name='dbo', table_name=xls_table_name)

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls_with_sheet.xls"

        ms_dbconn.xls_to_table(
            input_file=fp,
            table_name=xls_table_name,
            schema_name='dbo',
            sheet_name=1
        )

        # Check to see if table is in database
        assert ms_dbconn.table_exists(table_name=xls_table_name, schema_name='dbo')
        db_df = ms_dbconn.dfquery("select * from dbo.{table}".format(table=xls_table_name))
        if 'ogr_fid' in db_df.columns:
            db_df = db_df.drop(columns=['ogr_fid'])

        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp, sheet_name='AnotherSheet').rename(columns={"Unnamed: 0": "unnamed__0",
                                                                              "Unnamed: 0.1": "unnamed__0_1"})

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, xls_df)

        # Cleanup
        ms_dbconn.drop_table(schema_name='dbo', table_name=xls_table_name)

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

        if ms_dbconn.table_exists(schema='dbo', table_name=xls_table_name):
            ms_dbconn.query('drop table dbo.{table}'.format(table=xls_table_name))

        # Make large XLSX file
        data = []
        for i in range(0, 100000):
            data.append((i, i + 1))

        pd.DataFrame(data, columns=['ogr_ex_col_1', 'ogr_ex_col_2']).to_excel(fp, index=False)

        # Try via bulk loader
        ms_dbconn._bulk_xlsx_to_table(input_file=fp, table_name=xls_table_name, schema_name='dbo')

        # Check to see if table is in database
        # This example is linked to the mssql default server bug
        assert ms_dbconn.table_exists(schema='dbo', table_name=xls_table_name)
        sql_df = ms_dbconn.dfquery("select * from dbo.{table}".format(table=xls_table_name))

        # Get raw df from above
        raw_df = pd.DataFrame(data, columns=['ogr_ex_col_1', 'ogr_ex_col_2'])

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df.drop(['ogr_fid'], axis=1), raw_df, check_column_type=False)

        # Cleanup
        ms_dbconn.drop_table(schema_name='dbo', table_name=xls_table_name)
        os.remove(fp)

    def test_bulk_xls_to_table_default_schema(self):
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\Test.xlsx"

        # bulk_xls_to_table
        if ms_dbconn.table_exists(table_name=xls_table_name):
            ms_dbconn.query('drop table {table}'.format(table=xls_table_name))

        # Make large XLSX file
        data = []
        for i in range(0, 100000):
            data.append((i, i + 1))

        pd.DataFrame(data, columns=['ogr_ex_col_1', 'ogr_ex_col_2']).to_excel(fp, index=False)

        # Try via bulk loader
        ms_dbconn._bulk_xlsx_to_table(input_file=fp, table_name=xls_table_name)

        # Check to see if table is in database
        # This example is linked to the mssql default server bug
        assert ms_dbconn.table_exists(table_name=xls_table_name)
        sql_df = ms_dbconn.dfquery("select * from {table}".format(table=xls_table_name))

        # Get excel df via pd.read_excel
        raw_df = pd.DataFrame(data, columns=['ogr_ex_col_1', 'ogr_ex_col_2'])

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df.drop(['ogr_fid'], axis=1), raw_df, check_column_type=False)

        # Cleanup
        ms_dbconn.drop_table(schema_name=ms_dbconn.default_schema, table_name=xls_table_name)
        os.remove(fp)

    def test_bulk_xls_to_table_correct_functionality(self):
        ms_dbconn2 = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                                       host=config.get('SQL_DB', 'SERVER'),
                                       db_name=config.get('SQL_DB', 'DB_NAME'),
                                       username=config.get('SQL_DB', 'DB_USER'),
                                       password=config.get('SQL_DB', 'DB_PASSWORD'))

        fp_xlsx = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\Test.xlsx"
        fp_xls = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\Test2.xls"

        if ms_dbconn2.table_exists(schema_name=ms_dbconn2.default_schema, table_name=xls_table_name):
            ms_dbconn2.query('drop table {schema}.{table}'.format(schema=ms_dbconn2.default_schema, table=xls_table_name))

        if ms_dbconn2.table_exists(schema_name=ms_dbconn2.default_schema, table_name=f"{xls_table_name}_2"):
            ms_dbconn2.query('drop table {schema}.{table}'.format(schema=ms_dbconn2.default_schema, table=f"{xls_table_name}_2"))

        # Make large XLSX file
        data = []
        for i in range(0, 20000):
            data.append((j for j in range(0, 20)))

        cols = ['ogr_ex_col_{i}'.format(i=i) for i in range(0, 20)]

        sample_df = pd.DataFrame(data, columns=cols)
        sample_df.to_excel(fp_xlsx)
        sample_df.to_excel(fp_xls)

        # Try via bulk loader
        start_time = time.time()
        ms_dbconn2.xls_to_table(input_file=fp_xlsx, table_name=xls_table_name)
        end_xlsx_time = time.time()

        ms_dbconn2.xls_to_table(input_file=fp_xls, table_name=xls_table_name + "_2")
        end_xls_time = time.time()

        xlsx_time = (end_xlsx_time - start_time)/60.0
        xls_time = (end_xls_time - end_xlsx_time)/60.0

        # This is an approximation to ensure xls and xlsx both went via ogr (roughly the same time
        assert ((xls_time*1.0)/xlsx_time < 2) and ((xls_time*1.0)/xlsx_time > 0)

        # Check to see if table is in database
        assert ms_dbconn2.table_exists(table_name=xls_table_name)
        assert ms_dbconn2.table_exists(table_name=f"{xls_table_name}_2")

        # Df Equality
        df1 = ms_dbconn2.dfquery("select * from {table}".format(table=xls_table_name))
        df2 = ms_dbconn2.dfquery("select * from {table}".format(table=f"{xls_table_name}_2"))
        commons_cols = set(df1.columns) - (set(df1.columns) - set(df2.columns))
        pd.testing.assert_frame_equal(df1[commons_cols], df2[commons_cols])

        # Cleanup
        ms_dbconn2.drop_table(schema_name=ms_dbconn2.default_schema, table_name=xls_table_name)
        ms_dbconn2.drop_table(schema_name=ms_dbconn2.default_schema, table_name=f"{xls_table_name}_2")
        os.remove(fp_xlsx)
        os.remove(fp_xls)

    def test_bulk_xls_to_table_multisheet(self):
        ms_dbconn2 = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                                host=config.get('SQL_DB', 'SERVER'),
                                db_name=config.get('SQL_DB', 'DB_NAME'),
                                username=config.get('SQL_DB', 'DB_USER'),
                                password=config.get('SQL_DB', 'DB_PASSWORD'))

        fp_xlsx = os.path.dirname(os.path.abspath(__file__)) + "\\Test.xlsx"
        writer = pd.ExcelWriter(fp_xlsx)

        if ms_dbconn2.table_exists(schema_name=ms_dbconn2.default_schema, table_name=xls_table_name):
            ms_dbconn2.query('drop table {schema}.{table}'.format(schema=ms_dbconn2.default_schema, table=xls_table_name))

        # Save multi-sheet xlsx
        pd.DataFrame([1, 2], columns=["sheet1"]).to_excel(writer, 'Sheet1', index=False)
        pd.DataFrame([3, 4], columns=["sheet2"]).to_excel(writer, 'Sheet2', index=False)
        writer.save()

        # Try via bulk loader
        ms_dbconn2.xls_to_table(input_file=fp_xlsx, table_name=xls_table_name, sheet_name="Sheet2")

        # Check to see if table is in database (and only one table added)
        assert ms_dbconn2.table_exists(table_name=xls_table_name)

        # Df Equality
        df1 = ms_dbconn2.dfquery("select * from {table}".format(table=xls_table_name))
        df2 = pd.DataFrame([3, 4], columns=["sheet2"])
        pd.testing.assert_frame_equal(df1[['sheet2']], df2)

        # Cleanup
        ms_dbconn2.drop_table(schema_name=ms_dbconn2.default_schema, table_name=xls_table_name)
        # os.remove(fp_xlsx)  # TODO: this fails for some reason

    # Temp test is in logging tests

    @classmethod
    def teardown_class(cls):
        ms_dbconn.cleanup_new_tables()
