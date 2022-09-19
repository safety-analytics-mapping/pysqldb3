import os
import time

import configparser
import pandas as pd

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

pg_table_name = 'pg_test_table_{}'.format(db.user)
create_table_name = 'sample_acs_test_csv_to_table_{}'.format(db.user)
xls_table_name = 'sample_test_xls_to_table_{}'.format(db.user)

test_read_shp_table_name = 'test_read_shp_table_{}'.format(db.user)
test_write_shp_table_name = 'test_write_shp_table_{}'.format(db.user)
test_reuploaded_table_name = 'test_write_reuploaded_{}'.format(db.user)

pg_schema = 'working'
sql_schema='dbo'


# CSV imports tests

class TestCsvToTablePG:
    @classmethod
    def setup_class(cls):
        # helpers.set_up_test_table_pg(db)
        helpers.set_up_test_csv()

    def test_csv_to_table_basic(self):
        # csv_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema, create_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test.csv"
        db.import_data(file_type='csv', file_path=fp, table=create_table_name, schema=pg_schema)

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        db.drop_table(schema=pg_schema, table=create_table_name)

    def test_csv_to_table_basic_no_type(self):
        # csv_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema,create_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test.csv"
        db.import_data(file_path=fp, table=create_table_name, schema=pg_schema)

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema,create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        db.drop_table(schema=pg_schema, table=create_table_name)

    def test_csv_to_table_column_override(self):
        db.drop_table(schema=pg_schema, table=create_table_name)
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\csv_override_ex.csv"
        test_df = pd.DataFrame([{'a': 1, 'b': 2, 'c': 3, 'd': 'text'}, {'a': 4, 'b': 5, 'c': 6, 'd': 'another'}])
        test_df.to_csv(fp)
        db.import_data(file_path=fp, table=create_table_name, schema=pg_schema, column_type_overrides={'a': 'varchar'})

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema,create_table_name))

        # Modify to make column types altered
        altered_column_type_df = pd.DataFrame([{'a': '1.0', 'b': 2, 'c': 3, 'd': 'text'},
                                               {'a': '4.0', 'b': 5, 'c': 6, 'd': 'another'}])

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df[['a', 'b', 'c', 'd']], altered_column_type_df)

        # Assert df column types match override
        pd.testing.assert_frame_equal(pd.DataFrame([{"column_name": 'a', "data_type": 'character varying'},
                                                    {"column_name": 'b', "data_type": 'bigint'},
                                                    {"column_name": 'c', "data_type": 'bigint'},
                                                    {"column_name": 'd', "data_type": 'character varying'}]),
                                      db.dfquery("""

                                            select distinct column_name, data_type
                                            from information_schema.columns
                                            where table_name = '{}' and lower(column_name) not like '%unnamed%';

                                      """.format(create_table_name))
        )

        # Cleanup
        db.drop_table(schema=pg_schema, table=create_table_name)
        os.remove(fp)

    def test_csv_to_table_separator(self):
        # csv_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema, create_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test2.csv"
        db.import_data(file_path=fp, table=create_table_name, schema=pg_schema, sep="|")

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema,create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp, sep='|')
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        db.drop_table(schema=pg_schema, table=create_table_name)

    # what if the table has no header?
    def test_csv_to_table_no_header(self):
        # csv_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema, create_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test3.csv"
        db.import_data(file_path=fp, table=create_table_name, schema=pg_schema, sep="|")

        # did it enter the db correctly?
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp, sep='|')
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference
        csv_df.columns = [c.replace('.', '') for c in list(csv_df.columns)]
        csv_df.columns = [c.lower() for c in list(csv_df.columns)]

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        db.drop_table(schema=pg_schema, table=create_table_name)

    def test_csv_to_table_overwrite(self):
        # Make a table to fill the eventual table location and confirm it exists
        db.query('create table {}.{} as select 10'.format(pg_schema, create_table_name))
        assert db.table_exists(table=create_table_name, schema=pg_schema)

        # csv_to_table
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test.csv"
        db.import_data(file_path=fp, table=create_table_name, schema=pg_schema, overwrite=True)

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema,create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        db.drop_table(schema=pg_schema, table=create_table_name)

    def test_csv_to_table_long_column(self):
        # csv_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema,create_table_name))

        base_string = 'text'*150
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\varchar.csv"
        db.dfquery("select '{}' as long_col".format(base_string)).to_csv(fp, index=False)
        db.import_data(file_path=fp, table=create_table_name, schema=pg_schema, long_varchar_check=True)

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema,create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Confirm long columns are not truncated and are equal with table from long_varchar_check=True
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        db.drop_table(schema=pg_schema, table=create_table_name)

    # Temp test is in logging tests

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(db)
        db.cleanup_new_tables()


class TestCsvToTableMS:
    @classmethod
    def setup_class(cls):
        return

    def test_csv_to_table_basic(self):
        # csv_to_table
        if sql.table_exists(schema=sql_schema, table=create_table_name):
            sql.query('drop table {}.{}'.format(sql_schema, create_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test.csv"
        sql.import_data(file_type='csv', file_path=fp, table=create_table_name, schema=sql_schema)

        # Check to see if table is in database
        assert sql.table_exists(table=create_table_name, schema=sql_schema)
        sql_df = sql.dfquery("select * from {}.{}".format(sql_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df, csv_df)

        # Cleanup
        sql.drop_table(schema=sql_schema, table=create_table_name)

    def test_csv_to_table_basic_no_type(self):
        # csv_to_table
        if sql.table_exists(schema=sql_schema, table=create_table_name):
            sql.query('drop table {}.{}'.format(sql_schema, create_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test.csv"
        sql.import_data(file_path=fp, table=create_table_name, schema=sql_schema)

        # Check to see if table is in database
        assert sql.table_exists(table=create_table_name, schema=sql_schema)
        sql_df = sql.dfquery("select * from {}.{}".format(sql_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df, csv_df)

        # Cleanup
        sql.drop_table(schema=sql_schema, table=create_table_name)

    def test_csv_to_table_column_override(self):
        sql.drop_table(schema=sql_schema, table=create_table_name)

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\csv_override_ex.csv"
        test_df = pd.DataFrame([{'a': 1, 'b': 2, 'c': 3, 'd': 'text'}, {'a': 4, 'b': 5, 'c': 6, 'd': 'another'}])
        test_df.to_csv(fp)
        sql.import_data(file_path=fp, table=create_table_name, schema=sql_schema,
                         column_type_overrides={'a': 'numeric'})

        # Check to see if table is in database
        assert sql.table_exists(table=create_table_name, schema=sql_schema)

        # Assert df column types match override
        pd.testing.assert_frame_equal(pd.DataFrame(
            [{"column_name": 'a', "data_type": 'numeric'}, {"column_name": 'b', "data_type": 'bigint'},
             {"column_name": 'c', "data_type": 'bigint'}, {"column_name": 'd', "data_type": 'varchar'}]),
                                      sql.dfquery("""
                                            select distinct column_name, data_type
                                            from information_schema.columns
                                            where table_name = '{}' and lower(column_name) not like '%unnamed%';
                                      """.format(create_table_name))
                                      )

        # Cleanup
        sql.drop_table(schema=sql_schema, table=create_table_name)
        os.remove(fp)

    def test_csv_to_table_separator(self):
        # csv_to_table
        if sql.table_exists(schema=sql_schema, table=create_table_name):
            sql.query('drop table {}.{}'.format(sql_schema, create_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test2.csv"
        sql.import_data(file_path=fp, table=create_table_name, schema=sql_schema, sep="|")

        # Check to see if table is in database
        assert sql.table_exists(table=create_table_name, schema=sql_schema)
        sql_df = sql.dfquery("select * from {}.{}".format(sql_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp, sep='|')
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df, csv_df)

        # Cleanup
        sql.drop_table(schema=sql_schema, table=create_table_name)

    # what if the table has no header?
    def test_csv_to_table_no_header(self):
        # csv_to_table
        if sql.table_exists(schema=sql_schema, table=create_table_name):
            sql.query('drop table {}.{}'.format(sql_schema, create_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test3.csv"
        sql.import_data(file_path=fp, table=create_table_name, schema=sql_schema, sep="|")

        # did it enter the db correctly?
        assert sql.table_exists(table=create_table_name, schema=sql_schema)
        sql_df = sql.dfquery("select * from {}.{}".format(sql_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp, sep='|')
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference
        csv_df.columns = [c.replace('.', '') for c in list(csv_df.columns)]
        csv_df.columns = [c.lower() for c in list(csv_df.columns)]

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df, csv_df)

        # Cleanup
        sql.drop_table(schema=sql_schema, table=create_table_name)

    def test_csv_to_table_overwrite(self):
        if sql.table_exists(schema=sql_schema, table=create_table_name):
            sql.query('drop table {}.{}'.format(sql_schema, create_table_name))

        # Make a table to fill the eventual table location and confirm it exists
        # Add test_table
        sql.query("""
        create table {s}.{t} (test_col1 int, test_col2 int, geom geometry);
        insert into {s}.{t} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
        insert into {s}.{t} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
        """.format(s=sql_schema, t=create_table_name))
        assert sql.table_exists(table=create_table_name, schema=sql_schema)

        # csv_to_table
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test.csv"
        sql.import_data(file_path=fp, table=create_table_name, schema=sql_schema, overwrite=True)

        # Check to see if table is in database
        assert sql.table_exists(table=create_table_name, schema=sql_schema)
        sql_df = sql.dfquery("select * from {}.{}".format(sql_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference
        csv_df.columns = [c.replace('.', '') for c in list(csv_df.columns)]
        csv_df.columns = [c.lower() for c in list(csv_df.columns)]

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df, csv_df)

        # Cleanup
        sql.drop_table(schema=sql_schema, table=create_table_name)

    # Temp test is in logging tests

    def test_csv_to_table_long_column(self):
        # csv_to_table
        if sql.table_exists(schema=sql_schema, table=create_table_name):
            sql.drop_table(schema=sql_schema, table=create_table_name)

        base_string = 'text'*150
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\varchar.csv"
        sql.dfquery("select '{}' as long_col".format(base_string)).to_csv(fp, index=False)
        sql.import_data(file_path=fp, table=create_table_name, schema=sql_schema, long_varchar_check=True)

        # Check to see if table is in database
        assert sql.table_exists(table=create_table_name, schema=sql_schema)
        sql_df = sql.dfquery("select * from {}.{}".format(sql_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Confirm long columns are not truncated and are equal with table from long_varchar_check=True
        pd.testing.assert_frame_equal(sql_df, csv_df)

        # Cleanup
        sql.drop_table(schema=sql_schema, table=create_table_name)

    @classmethod
    def teardown_class(cls):
        sql.cleanup_new_tables()


# XLS imports tests

class TestXlsToTablePG:
    def test_xls_to_table_basic(self):
        # xls_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema,xls_table_name))
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls.xls"

        db.import_data(file_type='xls', file_path=fp, table=xls_table_name, schema=pg_schema)

        # Check to see if table is in database
        assert db.table_exists(table=xls_table_name, schema=pg_schema)
        db.query("alter table {}.{} drop column if exists ogc_fid".format(pg_schema, xls_table_name))
        db_df = db.dfquery("select * from {}.{}".format(pg_schema, xls_table_name))

        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp).rename(columns={"Unnamed: 0": "unnamed__0"})
        xls_df.columns = [c.lower().strip().replace(' ', '_').replace('.', '_').replace(':', '_') for c in xls_df.columns]

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, xls_df)

        # Cleanup
        db.drop_table(schema=pg_schema, table=xls_table_name)

    def test_xls_to_table_basic_no_type(self):
        # xls_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema,xls_table_name))
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls.xls"

        db.import_data(file_path=fp, table=xls_table_name, schema=pg_schema)

        # Check to see if table is in database
        assert db.table_exists(table=xls_table_name, schema=pg_schema)
        db.query("alter table {}.{} drop column if exists ogc_fid".format(pg_schema, xls_table_name))
        db_df = db.dfquery("select * from {}.{}".format(pg_schema, xls_table_name))

        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp).rename(columns={"Unnamed: 0": "unnamed__0"})
        xls_df.columns = [c.lower().strip().replace(' ', '_').replace('.', '_').replace(':', '_') for c in xls_df.columns]

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, xls_df)

        # Cleanup
        db.drop_table(schema=pg_schema, table=xls_table_name)

    def test_xls_to_table_override(self):
        # xls_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema, xls_table_name))
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls.xls"

        # Try first without column override; confirm will cast as bigint
        db.import_data(file_path=fp, table=xls_table_name, schema=pg_schema)

        # Check to see if table is in database
        assert db.table_exists(table=xls_table_name, schema=pg_schema)

        # Assert df column types match without override
        pd.testing.assert_frame_equal(
            pd.DataFrame(
                [{"column_name": 'a', "data_type": 'integer'}, {"column_name": 'b', "data_type": 'integer'}]),

            db.dfquery("""

                        select distinct column_name, data_type
                        from information_schema.columns
                        where table_name = '{}'
                        and table_schema = '{}'
                        and lower(column_name) not like '%unnamed%'
                        and lower(column_name) not like '%ogc_fid%';

                    """.format(xls_table_name, pg_schema))
        )

        # Now test with override
        db.import_data(file_path=fp, table=xls_table_name, schema=pg_schema, column_type_overrides={'b': 'varchar'},
            overwrite=True
        )

        # Check to see if table is in database
        assert db.table_exists(table=xls_table_name, schema=pg_schema)

        # Assert df column types match override
        pd.testing.assert_frame_equal(pd.DataFrame(
            [{"column_name": 'a', "data_type": 'bigint'}, {"column_name": 'b', "data_type": 'character varying'}]),

            db.dfquery("""

                select distinct column_name, data_type
                from information_schema.columns
                where table_name = '{}'
                and table_schema = '{}'
                and lower(column_name) not like '%unnamed%'
                and lower(column_name) not like '%ogc_fid%';

            """.format(xls_table_name, pg_schema))
        )

        # Cleanup
        db.drop_table(schema=pg_schema, table=xls_table_name)

    def test_xls_to_table_sheet(self):
        # xls_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema, xls_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls_with_sheet.xls"

        db.import_data(file_path=fp, sheet_name='AnotherSheet', overwrite=True, table=xls_table_name, schema=pg_schema)

        # Check to see if table is in database
        assert db.table_exists(table=xls_table_name, schema=pg_schema)
        db.query("alter table {}.{} drop column if exists ogc_fid".format(pg_schema, xls_table_name))
        db_df = db.dfquery("select * from {}.{}".format(pg_schema, xls_table_name))

        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp, sheet_name='AnotherSheet').rename(columns={"Unnamed: 0": "unnamed__0",
                                                                              "Unnamed: 0.1": "unnamed__0_1"})

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, xls_df)

        # Cleanup
        db.drop_table(schema=pg_schema, table=xls_table_name)

    def test_xls_to_table_sheet_int(self):
        # xls_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema, xls_table_name))

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls_with_sheet.xls"

        db.import_data(file_path=fp, table=xls_table_name, schema=pg_schema, sheet_name=1)

        # Check to see if table is in database
        assert db.table_exists(table=xls_table_name, schema=pg_schema)
        db.query("alter table {}.{} drop column if exists ogc_fid".format(pg_schema, xls_table_name))
        db_df = db.dfquery("select * from {}.{}".format(pg_schema, xls_table_name))


        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp, sheet_name='AnotherSheet').rename(columns={"Unnamed: 0": "unnamed__0",
                                                                              "Unnamed: 0.1": "unnamed__0_1"})

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, xls_df)

        # Cleanup
        db.drop_table(schema=pg_schema, table=xls_table_name)


class TestBulkXLSToTablePG:
    @classmethod
    def setup_class(cls):
        return

    def test_bulk_xls_to_table_multisheet(self):
        fp_xlsx = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\Test.xlsx"
        writer = pd.ExcelWriter(fp_xlsx)

        db.query('drop table if exists {}.{}'.format(db.default_schema, xls_table_name))

        # Save multi-sheet xlsx
        pd.DataFrame([1, 2], columns=["sheet1"]).to_excel(writer, 'Sheet1', index=False)
        pd.DataFrame([3, 4], columns=["sheet2"]).to_excel(writer, 'Sheet2', index=False)
        writer.save()

        # Try via bulk loader
        init_count = len(db.my_tables())
        db.import_data(file_path=fp_xlsx, table=xls_table_name, sheet_name="Sheet2")
        post_count = len(db.my_tables())

        # Check to see if table is in database (and only one table added)
        assert db.table_exists(schema=db.default_schema, table=xls_table_name)
        assert init_count + 1 == post_count

        # Df Equality
        db.query("alter table {} drop column if exists ogc_fid".format(xls_table_name))
        df1 = db.dfquery("select * from {}".format(xls_table_name))
        df2 = pd.DataFrame([3, 4], columns=["sheet2"])
        pd.testing.assert_frame_equal(df1, df2)

        # Cleanup
        db.drop_table(schema=db.default_schema, table=xls_table_name)
        # os.remove(fp_xlsx)  # TODO: this is failing and i have no idea why...

    @classmethod
    def teardown_class(cls):
        sql.cleanup_new_tables()


class TestXlsToTableMS:
    def test_xls_to_table_basic(self):
        # Define table name and cleanup
        if sql.table_exists(table=xls_table_name, schema=sql_schema):
            sql.query('drop table {}.{}'.format(sql_schema, xls_table_name))

        if sql.table_exists(table='stg_' + xls_table_name, schema=sql_schema):
            sql.query('drop table {}.{}'.format(sql_schema, 'stg_' + xls_table_name))

        # xls_to_table
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls.xls"
        sql.import_data(file_path=fp, table=xls_table_name, schema=sql_schema)

        # Check to see if table is in database
        assert sql.table_exists(table=xls_table_name, schema=sql_schema)
        sql_df = sql.dfquery("select * from {}.{}".format(sql_schema, xls_table_name))
        if 'ogr_fid' in sql_df.columns:
            sql_df = sql_df.drop(columns=['ogr_fid'])

        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp).rename(columns={"Unnamed: 0": "unnamed__0"})

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df, xls_df)

        # Cleanup
        sql.drop_table(schema=sql_schema, table=xls_table_name)

    def test_xls_to_table_override(self):
        # xls_to_table
        sql.drop_table(schema=sql_schema, table=xls_table_name)
        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls.xls"

        # Try first without column override; confirm will cast as bigint
        sql.import_data(file_path=fp,
            table=xls_table_name,
            schema=sql_schema
        )

        # Check to see if table is in database
        assert sql.table_exists(table=xls_table_name, schema=sql_schema)

        # Assert df column types match without override
        pd.testing.assert_frame_equal(pd.DataFrame(
            [{"column_name": 'a', "data_type": 'int'}, {"column_name": 'b', "data_type": 'int'}]),

            sql.dfquery("""

                        select distinct column_name, data_type
                        from information_schema.columns
                        where table_name = '{}'
                        and lower(column_name) not like '%unnamed%'
                        and lower(column_name) not like '%ogr_fid%';

                    """.format(xls_table_name))
        )

        # Now test with override
        sql.import_data(file_path=fp,
            table=xls_table_name,
            schema=sql_schema,
            column_type_overrides={'b': 'varchar'},
            overwrite=True
        )

        # Check to see if table is in database
        assert sql.table_exists(table=xls_table_name, schema=sql_schema)

        # Assert df column types match override
        pd.testing.assert_frame_equal(pd.DataFrame(
            [{"column_name": 'a', "data_type": 'bigint'}, {"column_name": 'b', "data_type": 'varchar'}]),

            sql.dfquery("""

                select distinct column_name, data_type
                from information_schema.columns
                where table_name = '{}' and lower(column_name) not like '%unnamed%'
                and lower(column_name) not like '%ogr_fid%';

            """.format(xls_table_name))
        )

        # Cleanup
        sql.drop_table(schema=sql_schema, table=xls_table_name)

    def test_xls_to_table_zsheet(self):
        # xls_to_table
        sql.drop_table(schema=sql_schema, table=xls_table_name)

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls_with_sheet.xls"

        sql.import_data(file_path=fp,
            table=xls_table_name, schema=sql_schema,
            sheet_name='AnotherSheet'
        )

        # Check to see if table is in database
        assert sql.table_exists(table=xls_table_name, schema=sql_schema)
        db_df = sql.dfquery("select * from {}.{}".format(sql_schema, xls_table_name))
        if 'ogr_fid' in db_df.columns:
            db_df = db_df.drop(columns=['ogr_fid'])

        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp, sheet_name='AnotherSheet').rename(columns={"Unnamed: 0": "unnamed__0",
                                                                              "Unnamed: 0.1": "unnamed__0_1"})

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, xls_df)

        # Cleanup
        sql.drop_table(schema=sql_schema, table=xls_table_name)

    def test_xls_to_table_sheet_int(self):
        # xls_to_table
        sql.drop_table(schema=sql_schema, table=xls_table_name)

        fp = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test_xls_with_sheet.xls"

        sql.import_data(file_path=fp, table=xls_table_name, schema=sql_schema, sheet_name=1)

        # Check to see if table is in database
        assert sql.table_exists(table=xls_table_name, schema=sql_schema)
        db_df = sql.dfquery("select * from {}.{}".format(sql_schema, xls_table_name))
        if 'ogr_fid' in db_df.columns:
            db_df = db_df.drop(columns=['ogr_fid'])

        # Get xls df via pd.read_excel; pd/ogr handle unnamed columns differently (: vs _)
        xls_df = pd.read_excel(fp, sheet_name='AnotherSheet').rename(columns={"Unnamed: 0": "unnamed__0",
                                                                              "Unnamed: 0.1": "unnamed__0_1"})

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, xls_df)

        # Cleanup
        sql.drop_table(schema=sql_schema, table=xls_table_name)


class TestBulkXLSToTableMS:
    @classmethod
    def setup_class(cls):
        return

    def test_bulk_xls_to_table_correct_functionality(self):
        sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                                server=config.get('SQL_DB', 'SERVER'),
                                database=config.get('SQL_DB', 'DB_NAME'),
                                user=config.get('SQL_DB', 'DB_USER'),
                                password=config.get('SQL_DB', 'DB_PASSWORD'))

        fp_xlsx = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\Test.xlsx"
        fp_xls = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\Test2.xls"

        if sql.table_exists(schema=sql.default_schema, table=xls_table_name):
            sql.query('drop table {}.{}'.format(sql.default_schema, xls_table_name))

        if sql.table_exists(schema=sql.default_schema, table=xls_table_name + "_2"):
            sql.query('drop table {}.{}'.format(sql.default_schema, xls_table_name + "_2"))

        # Make large XLSX file
        data = []
        for i in range(0, 20000):
            data.append((j for j in range(0, 20)))

        cols = ['ogr_ex_col_{}'.format(i) for i in range(0, 20)]

        sample_df = pd.DataFrame(data, columns=cols)
        sample_df.to_excel(fp_xlsx)
        sample_df.to_excel(fp_xls)

        # Try via bulk loader
        start_time = time.time()
        sql.import_data(file_path=fp_xlsx, table=xls_table_name)
        end_xlsx_time = time.time()

        sql.xls_to_table(input_file=fp_xls, table=xls_table_name + "_2")
        end_xls_time = time.time()

        xlsx_time = (end_xlsx_time - start_time)/60.0
        xls_time = (end_xls_time - end_xlsx_time)/60.0

        # This is an approximation to ensure xls and xlsx both went via ogr (roughly the same time
        assert ((xls_time*1.0)/xlsx_time < 2) and ((xls_time*1.0)/xlsx_time > 0)

        # Check to see if table is in database
        assert sql.table_exists(table=xls_table_name)
        assert sql.table_exists(table=xls_table_name + "_2")

        # Df Equality
        df1 = sql.dfquery("select * from {}".format(xls_table_name))
        df2 = sql.dfquery("select * from {}".format(xls_table_name + "_2"))
        commons_cols = set(df1.columns) - (set(df1.columns) - set(df2.columns))
        pd.testing.assert_frame_equal(df1[commons_cols], df2[commons_cols])

        # Cleanup
        sql.drop_table(schema=sql.default_schema, table=xls_table_name)
        sql.drop_table(schema=sql.default_schema, table=xls_table_name + "_2")
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
            sql.query('drop table {}.{}'.format(sql.default_schema, xls_table_name))

        # Save multi-sheet xlsx
        pd.DataFrame([1, 2], columns=["sheet1"]).to_excel(writer, 'Sheet1', index=False)
        pd.DataFrame([3, 4], columns=["sheet2"]).to_excel(writer, 'Sheet2', index=False)
        writer.save()

        # Try via bulk loader
        sql.import_data(file_path=fp_xlsx, table=xls_table_name, sheet_name="Sheet2")

        # Check to see if table is in database (and only one table added)
        assert sql.table_exists(table=xls_table_name)

        # Df Equality
        df1 = sql.dfquery("select sheet2 from {}".format(xls_table_name))
        df2 = pd.DataFrame([3, 4], columns=["sheet2"])
        pd.testing.assert_frame_equal(df1[['sheet2']], df2)

        # Cleanup
        sql.drop_table(schema=sql.default_schema, table=xls_table_name)
        # os.remove(fp_xlsx)  # TODO: this fails for some reason

    # Temp test is in logging tests

    @classmethod
    def teardown_class(cls):
        sql.cleanup_new_tables()


# todo: import shp and fc wrapper tests
class TestReadShpPG:
    @classmethod
    def setup_class(cls):
        helpers.set_up_shapefile()
        helpers.set_up_test_table_pg(db)

    def test_read_shp_basic(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        db.drop_table(schema=pg_schema, table=test_read_shp_table_name)

        # Read shp to new, test table
        db.import_data(file_path=os.path.join(fp, shp_name), table=test_read_shp_table_name, schema=pg_schema)


        # Assert read_shp happened successfully and contents are correct
        assert db.table_exists(schema=pg_schema, table=test_read_shp_table_name)
        table_df = db.dfquery('select * from {}.{}'.format(pg_schema, test_read_shp_table_name))

        assert set(table_df.columns) == {'gid', 'some_value', 'geom', 'ogc_fid'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently but mean the same (after mapping on QGIS)
        diff_df = db.dfquery("""
        select distinct st_distance(raw_inputs.geom,
        st_transform(st_setsrid(end_table.geom, 4326),2263)
        )::int as distance
        from (
            select 1 as id, st_setsrid(st_point(1015329.1, 213793.1), 2263) as geom
            union
            select 2 as id, st_setsrid(st_point(1015428.1, 213086.1), 2263) as geom
        ) raw_inputs
        join {}.{} end_table
        on raw_inputs.id=end_table.gid::int
        """.format(pg_schema, test_read_shp_table_name))

        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

        # Cleanup
        db.drop_table(schema=pg_schema, table=test_read_shp_table_name)

    """
    NEED TO CHANGE TEST FILE TO CHANGE TABLE NAME
    """
    def test_read_shp_no_table(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        db.drop_table(schema=pg_schema, table="test")

        # Read shp to new, test table
        db.import_data(file_path=os.path.join(fp, shp_name), schema=pg_schema)

        # Assert read_shp happened successfully and contents are correct
        assert db.table_exists(schema=pg_schema, table='test')
        table_df = db.dfquery(f'select * from {pg_schema}.test')

        assert set(table_df.columns) == {'some_value', 'ogc_fid', 'gid', 'geom'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded
        # differently but mean the same (after mapping on QGIS)
        diff_df = db.dfquery(f"""
        select distinct st_distance(raw_inputs.geom, 
            st_transform(st_setsrid(end_table.geom, 4326),2263))::int as distance
        from (
            select 1 as id, st_setsrid(st_point(1015329.1, 213793.1), 2263) as geom
            union
            select 2 as id, st_setsrid(st_point(1015428.1, 213086.1), 2263) as geom
        ) raw_inputs
        join {pg_schema}.test end_table
        on raw_inputs.id=end_table.gid::int
        """)
        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

        # Cleanup
        db.drop_table(schema=pg_schema, table='test')

    def test_read_shp_no_schema(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        db.drop_table(schema=db.default_schema, table=test_read_shp_table_name)

        # Read shp to new, test table
        db.import_data(file_path=os.path.join(fp, shp_name), table=test_read_shp_table_name)

        # Assert read_shp happened successfully and contents are correct
        assert db.table_exists(schema=db.default_schema, table=test_read_shp_table_name)
        table_df = db.dfquery('select * from {}'.format(test_read_shp_table_name))

        assert set(table_df.columns) == {'some_value', 'ogc_fid', 'gid', 'geom'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently but mean the same (after mapping on QGIS)
        diff_df = db.dfquery("""
        select distinct
        st_distance(raw_inputs.geom, st_transform(st_setsrid(end_table.geom, 4326),2263))::int distance
        from (
            select 1 as id, st_setsrid(st_point(1015329.1, 213793.1), 2263) as geom
            union
            select 2 as id, st_setsrid(st_point(1015428.1, 213086.1), 2263) as geom
        ) raw_inputs
        join {} end_table
        on raw_inputs.id=end_table.gid::int
        """.format(test_read_shp_table_name))

        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

        # Cleanup
        db.drop_table(schema=db.default_schema, table=test_read_shp_table_name)

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_shapefile()
        helpers.clean_up_test_table_pg(db)

#
class TestReadShpMS:
    @classmethod
    def setup_class(cls):
        helpers.set_up_shapefile()

    def test_read_shp_basic(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        sql.drop_table(schema=sql_schema, table=test_read_shp_table_name)

        # Read shp to new, test table
        sql.import_data(file_path=os.path.join(fp, shp_name), table=test_read_shp_table_name, schema=sql_schema)

        # Assert read_shp happened successfully and contents are correct
        assert sql.table_exists(schema=sql_schema, table=test_read_shp_table_name)

        # todo: this fails because odbc 17 driver isnt supporting geometry
        table_df = sql.dfquery('select ogr_fid, gid, some_value, geom.STAsText() geom from {}.{}'.format(sql_schema, test_read_shp_table_name))

        assert set(table_df.columns) == {'ogr_fid', 'gid', 'some_value', 'geom'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded
        # differently but mean the same (after mapping on QGIS)
        diff_df = sql.dfquery("""
        select distinct raw_inputs.geom.STDistance(end_table.geom) as distance
        from (
            --(select 1 as id, geometry::Point(1015329.1, 213793.1, 2263) as geom)
            (select 1 as id, geometry::Point(-73.887824777216764, 40.753434539618361, 2263) as geom)
            union all
            --(select 2 as id, geometry::Point(-73.887470730467783, 40.75149365677327, 2263) as geom)
            (select 2 as id, geometry::Point(-73.887470730467783, 40.75149365677327, 2263) as geom)
        ) raw_inputs
        join {}.{} end_table
        on raw_inputs.id=end_table.gid
        """.format(sql_schema, test_read_shp_table_name))


        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

        # Cleanup

    sql.drop_table(schema=sql_schema, table=test_read_shp_table_name)

    """
    NEED TO CHANGE TEST FILE TO CHANGE TABLE NAME
    """
    def test_read_shp_no_table(self):
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
        shp_name = "test.shp"

        # Assert successful
        assert shp_name in os.listdir(fp)
        sql.drop_table(schema=sql_schema, table='test')

        # Read shp to new, test table
        sql.import_data(file_path=os.path.join(fp, shp_name), schema=sql_schema)

        # Assert read_shp happened successfully and contents are correct
        assert sql.table_exists(schema=sql_schema, table='test')
        table_df = sql.dfquery(f'select ogr_fid, gid, some_value, geom.STAsText() geom from {sql_schema}.test')
        assert set(table_df.columns) == {'ogr_fid', 'gid', 'some_value', 'geom'}
        assert len(table_df) == 2

        # Assert distance between geometries is 0 when recreating from raw input
        # This method was used because the geometries themselves may be recorded differently but mean the same (after mapping on QGIS)
        diff_df = sql.dfquery(f"""
        select distinct raw_inputs.geom.STDistance(end_table.geom) as distance
        from (
            --(select 1 as id, geometry::Point(1015329.1, 213793.1, 2263) as geom)
            (select 1 as id, geometry::Point(-73.887824777216764, 40.753434539618361, 2263) as geom)
            union all
            --(select 2 as id, geometry::Point(-73.887470730467783, 40.75149365677327, 2263) as geom)
            (select 2 as id, geometry::Point(-73.887470730467783, 40.75149365677327, 2263) as geom)
        ) raw_inputs
        join {sql_schema}.test end_table
        on raw_inputs.id=end_table.gid
        """)
        assert len(diff_df) == 1
        assert int(diff_df.iloc[0]['distance']) == 0

        # Cleanup
        sql.drop_table(schema=sql_schema, table='test')

#     def test_read_shp_no_schema(self):
#         fp = os.path.join(os.path.dirname(os.path.abspath(__file__)))+'/test_data'
#         shp_name = "test.shp"
#
#         # Assert successful
#         assert shp_name in os.listdir(fp)
#         sql.drop_table(schema=sql.default_schema, table=test_read_shp_table_name)
#
#         # Read shp to new, test table
#         s = Shapefile(dbo=sql, path=fp, shp_name=shp_name, table=test_read_shp_table_name)
#         s.read_shp(print_cmd=True)
#
#         # Assert read_shp happened successfully and contents are correct
#         assert sql.table_exists(schema=sql.default_schema, table=test_read_shp_table_name)
#         table_df = sql.dfquery('select * from {}'.format(test_read_shp_table_name))
#         assert set(table_df.columns) == {'ogr_fid', 'gid', 'some_value', 'geom'}
#         assert len(table_df) == 2
#
#         # Assert distance between geometries is 0 when recreating from raw input
#         # This method was used because the geometries themselves may be recorded differently but mean the same (after mapping on QGIS)
#         diff_df = sql.dfquery("""
#         select distinct raw_inputs.geom.STDistance(end_table.geom) as distance
#         from (
#             (select 1 as id, geometry::Point(1015329.1, 213793.1, 2263) as geom)
#             union all
#             (select 2 as id, geometry::Point(1015428.1, 213086.1, 2263) as geom)
#         ) raw_inputs
#         join {} end_table
#         on raw_inputs.id=end_table.gid::int
#         """.format(test_read_shp_table_name))
#
#         assert len(diff_df) == 1
#         assert int(diff_df.iloc[0]['distance']) == 0
#
#         # Cleanup
#         sql.drop_table(schema=sql.default_schema, table=test_read_shp_table_name)
#
#     def test_read_shp_precision(self):
#         return
#
#     def test_read_shp_private(self):
#         # TODO: pending permissions defaults convo
#         return
#
#     def test_read_temp(self):
#         # TODO: pending temp functionality
#         return
#
#     def test_read_shp_encoding(self):
#         # TODO: add test with fix to special characters
#         return
#
#     @classmethod
#     def teardown_class(cls):
#         helpers.clean_up_shapefile()