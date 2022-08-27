import os

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
pg_schema = 'working'
sql_schema='dbo'


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
        assert db.table_exists(table=create_table_name, schema='working')
        db_df = db.dfquery("select * from working.{}".format(create_table_name))

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
        assert db.table_exists(table=create_table_name, schema='working')
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
        assert db.table_exists(table=create_table_name, schema='working')
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
        assert db.table_exists(table=create_table_name, schema='working')
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
        sql.import_data(file_path=fp, table=create_table_name, schema='dbo', sep="|")

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
