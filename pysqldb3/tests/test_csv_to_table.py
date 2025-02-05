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

dbt = pysqldb.DbConnect(allow_temp_tables=True, inherits_from=db)


sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                        server=config.get('SQL_DB', 'SERVER'),
                        database=config.get('SQL_DB', 'DB_NAME'),
                        user=config.get('SQL_DB', 'DB_USER'),
                        password=config.get('SQL_DB', 'DB_PASSWORD'))

sqlt = pysqldb.DbConnect(allow_temp_tables=True, inherits_from=sql)

pg_table_name = 'pg_test_table_{}'.format(db.user)
create_table_name = 'sample_acs_test_csv_to_table_{}'.format(db.user)
pg_schema = 'working'
sql_schema = 'dbo'

class TestCsvToTablePG:
    @classmethod
    def setup_class(cls):
        # helpers.set_up_test_table_pg(db)
        helpers.set_up_test_csv()

    def test_csv_to_table_basic(self):
        # csv_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema, create_table_name))

        fp = helpers.DIR + "\\test.csv"
        db.csv_to_table(input_file=fp, table=create_table_name, schema=pg_schema)

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

    def test_csv_to_table_basic_blank_col(self):
        # csv_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema, create_table_name))

        fp = helpers.DIR + "\\test6.csv"
        db.csv_to_table(input_file=fp, table=create_table_name, schema=pg_schema)

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns - non-null columns
        pd.testing.assert_frame_equal(db_df[['id', 'col1', 'col2']], csv_df[['id', 'col1', 'col2']])

        # assert null column in db is varchar
        assert [_ for _ in db.get_table_columns(create_table_name, schema=pg_schema) if _[0]=='col3'][0][1] == 'character varying (500)'

        # Cleanup
        db.drop_table(schema=pg_schema, table=create_table_name)

    def test_csv_to_table_basic_pyarrow(self):
        # csv_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema, create_table_name))

        fp = helpers.DIR + "\\test.csv"
        db.csv_to_table_pyarrow(input_file=fp, table=create_table_name, schema=pg_schema)

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

    def test_csv_to_table_basic_auto_date(self):
        data = {'id': {0: 1, 1: 2, 2: 3, 3: 4, 4: 5},
                'year': {0: 1981, 1: 2009, 2: 1954, 3: 1993, 4: 1973},
                'sale date': {0: '1/1/2015', 1: '2/25/2012', 2: '7/9/2018', 3: '12/2/2021', 4: '11/13/1995'},
                'false Date': {0: 0, 1: 0, 2: 0, 3:0, 4: 0}
                }
        df = pd.DataFrame(data)

        df.to_csv(helpers.DIR + "\\test_dates.csv", index=False)


        # csv_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema, create_table_name))

        fp = helpers.DIR + "\\test_dates.csv"
        db.csv_to_table(input_file=fp, table=create_table_name, schema=pg_schema)

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_').lower() for c in list(csv_df.columns)]  # TODO: look into this difference
        csv_df= csv_df.astype({'false_date': 'string'})

        # Assert df equality, excluding dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df, check_dtype=False)
        # assert that dates have been set to varchar
        for (name, typ) in db.get_table_columns(create_table_name, schema=pg_schema):
            if 'date' in name.lower():
                print (name, typ)
                assert typ == 'character varying (500)'
        # Cleanup
        db.drop_table(schema=pg_schema, table=create_table_name)
        os.remove(fp)

    def test_csv_to_table_basic_auto_date_w_override(self):
        data = {'id': {0: 1, 1: 2, 2: 3, 3: 4, 4: 5},
                'year': {0: 1981, 1: 2009, 2: 1954, 3: 1993, 4: 1973},
                'sale date': {0: '1/1/2015', 1: '2/25/2012', 2: '7/9/2018', 3: '12/2/2021', 4: '11/13/1995'},
                'false Date': {0: 0, 1: 0, 2: 0, 3:0, 4: 0}
                }
        df = pd.DataFrame(data)

        df.to_csv(helpers.DIR + "\\test_dates.csv", index=False)


        # csv_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema, create_table_name))

        fp = helpers.DIR + "\\test_dates.csv"
        db.csv_to_table(input_file=fp, table=create_table_name, schema=pg_schema, column_type_overrides={'false_date':'int'})

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_').lower() for c in list(csv_df.columns)]  # TODO: look into this difference
        csv_df= csv_df.astype({'false_date': 'int'})

        # Assert df equality, excluding dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df, check_dtype=False)
        # assert that dates have been set to varchar
        for (name, typ) in db.get_table_columns(create_table_name, schema=pg_schema):
            if 'date' in name.lower() and name.lower() !='false_date':
                print (name, typ)
                assert typ == 'character varying (500)'
        # Cleanup
        db.drop_table(schema=pg_schema, table=create_table_name)
        os.remove(fp)

    def test_csv_to_table_basic_skip_rows(self):
        # csv_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema, create_table_name))

        fp = helpers.DIR + "\\test4.csv"
        db.csv_to_table(input_file=fp, table=create_table_name, schema=pg_schema,
                        skiprows=1, skipfooter=1)

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp, skiprows=1, skipfooter=1)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        db.drop_table(schema=pg_schema, table=create_table_name)

    def test_csv_to_table_basic_skip_rows_pyarrow(self):
        # csv_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema, create_table_name))

        fp = helpers.DIR + "\\test4.csv"
        db.csv_to_table_pyarrow(input_file=fp, table=create_table_name, schema=pg_schema,
                        skip_rows=1)

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp, skiprows=1)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        db.drop_table(schema=pg_schema, table=create_table_name)

    def test_csv_to_table_column_override(self):
        db.drop_table(schema=pg_schema, table=create_table_name)
        fp = helpers.DIR + "\\csv_override_ex.csv"
        test_df = pd.DataFrame([{'a': 1, 'b': 2, 'c': 3, 'd': 'text'}, {'a': 4, 'b': 5, 'c': 6, 'd': 'another'}])
        test_df.to_csv(fp)
        db.csv_to_table(input_file=fp, table=create_table_name, schema=pg_schema, column_type_overrides={'a': 'varchar'})

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema, create_table_name))

        # Modify to make column types altered
        altered_column_type_df = pd.DataFrame([{'a': '1', 'b': 2, 'c': 3, 'd': 'text'}, {'a': '4', 'b': 5, 'c': 6, 'd': 'another'}])

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

    def test_csv_to_table_column_override_pyarrow(self):
        db.drop_table(schema=pg_schema, table=create_table_name)
        fp = helpers.DIR + "\\csv_override_ex.csv"
        test_df = pd.DataFrame([{'a': 1, 'b': 2, 'c': 3, 'd': 'text'}, {'a': 4, 'b': 5, 'c': 6, 'd': 'another'}])
        test_df.to_csv(fp)
        db.csv_to_table_pyarrow(
            input_file=fp, table=create_table_name, schema=pg_schema, column_type_overrides={'a': 'varchar'})

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema, create_table_name))

        # Modify to make column types altered
        altered_column_type_df = pd.DataFrame(
            [{'a': '1', 'b': 2, 'c': 3, 'd': 'text'}, {'a': '4', 'b': 5, 'c': 6, 'd': 'another'}])

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

        fp = helpers.DIR + "\\test2.csv"
        db.csv_to_table(
            input_file=fp,
            table=create_table_name, schema=pg_schema,
            sep="|"
        )

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema, create_table_name))

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
        db.query('drop table if exists {}.{}'.format(pg_schema,create_table_name))

        fp = helpers.DIR + "\\test3.csv"
        db.csv_to_table(
            input_file=fp,
            table=create_table_name, schema=pg_schema, sep="|")

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
        fp = helpers.DIR + "\\test.csv"
        db.csv_to_table(input_file=fp, table=create_table_name, schema=pg_schema, overwrite=True)

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

    def test_csv_to_table_long_column(self):
        # csv_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema, create_table_name))

        base_string = 'text'*150
        fp = helpers.DIR + "\\varchar.csv"
        db.dfquery("select '{}' as long_col".format(base_string)).to_csv(fp, index=False)
        db.csv_to_table(input_file=fp, table=create_table_name, schema=pg_schema, long_varchar_check=True)

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema, create_table_name))

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


class TestBulkCSVToTablePG:
    @classmethod
    def setup_class(cls):
        return

    def test_bulk_csv_to_table_basic(self):
        # bulk_csv_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema, create_table_name))

        fp = helpers.DIR + "\\test.csv"
        input_schema = db.dataframe_to_table_schema(df=pd.read_csv(fp), table=create_table_name, schema=pg_schema)
        db._bulk_csv_to_table(input_file=fp, table=create_table_name, schema=pg_schema, table_schema=input_schema)

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

    def test_bulk_csv_to_table_basic_blank_col(self):
        # bulk_csv_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema, create_table_name))

        fp = helpers.DIR + "\\test8.csv"
        input_schema = db.dataframe_to_table_schema(df=pd.read_csv(fp), table=create_table_name, schema=pg_schema)
        db._bulk_csv_to_table(input_file=fp, table=create_table_name, schema=pg_schema, table_schema=input_schema)

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference


        # Assert df equality, including dtypes and columns - non-null columns
        pd.testing.assert_frame_equal(db_df[['id', 'col1', 'col2']], csv_df[['id', 'col1', 'col2']])

        # assert null column in db is varchar
        assert [_ for _ in db.get_table_columns(create_table_name, schema=pg_schema) if _[0] == 'col3'][0][
                   1] == 'character varying'

        # Cleanup
        db.drop_table(schema=pg_schema, table=create_table_name)

    def test_bulk_csv_to_table_basic_sparse_data(self):
        # bulk_csv_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema, pg_table_name))

        fp = helpers.DIR + "\\test8.csv"
        input_schema = db.dataframe_to_table_schema(df=pd.read_csv(fp), table=pg_table_name, schema=pg_schema)
        db._bulk_csv_to_table(input_file=fp, table=pg_table_name, schema=pg_schema, table_schema=input_schema)

        # Check to see if table is in database
        assert db.table_exists(table=pg_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema, pg_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns - non-null columns
        pd.testing.assert_frame_equal(db_df[['id', 'col1', 'col2']], csv_df[['id', 'col1', 'col2']])

        # assert null column in db is varchar
        assert [_ for _ in db.get_table_columns(pg_table_name, schema=pg_schema) if _[0] == 'col3'][0][
                   1] == 'character varying'

        # Cleanup
        db.drop_table(schema=pg_schema, table=pg_table_name)

    def test_bulk_csv_to_table_basic_kwargs(self):
        # csv_to_table
        db.query('drop table if exists {}.{}'.format(pg_schema, create_table_name))

        fp = helpers.DIR + "\\test5.csv"
        db.csv_to_table(input_file=fp, table=create_table_name, schema=pg_schema,
                        skiprows=1)

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        db_df = db.dfquery("select * from {}.{}".format(pg_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp, skiprows=1)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

    def test_bulk_csv_to_table_default_schema(self):
        # bulk_csv_to_table
        db.query('drop table if exists {}'.format(create_table_name))

        fp = helpers.DIR + "\\test.csv"
        input_schema = db.dataframe_to_table_schema(df=pd.read_csv(fp), table=create_table_name)
        db._bulk_csv_to_table(input_file=fp, table=create_table_name, table_schema=input_schema)

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name)
        db_df = db.dfquery("select * from {}".format(create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        db.drop_table(schema=db.default_schema, table=create_table_name)

    def test_bulk_csv_to_table_long_column(self):
        # csv_to_table
        if db.table_exists(schema=pg_schema, table=create_table_name):
            db.drop_table(schema=pg_schema, table=create_table_name)

        fp = helpers.DIR + "\\varchar.csv"
        pd.DataFrame(['text'*150]*10000, columns=['long_column']).to_csv(fp)
        db.csv_to_table(input_file=fp, table=create_table_name, schema=pg_schema, long_varchar_check=True)

        # Check to see if table is in database
        assert db.table_exists(table=create_table_name, schema=pg_schema)
        sql_df = db.dfquery("select * from {}.{}".format(pg_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Confirm long columns are not truncated and are equal with table from long_varchar_check=True
        pd.testing.assert_frame_equal(sql_df[['long_column']], csv_df[['long_column']])

        # Cleanup
        db.drop_table(schema=pg_schema, table=create_table_name)

    def test_bulk_csv_to_table_input_schema(self):
        # Test input schema
        return

    # Temp test is in logging tests

    @classmethod
    def teardown_class(cls):
        db.cleanup_new_tables()


class TestCsvToTableMS:
    @classmethod
    def setup_class(cls):
        return

    def test_csv_to_table_basic(self):
        # csv_to_table
        if sql.table_exists(schema=sql_schema, table=create_table_name):
            sql.query('drop table {}.{}'.format(sql_schema, create_table_name))

        fp = helpers.DIR + "\\test.csv"
        sql.csv_to_table(input_file=fp, table=create_table_name, schema=sql_schema)

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

    def test_csv_to_table_basic_blank_col(self):
        # csv_to_table
        sql.query('drop table if exists {}.{}'.format(sql_schema, create_table_name))

        fp = helpers.DIR + "\\test6.csv"
        sql.csv_to_table(input_file=fp, table=create_table_name, schema=sql_schema)

        # Check to see if table is in database
        assert sql.table_exists(table=create_table_name, schema=sql_schema)
        db_df = sql.dfquery("select * from {}.{}".format(sql_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns - non-null columns
        pd.testing.assert_frame_equal(db_df[['id', 'col1', 'col2']], csv_df[['id', 'col1', 'col2']])

        # assert null column in db is varchar
        assert [_ for _ in sql.get_table_columns(create_table_name, schema=sql_schema) if _[0]=='col3'][0][1] == 'varchar (500)'

        # Cleanup
        sql.drop_table(schema=sql_schema, table=create_table_name)

    def test_csv_to_table_basic_pyarrow(self):
        # csv_to_table
        sql.query('drop table if exists {}.{}'.format(sql_schema, create_table_name))

        fp = helpers.DIR + "\\test.csv"
        sql.csv_to_table_pyarrow(input_file=fp, table=create_table_name, schema=sql_schema)

        # Check to see if table is in database
        assert sql.table_exists(table=create_table_name, schema=sql_schema)
        db_df = sql.dfquery("select * from {}.{}".format(sql_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        sql.drop_table(schema=sql_schema, table=create_table_name)

    def test_csv_to_table_basic_auto_date(self):
        data = {'id': {0: 1, 1: 2, 2: 3, 3: 4, 4: 5},
                'year': {0: 1981, 1: 2009, 2: 1954, 3: 1993, 4: 1973},
                'sale date': {0: '1/1/2015', 1: '2/25/2012', 2: '7/9/2018', 3: '12/2/2021', 4: '11/13/1995'},
                'false Date': {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}
                }
        df = pd.DataFrame(data)

        df.to_csv(helpers.DIR + "\\test_dates.csv", index=False)

        # csv_to_table
        sql.query('drop table if exists {}.{}'.format(sql_schema, create_table_name))

        fp = helpers.DIR + "\\test_dates.csv"
        sql.csv_to_table(input_file=fp, table=create_table_name, schema=sql_schema)

        # Check to see if table is in database
        assert sql.table_exists(table=create_table_name, schema=sql_schema)
        db_df = sql.dfquery("select * from {}.{}".format(sql_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_').lower() for c in list(csv_df.columns)]  # TODO: look into this difference
        csv_df = csv_df.astype({'false_date': 'string'})

        # Assert df equality, excluding dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df, check_dtype=False)
        # assert that dates have been set to varchar
        for (name, typ) in sql.get_table_columns(create_table_name, schema=sql_schema):
            if 'date' in name.lower():
                print(name, typ)
                assert typ == 'varchar (500)'
        # Cleanup
        sql.drop_table(schema=sql_schema, table=create_table_name)
        os.remove(fp)

    def test_csv_to_table_basic_auto_date_w_override(self):
        data = {'id': {0: 1, 1: 2, 2: 3, 3: 4, 4: 5},
                'year': {0: 1981, 1: 2009, 2: 1954, 3: 1993, 4: 1973},
                'sale date': {0: '1/1/2015', 1: '2/25/2012', 2: '7/9/2018', 3: '12/2/2021', 4: '11/13/1995'},
                'false Date': {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}
                }
        df = pd.DataFrame(data)

        df.to_csv(helpers.DIR + "\\test_dates.csv", index=False)

        # csv_to_table
        sql.query('drop table if exists {}.{}'.format(sql_schema, create_table_name))

        fp = helpers.DIR + "\\test_dates.csv"
        sql.csv_to_table(input_file=fp, table=create_table_name, schema=sql_schema,
                        column_type_overrides={'false_date': 'int'})

        # Check to see if table is in database
        assert sql.table_exists(table=create_table_name, schema=sql_schema)
        db_df = sql.dfquery("select * from {}.{}".format(sql_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_').lower() for c in list(csv_df.columns)]  # TODO: look into this difference
        csv_df = csv_df.astype({'false_date': 'int'})

        # Assert df equality, excluding dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df, check_dtype=False)
        # assert that dates have been set to varchar
        for (name, typ) in sql.get_table_columns(create_table_name, schema=sql_schema):
            if 'date' in name.lower() and name.lower() != 'false_date':
                print(name, typ)
                assert typ == 'varchar (500)'
        # Cleanup
        sql.drop_table(schema=sql_schema, table=create_table_name)
        os.remove(fp)

    def test_csv_to_table_basic_skip_rows(self):
        # csv_to_table
        sql.query('drop table if exists {}.{}'.format(sql_schema, create_table_name))

        fp = helpers.DIR + "\\test4.csv"
        sql.csv_to_table(input_file=fp, table=create_table_name, schema=sql_schema,
                        skiprows=1, skipfooter=1)

        # Check to see if table is in database
        assert sql.table_exists(table=create_table_name, schema=sql_schema)
        db_df = sql.dfquery("select * from {}.{}".format(sql_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp, skiprows=1, skipfooter=1)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        sql.drop_table(schema=sql_schema, table=create_table_name)

    def test_csv_to_table_basic_skip_rows_pyarrow(self):
        # csv_to_table
        sql.query('drop table if exists {}.{}'.format(sql_schema, create_table_name))

        fp = helpers.DIR + "\\test4.csv"
        sql.csv_to_table_pyarrow(input_file=fp, table=create_table_name, schema=sql_schema,
                        skip_rows=1)

        # Check to see if table is in database
        assert sql.table_exists(table=create_table_name, schema=sql_schema)
        db_df = sql.dfquery("select * from {}.{}".format(sql_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp, skiprows=1)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

        # Cleanup
        sql.drop_table(schema=sql_schema, table=create_table_name)

    def test_csv_to_table_column_override(self):
        sql.drop_table(schema=sql_schema, table=create_table_name)

        fp = helpers.DIR + "\\csv_override_ex.csv"
        test_df = pd.DataFrame([{'a': 1, 'b': 2, 'c': 3, 'd': 'text'}, {'a': 4, 'b': 5, 'c': 6, 'd': 'another'}])
        test_df.to_csv(fp)
        sql.csv_to_table(input_file=fp, table=create_table_name, schema=sql_schema,
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

    def test_csv_to_table_column_override_pyarrow(self):
        sql.drop_table(schema=sql_schema, table=create_table_name)
        fp = helpers.DIR + "\\csv_override_ex.csv"
        test_df = pd.DataFrame([{'a': 1, 'b': 2, 'c': 3, 'd': 'text'}, {'a': 4, 'b': 5, 'c': 6, 'd': 'another'}])
        test_df.to_csv(fp)
        sql.csv_to_table_pyarrow(
            input_file=fp, table=create_table_name, schema=sql_schema, column_type_overrides={'a': 'varchar'})

        # Check to see if table is in database
        assert sql.table_exists(table=create_table_name, schema=sql_schema)
        db_df = sql.dfquery("select * from {}.{}".format(sql_schema, create_table_name))

        # Modify to make column types altered
        altered_column_type_df = pd.DataFrame(
            [{'a': '1', 'b': 2, 'c': 3, 'd': 'text'}, {'a': '4', 'b': 5, 'c': 6, 'd': 'another'}])

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df[['a', 'b', 'c', 'd']], altered_column_type_df)

        # Assert df column types match override
        pd.testing.assert_frame_equal(pd.DataFrame([{"column_name": 'a', "data_type": 'varchar'},
                                                    {"column_name": 'b', "data_type": 'bigint'},
                                                    {"column_name": 'c', "data_type": 'bigint'},
                                                    {"column_name": 'd', "data_type": 'varchar'}]),
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

        fp = helpers.DIR + "\\test2.csv"
        sql.csv_to_table(
            input_file=fp,
            table=create_table_name, schema=sql_schema,
            sep="|"
        )

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

        fp = helpers.DIR + "\\test3.csv"
        sql.csv_to_table(
            input_file=fp,
            table=create_table_name, schema=sql_schema, sep="|")

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
        fp = helpers.DIR + "\\test.csv"
        sql.csv_to_table(input_file=fp, table=create_table_name, schema=sql_schema, overwrite=True)

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
        fp = helpers.DIR + "\\varchar.csv"
        sql.dfquery("select '{}' as long_col".format(base_string)).to_csv(fp, index=False)
        sql.csv_to_table(input_file=fp, table=create_table_name, schema=sql_schema, long_varchar_check=True)

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


class TestBulkCSVToTableMS:
    @classmethod
    def setup_class(cls):
        return

    def test_bulk_csv_to_table_basic(self):
        # bulk_csv_to_table
        if sql.drop_table(schema=sql_schema, table=create_table_name):
            sql.query('drop table {}.{}'.format(sql_schema, create_table_name))

        fp = helpers.DIR + "\\test.csv"
        input_schema = sql.dataframe_to_table_schema(df=pd.read_csv(fp), table=create_table_name, schema=sql_schema)
        sql._bulk_csv_to_table(input_file=fp, table=create_table_name, schema=sql_schema, table_schema=input_schema)

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

    def test_bulk_csv_to_table_basic_blank_col(self):
        # bulk_csv_to_table
        sql.query('drop table if exists {}.{}'.format(sql_schema, create_table_name))

        fp = helpers.DIR + "\\test8.csv"
        input_schema = sql.dataframe_to_table_schema(df=pd.read_csv(fp), table=create_table_name, schema=sql_schema)
        sql._bulk_csv_to_table(input_file=fp, table=create_table_name, schema=sql_schema, table_schema=input_schema)

        # Check to see if table is in database
        assert sql.table_exists(table=create_table_name, schema=sql_schema)
        db_df = sql.dfquery("select * from {}.{}".format(sql_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns - non-null columns
        pd.testing.assert_frame_equal(db_df[['id', 'col1', 'col2']], csv_df[['id', 'col1', 'col2']])

        # assert null column in db is varchar
        assert [_ for _ in sql.get_table_columns(create_table_name, schema=sql_schema) if _[0] == 'col3'][0][
                   1] == 'varchar (500)'

        # Cleanup
        sql.drop_table(schema=sql_schema, table=create_table_name)

    def test_bulk_csv_to_table_basic_sparse_data(self):
        # bulk_csv_to_table
        sql.query('drop table if exists {}.{}'.format(sql_schema, create_table_name))

        fp = helpers.DIR + "\\test8.csv"
        input_schema = sql.dataframe_to_table_schema(df=pd.read_csv(fp), table=create_table_name, schema=sql_schema)
        sql._bulk_csv_to_table(input_file=fp, table=create_table_name, schema=sql_schema, table_schema=input_schema)

        # Check to see if table is in database
        assert sql.table_exists(table=create_table_name, schema=sql_schema)
        db_df = sql.dfquery("select * from {}.{}".format(sql_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference


        # Assert df equality, including dtypes and columns - non-null columns
        pd.testing.assert_frame_equal(db_df[['id', 'col1', 'col2']], csv_df[['id', 'col1', 'col2']])

        # assert null column in db is varchar
        assert [_ for _ in sql.get_table_columns(create_table_name, schema=sql_schema) if _[0] == 'col3'][0][
                   1] == 'varchar (500)'

        # Cleanup
        sql.drop_table(schema=sql_schema, table=create_table_name)

    def test_bulk_csv_to_table_basic_kwargs(self):
        # csv_to_table
        sql.query('drop table if exists {}.{}'.format(sql_schema, create_table_name))

        fp = helpers.DIR + "\\test5.csv"
        sql.csv_to_table(input_file=fp, table=create_table_name, schema=sql_schema,
                        skiprows=1)

        # Check to see if table is in database
        assert sql.table_exists(table=create_table_name, schema=sql_schema)
        db_df = sql.dfquery("select * from {}.{}".format(sql_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp, skiprows=1)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(db_df, csv_df)

    def test_bulk_csv_to_table_default_schema(self):
        # bulk_csv_to_table
        if sql.table_exists(table=create_table_name):
            sql.query('drop table {}'.format(create_table_name))

        fp = helpers.DIR + "\\test.csv"
        input_schema = sql.dataframe_to_table_schema(df=pd.read_csv(fp), table=create_table_name)
        sql._bulk_csv_to_table(input_file=fp, table=create_table_name, table_schema=input_schema)

        # Check to see if table is in database
        # This example is linked to the mssql default server bug
        assert sql.table_exists(table=create_table_name)
        sql_df = sql.dfquery("select * from {}".format(create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Assert df equality, including dtypes and columns
        pd.testing.assert_frame_equal(sql_df, csv_df)

        # Cleanup
        sql.drop_table(schema=sql.default_schema, table=create_table_name)

    def test_bulk_csv_to_table_long_column(self):
        # csv_to_table
        if sql.table_exists(schema=sql_schema, table=create_table_name):
            sql.drop_table(schema=sql_schema, table=create_table_name)

        fp = helpers.DIR + "\\varchar.csv"
        pd.DataFrame(['text'*150]*10000, columns=['long_column']).to_csv(fp)
        sql.csv_to_table(input_file=fp, table=create_table_name, schema=sql_schema, long_varchar_check=True)

        # Check to see if table is in database
        assert sql.table_exists(table=create_table_name, schema=sql_schema)
        sql_df = sql.dfquery("select * from {}.{}".format(sql_schema, create_table_name))

        # Get csv df via pd.read_csv
        csv_df = pd.read_csv(fp)
        csv_df.columns = [c.replace(' ', '_') for c in list(csv_df.columns)]  # TODO: look into this difference

        # Confirm long columns are not truncated and are equal with table from long_varchar_check=True
        pd.testing.assert_frame_equal(sql_df[['long_column']], csv_df[['long_column']])

        # Cleanup
        sql.drop_table(schema=sql_schema, table=create_table_name)

    def test_bulk_csv_to_table_input_schema(self):
        # Test input schema
        return

    # Temp test is in logging tests

    @classmethod
    def teardown_class(cls):
        sql.cleanup_new_tables()


class TestCsvToTablePGTemp:
    @classmethod
    def setup_class(cls):
        # helpers.set_up_test_table_pg(db)
        helpers.set_up_test_csv()

    def test_basic_csv_to_table_tmp(self):
        # csv_to_table
        dbt.query('drop table if exists {}'.format(create_table_name))

        fp = helpers.DIR + "\\test.csv"
        dbt.csv_to_table(input_file=fp, table=create_table_name, temp_table=True)

        # Check to see if table is in database
        dbt.query(f"select * from {create_table_name}")
        assert len(dbt.data) == 5
        # check its not a real table
        assert not dbt.table_exists(table=create_table_name)

        # check it cant be accessed from another connection
        db.query(f"select * from {create_table_name}", strict=False)
        assert not db.data

        # disconnect and check table is no longer there
        dbt.disconnect(quiet=True)
        dbt.connect(quiet=True)
        dbt.query(f"select * from {create_table_name}", strict=False)
        assert not dbt.data

    def test_big_csv_to_table_tmp(self):
        # csv_to_table
        dbt.query('drop table if exists {}'.format(create_table_name))

        fp = helpers.DIR+"\\test8.csv"
        dbt.csv_to_table(input_file=fp, table=create_table_name, temp_table=True)

        # Check to see if table is in database
        dbt.query(f"select * from {create_table_name}")
        assert len(dbt.data) == 1000
        # check its not a real table
        assert not dbt.table_exists(table=create_table_name, schema=pg_schema)

        # check it cant be accessed from another connection
        db.query(f"select * from {create_table_name}", strict=False)
        assert not db.data

        # disconnect and check table is no longer there
        dbt.disconnect(quiet=True)
        dbt.connect(quiet=True)
        dbt.query(f"select * from {create_table_name}", strict=False)
        assert not dbt.data


class TestCsvToTableMSTemp:
    @classmethod
    def setup_class(cls):
        # helpers.set_up_test_table_pg(db)
        helpers.set_up_test_csv()

    def test_basic_csv_to_table_tmp(self):
        # csv_to_table
        sqlt.query('drop table {}'.format(create_table_name), strict=False)

        fp = helpers.DIR + "\\test.csv"
        sqlt.csv_to_table(input_file=fp, table=create_table_name, schema=pg_schema, temp_table=True)

        # Check to see if table is in database
        sqlt.query(f"select * from ##{create_table_name}")
        assert len(sqlt.data) == 5
        # check its not a real table
        assert not dbt.table_exists(table=create_table_name, schema=sql_schema)

        # check it can also be accessed from another connection
        sql.query(f"select * from ##{create_table_name}", strict=False)
        assert len(sql.data) == 5

        # disconnect and check table is no longer there
        sqlt.disconnect(quiet=True)
        sqlt.connect(quiet=True)
        sqlt.query(f"select * from ##{create_table_name}", strict=False)
        assert not sqlt.data

    def test_big_csv_to_table_tmp(self):
        # csv_to_table
        sqlt.query('drop table {}'.format(create_table_name), strict=False)

        fp = helpers.DIR+"\\test8.csv"
        sqlt.csv_to_table(input_file=fp, table=create_table_name, temp_table=True)

        # Check to see if table is in database
        sqlt.query(f"select * from ##{create_table_name}")
        assert len(sqlt.data) == 1000
        # check its not a real table
        assert not sqlt.table_exists(table=create_table_name, schema=sql_schema)

        # check it can also be accessed from another connection
        sql.query(f"select * from ##{create_table_name}", strict=False)
        assert len(sql.data) == 1000

        # disconnect and check table is no longer there
        sqlt.disconnect(quiet=True)
        sqlt.connect(quiet=True)
        sqlt.query(f"select * from ##{create_table_name}", strict=False)
        assert not sqlt.data