import os

import configparser
import pandas as pd

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

table_name = 'test_df_to_table_{}'.format(db.user)


class TestDfToTableSchemaPG:
    def test_df_to_table_schema_pg_basic(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        db.drop_table(table=table_name, schema='working')

        # Assert does not already exist
        assert not db.table_exists(table=table_name, schema='working')

        # dataframe_to_table_schema
        i_s = db.dataframe_to_table_schema(df=test_df, table=table_name, schema='working')

        # Assert table exists
        assert db.table_exists(table=table_name, schema='working')

        # Assert 0 length
        table_df = db.dfquery('select * from working.{}'.format(table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        # Cleanup
        db.drop_table(table=table_name, schema='working')

    def test_df_to_table_schema_pg_override(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        db.drop_table(table=table_name, schema='working')

        # Assert does not already exist
        assert not db.table_exists(table=table_name, schema='working')

        # dataframe_to_table_schema
        i_s = db.dataframe_to_table_schema(df=test_df, table=table_name, schema='working')

        # Assert table exists
        assert db.table_exists(table=table_name, schema='working')

        # Assert 0 length
        table_df = db.dfquery('select * from working.{}'.format(table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        assert list(db.dfquery("""
        select distinct data_type
        from information_schema.columns
        where table_name = '{}';
        """.format(table_name))['data_type'])[0] == 'bigint'

        # Overwrite table with new column_type_overrides table
        i_s = db.dataframe_to_table_schema(df=test_df, table=table_name, schema='working', overwrite=True,
                                           column_type_overrides={'a': 'varchar'})

        # Assert table exists
        assert db.table_exists(table=table_name, schema='working')

        # Assert 0 length
        table_df = db.dfquery('select * from working.{}'.format(table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        # Assert new types, including column a as varchar
        pd.testing.assert_frame_equal(pd.DataFrame([{"column_name": 'a', "data_type": 'character varying'}, {"column_name": 'b', "data_type": 'bigint'},{"column_name": 'c', "data_type": 'bigint'}]),
                                      db.dfquery("""
                                          select distinct column_name, data_type
                                          from information_schema.columns
                                          where table_name = '{}';
                                      """.format(table_name))
                                      )

        # Cleanup
        db.drop_table(table=table_name, schema='working')

    def test_df_to_table_schema_pg_default_schema(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        db.drop_table(table=table_name, schema=db.default_schema)

        # Assert does not already exist
        assert not db.table_exists(table=table_name, schema=db.default_schema)

        # dataframe_to_table_schema
        db.dataframe_to_table_schema(df=test_df, table=table_name)

        # Assert table exists
        assert db.table_exists(table=table_name, schema=db.default_schema)

        # Assert 0 length
        table_df = db.dfquery('select * from {}'.format(table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        # Cleanup
        db.drop_table(table=table_name, schema=db.default_schema)

    def test_df_to_table_schema_pg_overwrite(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        db.drop_table(table=table_name, schema='working')

        # Assert does not already exist
        assert not db.table_exists(table=table_name, schema='working')

        # Dataframe_to_table_schema
        db.dataframe_to_table_schema(df=test_df, table=table_name, schema='working')

        # Assert table exists
        assert db.table_exists(table=table_name, schema='working')

        # Assert 0 length
        table_df = db.dfquery('select * from working.{}'.format(table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        # Make a new table
        test_df2 = pd.DataFrame([{"x": 1, "y": 2, "z": 3, "1": 4}, {"x": 5, "y": 6, "z": 7, "1": 8}])

        # Dataframe_to_table overwrite
        db.dataframe_to_table_schema(df=test_df2, table=table_name, schema='working', overwrite=True)

        # Assert table exists
        assert db.table_exists(table=table_name, schema='working')

        # Assert 0 length
        table_df = db.dfquery('select * from working.{}'.format(table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df2.columns)):
            assert list(test_df2.columns)[i] == list(table_df.columns)[i]

        assert len(test_df2.columns) == len(table_df.columns)

        # Cleanup
        db.drop_table(table=table_name, schema='working')

    # Log tests are in test_dbconnect


class TestDfToTableSchemaMS:
    def test_df_to_table_schema_ms_basic(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        sql.drop_table(table=table_name, schema='dbo')

        # Assert does not already exist
        assert not sql.table_exists(table=table_name, schema='dbo')

        # dataframe_to_table_schema
        sql.dataframe_to_table_schema(df=test_df, table=table_name, schema='dbo')

        # Assert table exists
        assert sql.table_exists(table=table_name, schema='dbo')

        # Assert 0 length
        table_df = sql.dfquery('select * from [dbo].{}'.format(table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        # Cleanup
        sql.drop_table(table=table_name, schema='dbo')

    def test_df_to_table_schema_ms_override(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        sql.drop_table(table=table_name, schema='dbo')

        # Assert does not already exist
        assert not sql.table_exists(table=table_name, schema='dbo')

        # dataframe_to_table_schema
        i_s = sql.dataframe_to_table_schema(df=test_df, table=table_name, schema='dbo')

        # Assert table exists
        assert sql.table_exists(table=table_name, schema='dbo')

        # Assert 0 length
        table_df = sql.dfquery('select * from dbo.{}'.format(table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        assert list(sql.dfquery("""
        select distinct data_type
        from information_schema.columns
        where table_name = '{}';
        """.format(table_name))['data_type'])[0] == 'bigint'

        # Overwrite table with new column_type_overrides table
        i_s = sql.dataframe_to_table_schema(df=test_df, table=table_name, schema='dbo', overwrite=True,
                                            column_type_overrides={'a': 'varchar'})

        # Assert table exists
        assert sql.table_exists(table=table_name, schema='dbo')

        # Assert 0 length
        table_df = sql.dfquery('select * from dbo.{}'.format(table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        # Assert new types, including column a as varchar
        pd.testing.assert_frame_equal(pd.DataFrame([{"column_name": 'a', "data_type": 'varchar'}, {"column_name": 'b', "data_type": 'bigint'},{"column_name": 'c', "data_type": 'bigint'}]),
                                      sql.dfquery("""
                                          select distinct column_name, data_type
                                          from information_schema.columns
                                          where table_name = '{}';
                                      """.format(table_name))
                                      )

        # Cleanup
        sql.drop_table(table=table_name, schema='dbo')

    def test_df_to_table_schema_ms_default_schema(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        sql.drop_table(table=table_name, schema=sql.default_schema)

        # Assert does not already exist
        assert not sql.table_exists(table=table_name, schema=sql.default_schema)

        # dataframe_to_table_schema
        sql.dataframe_to_table_schema(df=test_df, table=table_name)

        # Assert table exists
        assert sql.table_exists(table=table_name, schema=sql.default_schema)

        # Assert 0 length
        table_df = sql.dfquery('select * from {}'.format(table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        # Cleanup
        sql.drop_table(table=table_name, schema=sql.default_schema)

    def test_df_to_table_schema_ms_overwrite(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        sql.drop_table(table=table_name, schema='dbo')

        # Assert does not already exist
        assert not sql.table_exists(table=table_name, schema='dbo')

        # Dataframe_to_table_schema
        sql.dataframe_to_table_schema(df=test_df, table=table_name, schema='dbo')

        # Assert table exists
        assert sql.table_exists(table=table_name, schema='dbo')

        # Assert 0 length
        table_df = sql.dfquery('select * from [dbo].{}'.format(table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        # Make a new table
        test_df2 = pd.DataFrame([{"x": 1, "y": 2, "z": 3, "1": 4}, {"x": 5, "y": 6, "z": 7, "1": 8}])

        # Dataframe_to_table overwrite
        sql.dataframe_to_table_schema(df=test_df2, table=table_name, schema='dbo', overwrite=True)

        # Assert table exists
        assert sql.table_exists(table=table_name, schema='dbo')

        # Assert 0 length
        table_df = sql.dfquery('select * from [dbo].{}'.format(table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df2.columns)):
            assert list(test_df2.columns)[i] == list(table_df.columns)[i]

        assert len(test_df2.columns) == len(table_df.columns)

        # Cleanup
        sql.drop_table(table=table_name, schema='dbo')

    # Log tests are in test_dbconnect


class TestDfToTablePG:
    def test_df_to_table_pg_basic(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])

        # Assert does not already exist
        assert not db.table_exists(table=table_name, schema='working')

        # Dataframe_to_table
        db.dataframe_to_table(df=test_df, table=table_name, schema='working')

        # Assert table exists
        assert db.table_exists(table=table_name, schema='working')

        # Assert table correctness
        table_df = db.dfquery('select * from working.{}'.format(table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        # Cleanup
        db.drop_table(table=table_name, schema='working')

    def test_df_to_table_pg_override(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])

        # Assert does not already exist
        assert not db.table_exists(table=table_name, schema='working')

        # Dataframe_to_table
        db.dataframe_to_table(df=test_df, table=table_name, schema='working')

        # Assert table exists
        assert db.table_exists(table=table_name, schema='working')

        # Assert table correctness
        table_df = db.dfquery('select * from working.{}'.format(table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        assert list(db.dfquery("""
         select distinct data_type
         from information_schema.columns
         where table_name = '{}';
         """.format(table_name))['data_type'])[0] == 'bigint'

        # Overwrite table with new column_type_overrides table
        db.dataframe_to_table(df=test_df, table=table_name, schema='working', overwrite=True,
                                                 column_type_overrides={'a': 'varchar'})

        # Assert table exists
        assert db.table_exists(table=table_name, schema='working')

        # Assert column type changed (column a is text)
        test_df = pd.DataFrame([{"a": '1', "b": 2, "c": 3}, {"a": '4', "b": 5, "c": 6}])
        table_df = db.dfquery('select * from working.{}'.format(table_name))

        pd.testing.assert_frame_equal(test_df, table_df)

        # Cleanup
        db.drop_table(table=table_name, schema='working')

    def test_df_to_table_pg_default_schema(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])

        # Assert does not already exist
        assert not db.table_exists(table=table_name, schema=db.default_schema)

        # Dataframe_to_table
        db.dataframe_to_table(df=test_df, table=table_name)

        # Assert table exists
        assert db.table_exists(table=table_name, schema=db.default_schema)

        # Assert table correctness
        table_df = db.dfquery('select * from {}'.format(table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        # Cleanup
        db.drop_table(table=table_name, schema=db.default_schema)

    def test_df_to_table_pg_overwrite(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])

        # Assert does not already exist
        assert not db.table_exists(table=table_name, schema='working')

        # Dataframe_to_table
        db.dataframe_to_table(df=test_df, table=table_name, schema='working')

        # Assert table exists
        assert db.table_exists(table=table_name, schema='working')

        # Assert table correctness
        table_df = db.dfquery('select * from working.{}'.format(table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        # Make a new table
        test_df2 = pd.DataFrame([{"x": 1, "y": 2, "z": 3}, {"x": 4, "y": 5, "z": 6}])

        # Dataframe_to_table overwrite
        db.dataframe_to_table(df=test_df2, table=table_name, schema='working', overwrite=True)

        # Assert table exists
        assert db.table_exists(table=table_name, schema='working')

        # Assert table correctness
        table_df = db.dfquery('select * from working.{}'.format(table_name))
        pd.testing.assert_frame_equal(test_df2, table_df)

        # Cleanup
        db.drop_table(table=table_name, schema='working')

    def test_df_to_table_pg_input_schema(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])

        # Assert does not already exist
        assert not db.table_exists(table=table_name, schema='working')

        # Dataframe_to_table
        db.dataframe_to_table(df=test_df, table=table_name, schema='working')

        # Assert table exists
        assert db.table_exists(table=table_name, schema='working')

        # Assert table correctness
        table_df = db.dfquery('select * from working.{}'.format(table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        # Add more with input schema for existing table
        additional_df = pd.DataFrame([{"a": 7, "b": 8, "c": 9}, {"a": 10, "b": 11, "c": 12}])
        db.dataframe_to_table(df=additional_df, table=table_name, schema='working',
                              table_schema=[['a', 'integer'], ['b', 'integer'], ['c', 'integer']])

        # Assert table correctness with addition
        table_df = db.dfquery('select * from working.{}'.format(table_name))

        pd.testing.assert_frame_equal(pd.concat([test_df, additional_df], axis=0).reset_index(drop=True), table_df)

        # Cleanup
        db.drop_table(table=table_name, schema='working')

    # Log tests are in test_dbconnect


class TestDfToTableMS:
    def test_df_to_table_ms_basic(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        sql.drop_table(table=table_name, schema=sql.default_schema)

        # Assert does not already exist
        assert not sql.table_exists(table=table_name)

        # Dataframe_to_table
        sql.dataframe_to_table(df=test_df, table=table_name)

        # Assert table exists
        assert sql.table_exists(table=table_name)

        # Assert table correctness
        table_df = sql.dfquery('select * from {}'.format(table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        # Cleanup
        sql.drop_table(table=table_name, schema=sql.default_schema)

    def test_df_to_table_ms_override(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])

        # Assert does not already exist
        assert not sql.table_exists(table=table_name, schema=sql.default_schema)

        # Dataframe_to_table
        sql.dataframe_to_table(df=test_df, table=table_name, schema=sql.default_schema)

        # Assert table exists
        assert sql.table_exists(table=table_name, schema=sql.default_schema)

        # Assert table correctness
        table_df = sql.dfquery('select * from {}'.format(table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        assert list(sql.dfquery("""
          select distinct data_type
          from information_schema.columns
          where table_name = '{}';
          """.format(table_name))['data_type'])[0] == 'bigint'

        # Overwrite table with new column_type_overrides table
        sql.dataframe_to_table(df=test_df, table=table_name, schema=sql.default_schema, overwrite=True,
                               column_type_overrides={'a': 'varchar'})

        # Assert table exists
        assert sql.table_exists(table=table_name, schema=sql.default_schema)

        # Assert column type changed (column a is text)
        test_df = pd.DataFrame([{"a": '1', "b": 2, "c": 3}, {"a": '4', "b": 5, "c": 6}])
        table_df = sql.dfquery('select * from {}'.format(table_name))

        pd.testing.assert_frame_equal(test_df, table_df)

        # Cleanup
        sql.drop_table(table=table_name, schema=sql.default_schema)

    def test_df_to_table_ms_schema(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])

        # Assert does not already exist
        assert not sql.table_exists(table=table_name, schema='dbo')

        # Dataframe_to_table
        sql.dataframe_to_table(df=test_df, table=table_name, schema='dbo')

        # Assert table exists
        assert sql.table_exists(table=table_name, schema='dbo')

        # Assert table correctness
        table_df = sql.dfquery('select * from [dbo].{}'.format(table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        # Cleanup
        sql.drop_table(table=table_name, schema='dbo')

    def test_df_to_table_ms_overwrite(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])

        # Assert does not already exist
        assert not sql.table_exists(table=table_name, schema='dbo')

        # Dataframe_to_table
        sql.dataframe_to_table(df=test_df, table=table_name, schema='dbo')

        # Assert table exists
        assert sql.table_exists(table=table_name, schema='dbo')

        # Assert table correctness
        table_df = sql.dfquery('select * from [dbo].{}'.format(table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        # Make a new table
        test_df2 = pd.DataFrame([{"x": 1, "y": 2, "z": 3}, {"x": 4, "y": 5, "z": 6}])

        # Dataframe_to_table overwrite
        sql.dataframe_to_table(df=test_df2, table=table_name, schema='dbo', overwrite=True)

        # Assert table (still) exists
        assert sql.table_exists(table=table_name, schema='dbo')

        # Assert table correctness
        table_df = sql.dfquery('select * from [dbo].{}'.format(table_name))
        pd.testing.assert_frame_equal(test_df2, table_df)

        # Cleanup
        sql.drop_table(table=table_name, schema='dbo')

    def test_df_to_table_ms_input_schema(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        sql.drop_table(table=table_name, schema='dbo')

        # Assert does not already exist
        assert not sql.table_exists(table=table_name, schema='dbo')

        # Dataframe_to_table
        sql.dataframe_to_table(df=test_df, table=table_name, schema='dbo')

        # Assert table exists
        assert sql.table_exists(table=table_name, schema='dbo')

        # Assert table correctness
        table_df = sql.dfquery('select * from [dbo].{}'.format(table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        # Add more with input schema for existing table
        additional_df = pd.DataFrame([{"a": 7, "b": 8, "c": 9}, {"a": 10, "b": 11, "c": 12}])
        sql.dataframe_to_table(df=additional_df, table=table_name, schema='dbo',
                               table_schema=[['a', 'integer'], ['b', 'integer'], ['c', 'integer']])

        # Assert table correctness with addition
        table_df = sql.dfquery('select * from [dbo].{}'.format(table_name))

        pd.testing.assert_frame_equal(pd.concat([test_df, additional_df], axis=0).reset_index(drop=True), table_df)

        # Cleanup
        sql.drop_table(table=table_name, schema='dbo')

    # Log tests are in test_dbconnect
