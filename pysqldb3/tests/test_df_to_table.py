import os

import configparser
import pandas as pd

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

table_name = 'test_df_to_table_{user}'.format(user=pg_dbconn.username)


class TestDfToTableSchemaPG:
    def test_df_to_table_schema_pg_basic(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        pg_dbconn.drop_table(table_name=table_name, schema_name='working')

        # Assert does not already exist
        assert not pg_dbconn.table_exists(table_name=table_name, schema_name='working')

        # dataframe_to_table_schema
        i_s = pg_dbconn.dataframe_to_table_schema(df=test_df, table_name=table_name, schema_name='working')

        # Assert table exists
        assert pg_dbconn.table_exists(table_name=table_name, schema_name='working')

        # Assert 0 length
        table_df = pg_dbconn.dfquery('select * from working.{table}'.format(table=table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        # Cleanup
        pg_dbconn.drop_table(table_name=table_name, schema_name='working')

    def test_df_to_table_schema_pg_override(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        pg_dbconn.drop_table(table_name=table_name, schema_name='working')

        # Assert does not already exist
        assert not pg_dbconn.table_exists(table_name=table_name, schema_name='working')

        # dataframe_to_table_schema
        i_s = pg_dbconn.dataframe_to_table_schema(df=test_df, table_name=table_name, schema_name='working')

        # Assert table exists
        assert pg_dbconn.table_exists(table_name=table_name, schema_name='working')

        # Assert 0 length
        table_df = pg_dbconn.dfquery('select * from working.{table}'.format(table=table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        assert list(pg_dbconn.dfquery("""
        select distinct data_type
        from information_schema.columns
        where table_name = '{table}';
        """.format(table=table_name))['data_type'])[0] == 'bigint'

        # Overwrite table with new column_type_overrides table
        i_s = pg_dbconn.dataframe_to_table_schema(df=test_df, table_name=table_name, schema_name='working', overwrite=True,
                                                  column_type_overrides={'a': 'varchar'})

        # Assert table exists
        assert pg_dbconn.table_exists(table_name=table_name, schema_name='working')

        # Assert 0 length
        table_df = pg_dbconn.dfquery('select * from working.{table}'.format(table=table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        # Assert new types, including column a as varchar
        pd.testing.assert_frame_equal(pd.DataFrame([{"column_name": 'a', "data_type": 'character varying'},
                                                    {"column_name": 'b', "data_type": 'bigint'},
                                                    {"column_name": 'c', "data_type": 'bigint'}]),
                                      pg_dbconn.dfquery("""
                                          select distinct column_name, data_type
                                          from information_schema.columns
                                          where table_name = '{table}';
                                      """.format(table=table_name))
                                      )

        # Cleanup
        pg_dbconn.drop_table(table_name=table_name, schema_name='working')

    def test_df_to_table_schema_pg_default_schema(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        pg_dbconn.drop_table(table_name=table_name, schema_name=pg_dbconn.default_schema)

        # Assert does not already exist
        assert not pg_dbconn.table_exists(table_name=table_name, schema_name=pg_dbconn.default_schema)

        # dataframe_to_table_schema
        pg_dbconn.dataframe_to_table_schema(df=test_df, table_name=table_name)

        # Assert table exists
        assert pg_dbconn.table_exists(table_name=table_name, schema_name=pg_dbconn.default_schema)

        # Assert 0 length
        table_df = pg_dbconn.dfquery('select * from {table}'.format(table=table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        # Cleanup
        pg_dbconn.drop_table(table_name=table_name, schema_name=pg_dbconn.default_schema)

    def test_df_to_table_schema_pg_overwrite(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        pg_dbconn.drop_table(table_name=table_name, schema_name='working')

        # Assert does not already exist
        assert not pg_dbconn.table_exists(table_name=table_name, schema_name='working')

        # Dataframe_to_table_schema
        pg_dbconn.dataframe_to_table_schema(df=test_df, table_name=table_name, schema_name='working')

        # Assert table exists
        assert pg_dbconn.table_exists(table_name=table_name, schema_name='working')

        # Assert 0 length
        table_df = pg_dbconn.dfquery('select * from working.{table}'.format(table=table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        # Make a new table
        test_df2 = pd.DataFrame([{"x": 1, "y": 2, "z": 3, "1": 4}, {"x": 5, "y": 6, "z": 7, "1": 8}])

        # Dataframe_to_table overwrite
        pg_dbconn.dataframe_to_table_schema(df=test_df2, table_name=table_name, schema_name='working', overwrite=True)

        # Assert table exists
        assert pg_dbconn.table_exists(table_name=table_name, schema_name='working')

        # Assert 0 length
        table_df = pg_dbconn.dfquery('select * from working.{table}'.format(table=table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df2.columns)):
            assert list(test_df2.columns)[i] == list(table_df.columns)[i]

        assert len(test_df2.columns) == len(table_df.columns)

        # Cleanup
        pg_dbconn.drop_table(table_name=table_name, schema_name='working')

    # Log tests are in test_dbconnect


class TestDfToTableSchemaMS:
    def test_df_to_table_schema_ms_basic(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        ms_dbconn.drop_table(table_name=table_name, schema_name='dbo')

        # Assert does not already exist
        assert not ms_dbconn.table_exists(table_name=table_name, schema_name='dbo')

        # dataframe_to_table_schema
        ms_dbconn.dataframe_to_table_schema(df=test_df, table_name=table_name, schema_name='dbo')

        # Assert table exists
        assert ms_dbconn.table_exists(table_name=table_name, schema_name='dbo')

        # Assert 0 length
        table_df = ms_dbconn.dfquery('select * from [dbo].{table}'.format(table=table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        # Cleanup
        ms_dbconn.drop_table(table_name=table_name, schema_name='dbo')

    def test_df_to_table_schema_ms_override(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        ms_dbconn.drop_table(table_name=table_name, schema_name='dbo')

        # Assert does not already exist
        assert not ms_dbconn.table_exists(table_name=table_name, schema_name='dbo')

        # dataframe_to_table_schema
        i_s = ms_dbconn.dataframe_to_table_schema(df=test_df, table_name=table_name, schema_name='dbo')

        # Assert table exists
        assert ms_dbconn.table_exists(table_name=table_name, schema_name='dbo')

        # Assert 0 length
        table_df = ms_dbconn.dfquery('select * from dbo.{table}'.format(table=table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        assert list(ms_dbconn.dfquery("""
        select distinct data_type
        from information_schema.columns
        where table_name = '{table}';
        """.format(table=table_name))['data_type'])[0] == 'bigint'

        # Overwrite table with new column_type_overrides table
        i_s = ms_dbconn.dataframe_to_table_schema(df=test_df, table_name=table_name, schema_name='dbo', overwrite=True,
                                                  column_type_overrides={'a': 'varchar'})

        # Assert table exists
        assert ms_dbconn.table_exists(table_name=table_name, schema_name='dbo')

        # Assert 0 length
        table_df = ms_dbconn.dfquery('select * from dbo.{table}'.format(table=table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        # Assert new types, including column a as varchar
        pd.testing.assert_frame_equal(pd.DataFrame([{"column_name": 'a', "data_type": 'varchar'},
                                                    {"column_name": 'b', "data_type": 'bigint'},
                                                    {"column_name": 'c', "data_type": 'bigint'}]),
                                      ms_dbconn.dfquery("""
                                          select distinct column_name, data_type
                                          from information_schema.columns
                                          where table_name = '{table}';
                                      """.format(table=table_name))
                                      )

        # Cleanup
        ms_dbconn.drop_table(table_name=table_name, schema_name='dbo')

    def test_df_to_table_schema_ms_default_schema(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        ms_dbconn.drop_table(table_name=table_name, schema_name=ms_dbconn.default_schema)

        # Assert does not already exist
        assert not ms_dbconn.table_exists(table_name=table_name, schema_name=ms_dbconn.default_schema)

        # dataframe_to_table_schema
        ms_dbconn.dataframe_to_table_schema(df=test_df, table_name=table_name)

        # Assert table exists
        assert ms_dbconn.table_exists(table_name=table_name, schema_name=ms_dbconn.default_schema)

        # Assert 0 length
        table_df = ms_dbconn.dfquery('select * from {table}'.format(table=table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        # Cleanup
        ms_dbconn.drop_table(table_name=table_name, schema_name=ms_dbconn.default_schema)

    def test_df_to_table_schema_ms_overwrite(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        ms_dbconn.drop_table(table_name=table_name, schema_name='dbo')

        # Assert does not already exist
        assert not ms_dbconn.table_exists(table_name=table_name, schema_name='dbo')

        # Dataframe_to_table_schema
        ms_dbconn.dataframe_to_table_schema(df=test_df, table_name=table_name, schema_name='dbo')

        # Assert table exists
        assert ms_dbconn.table_exists(table_name=table_name, schema_name='dbo')

        # Assert 0 length
        table_df = ms_dbconn.dfquery('select * from [dbo].{table}'.format(table=table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df.columns)):
            assert list(test_df.columns)[i] == list(table_df.columns)[i]

        assert len(test_df.columns) == len(table_df.columns)

        # Make a new table
        test_df2 = pd.DataFrame([{"x": 1, "y": 2, "z": 3, "1": 4}, {"x": 5, "y": 6, "z": 7, "1": 8}])

        # Dataframe_to_table overwrite
        ms_dbconn.dataframe_to_table_schema(df=test_df2, table_name=table_name, schema_name='dbo', overwrite=True)

        # Assert table exists
        assert ms_dbconn.table_exists(table_name=table_name, schema_name='dbo')

        # Assert 0 length
        table_df = ms_dbconn.dfquery('select * from [dbo].{table}'.format(table=table_name))
        assert len(table_df) == 0

        # Assert column correctness
        for i in range(0, len(test_df2.columns)):
            assert list(test_df2.columns)[i] == list(table_df.columns)[i]

        assert len(test_df2.columns) == len(table_df.columns)

        # Cleanup
        ms_dbconn.drop_table(table_name=table_name, schema_name='dbo')

    # Log tests are in test_dbconnect


class TestDfToTablePG:
    def test_df_to_table_pg_basic(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])

        # Assert does not already exist
        assert not pg_dbconn.table_exists(table_name=table_name, schema_name='working')

        # Dataframe_to_table
        pg_dbconn.dataframe_to_table(df=test_df, table_name=table_name, schema_name='working')

        # Assert table exists
        assert pg_dbconn.table_exists(table_name=table_name, schema_name='working')

        # Assert table correctness
        table_df = pg_dbconn.dfquery('select * from working.{table}'.format(table=table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        # Cleanup
        pg_dbconn.drop_table(table_name=table_name, schema_name='working')

    def test_df_to_table_pg_override(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])

        # Assert does not already exist
        assert not pg_dbconn.table_exists(table_name=table_name, schema_name='working')

        # Dataframe_to_table
        pg_dbconn.dataframe_to_table(df=test_df, table_name=table_name, schema_name='working')

        # Assert table exists
        assert pg_dbconn.table_exists(table_name=table_name, schema_name='working')

        # Assert table correctness
        table_df = pg_dbconn.dfquery('select * from working.{table}'.format(table=table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        assert list(pg_dbconn.dfquery("""
         select distinct data_type
         from information_schema.columns
         where table_name = '{table}';
         """.format(table=table_name))['data_type'])[0] == 'bigint'

        # Overwrite table with new column_type_overrides table
        pg_dbconn.dataframe_to_table(df=test_df, table_name=table_name, schema_name='working', overwrite=True,
                                     column_type_overrides={'a': 'varchar'})

        # Assert table exists
        assert pg_dbconn.table_exists(table_name=table_name, schema_name='working')

        # Assert column db_type changed (column a is text)
        test_df = pd.DataFrame([{"a": '1', "b": 2, "c": 3}, {"a": '4', "b": 5, "c": 6}])
        table_df = pg_dbconn.dfquery('select * from working.{table}'.format(table=table_name))

        pd.testing.assert_frame_equal(test_df, table_df)

        # Cleanup
        pg_dbconn.drop_table(table_name=table_name, schema_name='working')

    def test_df_to_table_pg_default_schema(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])

        # Assert does not already exist
        assert not pg_dbconn.table_exists(table_name=table_name, schema_name=pg_dbconn.default_schema)

        # Dataframe_to_table
        pg_dbconn.dataframe_to_table(df=test_df, table_name=table_name)

        # Assert table exists
        assert pg_dbconn.table_exists(table_name=table_name, schema_name=pg_dbconn.default_schema)

        # Assert table correctness
        table_df = pg_dbconn.dfquery('select * from {table}'.format(table=table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        # Cleanup
        pg_dbconn.drop_table(table_name=table_name, schema_name=pg_dbconn.default_schema)

    def test_df_to_table_pg_overwrite(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])

        # Assert does not already exist
        assert not pg_dbconn.table_exists(table_name=table_name, schema_name='working')

        # Dataframe_to_table
        pg_dbconn.dataframe_to_table(df=test_df, table_name=table_name, schema_name='working')

        # Assert table exists
        assert pg_dbconn.table_exists(table_name=table_name, schema_name='working')

        # Assert table correctness
        table_df = pg_dbconn.dfquery('select * from working.{table}'.format(table=table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        # Make a new table
        test_df2 = pd.DataFrame([{"x": 1, "y": 2, "z": 3}, {"x": 4, "y": 5, "z": 6}])

        # Dataframe_to_table overwrite
        pg_dbconn.dataframe_to_table(df=test_df2, table_name=table_name, schema_name='working', overwrite=True)

        # Assert table exists
        assert pg_dbconn.table_exists(table_name=table_name, schema_name='working')

        # Assert table correctness
        table_df = pg_dbconn.dfquery('select * from working.{table}'.format(table=table_name))
        pd.testing.assert_frame_equal(test_df2, table_df)

        # Cleanup
        pg_dbconn.drop_table(table_name=table_name, schema_name='working')

    def test_df_to_table_pg_input_schema(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])

        # Assert does not already exist
        assert not pg_dbconn.table_exists(table_name=table_name, schema_name='working')

        # Dataframe_to_table
        pg_dbconn.dataframe_to_table(df=test_df, table_name=table_name, schema_name='working')

        # Assert table exists
        assert pg_dbconn.table_exists(table_name=table_name, schema_name='working')

        # Assert table correctness
        table_df = pg_dbconn.dfquery('select * from working.{table}'.format(table=table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        # Add more with input schema for existing table
        additional_df = pd.DataFrame([{"a": 7, "b": 8, "c": 9}, {"a": 10, "b": 11, "c": 12}])
        pg_dbconn.dataframe_to_table(df=additional_df, table_name=table_name, schema_name='working',
                                     df_schema_name=[['a', 'integer'], ['b', 'integer'], ['c', 'integer']])

        # Assert table correctness with addition
        table_df = pg_dbconn.dfquery('select * from working.{table}'.format(table=table_name))

        pd.testing.assert_frame_equal(pd.concat([test_df, additional_df], axis=0).reset_index(drop=True), table_df)

        # Cleanup
        pg_dbconn.drop_table(table_name=table_name, schema_name='working')

    # Log tests are in test_dbconnect


class TestDfToTableMS:
    def test_df_to_table_ms_basic(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        ms_dbconn.drop_table(table_name=table_name, schema_name=ms_dbconn.default_schema)

        # Assert does not already exist
        assert not ms_dbconn.table_exists(table_name=table_name)

        # Dataframe_to_table
        ms_dbconn.dataframe_to_table(df=test_df, table_name=table_name)

        # Assert table exists
        assert ms_dbconn.table_exists(table_name=table_name)

        # Assert table correctness
        table_df = ms_dbconn.dfquery('select * from {table}'.format(table=table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        # Cleanup
        ms_dbconn.drop_table(table_name=table_name, schema_name=ms_dbconn.default_schema)

    def test_df_to_table_ms_override(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])

        # Assert does not already exist
        assert not ms_dbconn.table_exists(table_name=table_name, schema_name=ms_dbconn.default_schema)

        # Dataframe_to_table
        ms_dbconn.dataframe_to_table(df=test_df, table_name=table_name, schema_name=ms_dbconn.default_schema)

        # Assert table exists
        assert ms_dbconn.table_exists(table_name=table_name, schema_name=ms_dbconn.default_schema)

        # Assert table correctness
        table_df = ms_dbconn.dfquery('select * from {table}'.format(table=table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        assert list(ms_dbconn.dfquery("""
          select distinct data_type
          from information_schema.columns
          where table_name = '{table}';
          """.format(table=table_name))['data_type'])[0] == 'bigint'

        # Overwrite table with new column_type_overrides table
        ms_dbconn.dataframe_to_table(df=test_df, table_name=table_name, schema_name=ms_dbconn.default_schema, overwrite=True,
                                     column_type_overrides={'a': 'varchar'})

        # Assert table exists
        assert ms_dbconn.table_exists(table_name=table_name, schema_name=ms_dbconn.default_schema)

        # Assert column db_type changed (column a is text)
        test_df = pd.DataFrame([{"a": '1', "b": 2, "c": 3}, {"a": '4', "b": 5, "c": 6}])
        table_df = ms_dbconn.dfquery('select * from {table}'.format(table=table_name))

        pd.testing.assert_frame_equal(test_df, table_df)

        # Cleanup
        ms_dbconn.drop_table(table_name=table_name, schema_name=ms_dbconn.default_schema)

    def test_df_to_table_ms_schema(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])

        # Assert does not already exist
        assert not ms_dbconn.table_exists(table_name=table_name, schema_name='dbo')

        # Dataframe_to_table
        ms_dbconn.dataframe_to_table(df=test_df, table_name=table_name, schema_name='dbo')

        # Assert table exists
        assert ms_dbconn.table_exists(table_name=table_name, schema_name='dbo')

        # Assert table correctness
        table_df = ms_dbconn.dfquery('select * from [dbo].{table}'.format(table=table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        # Cleanup
        ms_dbconn.drop_table(table_name=table_name, schema_name='dbo')

    def test_df_to_table_ms_overwrite(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])

        # Assert does not already exist
        assert not ms_dbconn.table_exists(table_name=table_name, schema_name='dbo')

        # Dataframe_to_table
        ms_dbconn.dataframe_to_table(df=test_df, table_name=table_name, schema_name='dbo')

        # Assert table exists
        assert ms_dbconn.table_exists(table_name=table_name, schema_name='dbo')

        # Assert table correctness
        table_df = ms_dbconn.dfquery('select * from [dbo].{table}'.format(table=table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        # Make a new table
        test_df2 = pd.DataFrame([{"x": 1, "y": 2, "z": 3}, {"x": 4, "y": 5, "z": 6}])

        # Dataframe_to_table overwrite
        ms_dbconn.dataframe_to_table(df=test_df2, table_name=table_name, schema_name='dbo', overwrite=True)

        # Assert table (still) exists
        assert ms_dbconn.table_exists(table_name=table_name, schema_name='dbo')

        # Assert table correctness
        table_df = ms_dbconn.dfquery('select * from [dbo].{table}'.format(table=table_name))
        pd.testing.assert_frame_equal(test_df2, table_df)

        # Cleanup
        ms_dbconn.drop_table(table_name=table_name, schema_name='dbo')

    def test_df_to_table_ms_input_schema(self):
        # Setup test df
        test_df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        ms_dbconn.drop_table(table_name=table_name, schema_name='dbo')

        # Assert does not already exist
        assert not ms_dbconn.table_exists(table_name=table_name, schema_name='dbo')

        # Dataframe_to_table
        ms_dbconn.dataframe_to_table(df=test_df, table_name=table_name, schema_name='dbo')

        # Assert table exists
        assert ms_dbconn.table_exists(table_name=table_name, schema_name='dbo')

        # Assert table correctness
        table_df = ms_dbconn.dfquery('select * from [dbo].{table}'.format(table=table_name))
        pd.testing.assert_frame_equal(test_df, table_df)

        # Add more with input schema for existing table
        additional_df = pd.DataFrame([{"a": 7, "b": 8, "c": 9}, {"a": 10, "b": 11, "c": 12}])
        ms_dbconn.dataframe_to_table(df=additional_df, table_name=table_name, schema_name='dbo',
                                     df_schema_name=[['a', 'integer'], ['b', 'integer'], ['c', 'integer']])

        # Assert table correctness with addition
        table_df = ms_dbconn.dfquery('select * from [dbo].{table}'.format(table=table_name))

        pd.testing.assert_frame_equal(pd.concat([test_df, additional_df], axis=0).reset_index(drop=True), table_df)

        # Cleanup
        ms_dbconn.drop_table(table_name=table_name, schema_name='dbo')

    # Log tests are in test_dbconnect
