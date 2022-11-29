import datetime
import os

import configparser
import pandas as pd
from pytest import raises

from .. import pysqldb3 as pysqldb
from . import helpers

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

pg_dbconn = pysqldb.DbConnect(db_type=config.get('PG_DB', 'TYPE'),
                              host=config.get('PG_DB', 'SERVER'),
                              db_name=config.get('PG_DB', 'DB_NAME'),
                              username=config.get('PG_DB', 'DB_USER'),
                              password=config.get('PG_DB', 'DB_PASSWORD'),
                              allow_temp_tables=True
                              )

ms_dbconn = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                              host=config.get('SQL_DB', 'SERVER'),
                              db_name=config.get('SQL_DB', 'DB_NAME'),
                              username=config.get('SQL_DB', 'DB_USER'),
                              password=config.get('SQL_DB', 'DB_PASSWORD'),
                              allow_temp_tables=True)

pg_table_name = 'pg_test_table_{user}'.format(user=pg_dbconn.username)
create_table_name = '_testing_table_to_csv_{date}_{user}_'.format(
    date=datetime.datetime.now().strftime('%Y-%m-%d').replace('-', '_'), user=pg_dbconn.username)

ms_schema = 'dbo'
pg_schema = 'working'


class Test_Table_to_CSV_PG:
    @classmethod
    def setup_class(cls):
        # helpers.set_up_schema(db, ms_schema=ms_schema)
        helpers.set_up_test_table_pg(pg_dbconn, schema=pg_schema)

    def test_table_to_csv_check_file(self):
        schema = pg_schema
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # create table
        pg_dbconn.query("""
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE
                    {schema}.{table}
            AS SELECT
                id, test_col1, test_col2, geom
            FROM
                {schema}.{pg_table}
            LIMIT 10
        """.format(schema=schema, table=create_table_name, pg_table=pg_table_name))
        assert pg_dbconn.table_exists(table_name=create_table_name, schema_name=schema)

        # table to csv
        pg_dbconn.table_to_csv(table_name=create_table_name,
                               schema_name=schema,
                               output_file=os.path.join(dir, create_table_name + '.csv')
                               )

        # check table in folder
        assert os.path.isfile(os.path.join(dir, create_table_name + '.csv'))

        # clean up
        pg_dbconn.drop_table(schema, create_table_name)
        os.remove(os.path.join(dir, create_table_name + '.csv'))

    def test_table_to_csv_check_file_values(self):
        schema = pg_schema
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        pg_dbconn.query("""
                    CREATE TABLE {schema}.{table} (
                        id int,
                        name varchar(50)
                    );

                    insert into {schema}.{table}
                    values
                        (1,'seth'),
                        (2,'hisa'),
                        (3,'samir'),
                        (4,'ayanthi'),
                        (5,'sam'),
                        (6,'arthur'),
                        (7,'bryant'),
                        (8,'chris'),
                        (9,'james')
                """.format(schema=schema, table=create_table_name))
        assert pg_dbconn.table_exists(table_name=create_table_name, schema_name=schema)

        # get df of table in db
        dbdf = pg_dbconn.dfquery("select * from {schema}.{table}".format(schema=schema, table=create_table_name))

        # table to csv
        pg_dbconn.table_to_csv(table_name=create_table_name,
                               schema_name=schema,
                               output_file=os.path.join(dir, create_table_name + '.csv')
                               )

        # check table in folder
        assert os.path.isfile(os.path.join(dir, create_table_name + '.csv'))

        # get df of csv
        csvdf = pd.read_csv(os.path.join(dir, create_table_name + '.csv'))

        # clean up
        pg_dbconn.drop_table(schema_name=schema, table_name=create_table_name)
        pd.testing.assert_frame_equal(csvdf.fillna(0), dbdf.fillna(0), check_dtype=False)
        os.remove(os.path.join(dir, create_table_name + '.csv'))

    def test_table_to_csv_check_file_values2(self):
        schema = pg_schema
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # create table
        pg_dbconn.query("""
        DROP TABLE IF EXISTS {schema}.{table};
        CREATE TABLE
            {schema}.{table}
        AS SELECT
            id, test_col1, test_col2
        FROM
            {schema}.{pg_table}
        WHERE
            id < 100
        """.format(schema=schema, table=create_table_name, pg_table=pg_table_name))
        assert pg_dbconn.table_exists(table_name=create_table_name, schema_name=schema)

        # get df of table in db
        dbdf = pg_dbconn.dfquery("select * from {schema}.{table}".format(schema=schema, table=create_table_name))

        # table to csv
        pg_dbconn.table_to_csv(table_name=create_table_name,
                               schema_name=schema,
                               output_file=os.path.join(dir, create_table_name + '.csv')
                               )

        # check table in folder
        assert os.path.isfile(os.path.join(dir, create_table_name + '.csv'))

        # get df of csv
        csvdf = pd.read_csv(os.path.join(dir, create_table_name + '.csv'))
        pd.testing.assert_frame_equal(csvdf, dbdf, check_dtype=False)

        # clean up
        pg_dbconn.drop_table(schema, create_table_name)
        os.remove(os.path.join(dir, create_table_name + '.csv'))

    def test_table_to_csv_check_file_values_sep(self):
        sep = ';'
        schema = pg_schema
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        pg_dbconn.query("""
                            CREATE TABLE {schema}.{table} (
                                id int,
                                name varchar(50)
                            );

                            insert into {schema}.{table}
                            values
                                (1,'seth, test'),
                                (2,'hisa, test'),
                                (3,'samir'),
                                (4,'ayanthi'),
                                (5,'sam'),
                                (6,'arthur'),
                                (7,'bryant'),
                                (8,'chris'),
                                (9,'james')
                        """.format(schema=schema, table=create_table_name))
        assert pg_dbconn.table_exists(table_name=create_table_name, schema_name=schema)

        # get df of table in db
        dbdf = pg_dbconn.dfquery("select * from {schema}.{table}".format(schema=schema, table=create_table_name))

        # table to csv
        pg_dbconn.table_to_csv(table_name=create_table_name,
                               schema_name=schema,
                               output_file=os.path.join(dir, create_table_name + '.csv'),
                               sep=sep
                               )

        # check table in folder
        assert os.path.isfile(os.path.join(dir, create_table_name + '.csv'))

        # get df of csv
        csvdf = pd.read_csv(os.path.join(dir, create_table_name + '.csv'), sep=sep)

        # clean up
        pg_dbconn.drop_table(schema_name=schema, table_name=create_table_name)
        pd.testing.assert_frame_equal(csvdf.fillna(0), dbdf.fillna(0), check_dtype=False)
        os.remove(os.path.join(dir, create_table_name + '.csv'))

    def test_table_to_csv_check_file_values2_sep(self):
        sep = ';'
        schema = pg_schema
        pg_dbconn.drop_table(schema_name=schema, table_name=create_table_name)

        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        pg_dbconn.query("""
        CREATE TABLE
            {schema}.{table}
        AS SELECT
            id, test_col1, test_col2
        FROM
            {schema}.{pg_table}
        WHERE
            id < 100
        """.format(schema=schema, table=create_table_name, pg_table=pg_table_name))
        assert pg_dbconn.table_exists(table_name=create_table_name, schema_name=schema)

        # get df of table in db
        dbdf = pg_dbconn.dfquery("select * from {schema}.{table}".format(schema=schema, table=create_table_name))

        # table to csv
        pg_dbconn.table_to_csv(table_name=create_table_name,
                               schema_name=schema,
                               output_file=os.path.join(dir, create_table_name + '.csv'),
                               sep=sep
                               )

        # check table in folder
        assert os.path.isfile(os.path.join(dir, create_table_name + '.csv'))

        # get df of csv
        csvdf = pd.read_csv(os.path.join(dir, create_table_name + '.csv'), sep=sep)

        # clean up
        pg_dbconn.drop_table(schema_name=schema, table_name=create_table_name)
        pd.testing.assert_frame_equal(csvdf.fillna(0), dbdf.fillna(0), check_dtype=False)
        os.remove(os.path.join(dir, create_table_name + '.csv'))

    def test_table_to_csv_no_schema(self):
        schema = pg_schema
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # create table
        pg_dbconn.query("""
                    DROP TABLE IF EXISTS {table};
                    CREATE TABLE
                        {table}
                    AS SELECT
                        id, test_col1, test_col2, geom
                    FROM
                        {schema}.{pg_table}
                    LIMIT 10
                """.format(table=create_table_name, pg_table=pg_table_name, schema=schema))

        assert pg_dbconn.table_exists(create_table_name)

        # table to csv
        pg_dbconn.table_to_csv(table_name=create_table_name,
                               output_file=os.path.join(dir, create_table_name + '.csv')
                               )

        # check table in folder
        assert os.path.isfile(os.path.join(dir, create_table_name + '.csv'))

        # clean up
        pg_dbconn.drop_table(schema_name=pg_dbconn.default_schema, table_name=create_table_name)
        os.remove(os.path.join(dir, create_table_name + '.csv'))

    def test_table_to_csv_check_file_bad_path(self):
        schema = pg_schema
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data_no_folder')
        # create table
        pg_dbconn.query("""
            CREATE TABLE
                {schema}.{table}
            AS SELECT
                id, test_col1, test_col2, geom
            FROM
                {schema}.{pg_table}
            LIMIT 10
        """.format(schema=schema, table=create_table_name, pg_table=pg_table_name))
        assert pg_dbconn.table_exists(table_name=create_table_name, schema=schema)

        # table to csv
        with raises(OSError) as exc_info:
            pg_dbconn.table_to_csv(table_name=create_table_name,
                                   schema_name=schema,
                                   output_file=os.path.join(dir, create_table_name + '.csv')
                                   )
        assert exc_info.type is OSError

        # clean up
        pg_dbconn.disconnect(quiet=True)
        pg_dbconn.connect(quiet=True)
        pg_dbconn.drop_table(schema_name=schema, table_name=create_table_name)

    def test_table_to_csv_table_doesnt_exist(self):
        schema = pg_schema
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data_no_folder')

        pg_dbconn.drop_table(table_name=create_table_name, schema_name=schema)
        assert not pg_dbconn.table_exists(table_name=create_table_name, schema=schema)

        with raises(SystemExit) as exc_info:
            pg_dbconn.table_to_csv(table_name=create_table_name,
                                   schema_name=schema,
                                   output_file=os.path.join(dir, create_table_name + '.csv')
                                   )
        assert True

    def test_table_to_csv_no_output(self):
        schema = pg_schema

        # create table
        pg_dbconn.query("""
                    DROP TABLE IF EXISTS {schema}.{table};
                    CREATE TABLE
                        {schema}.{table}
                    AS SELECT
                        id, test_col1, test_col2, geom
                    FROM
                        {schema}.{pg_table}
                    LIMIT 10
                """.format(schema=schema, table=create_table_name, pg_table=pg_table_name))
        assert pg_dbconn.table_exists(table_name=create_table_name, schema=schema)

        # table to csv
        pg_dbconn.table_to_csv(table_name=create_table_name, schema_name=schema)

        # check table in folder (since no name specified, output goes to current dir + table name + .csv
        files = os.listdir(os.getcwd())
        assert any([create_table_name in f for f in files])

        # clean up
        pg_dbconn.drop_table(schema_name=schema, table_name=create_table_name)
        for f in [f for f in files if create_table_name in f]:
            os.remove(os.path.join(os.getcwd(), f))

    def test_table_to_csv_temp(self):
        schema = pg_schema
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        table = '_testing_table_to_csv_20200914_'
        # create table
        pg_dbconn.query("""
                   CREATE TEMPORARY TABLE
                           {table}
                   AS SELECT
                        id, test_col1, test_col2, geom
                   FROM
                       {schema}.{pg_table}
                   LIMIT 10
               """.format(table=table, pg_table=pg_table_name, schema=schema))

        pg_dbconn.query("""select * from _testing_table_to_csv_20200914_""")
        assert len(pg_dbconn.data) == 10

        pg_dbconn.table_to_csv(table, output_file=os.path.join(dir, table + '.csv'))

        # check table in folder
        assert os.path.isfile(os.path.join(dir, table + '.csv'))

        # clean up
        os.remove(os.path.join(dir, table + '.csv'))

    def test_table_to_csv_check_file_quote_name(self):
        schema = pg_schema
        table = '"' + create_table_name + '"'
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        pg_dbconn.drop_table(schema, table)

        # create table
        pg_dbconn.query("""
            CREATE TABLE
                    "{schema}".{table}
            AS SELECT
                id, test_col1, test_col2, geom
            FROM
                {schema}.{pg_table}
            LIMIT 10
        """.format(schema=schema, table=table, pg_table=pg_table_name))
        assert pg_dbconn.table_exists(table_name=table, schema=schema)

        # table to csv
        pg_dbconn.table_to_csv(table_name=table,
                               schema_name=schema,
                               output_file=os.path.join(dir, create_table_name + ".csv")
                               )

        # check table in folder
        assert os.path.isfile(os.path.join(dir,  create_table_name + ".csv"))

        # clean up
        pg_dbconn.drop_table(schema_name=schema, table_name=table)
        os.remove(os.path.join(dir,  create_table_name + ".csv"))

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(pg_dbconn)
        # helpers.clean_up_schema(db)
        pg_dbconn.cleanup_new_tables()


############################################
# TODO:
# 5 quotes / brackets in table names
############################################

class Test_Table_to_CSV_MS:
    @classmethod
    def setup_class(cls):
        helpers.set_up_schema(ms_dbconn, ms_schema=ms_schema)
        helpers.set_up_test_table_sql(ms_dbconn, schema=ms_schema)

    def test_table_to_csv_check_file_bad_path(self):
        schema = ms_schema
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data_no_folder')
        ms_dbconn.drop_table(schema_name=schema, table_name=create_table_name)

        # create table
        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (
                objectid int,
                nd int,
                street1 text
            );

            insert into {schema}.{table}
            values
                (1, 2, 'Mulberry'),
                (2, 3, 'Canal'),
                (3, 4, 'Fifth Ave.')
        """.format(schema=schema, table=create_table_name))
        assert ms_dbconn.table_exists(table_name=create_table_name, schema=schema)

        # table to csv
        with raises(OSError) as exc_info:
            ms_dbconn.table_to_csv(create_table_name,
                                   schema_name=schema,
                                   output_file=os.path.join(dir, create_table_name + '.csv')
                                   )
        assert exc_info.type is OSError

        # clean up
        ms_dbconn.disconnect(quiet=True)
        ms_dbconn.connect(quiet=True)
        ms_dbconn.drop_table(schema_name=schema, table_name=create_table_name)

    def test_table_to_csv_check_file(self):
        schema = ms_schema
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # create table
        if ms_dbconn.table_exists(schema=schema, table_name=create_table_name):
            ms_dbconn.drop_table(schema_name=schema, table_name=create_table_name)

        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (
                id int,
                name varchar(50)
            );

            insert into {schema}.{table}
            values
                (1,'seth'),
                (2,'hisa'),
                (3,'samir'),
                (4,'ayanthi'),
                (5,'sam'),
                (6,'arthur'),
                (7,'bryant'),
                (8,'chris'),
                (9,'james')
        """.format(schema=schema, table=create_table_name))
        assert ms_dbconn.table_exists(table_name=create_table_name, schema_name=schema)

        # table to csv
        ms_dbconn.table_to_csv(table_name=create_table_name,
                               schema_name=schema,
                               output_file=os.path.join(dir, create_table_name + '.csv')
                               )

        # check table in folder
        assert os.path.isfile(os.path.join(dir, create_table_name + '.csv'))

        # clean up
        ms_dbconn.drop_table(schema_name=schema, table_name=create_table_name)
        os.remove(os.path.join(dir, create_table_name + '.csv'))

    def test_table_to_csv_check_file_values(self):
        schema = ms_schema
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # create table
        if ms_dbconn.table_exists(schema_name=schema, table_name=create_table_name):
            ms_dbconn.drop_table(schema_name=schema, table_name=create_table_name)

        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (
                id int,
                name varchar(50)
            );

            insert into {schema}.{table}
            values
                (1,'seth'),
                (2,'hisa'),
                (3,'samir'),
                (4,'ayanthi'),
                (5,'sam'),
                (6,'arthur'),
                (7,'bryant'),
                (8,'chris'),
                (9,'james')
        """.format(schema=schema, table=create_table_name))
        assert ms_dbconn.table_exists(create_table_name, schema=schema)

        # get df of table in db
        dbdf = ms_dbconn.dfquery("select * from {schema}.{table}".format(schema=schema, table=create_table_name))

        # table to csv
        ms_dbconn.table_to_csv(table_name=create_table_name,
                               schema_name=schema,
                               output_file=os.path.join(dir, create_table_name + '.csv')
                               )

        # check table in folder
        assert os.path.isfile(os.path.join(dir, create_table_name + '.csv'))
        csvdf = pd.read_csv(os.path.join(dir, create_table_name + '.csv'))

        pd.testing.assert_frame_equal(csvdf, dbdf)

        # clean up
        ms_dbconn.drop_table(schema_name=schema, table_name=create_table_name)
        os.remove(os.path.join(dir, create_table_name + '.csv'))

    def test_table_to_csv_check_file_values2(self):
        schema = ms_schema
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # create table
        ms_dbconn.query("""
                CREATE TABLE {schema}.{table} (
                OBJECTID int,
                nd int,
                street1 varchar(16)
            );

            insert into {schema}.{table}
            values
                (1, 1, 'Atlantic'),
                (2, 2, 'Flatbush'),
                (3, 3, 'Canal')
        """.format(schema=schema, table=create_table_name))

        assert ms_dbconn.table_exists(table_name=create_table_name, schema_name=schema)

        # get df of table in db
        dbdf = ms_dbconn.dfquery("select * from {schema}.{table}".format(schema=schema, table=create_table_name))

        # table to csv
        ms_dbconn.table_to_csv(table_name=create_table_name,
                               schema_name=schema,
                               output_file=os.path.join(dir, create_table_name + '.csv')
                               )

        # check table in folder
        assert os.path.isfile(os.path.join(dir, create_table_name + '.csv'))
        csvdf = pd.read_csv(os.path.join(dir, create_table_name + '.csv'))

        # clean up
        ms_dbconn.drop_table(schema_name=schema, table_name=create_table_name)

        # check date matches
        pd.testing.assert_frame_equal(csvdf.fillna(0), dbdf.fillna(0), check_dtype=False)

        os.remove(os.path.join(dir, create_table_name + '.csv'))

    def test_table_to_csv_check_file_values_sep(self):
        sep = ';'
        schema = ms_schema
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        ms_dbconn.query("""
                            CREATE TABLE {schema}.{table} (
                                id int,
                                name varchar(50)
                            );

                            insert into {schema}.{table}
                            values
                                (1,'seth, test'),
                                (2,'hisa, test'),
                                (3,'samir'),
                                (4,'ayanthi'),
                                (5,'sam'),
                                (6,'arthur'),
                                (7,'bryant'),
                                (8,'chris'),
                                (9,'james')
                        """.format(schema=schema, table=create_table_name))
        assert ms_dbconn.table_exists(table_name=create_table_name, schema=schema)

        # get df of table in db
        dbdf = ms_dbconn.dfquery("select * from {schema}.{table}".format(schema=schema, table=create_table_name))

        # table to csv
        ms_dbconn.table_to_csv(table_name=create_table_name,
                               schema_name=schema,
                               output_file=os.path.join(dir, create_table_name + '.csv'),
                               sep=sep
                               )

        # check table in folder
        assert os.path.isfile(os.path.join(dir, create_table_name + '.csv'))

        # get df of csv
        csvdf = pd.read_csv(os.path.join(dir, create_table_name + '.csv'), sep=sep)

        pd.testing.assert_frame_equal(csvdf.fillna(0), dbdf.fillna(0), check_dtype=False)

        # clean up
        ms_dbconn.drop_table(schema_name=schema, table_name=create_table_name)
        os.remove(os.path.join(dir, create_table_name + '.csv'))

    def test_table_to_csv_check_file_values2_sep(self):
        sep = ';'
        schema = ms_schema
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # create table
        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (
                objectid int,
                nd int,
                street1 text
            );

            insert into {schema}.{table}
            values
                (1, 2, 'Mulberry'),
                (2, 3, 'Canal'),
                (3, 4, 'Fifth Ave.')
        """.format(schema=schema, table=create_table_name))
        assert ms_dbconn.table_exists(create_table_name, schema=schema)

        # get df of table in db
        dbdf = ms_dbconn.dfquery("select * from {schema}.{table}".format(schema=schema, table=create_table_name))

        # table to csv
        ms_dbconn.table_to_csv(table_name=create_table_name,
                               schema_name=schema,
                               output_file=os.path.join(dir, create_table_name + '.csv'),
                               sep=sep
                               )

        # check table in folder
        assert os.path.isfile(os.path.join(dir, create_table_name + '.csv'))

        # get df of csv
        csvdf = pd.read_csv(os.path.join(dir, create_table_name + '.csv'), sep=sep)

        pd.testing.assert_frame_equal(csvdf.fillna(0), dbdf.fillna(0), check_dtype=False)

        # clean up
        ms_dbconn.drop_table(schema_name=schema, table_name=create_table_name)
        os.remove(os.path.join(dir, create_table_name + '.csv'))

    def test_table_to_csv_no_schema(self):
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # create table
        ms_dbconn.query("""
                    CREATE TABLE {schema}.{table} (
                        id int,
                        name varchar(50)
                    );
    
                    insert into {schema}.{table}
                    values
                        (1,'seth'),
                        (2,'hisa'),
                        (3,'samir'),
                        (4,'ayanthi'),
                        (5,'sam'),
                        (6,'arthur'),
                        (7,'bryant'),
                        (8,'chris'),
                        (9,'james')
                """.format(table=create_table_name, schema=ms_dbconn.default_schema))
        assert ms_dbconn.table_exists(create_table_name, schema=ms_dbconn.default_schema)

        # table to csv
        ms_dbconn.table_to_csv(table_name=create_table_name,
                               output_file=os.path.join(dir, create_table_name + '.csv')
                               )

        # check table in folder
        assert os.path.isfile(os.path.join(dir, create_table_name + '.csv'))

        # clean up
        ms_dbconn.drop_table(ms_dbconn.default_schema, create_table_name)
        os.remove(os.path.join(dir, create_table_name + '.csv'))

    def test_table_to_csv_table_doesnt_exist(self):
        schema = ms_schema
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data_no_folder')

        ms_dbconn.drop_table(table_name=create_table_name, schema_name=schema)
        assert not ms_dbconn.table_exists(table_name=create_table_name, schema=schema)

        with raises(SystemExit) as exc_info:
            ms_dbconn.table_to_csv(table_name=create_table_name,
                                   schema_name=schema,
                                   output_file=os.path.join(dir, create_table_name + '.csv')
                                   )

    def test_table_to_csv_no_output(self):
        schema = ms_schema

        # create table
        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (
                objectid int,
                nd int,
                street1 text
            );

            insert into {schema}.{table}
            values
                (1, 2, 'Mulberry'),
                (2, 3, 'Canal'),
                (3, 4, 'Fifth Ave.')
        """.format(schema=schema, table=create_table_name))
        assert ms_dbconn.table_exists(table_name=create_table_name, schema_name=schema)

        # table to csv
        ms_dbconn.table_to_csv(create_table_name,
                               schema_name=schema
                               )

        # check table in folder (since no name specified, output goes to current dir + table name + .csv
        files = os.listdir(os.getcwd())
        assert any([create_table_name in f for f in files])

        # clean up
        ms_dbconn.drop_table(schema, create_table_name)
        for f in [f for f in files if create_table_name in f]:
            os.remove(os.path.join(os.getcwd(), f))

    def test_table_to_csv_check_file_bracket_name(self):
        schema = ms_schema
        table = '[' + create_table_name + ']'
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        ms_dbconn.drop_table(schema, table)

        # create table
        ms_dbconn.query("""
            CREATE TABLE {schema}.{table} (
                objectid int,
                nd int,
                street1 text
            );

            insert into {schema}.{table}
            values
                (1, 2, 'Mulberry'),
                (2, 3, 'Canal'),
                (3, 4, 'Fifth Ave.')
        """.format(schema=schema, table=table))
        assert ms_dbconn.table_exists(table_name=table, schema_name=schema)

        # table to csv
        ms_dbconn.table_to_csv(table_name=table,
                               schema_name=schema,
                               output_file=os.path.join(dir, create_table_name + ".csv")
                               )

        # check table in folder
        assert os.path.isfile(os.path.join(dir, create_table_name + ".csv"))

        # clean up
        ms_dbconn.drop_table(schema_name=schema, table_name=table)
        os.remove(os.path.join(dir, create_table_name + ".csv"))

    def test_table_to_csv_temp(self):
        table = '#_testing_table_to_csv_20200914_'
        dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # create table
        ms_dbconn.query("""
            CREATE TABLE {table} (
                objectid int,
                nd int,
                street1 text
            );

            insert into {table}
            values
                (1, 2, 'Mulberry'),
                (2, 3, 'Canal'),
                (3, 4, 'Fifth Ave.')
        """.format(table=table))
        ms_dbconn.query("""select * from {table}""".format(table=table))
        assert len(ms_dbconn.data) == 3

        ms_dbconn.table_to_csv(table, output_file=os.path.join(dir, table[1:] + '.csv'))

        # check table in folder
        assert os.path.isfile(os.path.join(dir, table[1:] + '.csv'))

        # clean up
        os.remove(os.path.join(dir, table[1:] + '.csv'))

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(ms_dbconn, schema=ms_schema)
        ms_dbconn.query("drop table {schema}.{table}".format(schema=ms_schema, table=ms_dbconn.log_table))
        ms_dbconn.cleanup_new_tables()
        # helpers.clean_up_schema(sql, ms_schema)
