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

pg_table_name = f'pg_test_table_{db.user}'
create_table_name = f"_testing_table_to_csv_{datetime.datetime.now().strftime('%Y-%m-%d').replace('-', '_')}_{db.user}_"

ms_schema = 'dbo'
pg_schema = 'working'


class Test_Table_to_CSV_PG:
    @classmethod
    def setup_class(cls):
        helpers.set_up_test_table_pg(db, schema=pg_schema)

    def test_table_to_csv_check_file(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # create table
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{create_table_name};
            CREATE TABLE
                    {pg_schema}.{create_table_name}
            AS SELECT
                id, test_col1, test_col2, geom
            FROM
                {pg_schema}.{pg_table_name}
            LIMIT 10
        """)
        assert db.table_exists(create_table_name, schema=pg_schema)

        # table to csv
        db.table_to_csv(create_table_name,
                        schema=pg_schema,
                        output_file=os.path.join(fldr, create_table_name + '.csv')
                        )

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, create_table_name + '.csv'))

        # clean up
        db.drop_table(pg_schema, create_table_name)
        os.remove(os.path.join(fldr, create_table_name + '.csv'))

    def test_table_to_csv_check_file_values(self):

        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        db.query(f"""
                    CREATE TABLE {pg_schema}.{create_table_name} (
                        id int,
                        name varchar(50)
                    );

                    insert into {pg_schema}.{create_table_name}
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
                """)
        assert db.table_exists(create_table_name, schema=pg_schema)

        # get df of table in db
        dbdf = db.dfquery(f"select * from {pg_schema}.{create_table_name}")

        # table to csv
        db.table_to_csv(create_table_name,
                        schema=pg_schema,
                        output_file=os.path.join(fldr, create_table_name + '.csv')
                        )

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, create_table_name + '.csv'))

        # get df of csv
        csvdf = pd.read_csv(os.path.join(fldr, create_table_name + '.csv'))

        # clean up
        db.drop_table(pg_schema, create_table_name)
        pd.testing.assert_frame_equal(csvdf.fillna(0), dbdf.fillna(0), check_dtype=False)
        os.remove(os.path.join(fldr, create_table_name + '.csv'))

    def test_table_to_csv_check_file_values2(self):

        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # create table
        db.query(f"""
        DROP TABLE IF EXISTS {pg_schema}.{create_table_name};
        CREATE TABLE
                {pg_schema}.{create_table_name}
        AS SELECT
            id, test_col1, test_col2
        FROM
            {pg_schema}.{pg_table_name}
        WHERE
            id < 100
        """)
        assert db.table_exists(create_table_name,  schema=pg_schema)

        # get df of table in db
        dbdf = db.dfquery(f"select * from {pg_schema}.{create_table_name}")

        # table to csv
        db.table_to_csv(table=create_table_name,
                        schema=pg_schema,
                        output_file=os.path.join(fldr, create_table_name + '.csv')
                        )

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, create_table_name + '.csv'))

        # get df of csv
        csvdf = pd.read_csv(os.path.join(fldr, create_table_name + '.csv'))
        pd.testing.assert_frame_equal(csvdf, dbdf, check_dtype=False)

        # clean up
        db.drop_table(pg_schema, create_table_name)
        os.remove(os.path.join(fldr, create_table_name + '.csv'))

    def test_table_to_csv_check_file_values_sep(self):
        sep = ';'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        db.query(f"""
                            CREATE TABLE {pg_schema}.{create_table_name} (
                                id int,
                                name varchar(50)
                            );

                            insert into {pg_schema}.{create_table_name}
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
                        """)
        assert db.table_exists(create_table_name, schema=pg_schema)

        # get df of table in db
        dbdf = db.dfquery(f"select * from {pg_schema}.{create_table_name}")

        # table to csv
        db.table_to_csv(create_table_name,
                        schema=pg_schema,
                        output_file=os.path.join(fldr, create_table_name + '.csv'),
                        sep=sep
                        )

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, create_table_name + '.csv'))

        # get df of csv
        csvdf = pd.read_csv(os.path.join(fldr, create_table_name + '.csv'), sep=sep)

        # clean up
        db.drop_table(pg_schema, create_table_name)
        pd.testing.assert_frame_equal(csvdf.fillna(0), dbdf.fillna(0), check_dtype=False)
        os.remove(os.path.join(fldr, create_table_name + '.csv'))

    def test_table_to_csv_check_file_values2_sep(self):
        sep = ';'
        db.drop_table(pg_schema, create_table_name)

        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        db.query(f"""
        CREATE TABLE
                {pg_schema}.{create_table_name}
        AS SELECT
            id, test_col1, test_col2
        FROM
            {pg_schema}.{pg_table_name}
        WHERE
            id < 100
        """)
        assert db.table_exists(create_table_name, schema=pg_schema)

        # get df of table in db
        dbdf = db.dfquery(f"select * from {pg_schema}.{create_table_name}")

        # table to csv
        db.table_to_csv(create_table_name,
                        schema=pg_schema,
                        output_file=os.path.join(fldr, create_table_name + '.csv'),
                        sep=sep
                        )

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, create_table_name + '.csv'))

        # get df of csv
        csvdf = pd.read_csv(os.path.join(fldr, create_table_name + '.csv'), sep=sep)

        # clean up
        db.drop_table(pg_schema, create_table_name)
        pd.testing.assert_frame_equal(csvdf.fillna(0), dbdf.fillna(0), check_dtype=False)
        os.remove(os.path.join(fldr, create_table_name + '.csv'))

    def test_table_to_csv_no_schema(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # create table
        db.query(f"""
                    DROP TABLE IF EXISTS {create_table_name};
                    CREATE TABLE
                            {create_table_name}
                    AS SELECT
                        id, test_col1, test_col2, geom
                    FROM
                        {pg_schema}.{pg_table_name}
                    LIMIT 10
                """)

        assert db.table_exists(table = create_table_name, schema = db.default_schema)

        # table to csv
        db.table_to_csv(create_table_name,
                        output_file=os.path.join(fldr, create_table_name + '.csv')
                        )

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, create_table_name + '.csv'))

        # clean up
        db.drop_table(db.default_schema, create_table_name)
        os.remove(os.path.join(fldr, create_table_name + '.csv'))

    def test_table_to_csv_table_doesnt_exist(self):
        schema = pg_schema
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data_no_folder')

        db.drop_table(table=create_table_name, schema=schema)
        assert not db.table_exists(create_table_name, schema=schema)

        with raises(RuntimeError) as exc_info:
            db.table_to_csv(create_table_name,
                            schema=schema,
                            output_file=os.path.join(fldr, create_table_name + '.csv')
                            )
        assert True

    def test_table_to_csv_no_output(self):
        # create table
        db.query(f"""
                    DROP TABLE IF EXISTS {pg_schema}.{create_table_name};
                    CREATE TABLE
                            {pg_schema}.{create_table_name}
                    AS SELECT
                        id, test_col1, test_col2, geom
                    FROM
                        {pg_schema}.{pg_table_name}
                    LIMIT 10
                """)
        assert db.table_exists(create_table_name, schema=pg_schema)

        # table to csv
        db.table_to_csv(create_table_name,
                        schema=pg_schema)

        # check table in folder (since no name specified, output goes to current dir + table name + .csv
        files = os.listdir(os.getcwd())
        assert any([create_table_name in f for f in files])

        # clean up
        db.drop_table(pg_schema, create_table_name)
        for f in [f for f in files if create_table_name in f]:
            os.remove(os.path.join(os.getcwd(), f))

    def test_table_to_csv_check_file_quote_name(self):
        table = '"' + create_table_name + '"'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        db.drop_table(pg_schema, table)

        # create table
        db.query(f"""
            CREATE TABLE
                    "{pg_schema}".{table}
            AS SELECT
                id, test_col1, test_col2, geom
            FROM
                {pg_schema}.{pg_table_name}
            LIMIT 10
        """)
        assert db.table_exists(table, schema=pg_schema)

        # table to csv
        db.table_to_csv(table,
                        schema=pg_schema,
                        output_file=os.path.join(fldr, create_table_name + ".csv")
                        )

        # check table in folder
        assert os.path.isfile(os.path.join(fldr,  create_table_name + ".csv"))

        # clean up
        db.drop_table(pg_schema, table)
        os.remove(os.path.join(fldr,  create_table_name + ".csv"))

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_pg(db)
        db.cleanup_new_tables()


############################################
# TODO:
# 5 quotes / brackets in table names
############################################

class Test_Table_to_CSV_MS:
    @classmethod
    def setup_class(cls):
        helpers.set_up_schema(sql, ms_schema=ms_schema)
        helpers.set_up_test_table_sql(sql, schema=ms_schema)

    def test_table_to_csv_check_file_bad_path(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data_no_folder')
        sql.drop_table(schema=ms_schema, table=create_table_name)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{create_table_name} (
                objectid int,
                nd int,
                street1 text
            );

            insert into {ms_schema}.{create_table_name}
            values
                (1, 2, 'Mulberry'),
                (2, 3, 'Canal'),
                (3, 4, 'Fifth Ave.')
        """)
        assert sql.table_exists(table=create_table_name, schema=ms_schema)

        # table to csv
        with raises(RuntimeError) as exc_info:
            sql.table_to_csv(create_table_name,
                             schema=ms_schema,
                             output_file=os.path.join(fldr, create_table_name + '.csv')
                             )
        assert exc_info.type is RuntimeError

        # clean up
        sql.disconnect(quiet=True)
        sql.connect(quiet=True)
        sql.drop_table(ms_schema, create_table_name)

    def test_table_to_csv_check_file(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # create table
        if sql.table_exists(schema=ms_schema, table=create_table_name):
            sql.drop_table(schema=ms_schema, table=create_table_name)

        sql.query(f"""
            CREATE TABLE {ms_schema}.{create_table_name} (
                id int,
                name varchar(50)
            );

            insert into {ms_schema}.{create_table_name}
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
        """
        )
        assert sql.table_exists(create_table_name, schema=ms_schema)

        # table to csv
        sql.table_to_csv(create_table_name,
                         schema=ms_schema,
                         output_file=os.path.join(fldr, create_table_name + '.csv')
                         )

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, create_table_name + '.csv'))

        # clean up
        sql.drop_table(ms_schema, create_table_name)
        os.remove(os.path.join(fldr, create_table_name + '.csv'))

    def test_table_to_csv_check_file_values(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # create table
        if sql.table_exists(schema=ms_schema, table=create_table_name):
            sql.drop_table(schema=ms_schema, table=create_table_name)

        sql.query(f"""
            CREATE TABLE {ms_schema}.{create_table_name} (
                id int,
                name varchar(50)
            );

            insert into {ms_schema}.{create_table_name}
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
        """)
        assert sql.table_exists(create_table_name, schema=ms_schema)

        # get df of table in db
        dbdf = sql.dfquery(f"select * from {ms_schema}.{create_table_name}")

        # table to csv
        sql.table_to_csv(create_table_name,
                         schema=ms_schema,
                         output_file=os.path.join(fldr, create_table_name + '.csv')
                         )

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, create_table_name + '.csv'))
        csvdf = pd.read_csv(os.path.join(fldr, create_table_name + '.csv'))

        pd.testing.assert_frame_equal(csvdf.drop(columns=['WKT']), dbdf)

        # clean up
        sql.drop_table(schema, create_table_name)
        os.remove(os.path.join(fldr, create_table_name + '.csv'))

    def test_table_to_csv_check_file_values2(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # create table
        sql.query(f"""
                CREATE TABLE {ms_schema}.{create_table_name} (
                OBJECTID int,
                nd int,
                street1 varchar(16)
            );

            insert into {ms_schema}.{create_table_name}
            values
                (1, 1, 'Atlantic'),
                (2, 2, 'Flatbush'),
                (3, 3, 'Canal')
        """)

        assert sql.table_exists(create_table_name, schema=ms_schema)

        # get df of table in db
        dbdf = sql.dfquery(f"select * from {ms_schema}.{create_table_name}")

        # table to csv
        sql.table_to_csv(create_table_name,
                         schema=ms_schema,
                         output_file=os.path.join(fldr, create_table_name + '.csv')
                         )

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, create_table_name + '.csv'))
        csvdf = pd.read_csv(os.path.join(fldr, create_table_name + '.csv'))

        # clean up
        sql.drop_table(ms_schema, create_table_name)

        # check date matches
        pd.testing.assert_frame_equal(csvdf.fillna(0).drop(columns=['WKT']), dbdf.fillna(0), check_dtype=False)

        os.remove(os.path.join(fldr, create_table_name + '.csv'))

    def test_table_to_csv_check_file_values_sep(self):
        sep = ';'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        sql.query(f"""
                            CREATE TABLE {ms_schema}.{create_table_name} (
                                id int,
                                name varchar(50)
                            );

                            insert into {ms_schema}.{create_table_name}
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
                        """)
        assert sql.table_exists(create_table_name, schema=ms_schema)

        # get df of table in db
        dbdf = sql.dfquery(f"select * from {ms_schema}.{create_table_name}")

        # table to csv
        sql.table_to_csv(create_table_name,
                         schema=ms_schema,
                         output_file=os.path.join(fldr, create_table_name + '.csv'),
                         sep=sep
                         )

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, create_table_name + '.csv'))

        # get df of csv
        csvdf = pd.read_csv(os.path.join(fldr, create_table_name + '.csv'), sep=sep)

        pd.testing.assert_frame_equal(csvdf.fillna(0).drop(columns=['WKT']), dbdf.fillna(0), check_dtype=False)

        # clean up
        sql.drop_table(ms_schema, create_table_name)
        os.remove(os.path.join(fldr, create_table_name + '.csv'))

    def test_table_to_csv_check_file_values2_sep(self):
        sep = ';'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{create_table_name} (
                objectid int,
                nd int,
                street1 text
            );

            insert into {ms_schema}.{create_table_name}
            values
                (1, 2, 'Mulberry'),
                (2, 3, 'Canal'),
                (3, 4, 'Fifth Ave.')
        """
        )
        assert sql.table_exists(create_table_name, schema=ms_schema)

        # get df of table in db
        dbdf = sql.dfquery(f"select * from {ms_schema}.{create_table_name}")

        # table to csv
        sql.table_to_csv(create_table_name,
                         schema=ms_schema,
                         output_file=os.path.join(fldr, create_table_name + '.csv'),
                         sep=sep
                         )

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, create_table_name + '.csv'))

        # get df of csv
        csvdf = pd.read_csv(os.path.join(fldr, create_table_name + '.csv'), sep=sep)

        pd.testing.assert_frame_equal(csvdf.fillna(0).drop(columns=['WKT']), dbdf.fillna(0), check_dtype=False)

        # clean up
        sql.drop_table(ms_schema, create_table_name)
        os.remove(os.path.join(fldr, create_table_name + '.csv'))

    def test_table_to_csv_no_schema(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        # create table
        sql.query(f"""
                    CREATE TABLE {sql.default_schema}.{create_table_name} (
                        id int,
                        name varchar(50)
                    );

                    insert into {sql.default_schema}.{create_table_name}
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
                """)
        assert sql.table_exists(create_table_name, schema=sql.default_schema)

        # table to csv
        sql.table_to_csv(create_table_name,
                         output_file=os.path.join(fldr, create_table_name + '.csv')
                         )

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, create_table_name + '.csv'))

        # clean up
        sql.drop_table(sql.default_schema, create_table_name)
        os.remove(os.path.join(fldr, create_table_name + '.csv'))

    def test_table_to_csv_table_doesnt_exist(self):
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data_no_folder')

        sql.drop_table(table=create_table_name, schema=ms_schema)
        assert not sql.table_exists(create_table_name, schema=schema)

        with raises(RuntimeError) as exc_info:
            sql.table_to_csv(create_table_name,
                             schema=schema,
                             output_file=os.path.join(fldr, create_table_name + '.csv')
                             )

    def test_table_to_csv_no_output(self):

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{create_table_name} (
                objectid int,
                nd int,
                street1 text
            );

            insert into {ms_schema}.{create_table_name}
            values
                (1, 2, 'Mulberry'),
                (2, 3, 'Canal'),
                (3, 4, 'Fifth Ave.')
        """)
        assert sql.table_exists(create_table_name, schema=ms_schema)

        # table to csv
        sql.table_to_csv(create_table_name,
                         schema=ms_schema
                         )

        # check table in folder (since no name specified, output goes to current dir + table name + .csv
        files = os.listdir(os.getcwd())
        assert any([create_table_name in f for f in files])

        # clean up
        sql.drop_table(ms_schema, create_table_name)
        for f in [f for f in files if create_table_name in f]:
            os.remove(os.path.join(os.getcwd(), f))

    def test_table_to_csv_check_file_bracket_name(self):
        table = '[' + create_table_name + ']'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        sql.drop_table(ms_schema, table)

        # create table
        sql.query(f"""
            CREATE TABLE {ms_schema}.{table} (
                objectid int,
                nd int,
                street1 text
            );

            insert into {ms_schema}.{table}
            values
                (1, 2, 'Mulberry'),
                (2, 3, 'Canal'),
                (3, 4, 'Fifth Ave.')
        """)
        assert sql.table_exists(table, schema=ms_schema)

        # table to csv
        sql.table_to_csv(table,
                         schema=ms_schema,
                         output_file=os.path.join(fldr, create_table_name + ".csv")
                         )

        # check table in folder
        assert os.path.isfile(os.path.join(fldr, create_table_name + ".csv"))

        # clean up
        sql.drop_table(ms_schema, table)
        os.remove(os.path.join(fldr, create_table_name + ".csv"))

    def test_table_to_csv_temp(self):
        table = '##_testing_table_to_csv_20200914_'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
    
        # create table
        sql.query(f"""
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
        """)
        sql.query(f"""select * from {table}""")
        print(sql.data)
        assert len(sql.data) == 3
    
        sql.table_to_csv(table, output_file=os.path.join(fldr, table[1:] + '.csv'))
    
        # check table in folder
        assert os.path.isfile(os.path.join(fldr, table[1:] + '.csv'))
    
        # clean up
        os.remove(os.path.join(fldr, table[1:] + '.csv'))

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_test_table_sql(sql, schema=ms_schema)
        sql.drop_table(schema = ms_schema, table = sql.log_table)
        sql.cleanup_new_tables()
