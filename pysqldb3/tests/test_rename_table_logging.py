import os

import configparser

from .. import pysqldb3 as pysqldb

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

ms_dbconn = pysqldb.DbConnect(db_type=config.get('SECOND_SQL_DB', 'TYPE'),
                              host=config.get('SECOND_SQL_DB', 'SERVER'),
                              db_name=config.get('SECOND_SQL_DB', 'DB_NAME'),
                              username=config.get('SECOND_SQL_DB', 'DB_USER'),
                              password=config.get('SECOND_SQL_DB', 'DB_PASSWORD'))

pg_dbconn = pysqldb.DbConnect(db_type=config.get('SECOND_PG_DB', 'TYPE'),
                              host=config.get('SECOND_PG_DB', 'SERVER'),
                              db_name=config.get('SECOND_PG_DB', 'DB_NAME'),
                              username=config.get('SECOND_PG_DB', 'DB_USER'),
                              password=config.get('SECOND_PG_DB', 'DB_PASSWORD'))



class Test_rename_ms():
    def test_rename_no_log_exists(self):
        table = '__test__'
        renamed_table = '%s_rename_' % table
        ms_dbconn.drop_table(schema_name=ms_dbconn.default_schema, table_name=table)
        # set up
        ms_dbconn.query("create table {schema}.{table} (test_col int)".format(schema=ms_dbconn.default_schema, table=table)) # this will create the log
        ms_dbconn.drop_table(schema_name=ms_dbconn.default_schema, table_name='__temp_log_table_None__') # drop it to test the known condition

        ms_dbconn.drop_table(schema_name=ms_dbconn.default_schema, table_name=renamed_table)
        ms_dbconn.query("EXEC sp_rename '{schema}.{table}', '{renamed}';".format(schema=ms_dbconn.default_schema, table=table, renamed=renamed_table))

        assert ms_dbconn.table_exists(table_name=renamed_table, schema_name=ms_dbconn.default_schema)

        assert not ms_dbconn.table_exists(table_name='__temp_log_table_None__', schema=ms_dbconn.default_schema)

        ms_dbconn.drop_table(schema_name=ms_dbconn.default_schema, table_name=table)
        ms_dbconn.drop_table(schema_name=ms_dbconn.default_schema, table_name=renamed_table)


    def test_rename_log_exists(self):
        table = '__test__' # table to be logged
        renamed_table = '%s_rename_' % table # renamed table to be logged
        table2 = '__test2__' # table not to be logged
        renamed_table2 = '%s_rename_' % table2 # renamed table not to be logged

        # create temp table
        ms_dbconn.drop_table(ms_dbconn.default_schema, '{table}'.format(table=table))
        ms_dbconn.query("create table {schema}.{table} (test_col int)".format(schema=ms_dbconn.default_schema, table=table)) # this will create the log

        # create non-temp table
        ms_dbconn.drop_table(ms_dbconn.default_schema, '{table}'.format(table=table2))
        ms_dbconn.query("create table {schema}.{table} (test_col int)".format(schema=ms_dbconn.default_schema, table=table2), temp=False)  # this will add a table to the log

        # rename temp table - should update log
        ms_dbconn.drop_table(ms_dbconn.default_schema, '{renamed}'.format(renamed=renamed_table))
        ms_dbconn.query("EXEC sp_rename '{schema}.{table}', '{renamed}';".format(schema=ms_dbconn.default_schema, table=table, renamed=renamed_table))

        # rename non-temp table should not effect log
        ms_dbconn.drop_table(ms_dbconn.default_schema, '{renamed}'.format(renamed=renamed_table2))
        ms_dbconn.query("EXEC sp_rename '{schema}.{table}', '{renamed}';".format(schema=ms_dbconn.default_schema, table=table2, renamed=renamed_table2))

        # make sure tables exist
        assert ms_dbconn.table_exists(table_name=renamed_table, schema=ms_dbconn.default_schema)
        assert ms_dbconn.table_exists(table_name=renamed_table2, schema=ms_dbconn.default_schema)

        # make sure temp rename is in th elog and non-temp rename is not
        ms_dbconn.query("select table_name from {schema}.{table}".format(schema=ms_dbconn.default_schema, table=ms_dbconn.log_table))
        assert renamed_table in renamed_table in [i[0] for i in ms_dbconn.data]
        assert not renamed_table2 in renamed_table in [i[0] for i in ms_dbconn.data]

        # clean up
        ms_dbconn.drop_table(table_name=ms_dbconn.default_schema, schema_name=table)
        ms_dbconn.drop_table(table_name=ms_dbconn.default_schema, schema_name=table2)
        ms_dbconn.drop_table(table_name=ms_dbconn.default_schema, schema_name=renamed_table)
        ms_dbconn.drop_table(table_name=ms_dbconn.default_schema, schema_name=renamed_table2)


class Test_rename_pg():
    def test_rename_no_log_exists(self):
        table = '__test__'
        renamed_table = '%s_rename_' % table
        pg_dbconn.drop_table(pg_dbconn.default_schema, '{table}'.format(table=table))
        # set up
        pg_dbconn.query("create table {schema}.{table} (test_col int)".format(schema=pg_dbconn.default_schema, table=table)) # this will create the log
        pg_dbconn.drop_table(pg_dbconn.default_schema, '__temp_log_table_None__') # drop it to test the known condition

        pg_dbconn.drop_table(pg_dbconn.default_schema, '{rename}'.format(rename=renamed_table))
        pg_dbconn.query("alter table {schema}.{table} rename to {renamed};".format(schema=pg_dbconn.default_schema, table=table, renamed=renamed_table))

        assert pg_dbconn.table_exists(table_name=renamed_table, schema_name=pg_dbconn.default_schema)

        assert not pg_dbconn.table_exists('__temp_log_table_None__', schema=pg_dbconn.default_schema)

        pg_dbconn.drop_table(schema_name=pg_dbconn.default_schema, table_name=table)
        pg_dbconn.drop_table(schema_name=pg_dbconn.default_schema, table_name=renamed_table)


    def test_rename_log_exists(self):
        tbl = '__test__' # table to be logged
        rename = '%s_rename_' % tbl # renamed table to be logged
        tbl2 = '__test2__' # table not to be logged
        rename2 = '%s_rename_' % tbl2 # renamed table not to be logged

        # create temp table
        pg_dbconn.drop_table(pg_dbconn.default_schema, '{table}'.format(table=tbl))
        pg_dbconn.query("create table {schema}.{table} (test_col int)".format(schema=pg_dbconn.default_schema, table=tbl)) # this will create the log

        # create non-temp table
        pg_dbconn.drop_table(pg_dbconn.default_schema, '{table}'.format(table=tbl2))
        pg_dbconn.query("create table {schema}.{table} (test_col int)".format(schema=pg_dbconn.default_schema, table=tbl2), temp=False)  # this will add a table to the log

        # rename temp table - should update log
        pg_dbconn.drop_table(pg_dbconn.default_schema, '{rename}'.format(rename=rename))
        pg_dbconn.query("alter table {schema}.{table} rename to {renamed};".format(schema=pg_dbconn.default_schema, table=tbl, renamed=rename))

        # rename non-temp table should not effect log
        pg_dbconn.drop_table(pg_dbconn.default_schema, '{rename}'.format(rename=rename2))
        pg_dbconn.query("alter table {schema}.{table} rename to {renamed};".format(schema=pg_dbconn.default_schema, table=tbl2, renamed=rename2))

        # make sure tables exist
        assert pg_dbconn.table_exists(table_name=rename, schema_name=pg_dbconn.default_schema)
        assert pg_dbconn.table_exists(table_name=rename2, schema_name=pg_dbconn.default_schema)

        # make sure temp rename is in th elog and non-temp rename is not
        pg_dbconn.query("select table_name from {schema}.{table}".format(schema=pg_dbconn.default_schema, table=pg_dbconn.log_table))
        assert rename in pg_dbconn.data[0]
        assert not rename2 in pg_dbconn.data[0]

        # clean up
        pg_dbconn.drop_table(schema_name=pg_dbconn.default_schema, table_name=tbl)
        pg_dbconn.drop_table(schema_name=pg_dbconn.default_schema, table_name=tbl2)
        pg_dbconn.drop_table(schema_name=pg_dbconn.default_schema, table_name=rename)
        pg_dbconn.drop_table(schema_name=pg_dbconn.default_schema, table_name=rename2)