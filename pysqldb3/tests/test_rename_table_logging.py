import os

import configparser

from .. import pysqldb3 as pysqldb

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

ms = pysqldb.DbConnect(type=config.get('SECOND_SQL_DB', 'TYPE'),
                       server=config.get('SECOND_SQL_DB', 'SERVER'),
                       database=config.get('SECOND_SQL_DB', 'DB_NAME'),
                       user=config.get('SECOND_SQL_DB', 'DB_USER'),
                       password=config.get('SECOND_SQL_DB', 'DB_PASSWORD'))

pg = pysqldb.DbConnect(type=config.get('SECOND_PG_DB', 'TYPE'),
                       server=config.get('SECOND_PG_DB', 'SERVER'),
                       database=config.get('SECOND_PG_DB', 'DB_NAME'),
                       user=config.get('SECOND_PG_DB', 'DB_USER'),
                       password=config.get('SECOND_PG_DB', 'DB_PASSWORD'))



class Test_rename_ms():
    def test_rename_no_log_exists(self):
        tbl = f'__test__{pg.user}'
        rename = '%s_rename_' % tbl
        ms.drop_table(ms.default_schema, '{t}'.format(t=tbl))
        # set up
        ms.query("create table {}.{} (test_col int)".format(ms.default_schema, tbl)) # this will create the log
        ms.drop_table(ms.default_schema, '__temp_log_table_None__') # drop it to test the known condition

        ms.drop_table(ms.default_schema, '{r}'.format(r=rename))
        ms.query("EXEC sp_rename '{s}.{t}', '{r}';".format(s=ms.default_schema, t=tbl, r=rename))

        assert ms.table_exists(rename, schema=ms.default_schema)

        assert not ms.table_exists('__temp_log_table_None__', schema=ms.default_schema)

        ms.drop_table(ms.default_schema, tbl)
        ms.drop_table(ms.default_schema, rename)


    def test_rename_log_exists(self):
        tbl = f'__test__{pg.user}' # table to be logged
        rename = '%s_rename_' % tbl # renamed table to be logged
        tbl2 = f'__test2__{pg.user}' # table not to be logged
        rename2 = '%s_rename_' % tbl2 # renamed table not to be logged

        # create temp table
        ms.drop_table(ms.default_schema, '{t}'.format(t=tbl))
        ms.query("create table {}.{} (test_col int)".format(ms.default_schema, tbl)) # this will create the log

        # create non-temp table
        ms.drop_table(ms.default_schema, '{t}'.format(t=tbl2))
        ms.query("create table {}.{} (test_col int)".format(ms.default_schema, tbl2), temp=False)  # this will add a table to the log

        # rename temp table - should update log
        ms.drop_table(ms.default_schema, '{r}'.format(r=rename))
        ms.query("EXEC sp_rename '{s}.{t}', '{r}';".format(s=ms.default_schema, t=tbl, r=rename))

        # rename non-temp table should not effect log
        ms.drop_table(ms.default_schema, '{r}'.format(r=rename2))
        ms.query("EXEC sp_rename '{s}.{t}', '{r}';".format(s=ms.default_schema, t=tbl2, r=rename2))

        # make sure tables exist
        assert ms.table_exists(rename, schema=ms.default_schema)
        assert ms.table_exists(rename2, schema=ms.default_schema)

        # make sure temp rename is in th elog and non-temp rename is not
        ms.query("select table_name from {}.{}".format(ms.default_schema, ms.log_table))
        assert rename in rename in [i[0] for i in ms.data]
        assert not rename2 in rename in [i[0] for i in ms.data]

        # clean up
        ms.drop_table(ms.default_schema, '{t}'.format(t=tbl))
        ms.drop_table(ms.default_schema, '{t}'.format(t=tbl2))
        ms.drop_table(ms.default_schema, '{r}'.format(r=rename))
        ms.drop_table(ms.default_schema, '{r}'.format(r=rename2))


class Test_rename_pg():
    def test_rename_no_log_exists(self):
        tbl = f'__test__{pg.user}'
        rename = '%s_rename_' % tbl
        pg.drop_table(pg.default_schema, '{t}'.format(t=tbl))
        # set up
        pg.query("create table {}.{} (test_col int)".format(pg.default_schema, tbl)) # this will create the log
        pg.drop_table(pg.default_schema, '__temp_log_table_None__') # drop it to test the known condition

        pg.drop_table(pg.default_schema, '{r}'.format(r=rename))
        pg.query("alter table {s}.{t} rename to {r};".format(s=pg.default_schema, t=tbl, r=rename))

        assert pg.table_exists(rename, schema=pg.default_schema)

        assert not pg.table_exists('__temp_log_table_None__', schema=pg.default_schema)

        pg.drop_table(pg.default_schema, tbl)
        pg.drop_table(pg.default_schema, rename)


    def test_rename_log_exists(self):
        tbl = f'__test__{pg.user}' # table to be logged
        rename = '%s_rename_' % tbl # renamed table to be logged
        tbl2 = f'__test2__{pg.user}' # table not to be logged
        rename2 = '%s_rename_' % tbl2 # renamed table not to be logged

        # create temp table
        pg.drop_table(pg.default_schema, '{t}'.format(t=tbl))
        pg.query("create table {}.{} (test_col int)".format(pg.default_schema, tbl)) # this will create the log

        # create non-temp table
        pg.drop_table(pg.default_schema, '{t}'.format(t=tbl2))
        pg.query("create table {}.{} (test_col int)".format(pg.default_schema, tbl2), temp=False)  # this will add a table to the log

        # rename temp table - should update log
        pg.drop_table(pg.default_schema, '{r}'.format(r=rename))
        pg.query("alter table {s}.{t} rename to {r};".format(s=pg.default_schema, t=tbl, r=rename))

        # rename non-temp table should not effect log
        pg.drop_table(pg.default_schema, '{r}'.format(r=rename2))
        pg.query("alter table {s}.{t} rename to {r};".format(s=pg.default_schema, t=tbl2, r=rename2))

        # make sure tables exist
        assert pg.table_exists(rename, schema=pg.default_schema)
        assert pg.table_exists(rename2, schema=pg.default_schema)

        # make sure temp rename is in the log and non-temp rename is not
        pg.query("select table_name from {}.{}".format(pg.default_schema, pg.log_table))
        assert rename in pg.data[0]
        assert not rename2 in pg.data[0]

        # clean up
        pg.drop_table(pg.default_schema, '{t}'.format(t=tbl))
        pg.drop_table(pg.default_schema, '{t}'.format(t=tbl2))
        pg.drop_table(pg.default_schema, '{r}'.format(r=rename))
        pg.drop_table(pg.default_schema, '{r}'.format(r=rename2))