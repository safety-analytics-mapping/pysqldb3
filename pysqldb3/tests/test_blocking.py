import os
import time
from multiprocessing import Process, Queue

import configparser
from .. import pysqldb3 as pysqldb
from . import helpers

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                       server=config.get('PG_DB', 'SERVER'),
                       database=config.get('PG_DB', 'DB_NAME'),
                       user=config.get('PG_DB', 'DB_USER'),
                       password=config.get('PG_DB', 'DB_PASSWORD'), allow_temp_tables=True)

db2 = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                        server=config.get('PG_DB', 'SERVER'),
                        database=config.get('PG_DB', 'DB_NAME'),
                        user=config.get('PG_DB', 'DB_USER'),
                        password=config.get('PG_DB', 'DB_PASSWORD'))

db3 = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                        server=config.get('PG_DB', 'SERVER'),
                        database=config.get('PG_DB', 'DB_NAME'),
                        user=config.get('PG_DB', 'DB_USER'),
                        password=config.get('PG_DB', 'DB_PASSWORD'))

pg_table_name = 'pg_test_table_{}'.format(db.user)
pg_table_name2 = 'pg_test_table_{}_2'.format(db.user)
create_table_name = 'long_time_table_{}'.format(db.user)


def blockfunc1():
    """
    This function intentionally uses a cartesian join to take a long time.
    """
    try:
        db.query("""
        --IntentionalBlockingQuery    
        insert into working.{}  
        select distinct n.id
        from (
        select * 
        from working.{}
        limit 10000
        ) l 
        join (
        select * 
        from working.{}
        limit 10000
        ) n
        on st_intersects(l.geom, n.geom);
       """.format(create_table_name, pg_table_name, pg_table_name2))
    except Exception as e:
        print(e)


def blockfunc2():
    """
    This function intentionally uses a lock to try to cause a block.
    """
    time.sleep(1)
    db2.query("""
    LOCK TABLE working.{} IN ACCESS EXCLUSIVE MODE;

    select * 
    from working.{}
    """.format(create_table_name, create_table_name))


def blockfunc3(q):
    """
    This functional intentionally uses sleep to make sure a block has been registered

    It then captures the results of blocking_me.
    """
    time.sleep(2)
    q.put(db3.blocking_me())


def blockfunc4(q):
    """
    This functional intentionally uses sleep to make sure a block has been registered

    It then captures the results of blocking_me and kills them.
    """
    time.sleep(2)
    q.put(db3.blocking_me())
    db3.kill_blocks()


class TestBlocking:
    @classmethod
    def setup_class(cls):
        helpers.set_up_two_test_tables_pg(db)

    def test_blocking_me(self):
        """
        Uses parallel processing to run each of the queries above (with minor lags in between,
        unless specified by time.sleep

        Since they run concurrently, they will have a block.
        """

        """
        Create new shell of table
        """
        q = Queue()
        db.drop_table(schema='working', table=create_table_name)

        db.query("""
        create table working.{} as 
        select distinct n.id
        from (
        select * 
        from working.{}
        limit 1
        ) l 
        join (
        select * 
        from working.{}
        limit 1
        ) n
        on st_intersects(l.geom, n.geom);
        """.format(create_table_name, pg_table_name, pg_table_name2))

        """
        Run queries in parallel
        """
        p1 = Process(target=blockfunc1)
        p1.start()
        p2 = Process(target=blockfunc2)
        p2.start()
        p3 = Process(target=blockfunc3, args=(q,))
        p3.start()

        """
        Waits for all functions to finish running
        """
        p1.join()
        p2.join()
        p3.join()
        blocking_df = q.get()

        """
        Retrieves the blocking_me df and asserts based off of this df 
        """
        assert blocking_df is not None
        assert len(blocking_df) == 1
        assert 'IntentionalBlockingQuery' in blocking_df.iloc[0]['current_statement_in_blocking_process']

        """
        Cleanup
        """
        db.drop_table(schema='working', table=create_table_name)

    def test_kill_blocks(self):
        """
        Uses parallel processing to run each of the queries above (with minor lags in between,
        unless specified by time.sleep

        Since they run concurrently, they will have a block.
        """

        """
        Create new shell of table
        """
        q = Queue()
        db.drop_table(schema='working', table=create_table_name)

        db.query("""
        create table working.{} as 
        select distinct n.id
        from (
        select * 
        from working.{}
        limit 1
        ) l 
        join (
        select * 
        from working.{}
        limit 1
        ) n
        on st_intersects(l.geom, n.geom);
        """.format(create_table_name, pg_table_name, pg_table_name2))

        """
        Run queries in parallel
        """
        p1 = Process(target=blockfunc1)
        p1.start()
        p2 = Process(target=blockfunc2)
        p2.start()
        p4 = Process(target=blockfunc4, args=(q,))
        p4.start()

        """
        Waits for all functions to finish running
        """
        p1.join()
        p2.join()
        blocking_df = q.get()
        p4.join()

        """
        Retrieves the blocking_me df and asserts based off of this df 
        """
        assert blocking_df is not None
        assert len(blocking_df) == 1
        assert 'IntentionalBlockingQuery' in blocking_df.iloc[0]['current_statement_in_blocking_process']

        """
        Confirms no records were added to created table, meaning the insert query was killed successfully.
        """
        end_result_df = db.dfquery("""select * from working.{}""".format(create_table_name))
        assert len(end_result_df) <= 1

        """
        Cleanup
        """
        db.drop_table(schema='working', table=create_table_name)

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_two_test_tables_pg(db)
        db.cleanup_new_tables()
        db2.cleanup_new_tables()
        db3.cleanup_new_tables()
