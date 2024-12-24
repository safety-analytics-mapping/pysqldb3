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

pg_table_name = f'pg_test_table_{db.user}'
pg_table_name2 = f'pg_test_table_{db.user}_2'
create_table_name = f'long_time_table_{db.user}'
pg_schema = 'working'

def blockfunc1():
    """
    This function intentionally uses a cartesian join to take a long time.
    """
    try:
        db.query(f"""
        --IntentionalBlockingQuery    
        insert into {pg_schema}.{create_table_name}  
        select distinct n.id
        from (
            select * 
            from {pg_schema}.{pg_table_name}
            limit 10000
            ) l 
        join (
            select * 
            from {pg_schema}.{pg_table_name2}
            limit 10000
            ) n
        on st_intersects(l.geom, n.geom);
       """)
    except Exception as e:
        print(e)


def blockfunc2():
    """
    This function intentionally uses a lock to try to cause a block.
    """
    time.sleep(1)
    db2.query(f"""
    LOCK TABLE {pg_schema}.{create_table_name} IN ACCESS EXCLUSIVE MODE;

    select * 
    from {pg_schema}.{create_table_name}
    """)


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
        db.drop_table(schema=pg_schema, table=create_table_name)

        db.query(f"""
        create table {pg_schema}.{create_table_name} as 
        select distinct n.id
        from (
            select * 
            from {pg_schema}.{pg_table_name}
            limit 1
            ) l 
        join (
            select * 
            from {pg_schema}.{pg_table_name2}
            limit 1
            ) n
        on st_intersects(l.geom, n.geom);
        """)

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
        db.drop_table(schema= pg_schema, table=create_table_name)

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
        db.drop_table(schema=pg_schema, table=create_table_name)

        db.query(f"""
        create table {pg_schema}.{create_table_name} as 
        select distinct n.id
        from (
            select * 
            from {pg_schema}.{ pg_table_name}
            limit 1
            ) l 
        join (
            select * 
            from {pg_schema}.{pg_table_name2}
            limit 1
            ) n
        on st_intersects(l.geom, n.geom);
        """)

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
        end_result_df = db.dfquery(f"""select * from {pg_schema}.{create_table_name}""")
        assert len(end_result_df) <= 1

        """
        Cleanup
        """
        db.drop_table(schema=pg_schema, table=create_table_name)

    @classmethod
    def teardown_class(cls):
        helpers.clean_up_two_test_tables_pg(db)
        db.cleanup_new_tables()
        db2.cleanup_new_tables()
        db3.cleanup_new_tables()
