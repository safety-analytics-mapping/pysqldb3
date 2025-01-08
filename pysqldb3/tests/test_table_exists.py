import os

import configparser

from .. import pysqldb3 as pysqldb

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                       server=config.get('PG_DB', 'SERVER'),
                       database=config.get('PG_DB', 'DB_NAME'),
                       user=config.get('PG_DB', 'DB_USER'),
                       password=config.get('PG_DB', 'DB_PASSWORD'),
                       allow_temp_tables=True)

ris_db = pysqldb.DbConnect(type=config.get('SECOND_PG_DB', 'TYPE'),
                           server=config.get('SECOND_PG_DB', 'SERVER'),
                           database=config.get('SECOND_PG_DB', 'DB_NAME'),
                           user=config.get('SECOND_PG_DB', 'DB_USER'),
                           password=config.get('SECOND_PG_DB', 'DB_PASSWORD'),
                           allow_temp_tables=True)

sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                        server=config.get('SQL_DB', 'SERVER'),
                        database=config.get('SQL_DB', 'DB_NAME'),
                        user=config.get('SQL_DB', 'DB_USER'),
                        password=config.get('SQL_DB', 'DB_PASSWORD'),
                        allow_temp_tables=True)

test_table = f'pytest_{db.user}'
test_table2 = f'Pytest_{db.user}'
test_table3 = f'3Pytest_{db.user}'

pg_schema = 'working'

class TestTableExistsPG():

    def test_table_exists_pg_public_create(self):
        db.query(f"""
            drop table if exists {test_table};
            create table {test_table} (id integer)
        """)

        assert db.table_exists(test_table)

        db.query(f"""
            drop table if exists {test_table}
        """)

    def test_table_exists_pg_public_drop(self):
        db.query(f"""
             drop table if exists {test_table};
             create table {test_table} (id integer)
         """)

        assert db.table_exists(test_table)

        db.query(f"""
             drop table if exists {test_table}
         """)

        assert not db.table_exists(test_table)

    def test_table_exists_pg_working_create(self):
        ris_db.query(f"""
            drop table if exists {pg_schema}.{test_table};
            create table {pg_schema}.{test_table} (id integer)
        """)

        assert ris_db.table_exists(schema=pg_schema, table=test_table)

        ris_db.drop_table(schema=pg_schema, table=test_table)

    def test_table_exists_pg_rename(self):
        renamed_test_table = test_table + "_renamed"

        ris_db.query(f"""
        drop table if exists {pg_schema}.{renamed_test_table};
        drop table if exists {pg_schema}.{test_table};
        create table {pg_schema}.{test_table} (id integer);
        """)

        assert ris_db.table_exists(schema=pg_schema, table=test_table)

        ris_db.query(f"""
            alter table {pg_schema}.{test_table} rename to {renamed_test_table}
        """)

        assert not ris_db.table_exists(schema=pg_schema, table=test_table)
        assert ris_db.table_exists(schema=pg_schema, table=renamed_test_table)

        ris_db.drop_table(schema=pg_schema, table=renamed_test_table)

    def test_table_exists_pg_working_drop(self):
        renamed_test_table = test_table + "_renamed"

        ris_db.query(f"""
        drop table if exists {pg_schema}.{renamed_test_table};
        drop table if exists {pg_schema}.{test_table};
        
        create table {pg_schema}.{renamed_test_table} (id integer);

        alter table {pg_schema}.{renamed_test_table} rename to {test_table};
        """)

        assert ris_db.table_exists(schema=pg_schema, table=test_table)
        assert not ris_db.table_exists(schema=pg_schema, table=renamed_test_table)

        ris_db.query(f"""
        drop table {pg_schema}.{test_table};
        drop table if exists {pg_schema}.{renamed_test_table}
        """)

        assert not ris_db.table_exists(schema=pg_schema, table=test_table)

    def test_table_exists_pg_temp_create(self):
        ris_db.query("""create temp table bf_pytest (id integer); """)
        assert ris_db.table_exists(table='bf_pytest') == False

    def test_table_exists_pg_temp_drop(self):
        ris_db.query("""drop table bf_pytest""")
        assert ris_db.table_exists(table='bf_pytest') == False

    def test_table_exists_special_chars(self):
        for tbl in (test_table2, test_table3):
            db.query(f"""
                drop table if exists "{tbl}";
                create table "{tbl}" (id integer)
            """)

            assert db.table_exists(tbl, case_sensitive=True)

            db.query(f"""
                drop table if exists "{tbl}"
            """)

    def test_table_exists_special_chars_auto(self):
        db.drop_table(schema=db.default_schema, table=test_table3)
        db.query(f"""
            create table "{test_table3}" (id integer)
        """)

        assert db.table_exists(test_table3)

        db.query(f"""
            drop table if exists "{test_table3}"
        """)




class TestTableExistsMS():

    def test_table_exists_ms_create(self):
        sql.drop_table(schema=sql.default_schema, table=test_table)

        sql.query(f"""
            create table {test_table} (id integer) 
        """)

        assert sql.table_exists(test_table)

        sql.drop_table(schema=sql.default_schema, table=test_table)

    def test_table_exists_ms_rename(self):
        renamed_test_table = test_table + "_renamed"

        sql.drop_table(schema=sql.default_schema, table=renamed_test_table)
        sql.drop_table(schema=sql.default_schema, table=test_table)

        sql.query(f"""
            create table {test_table} (id integer) 
        """)

        assert sql.table_exists(test_table)

        sql.query(f"""
            sp_rename {test_table}, {renamed_test_table}
        """)

        assert not sql.table_exists(test_table)
        assert sql.table_exists(renamed_test_table)

        # Cleanup
        sql.drop_table(schema=sql.default_schema, table=renamed_test_table)

    def test_table_exists_ms_drop(self):
        renamed_test_table = test_table + "_renamed"

        sql.drop_table(schema=sql.default_schema, table=test_table)
        sql.drop_table(schema=sql.default_schema, table=renamed_test_table)

        sql.query(f"""
            create table {renamed_test_table} (id integer) 
        """)

        assert not sql.table_exists(test_table)
        assert sql.table_exists(renamed_test_table)

        sql.query(f"""
            sp_rename {renamed_test_table}, {test_table};
            drop table {test_table};
        """)

        assert not sql.table_exists(test_table)

    def test_table_exists_ms_temp_create(self):
        sql.query("""create table #pytest (id integer)""")
        assert not sql.table_exists('#pytest')

    def test_table_exists_ms_temp_drop(self):
        sql.query("""drop table #pytest""")
        assert not sql.table_exists('#pytest')


    def test_table_exists_special_chars(self):
        for tbl in (test_table2, test_table3):
            sql.drop_table(schema=sql.default_schema, table=tbl)
            sql.query(f"""
                create table "{tbl}" (id integer)
            """)

            assert sql.table_exists(tbl, case_sensitive=True)

            sql.query(f"""
                drop table if exists "{tbl}"
            """)

    def test_table_exists_special_chars_auto(self):
        sql.drop_table(schema=sql.default_schema, table=test_table3)
        sql.query(f"""
            create table "{test_table3}" (id integer)
        """)

        assert sql.table_exists(test_table3)

        sql.query(f"""
            drop table if exists "{test_table3}"
        """)
