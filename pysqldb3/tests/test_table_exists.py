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

test_table = 'pytest_{}'.format(db.user)
test_table2 = 'Pytest_{}'.format(db.user)
test_table3 = '3Pytest_{}'.format(db.user)


class TestTableExistsPG():

    def test_table_exists_pg_public_create(self):
        db.query("""
            drop table if exists {};
            create table {} (id integer)
        """.format(test_table, test_table))

        assert db.table_exists(test_table)

        db.query("""
            drop table if exists {}
        """.format(test_table))

    def test_table_exists_pg_public_drop(self):
        db.query("""
             drop table if exists {};
             create table {} (id integer)
         """.format(test_table, test_table))

        assert db.table_exists(test_table)

        db.query("""
             drop table if exists {}
         """.format(test_table))

        assert not db.table_exists(test_table)

    def test_table_exists_pg_working_create(self):
        ris_db.query("""
            drop table if exists working.{};
            create table working.{} (id integer)
        """.format(test_table, test_table))

        assert ris_db.table_exists(schema='working', table=test_table)

        ris_db.drop_table(schema='working', table=test_table)

    def test_table_exists_pg_rename(self):
        renamed_test_table = test_table + "_renamed"

        ris_db.query("""
        drop table if exists working.{};
        drop table if exists working.{};
        create table working.{} (id integer);
        """.format(renamed_test_table, test_table, test_table))

        assert ris_db.table_exists(schema='working', table=test_table)

        ris_db.query("""
            alter table working.{} rename to {}
        """.format(test_table, renamed_test_table))

        assert not ris_db.table_exists(schema='working', table=test_table)
        assert ris_db.table_exists(schema='working', table=renamed_test_table)

        ris_db.drop_table(schema='working', table=renamed_test_table)

    def test_table_exists_pg_working_drop(self):
        renamed_test_table = test_table + "_renamed"

        ris_db.query("""
        drop table if exists working.{};
        drop table if exists working.{};
        
        create table working.{} (id integer);

        alter table working.{} rename to {};
        """.format(renamed_test_table, test_table, renamed_test_table, renamed_test_table, test_table))

        assert ris_db.table_exists(schema='working', table=test_table)
        assert not ris_db.table_exists(schema='working', table=renamed_test_table)

        ris_db.query("""
        drop table working.{};
        drop table if exists working.{}
        """.format(test_table, renamed_test_table))

        assert not ris_db.table_exists(schema='working', table=test_table)

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

        sql.query("""
            create table {} (id integer) 
        """.format(test_table))

        assert sql.table_exists(test_table)

        sql.drop_table(schema=sql.default_schema, table=test_table)

    def test_table_exists_ms_rename(self):
        renamed_test_table = test_table + "_renamed"

        sql.drop_table(schema=sql.default_schema, table=renamed_test_table)
        sql.drop_table(schema=sql.default_schema, table=test_table)

        sql.query("""
            create table {} (id integer) 
        """.format(test_table))

        assert sql.table_exists(test_table)

        sql.query("""
            sp_rename {}, {}
        """.format(test_table, renamed_test_table))

        assert not sql.table_exists(test_table)
        assert sql.table_exists(renamed_test_table)

        # Cleanup
        sql.drop_table(schema=sql.default_schema, table=renamed_test_table)

    def test_table_exists_ms_drop(self):
        renamed_test_table = test_table + "_renamed"

        sql.drop_table(schema=sql.default_schema, table=test_table)
        sql.drop_table(schema=sql.default_schema, table=renamed_test_table)

        sql.query("""
            create table {} (id integer) 
        """.format(renamed_test_table))

        assert not sql.table_exists(test_table)
        assert sql.table_exists(renamed_test_table)

        sql.query("""
            sp_rename {}, {};
            drop table {};
        """.format(renamed_test_table, test_table, test_table))

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
