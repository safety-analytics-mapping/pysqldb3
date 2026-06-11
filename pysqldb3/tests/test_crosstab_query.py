import os
import configparser
import pytest

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


test_query_table = 'test_query_table_{}'.format(db.user)
pg_schema= 'working'
ms_schema= 'dbo'

class TestQuery:
    def test_crosstab_query_simple_pg(self):
        db.drop_table(table=test_query_table, schema=pg_schema)

        db.query(f"create table {pg_schema}.{test_query_table} (col1 int, col2 varchar, col3 varchar, col4 int);")
        for i in range(97, 123):
            db.query(f"insert into {pg_schema}.{test_query_table} values ({i+1}, '{chr(i)}', case when {i} < 111 then 'low' else 'high' end, case when {i+1} < 118 then 1 else 2 end)", timeme=False)

        # Assert query successfully executed create table
        assert db.table_exists(table=test_query_table, schema=pg_schema)

        # Assert cross tab works - count
        qry = f"select * from {pg_schema}.{test_query_table}"

        # crosstab test table 1 row column
        db.crosstab_query(qry, ['col3'], 'col4', 'col2')
        x_data = db.data

        db.query(f"""
        select 
                col3, 
                count(distinct case when col4 = '1' then col2 else null end) as "1",
                count(distinct case when col4 = '2' then col2 else null end) as "2"
            from (select * from {pg_schema}.{test_query_table}) t 
            group by col3
        """)
        assert db.data == x_data

        # crosstab test table 2 row columns
        db.crosstab_query(qry, ['col2', 'col3'], 'col4', 'col2', 'col1')
        x_data = db.data

        db.query(f"""
        select 
            col2, 
            col3, 
                count(distinct case when col4 = '1' then col2 else null end) as "1",
                count(distinct case when col4 = '2' then col2 else null end) as "2",
                sum(distinct case when col4 = '1' then col1 else null end) as "1",
                sum(distinct case when col4 = '2' then col1 else null end) as "2"
            from (select * from {pg_schema}.{test_query_table}) t 
            group by col2, col3
        """)
        assert db.data == x_data

        # Cleanup
        db.drop_table(table=test_query_table, schema=pg_schema)


    def test_crosstab_query_simple_df(self):
        db.drop_table(table=test_query_table, schema=pg_schema)

        db.query(f"create table {pg_schema}.{test_query_table} (year int, month int, veh_type varchar, vehicle_id int);")
        # ['year', 'month'], 'veh_type', 'vehicle_id', df=True)
        db.query(f"insert into {pg_schema}.{test_query_table} values (2025, 1, 'bike',1)", timeme=False)
        db.query(f"insert into {pg_schema}.{test_query_table} values (2025, 1, 'bike',2)", timeme=False)
        db.query(f"insert into {pg_schema}.{test_query_table} values (2025, 2, 'bike',3)", timeme=False)
        db.query(f"insert into {pg_schema}.{test_query_table} values (2025, 2, 'car',4)", timeme=False)
        db.query(f"insert into {pg_schema}.{test_query_table} values (2026, 3, 'bike',5)", timeme=False)
        db.query(f"insert into {pg_schema}.{test_query_table} values (2026, 1, 'bike',6)", timeme=False)
        db.query(f"insert into {pg_schema}.{test_query_table} values (2026, 2, 'bike',7)", timeme=False)
        db.query(f"insert into {pg_schema}.{test_query_table} values (2026, 4, 'car',8)", timeme=False)

        # Assert query successfully executed create table
        assert db.table_exists(table=test_query_table, schema=pg_schema)

        # Assert cross tab works - count
        qry = f"select * from {pg_schema}.{test_query_table}"

        # crosstab test table 1 row column
        _df = db.crosstab_query(qry, ['year', 'month'], 'veh_type', 'vehicle_id', df=True)

        df_ = db.dfquery(f"""
            select 
            year, 
                month, 
                count(distinct case when veh_type = 'bike' then vehicle_id else null end) as "bike",
                count(distinct case when veh_type = 'car' then vehicle_id else null end) as "car" 
            from (select * from {pg_schema}.{test_query_table}) t 
            group by year, month
        """)
        assert _df.equals(df_)

        # Cleanup
        db.drop_table(table=test_query_table, schema=pg_schema)




    def test_crosstab_query_simple_ms(self):
        sql.drop_table(table=test_query_table, schema=ms_schema)

        sql.query(f"create table {ms_schema}.{test_query_table} (col1 int, col2 varchar, col3 varchar(10), col4 int);")
        for i in range(97, 123):
            sql.query(f"insert into {ms_schema}.{test_query_table} values ({i+1}, '{chr(i)}', case when {i} < 111 then 'low' else 'high' end, case when {i+1} < 118 then 1 else 2 end)", timeme=False)

        # Assert query successfully executed create table
        assert sql.table_exists(table=test_query_table, schema=ms_schema)

        # Assert cross tab works - count
        qry = f"select * from {ms_schema}.{test_query_table}"

        # crosstab test table 1 row column
        sql.crosstab_query(qry, ['col3'], 'col4', 'col2')
        x_data = sql.data

        sql.query(f"""
        select 
                col3, 
                count(distinct case when col4 = '1' then col2 else null end) as "1",
                count(distinct case when col4 = '2' then col2 else null end) as "2"
            from (select * from {ms_schema}.{test_query_table}) t 
            group by col3
        """)
        assert sql.data == x_data

        # crosstab test table 2 row columns
        sql.crosstab_query(qry, ['col2', 'col3'], 'col4', 'col2', 'col1')
        x_data = sql.data

        sql.query(f"""
        select 
            col2, 
            col3, 
                count(distinct case when col4 = '1' then col2 else null end) as "1",
                count(distinct case when col4 = '2' then col2 else null end) as "2",
                sum(distinct case when col4 = '1' then col1 else null end) as "1",
                sum(distinct case when col4 = '2' then col1 else null end) as "2"
            from (select * from {ms_schema}.{test_query_table}) t 
            group by col2, col3
        """)
        assert sql.data == x_data

        # Cleanup
        sql.drop_table(table=test_query_table, schema=ms_schema)

    def test_funky_columns_pg(self):
        db.drop_table(table=test_query_table, schema=pg_schema)
        db.query(f"""
            create table {pg_schema}.{test_query_table} 
            ("123- Col 1" int, "123- Col 2" varchar, col3 varchar, col4 int);""")
        for i in range(97, 123):
            db.query(
                f"insert into {pg_schema}.{test_query_table} values ({i + 1}, '{chr(i)}', case when {i} < 111 then 'low' else 'high' end, case when {i + 1} < 118 then 1 else 2 end)",
                timeme=False)

        # Assert query successfully executed create table
        assert db.table_exists(table=test_query_table, schema=pg_schema)

        # Assert cross tab works - count
        qry = f"select * from {pg_schema}.{test_query_table}"

        # crosstab test table 1 row column
        db.crosstab_query(qry, ['col3'], 'col4', '"123- Col 2"')
        x_data = db.data

        db.query(f"""
               select 
                       col3, 
                       count(distinct case when col4 = '1' then "123- Col 2" else null end) as "1",
                       count(distinct case when col4 = '2' then "123- Col 2" else null end) as "2"
                   from (select * from {pg_schema}.{test_query_table}) t 
                   group by col3
               """)
        assert db.data == x_data

        db.drop_table(table=test_query_table, schema=pg_schema)

    def test_funky_columns_ms(self):
        sql.drop_table(table=test_query_table, schema=ms_schema)
        sql.query(f"""
            create table {ms_schema}.{test_query_table} 
            ("123- Col 1" int, "123- Col 2" varchar(10), col3 varchar(10), col4 int);""")
        for i in range(97, 123):
            sql.query(
                f"insert into {ms_schema}.{test_query_table} values ({i + 1}, '{chr(i)}', case when {i} < 111 then 'low' else 'high' end, case when {i + 1} < 118 then 1 else 2 end)",
                timeme=False)

        # Assert query successfully executed create table
        assert sql.table_exists(table=test_query_table, schema=ms_schema)

        # Assert cross tab works - count
        qry = f"select * from {ms_schema}.{test_query_table}"

        # crosstab test table 1 row column
        sql.crosstab_query(qry, ['col3'], 'col4', '"123- Col 2"')
        x_data = sql.data

        sql.query(f"""
               select 
                       col3, 
                       count(distinct case when col4 = '1' then "123- Col 2" else null end) as "1",
                       count(distinct case when col4 = '2' then "123- Col 2" else null end) as "2"
                   from (select * from {ms_schema}.{test_query_table}) t 
                   group by col3
               """)
        assert sql.data == x_data

        sql.drop_table(table=test_query_table, schema=ms_schema)

    def test_query_creates_table_pg(self):
        db.drop_table(table=test_query_table, schema=pg_schema)
        db.drop_table(table=f"{test_query_table}_t", schema=pg_schema)

        db.query(f"create table {pg_schema}.{test_query_table} (col1 int, col2 varchar, col3 varchar, col4 int);")
        for i in range(97, 123):
            db.query(
                f"insert into {pg_schema}.{test_query_table} values ({i + 1}, '{chr(i)}', case when {i} < 111 then 'low' else 'high' end, case when {i + 1} < 118 then 1 else 2 end)",
                timeme=False)

        # Assert query successfully executed create table
        assert db.table_exists(table=test_query_table, schema=pg_schema)

        # Assert cross tab works - count
        qry = f"""
            drop table if exists {pg_schema}.{test_query_table}_t;
            create table {pg_schema}.{test_query_table}_t as 
            select * from {pg_schema}.{test_query_table};
            
            select col1, col2, col3, col4 as col_4 from  {pg_schema}.{test_query_table}_t
            """

        with pytest.raises(Exception) as e_info:
            # crosstab test table 1 row column
            db.crosstab_query(qry, ['col3'], 'col_4', 'col2')
            assert e_info.value.args[0] == "Crosstab query must not include drop table"
        db.drop_table(table=test_query_table, schema=pg_schema)
        db.drop_table(table=f"{test_query_table}_t", schema=pg_schema)

    def test_query_creates_table_ms(self):
        sql.drop_table(table=test_query_table, schema=ms_schema)
        sql.drop_table(table=f"{test_query_table}_t", schema=ms_schema)

        sql.query(f"create table {ms_schema}.{test_query_table} (col1 int, col2 varchar(10), col3 varchar(10), col4 int);")
        for i in range(97, 123):
            sql.query(
                f"insert into {ms_schema}.{test_query_table} values ({i + 1}, '{chr(i)}', case when {i} < 111 then 'low' else 'high' end, case when {i + 1} < 118 then 1 else 2 end)",
                timeme=False)

        # Assert query successfully executed create table
        assert sql.table_exists(table=test_query_table, schema=ms_schema)

        # Assert cross tab works - count
        qry = f"""
            drop table if exists {ms_schema}.{test_query_table}_t;
            create table {ms_schema}.{test_query_table}_t as 
            select * from {ms_schema}.{test_query_table};
            
            select col1, col2, col3, col4 as col_4 from  {ms_schema}.{test_query_table}_t
            """

        with pytest.raises(Exception) as e_info:
            # crosstab test table 1 row column
            sql.crosstab_query(qry, ['col3'], 'col_4', 'col2')
            assert e_info.value.args[0] == "Crosstab query must not include drop table"
        sql.drop_table(table=test_query_table, schema=ms_schema)
        sql.drop_table(table=f"{test_query_table}_t", schema=ms_schema)
