import configparser, os

from .. import query
from ..util import PG, MS
from .. import pysqldb3 as pysqldb

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

pg_schema = 'working'
pg_table = f'_creates_tables_testing_{db.user}'
ms_schema = 'dbo'
ms_table = f'_creates_tables_testing_{db.user}'

class TestQueryCreatesTablesSql():
    def test_complicated(self):
        query_string = """
            -- create table test.test; 
            /* 
            CREATE TABLE WTF.TEST as select ; 
            */
            --/* 
            CREATE TABLE Schema.fake_out as select; 
            --*/
            create table server.db.schema.table as select ;
            create table [server].db.schema.[table] as select ;
            create table server.[db].schema.[CapitalTable] as select ;
            create table server.[db].schema.BadCapitalTable as select ;
            create table server.db."schema".[Capital Table] as select ;
            create table server.db.schema.[1-able] (id_ int) ;
            create table [server]."db".schema._table as select ;
            create table server.db.schema.[-_ta^b l*e] as select ;
            create table db.schema."-_ta&$b l*e" as select ;

            create table [schema]."123456-_ta&$b l*e" as select ; /* comment */
            create table [123456-_ta&$b l*e] as select ; -- comment 
                     
                """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [
            (None, None, 'schema', 'fake_out'),
            ('server', 'db','schema','table'),
            ('server', 'db','schema','table'),
            ('server', 'db','schema','CapitalTable'),
            ('server', 'db','schema','badcapitaltable'),
            ('server', 'db','schema','Capital Table'),
            ('server', 'db','schema','1-able'),
            ('server', 'db','schema','_table'),
            ('server', 'db','schema','-_ta^b l*e'),
            (None, 'db','schema','-_ta&$b l*e'),

            (None, None, 'schema', '123456-_ta&$b l*e'),
            (None, None, ms_schema, '123456-_ta&$b l*e')

        ]
        query.Query.query_creates_table(query_string, ms_schema, MS)

    def test_query_creates_table_from_qry(self):
        query_string = f"""
            CREATE TABLE {ms_schema}.{ms_table} (col_1 int, col_2 varchar(4), col_3 geometry);
            INSERT INTO db.test_Table_1 values (1, 'lala', )
        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [(None,None,ms_schema, ms_table)]

    def test_query_creates_table_from_qry_wdb(self):
        query_string = f"""
            CREATE TABLE [riscrashdata].{ms_schema}.{ms_table} (col_1 int, col_2 varchar(4), col_3 geometry);
            INSERT INTO [riscrashdata].{ms_schema}.{ms_table} values (1, 'lala', geometry::StGeomFromText('100 100', 0))
        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [(None, 'riscrashdata', ms_schema, ms_table)]

    def test_query_creates_table_from_qry_into(self):
        query_string = f"""
            select distinct      
                i.fid
                , count(distinct i.victimid) fatals
                , count(distinct case when isnull(v.preaction,'')!='Parked' then v.vehicleid else null end) vehicle_count
                , sum(case when i.second_level = 'ped' then 1 else 0 end) + count(distinct case when isnull(v.preaction,'')!='Parked' then v.vehicleid else null end) as actors
            into {ms_schema}.dashboad_fact 
            from {ms_schema}.dashboard_victim_base i
            left outer join dashboard_other_vehicle v
            on i.fid = v.fid
            group by i.fid
        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [
            (None, None, ms_schema, 'dashboad_fact')
        ]

    def test_query_creates_table_from_qry_wserver(self):
        query_string = f"""
            CREATE TABLE dotdevgissql01.RISCRASHDATA.{ms_schema}.test (col_1 int, col_2 varchar(4), col_3 geometry);
        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [('dotdevgissql01','riscrashdata',ms_schema,'test')]

    def test_query_creates_table_from_qry_brackets(self):
        query_string = f"""
                    CREATE TABLE [RISCRASHDATA].[{ms_schema}].[test] (col_1 int, col_2 varchar(4), col_3 geometry);
                """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [(None, 'RISCRASHDATA',ms_schema,'test')]

        query_string = f"""
                    CREATE TABLE [RISCRASHDATA].[{ms_schema}].[123 test] (col_1 int, col_2 varchar(4), col_3 geometry);
                        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [(None, 'RISCRASHDATA',ms_schema,'123 test')]

        query_string = f"""
                            CREATE TABLE [RISCRASHDATA].[{ms_schema}].test (col_1 int, col_2 varchar(4), col_3 geometry);
                        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [(None, 'RISCRASHDATA',ms_schema,'test')]

    def test_query_creates_table_from_simple(self):
        query_string = f"""
            CREATE TABLE riscrashdata.{ms_schema}.test (
                PersonID int
            );
            """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [(None, 'riscrashdata',ms_schema,'test')]

    def test_query_creates_table_from_simple_brackets(self):
        query_string = f"""
                    CREATE TABLE [riscrashdata].[{ms_schema}].[test] (
                        PersonID int
                    );
                    """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [(None, 'riscrashdata',ms_schema,'test')]

        query_string = f"""
                            CREATE TABLE [riscrashdata].[{ms_schema}].[123 test] (
                                PersonID int
                            );
                            """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [(None,'riscrashdata',ms_schema,'123 test')]

    def test_query_creates_table_from_with_server(self):
        query_string = f"""
                CREATE TABLE dotdevgissql01.riscrashdata.{ms_schema}.test (
                    PersonID int
                );
                """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [('dotdevgissql01','riscrashdata',ms_schema,'test')]

    def test_query_creates_table_from_with_server_brackets(self):
        query_string = f"""
                                CREATE TABLE dotdevgissql01.riscrashdata.{ms_schema}.[test] (
                                    PersonID int
                                );
                                """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [('dotdevgissql01','riscrashdata',ms_schema,'test')]

        query_string = f"""
                        CREATE TABLE dotdevgissql01.riscrashdata.{ms_schema}.[123 test] (
                            PersonID int
                        );
                        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [('dotdevgissql01','riscrashdata',ms_schema,'123 test')]

    def test_query_creates_table_multiple_tables(self):
        query_string = f"""
                    CREATE TABLE riscrashdata.{ms_schema}.test (
                        PersonID int
                    );

                    CREATE TABLE riscrashdata.{ms_schema}.test2 (
                        PersonID int
                    );
                    CREATE TABLE RISCRASHDATA.{ms_schema}.test3 (col_1 int, col_2 varchar(4), col_3 geometry);
                    """
        x = query.Query.query_creates_table(query_string, ms_schema, MS)
        x.sort()
        assert x == [(None, 'riscrashdata',ms_schema,'test'), (None, 'riscrashdata',ms_schema,'test2'), (None, 'riscrashdata',ms_schema,'test3')]

    def test_query_creates_table_multiple_tables_brackets(self):
        query_string = f"""
                            CREATE TABLE riscrashdata.{ms_schema}.[test] (
                                PersonID int
                            );

                            CREATE TABLE [riscrashdata].{ms_schema}.[test2] (
                                PersonID int
                            );

                            CREATE TABLE RISCRASHDATA.{ms_schema}.[test3] (col_1 int, col_2 varchar(4), col_3 geometry);
                            """
        x = query.Query.query_creates_table(query_string, ms_schema, MS)

        assert x == [(None, 'riscrashdata',ms_schema,'test'), (None, 'riscrashdata',ms_schema,'test2'), (None, 'riscrashdata',ms_schema,'test3')]

        query_string = f"""
                                    CREATE TABLE riscrashdata.{ms_schema}.[123 test] (
                                        PersonID int
                                    );

                                    CREATE TABLE riscrashdata.{ms_schema}.[123 test2] (
                                        PersonID int
                                    );

                                    CREATE TABLE [server_path].[RISCRASHDATA].[{ms_schema}].[123 test3] (col_1 int, col_2 varchar(4), col_3 geometry);
                                    """
        x = query.Query.query_creates_table(query_string, ms_schema, MS)

        assert x == [(None, 'riscrashdata',ms_schema,'123 test'), (None, 'riscrashdata',ms_schema,'123 test2'),
                     ('server_path','RISCRASHDATA',ms_schema,'123 test3')]

    def test_query_creates_table_view(self):
        query_string = f"""
                        CREATE view dotdevgissql01.riscrashdata.{ms_schema}.test (
                            PersonID int
                        );
                        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == []

    def test_query_creates_table_view_brackets(self):
        query_string = f"""
                        CREATE VIEW [dotdevgissql01].[riscrashdata].[{ms_schema}].[test] (
                            PersonID int
                        );
                        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == []

        query_string = f"""
                        CREATE VIEW [dotdevgissql01].[riscrashdata].[{ms_schema}].[123 test] (
                            PersonID int
                        );
                        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == []

    def test_query_creates_table_function(self):

        # cannot use select into @tbl in sql, but adding some function creation code to be sure

        query_string = f"""
        CREATE FUNCTION [{ms_schema}].[fnTblData]()
        RETURNS VARCHAR(256)
        AS
        BEGIN
        DECLARE @ret varchar(100)
        declare @tbl TABLE(seg int, street varchar(100))
        insert into @tbl values (1000, 'Main Street')
        insert into @tbl values (1001, 'Main Street 1')
        insert into @tbl values (1002, 'Main Street 2' )

        select @ret = street from @tbl where seg = 1000

        RETURN @ret

        END

        select {ms_schema}.fnTblData()

        declare @v varchar(200)
        set @v = {ms_schema}.fnTblData()
        select @v
        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == []

    def test_query_creates_stored_proceedure(self):
        query_string = f"""
            create procedure {ms_schema}.fnTestTempTbl
            as
            begin
            select * into #temp1 from fatality.{ms_schema}.FARS_Fatal_Other
            select * from #temp1
            drop table #temp1
            end
        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == []

        # unlikley scenario where a real table if generated durring a stored proceedure
        query_string = f"""
                    create procedure {ms_schema}.fnTestTempTbl
                    as
                    begin
                    select * into temp1 from fatality.{ms_schema}.FARS_Fatal_Other
                    drop table temp1
                    end
                """
        print(query.Query.query_creates_table(query_string, ms_schema, MS))
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [(None, None, ms_schema,'temp1')]

    def test_query_creates_table_temp_table(self):
        query_string = f"""
                        CREATE table #test (
                            PersonID int
                        );
                        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == []

        query_string = f"""
                                    CREATE table ##test (
                                        PersonID int
                                    );
                                    """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == []

    def test_query_creates_table_temp_table_brackets(self):
        query_string = f"""
                        CREATE table [#test] (
                            PersonID int
                        );
                        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == []

        query_string = f"""
                        CREATE table [##test] (
                            PersonID int
                        );
                        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == []

    def test_query_creates_table_from_into(self):
        query_string = f"""
            SELECT *
            INTO riscrashdata.{ms_schema}.test2
            FROM riscrashdata.{ms_schema}.test1
        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [(None, 'riscrashdata',ms_schema,'test2')]

    def test_query_creates_table_from_into_brackets(self):
        query_string = f"""
                    SELECT *
                    INTO RIScrashdata.[{ms_schema}].[test2]
                    FROM riscrashdata.{ms_schema}.test1
                """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [(None,'riscrashdata',ms_schema,'test2')]

        query_string = f"""
                            SELECT *
                            INTO riscrashdata.[{ms_schema}].[123 test2]
                            FROM riscrashdata.{ms_schema}.test1
                        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [(None,'riscrashdata',ms_schema,'123 test2')]

    def test_query_creates_table_from_into_multiple(self):
        query_string = f"""
            SELECT *
            INTO riscrashdata.{ms_schema}.test2
            FROM riscrashdata.{ms_schema}.test1;

            SELECT *
            INTO riscrashdata.{ms_schema}.test1
            FROM riscrashdata.{ms_schema}.test2

            CREATE TABLE RISCRASHDATA.{ms_schema}.test3 (col_1 int, col_2 varchar(4), col_3 geometry);
        """
        x = query.Query.query_creates_table(query_string, ms_schema, MS)
        x = set(x)
        assert x == set([(None,'riscrashdata',ms_schema,'test2'), (None,'riscrashdata',ms_schema,'test1'), (None,'riscrashdata',ms_schema,'test3')])


    def test_query_creates_table_from_into_multiple_brackets(self):
        query_string = f"""
            SELECT *
            INTO riscrashdata.{ms_schema}.[123 test2]
            FROM riscrashdata.{ms_schema}.test1;

            SELECT *
            INTO riscrashdata.{ms_schema}.[test1]
            FROM riscrashdata.{ms_schema}.test2

            CREATE TABLE [RISCRASHDATA].[{ms_schema}].[test3] (col_1 int, col_2 varchar(4), col_3 geometry);
        """
        x = query.Query.query_creates_table(query_string, ms_schema, MS)
        x = set(x)
        assert x == set([(None, 'riscrashdata', ms_schema, '123 test2'), (None, 'riscrashdata', ms_schema, 'test1'),
         (None, 'RISCRASHDATA', ms_schema, 'test3')])

    def test_query_creates_table_from_into_multiple_wtemp(self):
        query_string = f"""
            SELECT *
            INTO riscrashdata.{ms_schema}.test2
            FROM riscrashdata.{ms_schema}.test1;

            CREATE TABLE  #test4 (col_1 int, col_2 varchar(4), col_3 geometry);

            CREATE TABLE  ##test5 (col_1 int, col_2 varchar(4), col_3 geometry);
        """
        query.Query.query_creates_table(query_string, ms_schema, MS)
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [(None,'riscrashdata',ms_schema,'test2')]

    def test_query_creates_table_from_into_multiple_wtemp_brackets(self):
        query_string = f"""
                    SELECT *
                    INTO riscrashdata.{ms_schema}.[test2]
                    FROM riscrashdata.{ms_schema}.test1;

                    SELECT *
                    INTO riscrashdata.{ms_schema}.[123 test2]
                    FROM riscrashdata.{ms_schema}.test1;

                    CREATE TABLE  [#test4] (col_1 int, col_2 varchar(4), col_3 geometry);

                    CREATE TABLE  [##123 test5] (col_1 int, col_2 varchar(4), col_3 geometry);
                """

        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [(None, 'riscrashdata',ms_schema,'test2'),
                                                                            (None,'riscrashdata',ms_schema,'123 test2')]

        query_string = f"""
                            SELECT *
                            INTO riscrashdata.{ms_schema}.[abc test2]
                            FROM riscrashdata.{ms_schema}.test1;

                            SELECT *
                            INTO riscrashdata.{ms_schema}.[123 test2]
                            FROM riscrashdata.{ms_schema}.test1;

                            CREATE TABLE  [#test4] (col_1 int, col_2 varchar(4), col_3 geometry);

                            CREATE TABLE  [##123 test5] (col_1 int, col_2 varchar(4), col_3 geometry);
                        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [(None, 'riscrashdata',ms_schema,'abc test2'),
                                                                            (None,'riscrashdata',ms_schema,'123 test2')]

    def test_multiple_qrys_select_into_dif_parts(self):
            query_string = """
                insert into working.safety_projects_segs (id, src, segmentid, mft, dte, conservative)

                select control_id, 'signal', segmentid, mft::int, install_date::date, True
                from signal_controller;


                insert into working.safety_projects_segs (id, src, segmentid, mft, dte, conservative)

                with i as (select control_id, 'signal', masterid, install_date::date from signal_controller)
                select distinct control_id, 'signal', l.segmentid,l.mft,  install_date::date, False
                from lion l
                join i on i.masterid in (l.masteridfrom, masteridto)
                where mft not in (select distinct mft from working.safety_projects_segs where src='signal') -- if conservative already exists dont add another
                ;


                select src, conservative, count(*) from working.safety_projects_segs group by 1,2;
                   """
            assert query.Query.query_creates_table(query_string, 'working', MS) == []

            query_string = f"""
                                select * 
                                ;into 
                                temp1 from fatality.{ms_schema}.FARS_Fatal_Other
                            """
            assert query.Query.query_creates_table(query_string, ms_schema, MS) == []


    def test_semi_col_in_comment(self):
        #TODO: the comment parsing isnt working correctly for this
        query_string = f"""
                    select * 
                    --;
                    into temp1 from fatality.{ms_schema}.FARS_Fatal_Other
                """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [(None, None, ms_schema,'temp1')]

        query_string = f"""
                            select * 
                            /*;*/
                            into temp2 from fatality.{ms_schema}.FARS_Fatal_Other
                        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [(None, None, ms_schema,'temp2')]


class TestQueryCreatesTablesPgSql():
    def test_query_creates_table_from_qry(self):
        query_string = f"""
                    CREATE TABLE working.test (col_1 int, col_2 varchar(4), col_3 geometry);
                """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'working','test')]
        query_string = """
            CREATE TABLE test (col_1 int, col_2 varchar(4), col_3 geometry);
        """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'public','test')]

    def test_query_creates_table_from_qry_w_comments(self):
        query_string = """
                    -- create table error.error1
                    *  create table error.error2 */
                    /*
                         create table error.error3
                    */
                    CREATE TABLE working.test (col_1 int, col_2 varchar(4), col_3 geometry);
                """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'working','test')]
        query_string = """
        -- create table error
                    /*  create table error */
                    /*
                         create table error
                    */
            CREATE TABLE test (col_1 int, col_2 varchar(4), col_3 geometry);
        """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'public','test')]

    def test_query_creates_table_from_qry_quotes(self):
        query_string = """
                    CREATE TABLE working."test" (col_1 int, col_2 varchar(4), col_3 geometry);
                """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'working','test')]

        query_string = """
                            CREATE TABLE working."123 test" (col_1 int, col_2 varchar(4), col_3 geometry);
                        """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'working','123 test')]

        query_string = """
            CREATE TABLE "test" (col_1 int, col_2 varchar(4), col_3 geometry);
        """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'public','test')]

        query_string = """
                   CREATE TABLE "123 test" (col_1 int, col_2 varchar(4), col_3 geometry);
               """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'public','123 test')]

    def test_query_creates_table_from_simple(self):
        query_string = """
               CREATE TABLE working.test (
                   PersonID int
               );
               """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'working','test')]
        query_string = """
                       CREATE TABLE test (
                           PersonID int
                       );
                       """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'public','test')]

    def test_query_creates_table_from_simple_quotes(self):
        query_string = """
               CREATE TABLE working."test" (
                   PersonID int
               );
               """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'working','test')]

        query_string = """
                       CREATE TABLE "working"."test" (
                           PersonID int
                       );
                       """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'working','test')]

        query_string = """
                       CREATE TABLE "Test" (
                           PersonID int
                       );
                       """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'public','Test')]

        query_string = """
                               CREATE TABLE "123 test" (
                                   PersonID int
                               );
                               """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'public','123 test')]

    def test_query_creates_table_multiple_tables(self):
        query_string = f"""
                       CREATE TABLE working.test (
                           PersonID int
                       );

                       CREATE TABLE test2 (
                           PersonID int
                       );
                       CREATE TABLE staging.test3 (col_1 int, col_2 varchar(4), col_3 geometry);
                       """
        x = query.Query.query_creates_table(query_string, 'public', PG)
        assert x == [(None, None,'working','test'), (None, None,'public','test2'), (None, None,'staging','test3')]

    def test_query_creates_table_multiple_tables_quotes(self):
        query_string = f"""
                       CREATE TABLE working."test" (
                           PersonID int
                       );

                       CREATE TABLE "test2" (
                           PersonID int
                       );
                       CREATE TABLE "staging"."test3" (col_1 int, col_2 varchar(4), col_3 geometry);
                       """
        x = query.Query.query_creates_table(query_string, 'public', PG)

        assert x == [ (None, None,'working','test'), (None, None,'public','test2'),(None, None,'staging','test3')]

    def test_query_creates_table_view(self):
        query_string = """
                           CREATE VIEW public.test as
                           select *
                           FROM node
                           WHERE nodeid=123
                           """
        assert query.Query.query_creates_table(query_string, 'public', PG) == []

    def test_query_creates_table_view_quote(self):
        query_string = """
                           CREATE VIEW "public"."test" as
                           select *
                           FROM node
                           WHERE nodeid=123
                           """
        assert query.Query.query_creates_table(query_string, 'public', PG) == []

    def test_query_creates_table_function(self):

        query_string = """
                           CREATE function first_node()
                           returns integer
                           language plpgsql
                            as
                            $$
                            declare
                               f_node integer;
                            begin
                              select nodeid
                              into temp f_node
                              from node
                              limit 1;
                            return f_node;
                            end;
                            $$;
                           """
        assert query.Query.query_creates_table(query_string, 'public', PG) == []

        query_string = """
           drop FUNCTION if exists cl_get_from_street(segmentids text[], street text);
            CREATE OR REPLACE FUNCTION cl_get_from_street(segmentids text[], street_in text default NULL)
            returns varchar
            LANGUAGE 'plpgsql'
                COST 100
                volatile
            AS $BODY$
            declare
             from_street_out varchar;
            begin
                drop table if exists tmp_oft_fnc_tbl;
                create temporary table tmp_oft_fnc_tbl as
                    with t as (
                    select
                        coalesce(street_in, cl_get_on_street(segmentids)) street,
                        array_agg(segmentid) as segs
                    from {s}.{l} l
                    where segmentid = any(segmentids)
                    )
                    select *
                    from t, cl_get_oft(t.segs, t.street) oft;

                select from_street into from_street_out  from tmp_oft_fnc_tbl;
            return from_street_out;
            end;
            $BODY$;
           """
        assert query.Query.query_creates_table(query_string, 'public', PG) == []

    def test_query_creates_table_function_var(self):
        query_string = """
                                   CREATE function public.first_node(nd int)
                                   returns integer
                                   language plpgsql
                                    as
                                    $$
                                    declare
                                       f_node integer;
                                    begin
                                      select nodeid
                                      into f_node
                                      from node
                                      wherre nodeid > nd
                                      limit 1;
                                    return f_node;
                                    end;
                                    $$;
                                   """
        print(query.Query.query_creates_table(query_string, 'public', PG))
        assert query.Query.query_creates_table(query_string, 'public', PG) == []

    def test_query_creates_table_temp_table(self):
        query_string = """
                           CREATE temporary table test (
                               PersonID int
                           );
                           """
        assert query.Query.query_creates_table(query_string, 'public', PG) == []

    def test_query_creates_table_temp_table_quote(self):
        query_string = """
                           CREATE temporary table "123 Test" (
                               PersonID int
                           );
                           """
        assert query.Query.query_creates_table(query_string, 'public', PG) == []

    def test_query_creates_table_from_into(self):
        query_string = """
                -- SELECT * into error
                /*  SELECT * into error2 */
                /*
                     SELECT * into error3
                */
               SELECT *
               INTO working.test2
               FROM working.test1
           """
        print(query.Query.query_creates_table(query_string, 'public', PG))
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None, 'working','test2')]

        query_string = """
                       SELECT *
                       INTO test2
                       FROM working.test1
                   """

        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None, 'public','test2')]

    def test_query_creates_table_from_into_quote(self):
        query_string = """
                       SELECT *
                       INTO "test2"
                       FROM working.test1
                   """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None, 'public','test2')]

        query_string = """
                       SELECT *
                       INTO "working"."test2"
                       FROM working.test1
                   """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None, 'working','test2')]

        query_string = """
                       SELECT *
                       INTO working."Test 21"
                       FROM working.test1
                   """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None, 'working','Test 21')]

    def test_query_creates_table_from_into_multiple(self):
        query_string = """
            SELECT *
            INTO working.test2
            FROM test1;

            SELECT *
            INTO test1
            FROM working.test2

            CREATE TABLE public.test3 (col_1 int, col_2 varchar(4), col_3 geometry);
        """
        x = query.Query.query_creates_table(query_string, 'public', PG)
        x = set(x)
        assert x == set([(None, None, 'public','test3'), (None, None, 'public','test1'), (None, None, 'working','test2')])

    def test_query_creates_table_from_into_multiple_quote(self):
        query_string = """
            SELECT *
            INTO working."Test2"
            FROM test1;

            SELECT *
            INTO "test1"
            FROM working.test2

            CREATE TABLE "public"."test 3"(col_1 int, col_2 varchar(4), col_3 geometry);
        """
        x = query.Query.query_creates_table(query_string, 'public', PG)
        x = set(x)
        assert x == set([(None, None, 'public','test 3'), (None, None, 'public','test1'), (None, None, 'working','Test2')])

    def test_query_creates_table_from_into_multiple_wtemp(self):
        query_string = """
                    SELECT *
                    INTO working.test2
                    FROM test1;

                    SELECT *
                    INTO temporary table test1
                    FROM working.test2

                    CREATE TEMPORARY TABLE test3 (col_1 int, col_2 varchar(4), col_3 geometry);
                """
        x = query.Query.query_creates_table(query_string, 'public', PG)
        assert x == [(None, None, 'working','test2')]

    def test_query_creates_table_from_into_quotes_brackets(self):
        query_string = """
                    SELECT *
                    INTO working.["test2"]
                    FROM test1;
                """
        x = query.Query.query_creates_table(query_string, ms_schema, MS)
        assert x == [(None, None, 'working','"test2"')]

    def test_query_creates_table_from_qry_if_not_exists(self):
        query_string = """
                    CREATE TABLE if not exists working.test (col_1 int, col_2 varchar(4), col_3 geometry);
                    LIMIT 10
                """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None, 'working','test')]

        query_string = """
            CREATE TABLE if not exists test (col_1 int, col_2 varchar(4), col_3 geometry);
        """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None, 'public','test')]

    def test_query_creates_table_table_quote_if_not_exists(self):
        query_string = """
                           CREATE table if not exists "123"."Test" (
                               PersonID int
                           );
                           """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None, '123','Test')]

    def test_query_creates_table_temp_table_quote_if_not_exists(self):
        query_string = """
                           CREATE temporary table if not exists "123 Test" (
                               PersonID int
                           );
                           """
        assert query.Query.query_creates_table(query_string, 'public', PG) == []

    def test_query_creates_table_with_cte(self):
        query_string = f"""
            CREATE TABLE {ms_schema}.test_with_cte AS
             with d as (
                SELECT TOP 10 *
                FROM RISCRASHDATA.{ms_schema}.node) 
            SELECT * FROM d;
        """
        assert query.Query.query_creates_table(query_string, ms_schema, MS) == [(None,None,ms_schema,'test_with_cte')]


    def test_multiple_qrys_select_into_dif_parts(self):
        query_string = """
            insert into working.safety_projects_segs (id, src, segmentid, mft, dte, conservative)

            select control_id, 'signal', segmentid, mft::int, install_date::date, True
            from signal_controller;


            insert into working.safety_projects_segs (id, src, segmentid, mft, dte, conservative)

            with i as (select control_id, 'signal', masterid, install_date::date from signal_controller)
            select distinct control_id, 'signal', l.segmentid,l.mft,  install_date::date, False
            from lion l
            join i on i.masterid in (l.masteridfrom, masteridto)
            where mft not in (select distinct mft from working.safety_projects_segs where src='signal') -- if conservative already exists dont add another
            ;


            select src, conservative, count(*) from working.safety_projects_segs group by 1,2;
               """
        assert query.Query.query_creates_table(query_string, 'working', PG) == []

        query_string = f"""
                                       select * from a
                                       ;into 
                                       working.temp1 from fatality.{ms_schema}.FARS_Fatal_Other
                                   """
        assert query.Query.query_creates_table(query_string, 'working', PG) == []

    def test_semi_col_in_comment(self):
        query_string = f"""
                           select * 
                           --;
                           into working.temp1 
                           from fatality.{ms_schema}.FARS_Fatal_Other
                       """
        assert query.Query.query_creates_table(query_string, 'working', PG) == [(None, None, 'working','temp1')]

        query_string = f"""
                                   select * 
                                   /*;*/
                                   into working.temp1 
                                   from fatality.{ms_schema}.FARS_Fatal_Other
                               """
        assert query.Query.query_creates_table(query_string, 'working', PG) == [(None, None, 'working','temp1')]


class TestTablesCreatedPG:
    def test_tables_created_simple(self):
        db.drop_table(pg_schema, pg_table)
        db.query(f"""
            CREATE TABLE IF NOT EXISTS {pg_schema}.{pg_table}
                    (
                        shape_id character varying,
                        shape_pt_lat double precision,
                        shape_pt_lon double precision,
                        shape_pt_sequence bigint,
                        geom geometry(Point,2263)
                    );""")
        assert db.table_exists(pg_table, schema=pg_schema)
        assert db.tables_created == [(db.server, db.database, pg_schema, pg_table)]
        db.drop_table(pg_schema, pg_table)
        assert (db.server, db.database, pg_schema, pg_table) not in db.tables_created


    def test_tables_created_simple_w_drop(self):
        db.drop_table(pg_schema, pg_table)
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{pg_table};
            CREATE TABLE IF NOT EXISTS {pg_schema}.{pg_table}
                    (
                        shape_id character varying,
                        shape_pt_lat double precision,
                        shape_pt_lon double precision,
                        shape_pt_sequence bigint,
                        geom geometry(Point,2263)
                    );""")
        assert db.table_exists(pg_table, schema=pg_schema)
        assert db.tables_created == [(db.server, db.database, pg_schema, pg_table)]
        db.drop_table(pg_schema, pg_table)
        assert (db.server, db.database, pg_schema, pg_table) not in db.tables_created

    def test_tables_created_simple_w_drop_query(self):
        db.drop_table(pg_schema, pg_table)
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{pg_table};
            CREATE TABLE IF NOT EXISTS {pg_schema}.{pg_table}
                    (
                        shape_id character varying,
                        shape_pt_lat double precision,
                        shape_pt_lon double precision,
                        shape_pt_sequence bigint,
                        geom geometry(Point,2263)
                    );""")
        assert db.table_exists(pg_table, schema=pg_schema)
        assert db.tables_created == [(db.server, db.database, pg_schema, pg_table)]

        db.query(f"DROP TABLE IF EXISTS {pg_schema}.{pg_table};")
        assert (db.server, db.database, pg_schema, pg_table) not in db.tables_created

    def test_tables_create_drop_create(self):
        db.drop_table(pg_schema, pg_table)

        # drop and create
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{pg_table};
            CREATE TABLE IF NOT EXISTS {pg_schema}.{pg_table}
                    (
                        shape_id character varying,
                        shape_pt_lat double precision,
                        shape_pt_lon double precision,
                        shape_pt_sequence bigint,
                        geom geometry(Point,2263)
                    );""")
        assert db.table_exists(pg_table, schema=pg_schema)
        assert db.tables_created == [(db.server, db.database, pg_schema, pg_table)]
        
        # drop and create again
        db.query(f"""
            DROP TABLE IF EXISTS {pg_schema}.{pg_table};
            CREATE TABLE IF NOT EXISTS {pg_schema}.{pg_table}
                    (
                        shape_id character varying,
                        shape_pt_lat double precision,
                        shape_pt_lon double precision,
                        shape_pt_sequence bigint,
                        geom geometry(Point,2263)
                    );""")
        assert db.table_exists(pg_table, schema=pg_schema)
        # check that only 1 table should be created after all of this
        assert db.tables_created == [(db.server, db.database, pg_schema, pg_table)]

        db.drop_table(pg_schema, pg_table)
        assert (db.server, db.database, pg_schema, pg_table) not in db.tables_created

    def test_tables_created_multi_tables(self):
        # check if it still works if multiple tables are created at the same time

        for i in range(1, 3):
            db.drop_table(schema = pg_schema, table = f'test_table_{i}')
            assert not db.table_exists(schema = pg_schema, table = f'test_table_{i}')


        db.query(f"""
                CREATE TABLE IF NOT EXISTS {pg_schema}.test_table_1
                (   shape_id character varying,
                    shape_pt_lat double precision,
                    shape_pt_lon double precision,
                    shape_pt_sequence bigint,
                    geom geometry(Point,2263)
                );
                
                CREATE TABLE IF NOT EXISTS {pg_schema}.test_table_2
                (   shape_id character varying,
                    shape_pt_lat double precision,
                    shape_pt_lon double precision,
                    shape_pt_sequence bigint,
                    geom geometry(Point,2263)
                );
                """)
        for i in range(1, 3):
            assert db.table_exists(schema = pg_schema, table = f'test_table_{i}')

            assert (db.server, db.database, pg_schema, f'test_table_{i}') in db.tables_created

        for i in range(1, 3):
            db.drop_table(schema = pg_schema, table = f'test_table_{i}')

    def test_tables_created_edited(self):
        # create table, make changes to table, and add another table and see if both still appear

        db.drop_table(pg_schema, pg_table)
        db.query(f"""
            CREATE TABLE IF NOT EXISTS {pg_schema}.{pg_table}
                    (
                        shape_id character varying,
                        shape_pt_lat double precision,
                        shape_pt_lon double precision,
                        shape_pt_sequence bigint,
                        geom geometry(Point,2263)
                    );""")

        db.query(f"""
                insert into {pg_schema}.{pg_table} (shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, geom)
                values (1234957, 84373.29287, 11.00009, 5000, st_setsrid(st_point(1015329.1, 213793.1), 2263));
                 """)
        
        assert db.table_exists(pg_table, schema=pg_schema)

        db.query(f"""
            CREATE TABLE IF NOT EXISTS {pg_schema}.{pg_table}_2
                    (
                        shape_id character varying,
                        shape_pt_lat double precision,
                        shape_pt_lon double precision,
                        shape_pt_sequence bigint,
                        geom geometry(Point,2263)
                    );""")
        
        assert db.table_exists(pg_table + '_2', schema=pg_schema)

        assert (db.server, db.database, pg_schema, pg_table) in db.tables_created
        assert (db.server, db.database, pg_schema, pg_table + '_2') in db.tables_created

        db.drop_table(schema = pg_schema, table = pg_table)
        db.drop_table(schema = pg_schema, table = pg_table + '_2')



class TestTablesCreatedMS:
    
    def test_tables_created_simple(self):
        sql.drop_table(schema = ms_schema, table = ms_table)
        sql.query(f"""
                CREATE TABLE {ms_schema}.{ms_table}
                    (   shape_id character varying,
                        shape_pt_lat double precision,
                        shape_pt_lon double precision,
                        shape_pt_sequence bigint,
                        geom geometry
                    );""")
        assert sql.table_exists(ms_table, schema=ms_schema)
        assert sql.tables_created == [(sql.server, sql.database, ms_schema, ms_table)]
        sql.drop_table(schema = ms_schema, table = ms_table)
        assert (sql.server, sql.database, ms_schema, ms_table) not in sql.tables_created

    def test_tables_created_simple_w_drop(self):
        sql.drop_table(ms_schema, ms_table)
        sql.query(f"""
            DROP TABLE IF EXISTS {ms_schema}.{ms_table};
            CREATE TABLE {ms_schema}.{ms_table}
                    (   shape_id character varying,
                        shape_pt_lat double precision,
                        shape_pt_lon double precision,
                        shape_pt_sequence bigint,
                        geom geometry
                    );""")
        assert sql.table_exists(ms_table, schema=ms_schema)
        assert sql.tables_created == [(sql.server, sql.database, ms_schema, ms_table)]
        sql.drop_table(ms_schema, ms_table)
        assert (sql.server, sql.database, ms_schema, ms_table) not in sql.tables_created

    def test_tables_created_simple_w_drop_query(self):
        sql.drop_table(ms_schema, ms_table)
        sql.query(f"""
            DROP TABLE IF EXISTS {ms_schema}.{ms_table};
            CREATE TABLE {ms_schema}.{ms_table}
                    (   shape_id character varying,
                        shape_pt_lat double precision,
                        shape_pt_lon double precision,
                        shape_pt_sequence bigint,
                        geom geometry
                    );""")
        assert sql.table_exists(ms_table, schema=ms_schema)
        assert sql.tables_created == [(sql.server, sql.database, ms_schema, ms_table)]
        sql.query(f"DROP TABLE IF EXISTS {ms_schema}.{ms_table};")
        assert (sql.server, sql.database, ms_schema, ms_table) not in sql.tables_created


    def test_tables_create_drop_create(self):
        sql.drop_table(ms_schema, ms_table)

        # drop and create
        sql.query(f"""
            DROP TABLE IF EXISTS {ms_schema}.{ms_table};
            CREATE TABLE {ms_schema}.{ms_table}
                    (   shape_id character varying,
                        shape_pt_lat double precision,
                        shape_pt_lon double precision,
                        shape_pt_sequence bigint,
                        geom geometry
                    );""")
        assert sql.table_exists(ms_table, schema=ms_schema)
        assert sql.tables_created == [(sql.server, sql.database, ms_schema, ms_table)]
        
        # drop and create again
        sql.query(f"""
            DROP TABLE IF EXISTS {ms_schema}.{ms_table};
            CREATE TABLE {ms_schema}.{ms_table}
                    (   shape_id character varying,
                        shape_pt_lat double precision,
                        shape_pt_lon double precision,
                        shape_pt_sequence bigint,
                        geom geometry
                    );""")
        
        assert sql.table_exists(ms_table, schema=ms_schema)
        # should be the only net new table
        assert sql.tables_created == [(sql.server, sql.database, ms_schema, ms_table)]
        
        sql.drop_table(ms_schema, ms_table)
        assert (sql.server, sql.database, ms_schema, ms_table) not in sql.tables_created   

    def test_tables_created_multi_tables(self):
        # check if it still works if multiple tables are created at the same time

        for i in range(1, 3):
            sql.drop_table(schema = ms_schema, table = f'test_table_{i}')
            assert not sql.table_exists(schema = ms_schema, table = f'test_table_{i}')


        sql.query(f"""
                CREATE TABLE {ms_schema}.test_table_1
                (   shape_id character varying,
                    shape_pt_lat double precision,
                    shape_pt_lon double precision,
                    shape_pt_sequence bigint,
                    geom geometry
                );
                
                CREATE TABLE {ms_schema}.test_table_2
                (   shape_id character varying,
                    shape_pt_lat double precision,
                    shape_pt_lon double precision,
                    shape_pt_sequence bigint,
                    geom geometry
                );
        """)
        for i in range(1, 3):
            assert sql.table_exists(schema = ms_schema, table = f'test_table_{i}')

            assert (sql.server, sql.database, ms_schema, f'test_table_{i}') in sql.tables_created

        for i in range(1, 3):
            sql.drop_table(schema = ms_schema, table = f'test_table_{i}')


    def test_tables_created_edited(self):
        # create table, make changes to table and see if it still appears

        sql.drop_table(ms_schema, ms_table)
        sql.query(f"""
           CREATE TABLE {ms_schema}.{ms_table}
                    (   shape_id character varying,
                        shape_pt_lat double precision,
                        shape_pt_lon double precision,
                        shape_pt_sequence bigint,
                        geom geometry
                    );""")
        assert sql.table_exists(ms_table, schema=ms_schema)

        sql.query(f"""
                insert into {ms_schema}.{ms_table}(shape_id, shape_pt_sequence, geom)
                VALUES (100, 123, geometry::STGeomFromText('POINT(100 100)', 0))
                 """)

        sql.query(f"""
           CREATE TABLE {ms_schema}.{ms_table}_2
                    (   shape_id character varying,
                        shape_pt_lat double precision,
                        shape_pt_lon double precision,
                        shape_pt_sequence bigint,
                        geom geometry
                    );""")
        
        assert sql.table_exists(ms_table + '_2', schema=ms_schema)
                
        assert (sql.server, sql.database, ms_schema, ms_table) in sql.tables_created
        assert (sql.server, sql.database, ms_schema, ms_table + '_2') in sql.tables_created

        sql.drop_table(schema = ms_schema, table = ms_table)
        sql.drop_table(schema = ms_schema, table = ms_table + '_2')