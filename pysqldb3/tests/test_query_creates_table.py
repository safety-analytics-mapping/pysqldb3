from .. import query
from ..util import PG, MS


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
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [
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
            (None, None, 'dbo', '123456-_ta&$b l*e')

        ]
        query.Query.query_creates_table(query_string, 'dbo', MS)




    def test_query_creates_table_from_qry(self):
        query_string = """
            CREATE TABLE dbo.test AS
             SELECT TOP 10 *
             FROM RISCRASHDATA.dbo.node
        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [(None,None,'dbo','test')]

    def test_query_creates_table_from_qry_wdb(self):
        query_string = """
            CREATE TABLE RISCRASHDATA.dbo.test AS
             SELECT TOP 10 *
             FROM RISCRASHDATA.dbo.node
        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [(None, 'riscrashdata','dbo','test')]

    def test_query_creates_table_from_qry_into(self):
        query_string = """
            select distinct      
                i.fid
                , count(distinct i.victimid) fatals
                , count(distinct case when isnull(v.preaction,'')!='Parked' then v.vehicleid else null end) vehicle_count
                , sum(case when i.second_level = 'ped' then 1 else 0 end) + count(distinct case when isnull(v.preaction,'')!='Parked' then v.vehicleid else null end) as actors
            into dbo.dashboad_fact 
            from dbo.dashboard_victim_base i
            left outer join dashboard_other_vehicle v
            on i.fid = v.fid
            group by i.fid
        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [
            (None, None, 'dbo', 'dashboad_fact')
        ]

    def test_query_creates_table_from_qry_wserver(self):
        query_string = """
            CREATE TABLE dotdevgissql01.RISCRASHDATA.dbo.test AS
             SELECT TOP 10 *
             FROM RISCRASHDATA.dbo.node
        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [('dotdevgissql01','riscrashdata','dbo','test')]

    def test_query_creates_table_from_qry_brackets(self):
        query_string = """
                    CREATE TABLE [RISCRASHDATA].[dbo].[test] AS
                     SELECT TOP 10 *
                     FROM RISCRASHDATA.dbo.node
                """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [(None, 'RISCRASHDATA','dbo','test')]

        query_string = """
                            CREATE TABLE [RISCRASHDATA].[dbo].[123 test] AS
                             SELECT TOP 10 *
                             FROM RISCRASHDATA.dbo.node
                        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [(None, 'RISCRASHDATA','dbo','123 test')]

        query_string = """
                            CREATE TABLE [RISCRASHDATA].[dbo].test AS
                             SELECT TOP 10 *
                             FROM RISCRASHDATA.dbo.node
                        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [(None, 'RISCRASHDATA','dbo','test')]

    def test_query_creates_table_from_simple(self):
        query_string = """
            CREATE TABLE riscrashdata.dbo.test (
                PersonID int
            );
            """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [(None, 'riscrashdata','dbo','test')]

    def test_query_creates_table_from_simple_brackets(self):
        query_string = """
                    CREATE TABLE [riscrashdata].[dbo].[test] (
                        PersonID int
                    );
                    """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [(None, 'riscrashdata','dbo','test')]

        query_string = """
                            CREATE TABLE [riscrashdata].[dbo].[123 test] (
                                PersonID int
                            );
                            """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [(None,'riscrashdata','dbo','123 test')]

    def test_query_creates_table_from_with_server(self):
        query_string = """
                CREATE TABLE dotdevgissql01.riscrashdata.dbo.test (
                    PersonID int
                );
                """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [('dotdevgissql01','riscrashdata','dbo','test')]

    def test_query_creates_table_from_with_server_brackets(self):
        query_string = """
                                CREATE TABLE dotdevgissql01.riscrashdata.dbo.[test] (
                                    PersonID int
                                );
                                """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [('dotdevgissql01','riscrashdata','dbo','test')]

        query_string = """
                        CREATE TABLE dotdevgissql01.riscrashdata.dbo.[123 test] (
                            PersonID int
                        );
                        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [('dotdevgissql01','riscrashdata','dbo','123 test')]

    def test_query_creates_table_multiple_tables(self):
        query_string = """
                    CREATE TABLE riscrashdata.dbo.test (
                        PersonID int
                    );

                    CREATE TABLE riscrashdata.dbo.test2 (
                        PersonID int
                    );
                    CREATE TABLE RISCRASHDATA.dbo.test3 AS
                    SELECT TOP 10 *
                    FROM RISCRASHDATA.dbo.node
                    """
        x = query.Query.query_creates_table(query_string, 'dbo', MS)
        x.sort()
        assert x == [(None, 'riscrashdata','dbo','test'), (None, 'riscrashdata','dbo','test2'), (None, 'riscrashdata','dbo','test3')]

    def test_query_creates_table_multiple_tables_brackets(self):
        query_string = """
                            CREATE TABLE riscrashdata.dbo.[test] (
                                PersonID int
                            );

                            CREATE TABLE [riscrashdata].dbo.[test2] (
                                PersonID int
                            );

                            CREATE TABLE RISCRASHDATA.dbo.[test3] AS
                            SELECT TOP 10 *
                            FROM RISCRASHDATA.dbo.node
                            """
        x = query.Query.query_creates_table(query_string, 'dbo', MS)

        assert x == [(None, 'riscrashdata','dbo','test'), (None, 'riscrashdata','dbo','test2'), (None, 'riscrashdata','dbo','test3')]

        query_string = """
                                    CREATE TABLE riscrashdata.dbo.[123 test] (
                                        PersonID int
                                    );

                                    CREATE TABLE riscrashdata.dbo.[123 test2] (
                                        PersonID int
                                    );

                                    CREATE TABLE [server_path].[RISCRASHDATA].[dbo].[123 test3] AS
                                    SELECT TOP 10 *
                                    FROM RISCRASHDATA.dbo.node
                                    """
        x = query.Query.query_creates_table(query_string, 'dbo', MS)

        assert x == [(None, 'riscrashdata','dbo','123 test'), (None, 'riscrashdata','dbo','123 test2'),
                     ('server_path','RISCRASHDATA','dbo','123 test3')]

    def test_query_creates_table_view(self):
        query_string = """
                        CREATE view dotdevgissql01.riscrashdata.dbo.test (
                            PersonID int
                        );
                        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == []

    def test_query_creates_table_view_brackets(self):
        query_string = """
                        CREATE VIEW [dotdevgissql01].[riscrashdata].[dbo].[test] (
                            PersonID int
                        );
                        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == []

        query_string = """
                        CREATE VIEW [dotdevgissql01].[riscrashdata].[dbo].[123 test] (
                            PersonID int
                        );
                        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == []

    def test_query_creates_table_function(self):

        # cannot use select into @tbl in sql, but adding some function creation code to be sure

        query_string = """
        CREATE FUNCTION [dbo].[fnTblData]()
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

        select dbo.fnTblData()

        declare @v varchar(200)
        set @v = dbo.fnTblData()
        select @v
        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == []

    def test_query_creates_stored_proceedure(self):
        query_string = """
            create procedure dbo.fnTestTempTbl
            as
            begin
            select * into #temp1 from fatality.dbo.FARS_Fatal_Other
            select * from #temp1
            drop table #temp1
            end
        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == []

        # unlikley scenario where a real table if generated durring a stored proceedure
        query_string = """
                    create procedure dbo.fnTestTempTbl
                    as
                    begin
                    select * into temp1 from fatality.dbo.FARS_Fatal_Other
                    drop table temp1
                    end
                """
        print(query.Query.query_creates_table(query_string, 'dbo', MS))
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [(None, None, 'dbo','temp1')]

    def test_query_creates_table_temp_table(self):
        query_string = """
                        CREATE table #test (
                            PersonID int
                        );
                        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == []

        query_string = """
                                    CREATE table ##test (
                                        PersonID int
                                    );
                                    """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == []

    def test_query_creates_table_temp_table_brackets(self):
        query_string = """
                        CREATE table [#test] (
                            PersonID int
                        );
                        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == []

        query_string = """
                        CREATE table [##test] (
                            PersonID int
                        );
                        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == []

    def test_query_creates_table_from_into(self):
        query_string = """
            SELECT *
            INTO riscrashdata.dbo.test2
            FROM riscrashdata.dbo.test1
        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [(None, 'riscrashdata','dbo','test2')]

    def test_query_creates_table_from_into_brackets(self):
        query_string = """
                    SELECT *
                    INTO RIScrashdata.[dbo].[test2]
                    FROM riscrashdata.dbo.test1
                """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [(None,'riscrashdata','dbo','test2')]

        query_string = """
                            SELECT *
                            INTO riscrashdata.[dbo].[123 test2]
                            FROM riscrashdata.dbo.test1
                        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [(None,'riscrashdata','dbo','123 test2')]

    def test_query_creates_table_from_into_multiple(self):
        query_string = """
            SELECT *
            INTO riscrashdata.dbo.test2
            FROM riscrashdata.dbo.test1;

            SELECT *
            INTO riscrashdata.dbo.test1
            FROM riscrashdata.dbo.test2

            CREATE TABLE RISCRASHDATA.dbo.test3 AS
            SELECT TOP 10 *
            FROM RISCRASHDATA.dbo.node
        """
        x = query.Query.query_creates_table(query_string, 'dbo', MS)
        x = set(x)
        assert x == set([(None,'riscrashdata','dbo','test2'), (None,'riscrashdata','dbo','test1'), (None,'riscrashdata','dbo','test3')])


    def test_query_creates_table_from_into_multiple_brackets(self):
        query_string = """
            SELECT *
            INTO riscrashdata.dbo.[123 test2]
            FROM riscrashdata.dbo.test1;

            SELECT *
            INTO riscrashdata.dbo.[test1]
            FROM riscrashdata.dbo.test2

            CREATE TABLE [RISCRASHDATA].[dbo].[test3] AS
            SELECT TOP 10 *
            FROM RISCRASHDATA.dbo.node
        """
        x = query.Query.query_creates_table(query_string, 'dbo', MS)
        x = set(x)
        assert x == set([(None, 'riscrashdata', 'dbo', '123 test2'), (None, 'riscrashdata', 'dbo', 'test1'),
         (None, 'RISCRASHDATA', 'dbo', 'test3')])

    def test_query_creates_table_from_into_multiple_wtemp(self):
        query_string = """
            SELECT *
            INTO riscrashdata.dbo.test2
            FROM riscrashdata.dbo.test1;

            CREATE TABLE  #test4 AS
            SELECT
            TOP 10 *
            FROM RISCRASHDATA.dbo.node

            CREATE TABLE  ##test5 AS
            SELECT
            TOP 10 *
            FROM RISCRASHDATA.dbo.node
        """
        query.Query.query_creates_table(query_string, 'dbo', MS)
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [(None,'riscrashdata','dbo','test2')]

    def test_query_creates_table_from_into_multiple_wtemp_brackets(self):
        query_string = """
                    SELECT *
                    INTO riscrashdata.dbo.[test2]
                    FROM riscrashdata.dbo.test1;

                    SELECT *
                    INTO riscrashdata.dbo.[123 test2]
                    FROM riscrashdata.dbo.test1;

                    CREATE TABLE  [#test4] AS
                    SELECT
                    TOP 10 *
                    FROM RISCRASHDATA.dbo.node

                    CREATE TABLE  [##123 test5] AS
                    SELECT
                    TOP 10 *
                    FROM RISCRASHDATA.dbo.node
                """

        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [(None, 'riscrashdata','dbo','test2'),
                                                                            (None,'riscrashdata','dbo','123 test2')]

        query_string = """
                            SELECT *
                            INTO riscrashdata.dbo.[abc test2]
                            FROM riscrashdata.dbo.test1;

                            SELECT *
                            INTO riscrashdata.dbo.[123 test2]
                            FROM riscrashdata.dbo.test1;

                            CREATE TABLE  [#test4] AS
                            SELECT
                            TOP 10 *
                            FROM RISCRASHDATA.dbo.node

                            CREATE TABLE  [##123 test5] AS
                            SELECT
                            TOP 10 *
                            FROM RISCRASHDATA.dbo.node
                        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [(None, 'riscrashdata','dbo','abc test2'),
                                                                            (None,'riscrashdata','dbo','123 test2')]

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

            query_string = """
                                select * 
                                ;into 
                                temp1 from fatality.dbo.FARS_Fatal_Other
                            """
            assert query.Query.query_creates_table(query_string, 'dbo', MS) == []


    def test_semi_col_in_comment(self):
        #TODO: the comment parsing isnt working correctly for this
        query_string = """
                    select * 
                    --;
                    into temp1 from fatality.dbo.FARS_Fatal_Other
                """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [(None, None, 'dbo','temp1')]

        query_string = """
                            select * 
                            /*;*/
                            into temp2 from fatality.dbo.FARS_Fatal_Other
                        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [(None, None, 'dbo','temp2')]


class TestQueryCreatesTablesPgSql():
    def test_query_creates_table_from_qry(self):
        query_string = """
                    CREATE TABLE working.test AS
                    SELECT *
                    FROM node
                    LIMIT 10
                """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'working','test')]
        query_string = """
            CREATE TABLE test AS
            SELECT *
            FROM node
            LIMIT 10
        """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'public','test')]

    def test_query_creates_table_from_qry_w_comments(self):
        query_string = """
                    -- create table error.error1
                    *  create table error.error2 */
                    /*
                         create table error.error3
                    */
                    CREATE TABLE working.test AS
                    SELECT *
                    FROM node
                    LIMIT 10
                """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'working','test')]
        query_string = """
        -- create table error
                    /*  create table error */
                    /*
                         create table error
                    */
            CREATE TABLE test AS
            SELECT *
            FROM node
            LIMIT 10
        """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'public','test')]

    def test_query_creates_table_from_qry_quotes(self):
        query_string = """
                    CREATE TABLE working."test" AS
                    SELECT *
                    FROM node
                    LIMIT 10
                """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'working','test')]

        query_string = """
                            CREATE TABLE working."123 test" AS
                            SELECT *
                            FROM node
                            LIMIT 10
                        """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'working','123 test')]

        query_string = """
            CREATE TABLE "test" AS
            SELECT *
            FROM node
            LIMIT 10
        """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None,'public','test')]

        query_string = """
                   CREATE TABLE "123 test" AS
                   SELECT *
                   FROM node
                   LIMIT 10
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
        query_string = """
                       CREATE TABLE working.test (
                           PersonID int
                       );

                       CREATE TABLE test2 (
                           PersonID int
                       );
                       CREATE TABLE staging.test3 AS
                       SELECT TOP 10 *
                       FROM dbo.node
                       """
        x = query.Query.query_creates_table(query_string, 'public', PG)
        assert x == [(None, None,'working','test'), (None, None,'public','test2'), (None, None,'staging','test3')]

    def test_query_creates_table_multiple_tables_quotes(self):
        query_string = """
                       CREATE TABLE working."test" (
                           PersonID int
                       );

                       CREATE TABLE "test2" (
                           PersonID int
                       );
                       CREATE TABLE "staging"."test3" AS
                       SELECT TOP 10 *
                       FROM dbo.node
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

            CREATE TABLE public.test3 AS
            SELECT TOP 10 *
            FROM public.node
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

            CREATE TABLE "public"."test 3" AS
            SELECT TOP 10 *
            FROM public.node
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

                    CREATE TEMPORARY TABLE test3 AS
                    SELECT TOP 10 *
                    FROM public.node
                """
        x = query.Query.query_creates_table(query_string, 'public', PG)
        assert x == [(None, None, 'working','test2')]

    def test_query_creates_table_from_into_quotes_brackets(self):
        query_string = """
                    SELECT *
                    INTO working.["test2"]
                    FROM test1;
                """
        x = query.Query.query_creates_table(query_string, 'dbo', MS)
        assert x == [(None, None, 'working','"test2"')]

    def test_query_creates_table_from_qry_if_not_exists(self):
        query_string = """
                    CREATE TABLE if not exists working.test AS
                    SELECT *
                    FROM node
                    LIMIT 10
                """
        assert query.Query.query_creates_table(query_string, 'public', PG) == [(None, None, 'working','test')]

        query_string = """
            CREATE TABLE if not exists test AS
            SELECT *
            FROM node
            LIMIT 10
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
        query_string = """
            CREATE TABLE dbo.test_with_cte AS
             with d as (
                SELECT TOP 10 *
                FROM RISCRASHDATA.dbo.node) 
            SELECT * FROM d;
        """
        assert query.Query.query_creates_table(query_string, 'dbo', MS) == [(None,None,'dbo','test_with_cte')]


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

        query_string = """
                                       select * from a
                                       ;into 
                                       working.temp1 from fatality.dbo.FARS_Fatal_Other
                                   """
        assert query.Query.query_creates_table(query_string, 'working', PG) == []

    def test_semi_col_in_comment(self):
        query_string = """
                           select * 
                           --;
                           into working.temp1 
                           from fatality.dbo.FARS_Fatal_Other
                       """
        assert query.Query.query_creates_table(query_string, 'working', PG) == [(None, None, 'working','temp1')]

        query_string = """
                                   select * 
                                   /*;*/
                                   into working.temp1 
                                   from fatality.dbo.FARS_Fatal_Other
                               """
        assert query.Query.query_creates_table(query_string, 'working', PG) == [(None, None, 'working','temp1')]