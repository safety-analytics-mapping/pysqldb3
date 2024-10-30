from .. import query


class TestQueryDropsTablesSql():
    def test_query_drops_table_from_qry(self):
        query_string = """
        DROP TABLE RISCRASHDATA.dbo.test;
        """

        assert query.Query.query_drops_table(query_string, 'MS') == ['riscrashdata.dbo.test']

    def test_query_drops_table_from_qry_w_comment(self):
        query_string = """
        -- DROP TABLE RISCRASHDATA.dbo.error1;
        --DROP TABLE RISCRASHDATA.dbo.error2;
        /*DROP TABLE RISCRASHDATA.dbo.error3;*/
        /*
            DROP TABLE RISCRASHDATA.dbo.error4;
        */
        DROP TABLE "RISCRASHDATA".dbo.[Test];
        """

        assert query.Query.query_drops_table(query_string,'MS') == ['RISCRASHDATA.dbo.Test']

    def test_query_drops_table_from_qry_brackets(self):
        query_string = """
        DROP TABLE [RISCRASHDATA].[dbo].[test];
        """
        assert query.Query.query_drops_table(query_string, 'MS') == ['RISCRASHDATA.dbo.test']

        query_string = """
        DROP TABLE [RISCRASHDATA].[dbo].[123 test];
        """
        assert query.Query.query_drops_table(query_string, 'MS') == ['RISCRASHDATA.dbo.123 test']

        query_string = """
        DROP TABLE [RISCRASHDATA].[dbo].test;
        """
        assert query.Query.query_drops_table(query_string, 'MS') == ['RISCRASHDATA.dbo.test']

    def test_query_drops_table_from_qry_brackets_quotes(self):
        query_string = """
        DROP TABLE [RISCRASHDATA].[dbo].["test"];
        """
        assert query.Query.query_drops_table(query_string, 'MS') == ['RISCRASHDATA.dbo."test"']

        query_string = """
        DROP TABLE [RISCRASHDATA].[dbo].["123 test"];
        """
        assert query.Query.query_drops_table(query_string, 'MS') == ['RISCRASHDATA.dbo."123 test"']

    def test_query_drops_table_from_with_server(self):
        query_string = """
        DROP TABLE dotdevgissql01.riscrashdata.dbo.test;
        """

        assert query.Query.query_drops_table(query_string, 'MS') == ['dotdevgissql01.riscrashdata.dbo.test']

    def test_query_drops_table_from_with_server_brackets(self):
        query_string = """
        DROP TABLE dotdevgissql01.riscrashdata.dbo.[test];
        """
        assert query.Query.query_drops_table(query_string, 'MS') == ['dotdevgissql01.riscrashdata.dbo.test']

        query_string = """
         DROP TABLE dotdevgissql01.riscrashdata.dbo.[123 test];
         """
        assert query.Query.query_drops_table(query_string, 'MS') == ['dotdevgissql01.riscrashdata.dbo.123 test']

    def test_query_drops_table_multiple_tables(self):
        query_string = """
        DROP TABLE riscrashdata.dbo.test; 
        
        DROP TABLE riscrashdata.dbo.test2;
        
        DROP TABLE RISCRASHDATA.dbo.test3;
        """
        x = query.Query.query_drops_table(query_string, 'MS')
        assert x == ['riscrashdata.dbo.test', 'riscrashdata.dbo.test2', 'riscrashdata.dbo.test3']

    def test_query_drops_table_multiple_tables_brackets(self):
        query_string = """
        DROP TABLE dbo.[test];
        
        DROP TABLE riscrashdata.dbo.[test2];
        
        DROP TABLE [RISCRASHDATA].dbo.[test3];
        """
        x = query.Query.query_drops_table(query_string, 'MS')
        x = set(x)
        assert x == set(['RISCRASHDATA.dbo.test3', 'riscrashdata.dbo.test2', 'dbo.test'])

        query_string = """
        DROP TABLE riscrashdata.dbo.[123 test];
        DROP TABLE riscrashdata.dbo.[123 test2];
        DROP TABLE [RISCRASHDATA].[dbo].[123 test3];
        """
        x = query.Query.query_drops_table(query_string, 'MS')
        x = set(x)
        assert x == set(['riscrashdata.dbo.123 test2', 'RISCRASHDATA.dbo.123 test3', 'riscrashdata.dbo.123 test'])

    def test_query_drops_table_view(self):
        query_string = """
        DROP view dotdevgissql01.riscrashdata.dbo.test;
        """

        assert query.Query.query_drops_table(query_string, 'MS') == []

    def test_query_drops_table_view_brackets(self):
        query_string = """
        DROP VIEW [dotdevgissql01].[riscrashdata].[dbo].[test];
        """
        assert query.Query.query_drops_table(query_string, 'MS') == []

        query_string = """
        DROP VIEW [dotdevgissql01].[riscrashdata].[dbo].[123 test];
        """
        assert query.Query.query_drops_table(query_string, 'MS') == []

    def test_query_drops_table_temp_table(self):
        query_string = """
        DROP table #test;
        """
        assert query.Query.query_drops_table(query_string, 'MS') == []

        query_string = """
        DROP table ##test;
        """

        assert query.Query.query_drops_table(query_string, 'MS') == []

    def test_query_drops_table_temp_table_brackets(self):
        query_string = """
        DROP table [#test];
        
        """
        assert query.Query.query_drops_table(query_string, 'MS') == []

        query_string = """
        DROP table [##test];

        """
        assert query.Query.query_drops_table(query_string, 'MS') == []

    def test_query_drops_table_from_into_multiple_statements(self):
        query_string = """
            CREATE TABLE [RISCRASHDATA].[dbo].[test3] AS
            SELECT TOP 10 *
            FROM RISCRASHDATA.dbo.node;
            
            DROP TABLE riscrashdata.dbo.[test1];
        """
        x = query.Query.query_drops_table(query_string, 'MS')
        x = set(x)
        assert x == {'riscrashdata.dbo.test1'}

    def test_query_drops_creates_drops_table(self):
        query_string = """
            DROP TABLE riscrashdata.dbo.[test1];
            
            CREATE TABLE [RISCRASHDATA].[dbo].[test3] AS
            SELECT TOP 10 *
            FROM RISCRASHDATA.dbo.node;

            DROP TABLE riscrashdata.dbo.[test1];
        """
        x = query.Query.query_drops_table(query_string, 'MS')
        assert x == ['riscrashdata.dbo.test1', 'riscrashdata.dbo.test1']


class TestQueryDropsTablesPgSql():
    def test_query_drops_table_from_qry(self):
        query_string = """
        -- DROP TABLE error1;
        --DROP TABLE error2;
        /*DROP TABLE error3;*/
        /*
            DROP TABLE error4;
        */
        DROP TABLE test;
        """
        assert query.Query.query_drops_table(query_string, 'PG') == ['test']

        query_string = """
        -- DROP TABLE if exists error1;
        --DROP TABLE if exists error2;
        /*DROP TABLE if exists error3;*/
        /*
            DROP TABLE if exists error4;
        */
        DROP TABLE IF EXISTS test;
        """
        assert query.Query.query_drops_table(query_string, 'PG') == ['test']

        query_string = """
        -- DROP TABLE working.error1;
        --DROP TABLE working.error2;
        /*DROP TABLE working.error3;*/
        /*
            DROP TABLE working.error4;
        */
        DROP TABLE working.test;
        """
        assert query.Query.query_drops_table(query_string, 'PG') == ['working.test']

        query_string = """
        -- DROP TABLE if exists working.error1;
        --DROP TABLE if exists working.error2;
        /*DROP TABLE if exists working.error3;*/
        /*
            DROP TABLE if exists working.error4;
        */
        DROP TABLE IF EXISTS working.test;
        """
        assert query.Query.query_drops_table(query_string, 'PG') == ['working.test']

    def test_query_drops_table_from_qry_w_comments(self):
        query_string = """
        DROP TABLE test;
        """
        assert query.Query.query_drops_table(query_string, 'PG') == ['test']

        query_string = """
        DROP TABLE IF EXISTS test;
        """
        assert query.Query.query_drops_table(query_string, 'PG') == ['test']

        query_string = """
        DROP TABLE working.test;
        """
        assert query.Query.query_drops_table(query_string, 'PG') == ['working.test']

        query_string = """
        DROP TABLE IF EXISTS working.test;
        """
        assert query.Query.query_drops_table(query_string, 'PG') == ['working.test']


    def test_query_drops_table_from_qry_quotes(self):
        query_string = """
        DROP TABLE working."test";
        """
        assert query.Query.query_drops_table(query_string, 'PG') == ['working.test']

        query_string = """
        DROP TABLE IF EXISTS working."Test";
        """
        assert query.Query.query_drops_table(query_string, 'PG') == ['working.Test']

        query_string = """
        DROP TABLE working."123 test";
        """

        assert query.Query.query_drops_table(query_string, 'PG') == ['working.123 test']

        query_string = """
        DROP TABLE "test";
        """

        assert query.Query.query_drops_table(query_string, 'PG') == ['test']

        query_string = """
        DROP TABLE "123 test";
        """

        assert query.Query.query_drops_table(query_string, 'PG') == ['123 test']

    def test_query_drops_table_other_quotes(self):
        query_string = """
        DROP TABLE "working"."test";
        """

        assert query.Query.query_drops_table(query_string, 'PG') == ['working.test']

        query_string = """
        DROP TABLE IF EXISTS "working"."t3st";
        """

        assert query.Query.query_drops_table(query_string, 'PG') == ['working.t3st']

        query_string = """
        DROP TABLE "Test";
        """
        assert query.Query.query_drops_table(query_string, 'PG') == ['Test']

        query_string = """
        DROP TABLE IF EXISTS "Test";
        """
        assert query.Query.query_drops_table(query_string, 'PG') == ['Test']

    def test_query_drops_table_multiple_tables_quotes(self):
        query_string = """
        DROP TABLE working."test";
        
        DROP TABLE IF EXISTS "test2";
        
        DROP TABLE "staging"."test3";
        """
        x = query.Query.query_drops_table(query_string, 'PG')

        assert x == [ 'working.test', 'test2', 'staging.test3']

    def test_query_drops_table_view_quote(self):
        query_string = """
        DROP VIEW "public"."test";
        """
        assert query.Query.query_drops_table(query_string, 'PG') == []

    def test_query_drops_table_temp_table(self):
        query_string = """
        DROP temporary table test; 
        """
        assert query.Query.query_drops_table(query_string, 'PS') == []

        query_string = """
        DROP temp table test; 
        """
        assert query.Query.query_drops_table(query_string, 'PS') == []

    def test_query_drops_table_from_into_multiple_wtemp(self):
        query_string = """
        DROP TABLE working.test2;
        
        DROP TEMPORARY TABLE test3;
        """
        x = query.Query.query_drops_table(query_string, 'PG')
        x.sort()
        assert x == ['working.test2']

    def test_query_drops_creates_drops(self):
        query_string = """
        DROP TABLE working.test1;
        create table working.test1 as 
        select * from node limit 5; 

        DROP TABLE working.test1;
        """
        x = query.Query.query_drops_table(query_string, 'PG')
        assert x == ['working.test1', 'working.test1']
