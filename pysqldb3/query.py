import csv
import re
import sys

import psycopg2

from .shapefile import *
from .util import parse_table_string

RE_ENCAPSULATED_SCHEMA_NAME = r'((\[)(.)+?(\]))|((\")(.)+?(\"))'
RE_ENCAPSULATED_TABLE_NAME = r'((\[)([^#.])+?(\]))|((\")(.)+?(\"))'

RE_NON_ENCAPSULATED_TABLE_NAME = r'([a-zA-Z_]+[\w]+)'

class Query:
    """
    Query class for use by DbConnect
    """

    def __str__(self):
        # type (Query) -> str
        """
        String query info
        :return: Query print statement (time and results)
        """

        qt = self.__query_time_format()
        records = 0

        if self.data:
            records = len(self.data)

        return '- Query run {dt}\n Query time: {qt} \n * Returned {r} rows *'.format(
            dt=datetime.datetime.now(),
            r=records,
            qt=qt)

    def __init__(self, dbo, query_string, strict=True, permission=True, temp=True, comment='', no_comment=False,
                 timeme=True, iterate=False, lock_table=None, internal=False, no_print_out=False):
        """
        :param dbo: DbConnect object
        :param query_string: String/unicode sql query to be run
        :param strict: If true will run sys.exit on failed query attempts
        :param permission:
        :param temp: if True any new tables will be logged for deletion at a future date
        :param comment: String to add to default comment
        :param no_comment:
        :param timeme: Flag for if table is temporary or permanent (defaults toTrue)
        :param iterate:
        :param lock_table:
        """
        # Explicitly in __init__
        self.dbo = dbo
        self.query_string = query_string
        self.strict = strict
        self.permission = permission
        self.temp = temp
        self.comment = comment
        self.no_comment = no_comment
        self.timeme = timeme
        self.iterate = iterate
        self.lock_table = lock_table
        self.no_print_out = no_print_out

        # Other initialized variables
        self.query_start = datetime.datetime.now()
        self.query_end = datetime.datetime.now()
        self.query_time = None
        self.has_data = False
        self.data_description = None
        self.data_columns = None
        self.data = None
        self.new_tables = list()
        self.renamed_tables = list()
        self.dropped_tables = list()
        self.current_cur = None

        # Run (execute) query, comments, and logging.
        self.__run_query(internal)
        self.__auto_comment()

    def __query_time_format(self):
        # type: (Query) -> str
        """
        Formats the query duration time string
        :return:
        """
        if self.query_time.seconds < 60:
            if self.query_time.seconds < 1:
                return 'Query run in {} microseconds'.format(self.query_time.microseconds)
            else:
                return 'Query run in {} seconds'.format(self.query_time.seconds)
        else:
            return 'Query run in {} seconds'.format(self.query_time)

    def __safe_commit(self):
        # type: (Query) -> None
        """
        After any query, commit changes or rollback if not possible
        :return: None
        """
        try:
            self.dbo.conn.commit()
        except Exception as e:
            self.dbo.conn.rollback()
            if not self.no_print_out:
                print("- ERROR: Query could not be committed. Query rolled back.")
            raise e

    def __perform_lock_routine(self, cur):
        if self.lock_table:
            try:
                if not self.no_print_out:
                    print("- Trying to obtain exclusive lock on table {}.".format(self.lock_table))
                cur.execute("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE NOWAIT".format(self.lock_table))
            except psycopg2.errors.LockNotAvailable as a:
                if not self.no_print_out:
                    print("- Failed to obtain exclusive lock on table {}. Try again.".format(self.lock_table))
                raise a

    def __query_data(self, cur):
        """
        Parses the results of the query and stores data and columns in Query class attribute
        :param cur:
        :return:
        """
        self.has_data = True
        self.data_description = cur.description
        self.data_columns = [desc[0] for desc in self.data_description]
        self.data = cur.fetchall()

    def __update_log_for_renamed_table(self, new_schema_table, old_table):
        _serv, _dab, schema, new_table = parse_table_string(new_schema_table, self.dbo.default_schema, self.dbo.type)

        if self.dbo.table_exists(self.dbo.log_table, schema=schema, internal=True):
            self.dbo.query("update {s}.{l} set table_name = '{nt}' where table_name = '{ot}'".format(
                s=schema, l=self.dbo.log_table, nt=new_table, ot=old_table), internal=True, strict=False)
            self.dbo.check_conn()

    def __run_query(self, internal):
        # type: (Query) -> None
        """
        Runs SQL query via steps below.
        :return: None
        """

        self.query_start = datetime.datetime.now()

        # 1. Get connection cursor
        if self.iterate and self.dbo.type == PG:
            cur = self.dbo.conn.cursor(name='ss')
        else:
            cur = self.dbo.conn.cursor()

        # 2. Query string replacement for special characters
        self.query_string = clean_query_special_characters(self.query_string)

        # 3. Lock table, if required
        self.__perform_lock_routine(cur)

        # 4. Query Execution
        try:
            # 4.1 Attempt to execute query string
            cur.execute(self.query_string)

        except Exception as e:
            # 4.2.1 If failure, return failure reason and time
            if self.dbo.type == MS:
                if 'encode' in str(e).lower() or 'ascii' in str(e).lower():
                    if not self.no_print_out:
                        print("- Query failed: use a Unicode string (u'string') to perform queries with special characters."
                          "\n\t {}  \n\t".format(e))
                else:
                    err = str(e).split("[SQL Server]")
                    if len(err)>1:
                        if not self.no_print_out:
                            print("- Query failed: " + err[1][:-2] + "\n\t")
                    else:
                        if not self.no_print_out:
                            print("- Query failed: " + err[0] + "\n\t")

            if self.dbo.type == PG:
                if not self.no_print_out:
                    print("- Query failed: " + str(e) + '\n\t')
            if not self.no_print_out:
                print('- Query run {dt}\n\t{q}'.format(
                dt=datetime.datetime.now(),
                q=self.query_string))

            del cur

            # 4.2.2 Then rollback
            self.dbo.conn.rollback()

            # 4.2.3 Exit
            if self.strict:
                sys.exit()
            else:
                return

        # 5. Document times
        self.query_end = datetime.datetime.now()
        self.query_time = self.query_end - self.query_start

        # 6. After execution:
        if self.iterate:
            # 6.1 Iterate if an iterating query
            if self.dbo.type == PG:
                cur.itersize = 20000
            self.current_cur = cur
        else:
            # 6.2 Query data if any has been returned
            if cur.description:
                self.__query_data(cur)

            # 6.3 Commit and run new/dropped/renamed tables routine
            self.__safe_commit()
            if not internal:
                self.renamed_tables = self.query_renames_table(self.query_string, self.dbo.default_schema, self.dbo.type)
                self.new_tables = self.query_creates_table(self.query_string, self.dbo.default_schema, self.dbo.type)

                # Add renamed tables to query's new table list
                # self.new_tables += [t for t in self.renamed_tables.keys()]
                self.dropped_tables = self.query_drops_table(self.query_string, self.dbo.type)

                if self.permission:
                    for row in self.new_tables:
                        obj = '.'.join([f'"{x}"' for x in row if x])
                        self.dbo.query(f'grant select on {obj} to public;',
                                       strict=False, timeme=False, internal=True)

                if self.renamed_tables:
                    for i in self.renamed_tables.keys():
                        self.__update_log_for_renamed_table(i, self.renamed_tables[i])

                        # Rename index
                        self.rename_index(i, self.renamed_tables[i])

                        # Add standardized previous table name to dropped tables to remove from log
                        server, database, sch, tbl = parse_table_string(i, self.dbo.default_schema, self.dbo.type)
                        org_table = get_query_table_schema_name(
                            self.renamed_tables[i], self.dbo.type)
                        # org_table = get_query_table_schema_name(sch, self.dbo.type) + '.' + get_query_table_schema_name(
                        #     self.renamed_tables[i], self.dbo.type)
                        self.dropped_tables.append((server, database, sch,org_table))

                        # If the standardized previous table name is in this query's new tables, replace with new name
                        if org_table in self.new_tables:
                            self.new_tables[self.new_tables.index(org_table)] = \
                                get_query_table_schema_name(sch, self.dbo.type) + '.' + get_query_table_schema_name(
                                tbl, self.dbo.type)
                            # self.new_tables.remove(org_table)

                        # If the standardized previous table name is in the dbconnects's new tables, remove
                        if (server, database, sch,org_table) in self.dbo.tables_created:
                            # self.dbo.tables_created.remove(org_table)
                            self.dbo.tables_created[self.dbo.tables_created.index( (server, database, sch,org_table))] = \
                                get_query_table_schema_name(sch, self.dbo.type) + '.' + get_query_table_schema_name(
                                tbl, self.dbo.type)

                if self.timeme:
                    print(self)

    def dfquery(self):
        """
        Returns data from query as a Pandas DataFrame
        Note: cannot use pd.read_sql() because the structure will necessitate running query twice
        :return: Pandas DataFrame of the results of the query
        """
        if self.dbo.type in (MS, AZ):
            self.data = [tuple(i) for i in self.data]
            df = pd.DataFrame(self.data, columns=self.data_columns)
        else:
            df = pd.DataFrame(self.data, columns=self.data_columns)
        return df

    @staticmethod
    def query_creates_table(query_string, default_schema, db_type):
        """
        Checks if query generates new tables
        :return: list of sets of {schema.table}
        """
        # remove lines after '--'
        new_query_string = list()
        parsed_tables = []

        # remove multiline comments
        comments = r'((/\*)+?[\w\W]+?(\*/)+)|(--.*)'
        matches = re.findall(comments, query_string, re.IGNORECASE)
        for m in matches:
            if m:
                m = [s for s in m if s!='']
                query_string =''.join(query_string.split(m[0]))


        # remove mulitple spaces
        # _ = query_string.split()
        # query_string = ' '.join(_)
        new_tables = list()

        # # OlD
        # create_pattern =r"""
        #     (?<!\*)(?<!\*\s)(?<!--)(?<!--\s)    # ignore comments
        #     ((create\s+table\s+)(?!temp\s+|temporary\s+|#{0,2})(if\s+not\s+exists\s+)*)(\s+)*  # create non-temp table expression
        #     (((([\[|\"])?)([\w!@#$%^&*()\s0-9-]+)(([\]|\"])?\.)){0,3}   # server.db.schema. 0-3 times
        #     ((([\[|\"])?)([\w!@#$%^&*()\s0-9-]+)(([\]|\"])?\.?)\s+){1}){1} # table (whole thing only once
        # """
        # create_pattern =r"""
        #     (?<!\*)(?<!\*\s)(?<!--)(?<!--\s)    # ignore comments
        #     ((?<!\$BODY\$))
        #     (create\s+table\s+)
        #     (?!temp\s+|temporary\s+)
        #     (if\s+not\s+exists\s+)?
        #     (((([\[|\"])?)([\w!@$%^&*()\s0-9-]+)(([\]|\"])?\.)){0,3}
        #     ((([\[|\"])?)((?!\#)[\w!@$%^&*()\s0-9-]+)(([\]|\"])?\.?)\s+){1})
        #     (?=(as\s+select)|(\())\s?
        #     ((?!\$BODY\$))
        # """

        create_pattern = """
            (?<!\*)(?<!\*\s)(?<!--)(?<!--\s)
            (create\s+table\s+)
            (?!temp\s+|temporary|\s+)
            (if\s+not\s+exists\s+)?
            (
                (({encaps} | {nonencaps})\.){sds}
                (\[?\#{temp_mark}){tmp_time}
                (({encapst} | {nonencaps})\s*){tbl_time}
            )((as\s+select)|(\())\s?
        """.format(encaps=RE_ENCAPSULATED_SCHEMA_NAME, nonencaps=RE_NON_ENCAPSULATED_TABLE_NAME,
                          encapst=RE_ENCAPSULATED_TABLE_NAME,
                          sds="{0,3}", tbl_time="{1}", tmp_time="{0}", temp_mark="{1,2}")

        create_table = re.compile(create_pattern, re.VERBOSE | re.IGNORECASE)

        # matches = re.findall(create_table, query_string, re.IGNORECASE)
        matches = re.findall(create_table, query_string)

        tables = [i[2].strip() for i in matches]
        new_tables+=tables

        into_pattern = r"""
            (?<!\*)(?<!\*\s)(?<!--)(?<!--\s)                       # ignore comments
            
            (select([.\n\w\*\s\",^,\[\],',!,=,+,(,)])+?into\s+)+?    # find select into
            (?!temp\s+|temporary\s+)                                # lookahead for temp
            (
               (({encaps} | {nonencaps})\.){sds}
               (\[?\#{temp_mark}){tmp_time}
               (({encapst} | {nonencaps})\s+){tbl_time}
           )
           (?=from)                                                # lookahead for 'from'
            """.format(encaps=RE_ENCAPSULATED_SCHEMA_NAME, nonencaps=RE_NON_ENCAPSULATED_TABLE_NAME,
                          encapst=RE_ENCAPSULATED_TABLE_NAME,
                          sds="{0,3}", tbl_time="{1}", tmp_time="{0}", temp_mark="{1,2}")


        create_table_into = re.compile(into_pattern, re.VERBOSE | re.IGNORECASE)
        into_matches = re.findall(create_table_into, query_string)

        into_tables = [i[2].strip() for i in into_matches]
        new_tables += into_tables

        if new_tables:
            all_tables = [i for i in new_tables if len(i) > 0]

            all_tables = [t if ('"' in t or "[" in t) else t.lower() for t in all_tables]

        #     # Clean table names via parse_table_string, get_query_table_schema_name
            parsed_tables = [parse_table_string(a, default_schema, db_type) for a in all_tables]
        return parsed_tables
            # TODO create table query tables will always be 1st in the list over select into queries... does order matter?


    @staticmethod
    def query_drops_table(query_string, db_type):
        """
        Checks if query drops any tables.
        Tables that were dropped should be removed from the to drop queue. This should help
        avoid dropping tables with coincident names.
        :return: list of tables dropped (including any db/schema info)
        """

        # remove multiline comments
        comments = r'((/\*)+?[\w\W]+?(\*/)+)|(--.*)'
        matches = re.findall(comments, query_string, re.IGNORECASE)
        for m in matches:
            if m:
                m = [s for s in m if s != '']
                query_string = ''.join(query_string.split(m[0]))

        dropped_tables = list()

        drop_pattern = r"""
                   (?<!--\s)(?<!--)(?<!\*\s)(?<!\*)
                   (\s?drop\s+table\s(if\s+exists\s+)?)
                   ((
                       (({encaps} | {nonencaps})\.)){sds}
                   (
                       (({encapst} | {nonencaps})){tbl_time}
                   ))(\s | \;)*?
                       """.format(encaps=RE_ENCAPSULATED_SCHEMA_NAME,
                                  encapst=RE_ENCAPSULATED_TABLE_NAME,
                                  nonencaps=RE_NON_ENCAPSULATED_TABLE_NAME,
                                  sds="{0,3}", tbl_time="{1}")
        drop_pattern = re.compile(drop_pattern, re.VERBOSE | re.IGNORECASE)

        matches = re.findall(drop_pattern, query_string)


        # drop_table = r'(?<!--\s)(?<!--)(?<!\*\s)(?<!\*)(drop\s+table\s+(if\s+exists\s+)?)((([\[][\w\s\.\"]*[\]])|' \
        #              r'([\"][\w\s\.]*[\"])|([\w]+))([.](([\[][\w\s\.\"]*[\]])|([\"][\w\s\.]*[\"])|([\w]+)))?([.]' \
        #              r'(([\[][\w\s\.\"]*[\]])|([\"][\w\s\.]*[\"])|([\w]+)))?([.](([\[][\w\s\.\"]*[\]])|([\"][\w\s\.]*' \
        #              r'[\"])|([\w]+)))?)([\s\n\r\t]*(\;?)[\s\n\r\t]*)'
        # matches = re.findall(drop_table, query_string, re.IGNORECASE)

        if matches:
            for row in matches:
                dropped_tables.append(
                    '.'.join([
                        get_unique_table_schema_string(t, db_type)
                        for t in row[2].split('.')])
                )
        return dropped_tables


    @staticmethod
    def query_renames_table(query_string, default_schema, db_type):
        """
        Checks if a rename query is run
        :return: Dict {schema.new table name: original table name}
        """
        _ = query_string.split()
        query_string = ' '.join(_)
        new_tables = dict()

        rename_pattern = r"""
            (?<!--\s)(?<!--)(?<!\*\s)(?<!\*)
            (\s?alter\s+table\s+(if exists\s+)?)
            ((
                (({encaps} | {nonencaps})\.)){sds}                                           
            (
                (({encaps} | {nonencaps})\s+){tbl_time}
            ))
            (rename\s+to\s+)
            ((({encaps} | {nonencaps}))+)
                """.format(encaps=RE_ENCAPSULATED_SCHEMA_NAME,
                           nonencaps=RE_NON_ENCAPSULATED_TABLE_NAME,
                           sds="{0,3}", tbl_time="{1}")

        rename_tables = re.compile(rename_pattern, re.VERBOSE | re.IGNORECASE)


        matches = re.findall(rename_tables, query_string)
        for row in matches:
            old_schema = row[5]
            old_table = row[17]
            new_table = row[28]
            if old_schema:
                new_tables[get_unique_table_schema_string(old_schema, db_type) + '.' + get_unique_table_schema_string(new_table, db_type)] = \
                    get_unique_table_schema_string(old_table, db_type)
            else:
                new_tables[default_schema + '.' + get_unique_table_schema_string(
                    new_table, db_type)] = \
                    get_unique_table_schema_string(old_table, db_type)

        if not matches:
            if not re.search(r"""(,?\s*n?'(column|index)'?\s*)""", query_string, re.IGNORECASE):
                rename_tables_sql = """
                (?<!--\s)(?<!--)(?<!\*\s)(?<!\*)
                (exec\s+sp_rename)(\s+?')
                ((.)+?)
                ('\s?,\s?')
                ((.)*?)
                ('\s?)(;)?
                (?!,?\s*n?'(column|index)'?\s*)
                """

                rename_tables = re.compile(rename_tables_sql, re.VERBOSE | re.IGNORECASE)
                matches = re.findall(rename_tables, query_string)


            for row in matches:
                from_data = row[2].split('.')
                _ = [None for i in range(3)] + from_data
                from_server, from_database, from_schema, from_table = _[-4:]
                to_table = row[5]

                if not from_schema:
                    new_tables[default_schema + '.' + get_unique_table_schema_string(to_table, db_type)] = \
                        get_unique_table_schema_string(from_table, db_type)

                else:
                    new_tables[get_unique_table_schema_string(from_schema, db_type) +'.'+
                           get_unique_table_schema_string(to_table, db_type)] = get_unique_table_schema_string(from_table, db_type)

                #
                # row = [r for r in row if r and r.strip() != "'"]
                #
                # if len(row) == 3:
                #     old_schema = default_schema + "."
                #     old_table = row[1]
                #
                # if len(row) == 4:
                #     old_schema = row[1]
                #     old_table = row[2]
                #
                # if len(row) == 5:
                #     # old_database = row[1]
                #     old_schema = row[2]
                #     old_table = row[3]
                #
                # if len(row) == 6:
                #     # old_server = row[1]
                #     # old_database = row[2]
                #     old_schema = row[3]
                #     old_table = row[4]
                #
                # new_table = row[-1]
                # new_tables[old_schema + new_table] = old_table

        return new_tables

    def rename_index(self, new_table, old_table):
        """
        Renames any indexes associated with a table. Used when a table is renamed to keep the indexes up to date
        :param new_table: new table name
        :param old_table: original table name
        :return:
        """
        # Get indices for new table
        server, database, sch, tbl = parse_table_string(new_table, self.dbo.default_schema, self.dbo.type)
        if not database:
            database = self.dbo.database

        if not server:
            server = self.dbo.server

        if self.dbo.type == 'PG':
            old_table = old_table.replace('"', '').replace('\n', '').strip()
            new_table = new_table.replace('"', '').replace('\n', '').strip()
            query = """
                SELECT indexname
                FROM pg_indexes
                WHERE tablename = '{t}'
                AND schemaname='{s}';
            """.format(t=tbl, s=sch)
        elif self.dbo.type == 'MS':
            query = """      
                SELECT a.name AS indexname
                FROM {d}.sys.indexes AS a
                INNER JOIN {d}.sys.index_columns AS b
                    ON a.object_id = b.object_id AND a.index_id = b.index_id
                WHERE
                    a.is_hypothetical = 0 
                    AND a.object_id = OBJECT_ID('{s}.{t}')
            """.format(t=tbl, s=sch, d=database)
        # make sure looking in the right server, dont need to worry about db, since this is a sys table

        if not get_unique_table_schema_string(server, self.dbo.type) == self.dbo.server:
            print('Warning: any associated indexes will not be renamed on {ser}.{db}.{sch}.{tbl}'.format(
                ser=server, db=database, sch=sch, tbl=tbl
            ))
            indices = []
        else:
            self.dbo.query(query, strict=True, timeme=False, internal=True)
            indices = self.dbo.internal_data or []

        for idx in indices:
            if old_table in idx[0]:
                new_idx = idx[0].replace(old_table, tbl)

                if self.dbo.type == 'PG':
                    new_idx_qry = 'ALTER INDEX IF EXISTS {s}."{i}" RENAME to "{i2}"'.format(s=sch, i=idx[0], i2=new_idx)
                elif self.dbo.type == 'MS':
                    new_idx_qry = "EXEC sp_rename '{t}.{i}', '{i2}', N'INDEX';".format(i=idx[0], i2=new_idx,
                                                                                       t=new_table)

                self.dbo.query(query=new_idx_qry, strict=False, timeme=False, internal=True)

    def __auto_comment(self):
        """
        Automatically generates comment for PostgreSQL tables if created with Query
        :return:
        """
        if self.dbo.type == PG and not self.no_comment:
            # tables in new_tables list will contain schema if provided, otherwise will default to public
            for row in self.new_tables:
                obj = '.'.join([f'"{x}"' for x in row if x])
                self.dbo.query(f'''COMMENT ON TABLE {obj} 
                IS 'Created by {self.dbo.user} 
                on {self.query_start.strftime('%Y-%m-%d %H:%M')}
                {self.comment}'
                ''',
                               strict=False, timeme=False, internal=True)

    @staticmethod
    def query_to_shp(dbo, query, path=None, shp_name=None, cmd=None, gdal_data_loc=GDAL_DATA_LOC, print_cmd=False,
                     srid=2263):
        """
        Writes results of the query to a shp file by calling Shapefile ogr command's in write_shp fn
        :param dbo:
        :param query:
        :param path:
        :param shp_name:
        :param cmd:
        :param gdal_data_loc:
        :param print_cmd: Optional flag to print the GDAL command being used; defaults to False
        :param srid: sets SRID
        :return:
        """

        shp = Shapefile(dbo=dbo,
                        path=path,
                        query=query,
                        shp_name=shp_name,
                        cmd=cmd,
                        gdal_data_loc=gdal_data_loc,
                        srid=srid)

        shp.write_shp(print_cmd)

    @staticmethod
    def print_query(query_string):
        """
        Prints query string with basic formatting
        :param query_string: String on input query to be formatted 
        :return: None
        """
        for _ in query_string.split('\n'):
            print(_)
