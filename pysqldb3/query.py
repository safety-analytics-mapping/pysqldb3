import csv
import sys

import psycopg2

from .shapefile import *
from .util import parse_table_string


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

        return f'- Query run {datetime.datetime.now()}\n Query time: {qt} \n * Returned {records} rows *'

    def __init__(self, dbo, query_string, strict=True, permission=True, temp=True, comment='', no_comment=False,
                 timeme=True, iterate=False, lock_table=None, internal=False):
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
                return f'Query run in {self.query_time.microseconds} microseconds'
            else:
                return f'Query run in {self.query_time.seconds} seconds'
        else:
            return f'Query run in {self.query_time} seconds'

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
            print("- ERROR: Query could not be committed. Query rolled back.")
            raise e

    def __perform_lock_routine(self, cur):
        if self.lock_table:
            try:
                print(f"- Trying to obtain exclusive lock on table {self.lock_table}.")
                cur.execute(f"LOCK TABLE {self.lock_table} IN ACCESS EXCLUSIVE MODE NOWAIT")
            except psycopg2.errors.LockNotAvailable as a:
                print(f"- Failed to obtain exclusive lock on table {self.lock_table}. Try again.")
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
            self.dbo.query(f"UPDATE {schema}.{self.dbo.log_table} SET table_name = '{new_table}' WHERE table_name = '{old_table}'", internal=True, strict=False)
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
                    print("- Query failed: use a Unicode string (u'string') to perform queries with special characters."
                          f"\n\t {e}  \n\t")
                else:
                    err = str(e).split("[SQL Server]")
                    if len(err)>1:
                        print(f"- Query failed: {err[1][:-2]}\n\t")
                    else:
                        print(f"- Query failed: {err[0]}\n\t")

            if self.dbo.type == PG:
                print(f"- Query failed: {str(e)}\n\t")

            print(f'- Query run {datetime.datetime.now()}\n\t{self.query_string}')

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
                self.renamed_tables = self.query_renames_table(self.query_string, self.dbo.default_schema)
                self.new_tables = self.query_creates_table(self.query_string, self.dbo.default_schema, self.dbo.type)

                # Add renamed tables to query's new table list
                # self.new_tables += [t for t in self.renamed_tables.keys()]
                self.dropped_tables = self.query_drops_table(self.query_string)

                if self.permission:
                    for t in self.new_tables:
                        self.dbo.query(f'GRANT SELECT ON {t} TO public;', strict=False, timeme=False, internal=True)

                if self.renamed_tables:
                    for i in self.renamed_tables.keys():
                        self.__update_log_for_renamed_table(i, self.renamed_tables[i])

                        # Rename index
                        self.rename_index(i, self.renamed_tables[i])

                        # Add standardized previous table name to dropped tables to remove from log
                        server, database, sch, tbl = parse_table_string(i, self.dbo.default_schema, self.dbo.type)
                        orig_schema_name = get_query_table_schema_name(sch, self.dbo.type)
                        orig_table_name = get_query_table_schema_name(self.renamed_tables[i], self.dbo.type)
                        org_table = f"{orig_schema_name}.{orig_table_name}"
                        self.dropped_tables.append(org_table)

                        # If the standardized previous table name is in this query's new tables, replace with new name
                        if org_table in self.new_tables:
                            orig_table_schema_name = get_query_table_schema_name(tbl, self.dbo.type)
                            idx = self.new_tables.index(org_table)
                            self.new_tables[idx] = get_query_table_schema_name(f'{orig_schema_name}.{orig_table_schema_name}')
                            # self.new_tables.remove(org_table)

                        # If the standardized previous table name is in the dbconnects's new tables, remove
                        if org_table in self.dbo.tables_created:
                            # self.dbo.tables_created.remove(org_table)
                            orig_table_schema_name = get_query_table_schema_name(tbl, self.dbo.type)
                            idx = self.dbo.tables_created.index(org_table)
                            self.dbo.tables_created[idx] = f'{orig_schema_name}.{orig_table_schema_name}'

                if self.timeme:
                    print(self)

    def dfquery(self):
        """
        Returns data from query as a Pandas DataFrame
        Note: cannot use pd.read_sql() because the structure will necessitate running query twice
        :return: Pandas DataFrame of the results of the query
        """
        if self.dbo.type == MS:
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
        # remove mulitple spaces
        _ = query_string.split()
        query_string = ' '.join(_)
        new_tables = list()
        create_table = r'((?<!\*)(?<!\*\s)(?<!--)(?<!--\s)\s*create\s+table\s+(if\s+not\s+exists\s+)?)' \
                       r'((([\[][\w\s\.\"]*[\]])|([\"][\w\s\.]*[\"])|([\w]+))([.](([\[][\w\s\.\"]*[\]])|([\"]' \
                       r'[\w\s\.]*[\"])|([\w]+)))?([.](([\[][\w\s\.\"]*[\]])|([\"][\w\s\.]*[\"])|([\w]+)))?([.]' \
                       r'(([\[][\w\s\.\"]*[\]])|([\"][\w\s\.]*[\"])|([\w]+)))?)'
        matches = re.findall(create_table, query_string, re.IGNORECASE)

        # Get all schema and table pairs remove create table match
        new_tables += [set(match[2:3]) for match in matches]

        # Adds catch for MS [database].[schema].[table]
        select_into = r'(((\$body\$)([^\$]*)(\$body\$;$))|((\$\$)([^\$]*)(\$\$;$)))|(?<!\*)(?<!\*\s)(?<!--)' \
                      r'(?<!--\s)\s*(select[^\.]*into\s+)(?!temp\s|temporary\s)((([\[][\w\s\.\"]*[\]])|([\"][\w\s\.]*' \
                      r'[\"])|([\w]+))([.](([\[][\w\s\.\"]*[\]])|([\"][\w\s\.]*[\"])|([\w]+)))?([.](([\[][\w\s\.\"]*' \
                      r'[\]])|([\"][\w\s\.]*[\"])|([\w]+)))?([.](([\[][\w\s\.\"]*[\]])|([\"][\w\s\.]*[\"])|([\w]+)))?)'
        matches = re.findall(select_into, query_string, re.IGNORECASE)

        # [[select ... into], [table], [misc]]
        # new_tables += [set(match[1:2]) for match in matches]
        new_tables += [set(match[10:11]) for match in matches]

        # Clean up
        for _ in new_tables:
            if '' in _:
                _.remove('')

        if new_tables and new_tables != [set()]:
            all_tables = [i.pop() for i in new_tables if len(i) > 0]
            all_tables = [t if ('"' in t or "[" in t) else t.lower() for t in all_tables]

            # Clean table names via parse_table_string, get_query_table_schema_name
            parsed_tables = [parse_table_string(a, default_schema, db_type) for a in all_tables]
            parsed_tables_clean = []
            for ser, db, s, t in parsed_tables:
                # format variables
                ser = get_query_table_schema_name(ser, db_type)
                db = get_query_table_schema_name(db, db_type)
                s = get_query_table_schema_name(s, db_type)
                t = get_query_table_schema_name(t, db_type)
                if ser:
                    parsed_tables_clean.append('.'.join([ser, db, s, t]))
                elif db:
                    parsed_tables_clean.append('.'.join([db, s, t]))
                else:
                    parsed_tables_clean.append('.'.join([s, t]))
            return parsed_tables_clean
        else:
            return []

    @staticmethod
    def query_drops_table(query_string):
        """
        Checks if query drops any tables.
        Tables that were dropped should be removed from the to drop queue. This should help
        avoid dropping tables with coincident names.
        :return: list of tables dropped (including any db/schema info)
        """
        _ = query_string.split()
        query_string = ' '.join(_)
        dropped_tables = list()
        drop_table = r'(?<!--\s)(?<!--)(?<!\*\s)(?<!\*)(drop\s+table\s+(if\s+exists\s+)?)((([\[][\w\s\.\"]*[\]])|' \
                     r'([\"][\w\s\.]*[\"])|([\w]+))([.](([\[][\w\s\.\"]*[\]])|([\"][\w\s\.]*[\"])|([\w]+)))?([.]' \
                     r'(([\[][\w\s\.\"]*[\]])|([\"][\w\s\.]*[\"])|([\w]+)))?([.](([\[][\w\s\.\"]*[\]])|([\"][\w\s\.]*' \
                     r'[\"])|([\w]+)))?)([\s\n\r\t]*(\;?)[\s\n\r\t]*)'
        matches = re.findall(drop_table, query_string, re.IGNORECASE)

        if matches:
            for match in matches:
                if len(match) == 0:
                    continue

                tb = [m for m in match][2:3]
                dropped_tables += tb
            return dropped_tables
        else:
            return []

    @staticmethod
    def query_renames_table(query_string, default_schema):
        """
        Checks if a rename query is run
        :return: Dict {schema.new table name: original table name}
        """
        _ = query_string.split()
        query_string = ' '.join(_)
        new_tables = dict()

        rename_tables = r'(?<!--\s)(?<!--)(?<!\*\s)(?<!\*)(ALTER TABLE\s+(IF EXISTS\s+)?)(\"?[*\w\s]*\"?\.)?' \
                        r'(\"?[\w\s]*\"?)\s+(RENAME TO )(\"?[\w\s]*\"?)\;?'
        matches = re.findall(rename_tables, query_string.lower())
        for row in matches:
            old_schema = row[2]
            old_table = row[3]
            new_table = row[-1]
            new_tables[old_schema + new_table] = old_table

        if not matches:
            rename_tables_sql = r"(?<!--\s)(?<!--)(?<!\*\s)(?<!\*)(exec\s+sp_rename)(\s*')(\[?[\w\s+]*\]?\.)?" \
                                r"(\[?[\w\s+]*\]?\.)?(\[?[\w\s+]*\]?\.)?(\[?[\w\s+]*\]?)',\s*'(\[?[\w\s+]*\]?)'" \
                                r"(?!,?\s*n?'(column|index)'?\s*)"
            matches = re.findall(rename_tables_sql, query_string.lower())

            for row in matches:
                row = [r for r in row if r and r.strip() != "'"]

                if len(row) == 3:
                    old_schema = f"{default_schema}."
                    old_table = row[1]

                if len(row) == 4:
                    old_schema = row[1]
                    old_table = row[2]

                if len(row) == 5:
                    # old_database = row[1]
                    old_schema = row[2]
                    old_table = row[3]

                if len(row) == 6:
                    # old_server = row[1]
                    # old_database = row[2]
                    old_schema = row[3]
                    old_table = row[4]

                new_table = row[-1]
                new_tables[old_schema + new_table] = old_table

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
            query = f"SELECT indexname FROM pg_indexes WHERE tablename = '{tbl}' AND schemaname='{sch}';"
        elif self.dbo.type == 'MS':
            query = f"""      
                SELECT a.name AS indexname
                FROM {database}.sys.indexes AS a
                INNER JOIN {database}.sys.index_columns AS b
                    ON a.object_id = b.object_id AND a.index_id = b.index_id
                WHERE
                    a.is_hypothetical = 0 
                    AND a.object_id = OBJECT_ID('{sch}.{tbl}')"""
        # make sure looking in the right server, dont need to worry about db, since this is a sys table

        if not get_unique_table_schema_string(server, self.dbo.type) == self.dbo.server:
            print(f'Warning: any associated indexes will not be renamed on {server}.{database}.{sch}.{tbl}')
            indices = []
        else:
            self.dbo.query(query, strict=True, timeme=False, internal=True)
            indices = self.dbo.internal_data or []

        for idx in indices:
            if old_table in idx[0]:
                new_idx = idx[0].replace(old_table, tbl)

                if self.dbo.type == 'PG':
                    new_idx_qry = f'ALTER INDEX IF EXISTS {sch}."{idx[0]}" RENAME to "{new_idx}"'
                elif self.dbo.type == 'MS':
                    new_idx_qry = f"EXEC sp_rename '{new_table}.{idx[0]}', '{new_idx}', N'INDEX';"

                self.dbo.query(query=new_idx_qry, strict=False, timeme=False, internal=True)

    def __auto_comment(self):
        """
        Automatically generates comment for PostgreSQL tables if created with Query
        :return:
        """
        if self.dbo.type == PG and not self.no_comment:
            for t in self.new_tables:
                # tables in new_tables list will contain schema if provided, otherwise will default to public
                q = f"COMMENT ON TABLE {t} IS 'Created by {self.dbo.user} on {self.query_start.strftime('%Y-%m-%d %H:%M')}\n{self.comment}'"
                self.dbo.query(q, strict=False, timeme=False, internal=True)

    def iterable_query_to_csv(self, output, open_file, quote_strings, sep):
        """
        Writes results of the iterable query to a csv file; iterating over cursor results.
        This is different from chunked_write_csv in that the cursor only obtains 20k records at a time, meaning
        it works if the data itself is too big to even load at once.
        :param output:
        :param open_file:
        :param quote_strings:
        :param sep:
        :return:
        """
        self.has_data = True
        cur = self.current_cur

        batch = []
        i = 0
        first_iteration = True

        # Iterate over server-side cursor
        for row in cur:
            if i == 0:
                self.data_description = cur.description
                self.data_columns = [desc[0] for desc in self.data_description]

            # Get data
            batch.append(row)
            i = i + 1

            # After each iteration, dump data to csv
            if i % 20000 == 0:

                # Make and clean df, as per usual
                self.data = batch
                df = self.dfquery()
                df.columns = self.data_columns
                df = convert_geom_col(df)
                df = df.dropna(how='all')

                # Include header with first iteration
                if first_iteration:
                    # Write out 1st chunk
                    if quote_strings:
                        df.to_csv(output, index=False, quoting=csv.QUOTE_ALL, sep=sep)
                    else:
                        df.to_csv(output, index=False, sep=sep)

                    first_iteration = False
                else:
                    # Otherwise, append to existing csv
                    if quote_strings:
                        df.to_csv(output, index=False, quoting=csv.QUOTE_ALL, sep=sep, mode='a', header=False)
                    else:
                        df.to_csv(output, index=False, sep=sep, mode='a', header=False)

                # Safeguard for memory issues
                del df
                del batch
                self.data = None

                i = 0
                batch = []

        # For remaining rows...
        self.data = batch
        df = self.dfquery()
        df.columns = self.data_columns
        df = clean_df_before_output(df)
        df = df.dropna(how='all')

        if first_iteration:
            # Write out 1st chunk
            if quote_strings:
                df.to_csv(output, index=False, quoting=csv.QUOTE_ALL, sep=sep)
            else:
                df.to_csv(output, index=False, sep=sep)
        else:
            # Otherwise, append to existing csv
            if quote_strings:
                df.to_csv(output, index=False, quoting=csv.QUOTE_ALL, sep=sep, mode='a', header=False)
            else:
                df.to_csv(output, index=False, sep=sep, mode='a', header=False)

        del df
        del batch
        self.data = None

        # Close connections
        self.__safe_commit()
        self.dbo.conn.close()

        if open_file:
            os.startfile(output)

    def __chunked_write_csv(self, output, open_file=False, quote_strings=True, sep=','):
        """
        Writes results of the query to a csv file.
        Performs the same operations as query_to_csv, but brakes data into chunks
        to deal with memory errors for large files.
        :param output: String for csv output file location (defaults to current directory)
        :param open_file: Boolean flag to auto open output file
        :param quote_strings: Boolean flag for adding quote strings to output (defaults to True, QUOTE_ALL)
        :param sep: Separator for csv (defaults to ',')
        :return:
        """

        def chunks(size=100000):
            # Break data into chunks
            """
            Breaks large datasets into smaller subsets
            :param size: Integer for the size of the chunks (defaults to 100,000)
            :return: Generator for data in 100,000 record chunks (list of lists)
            """
            n = len(self.data) / size
            for i in range(0, n):
                yield self.data[i::n], i

        print(f'Large file...Writing {len(self.data)} rows of data...')

        # write to csv
        _ = chunks()
        for (chunk, pos) in _:
            # convert to data frame
            if self.dbo.type == MS:
                self.data = [tuple(i) for i in self.data]
                df = pd.DataFrame(self.data, columns=self.data_columns)
            else:
                df = pd.DataFrame(chunk, columns=self.data_columns)
            # Only write header for 1st chunk
            if pos == 0:
                # Write out 1st chunk
                if quote_strings:
                    df.to_csv(output, index=False, quoting=csv.QUOTE_ALL, sep=sep)
                else:
                    df.to_csv(output, index=False, sep=sep)
            else:
                if quote_strings:
                    df.to_csv(output, index=False, quoting=csv.QUOTE_ALL, sep=sep, mode='a', header=False)
                else:
                    df.to_csv(output, index=False, sep=sep, mode='a', header=False)

        if open_file:
            os.startfile(output)

    def query_to_csv(self, output=None, open_file=False, quote_strings=True, sep=','):
        """
        Writes results of the query to a csv file
        :param output: String for csv output file location (defaults to current directory)
        :param open_file: Boolean flag to auto open output file
        :param quote_strings: Boolean flag for adding quote strings to output (defaults to true, QUOTE_ALL)
        :param sep: Separator for csv (defaults to ',')
        :return:
        """
        if not output:
            output = os.path.join(os.getcwd(), f"data_{datetime.datetime.now().strftime('%Y%m%d%H%M')}.csv")

        if len(self.data) > 100000:
            self.__chunked_write_csv(output=output, open_file=open_file, quote_strings=quote_strings, sep=sep)
        else:
            df = self.dfquery()
            df = clean_df_before_output(df)

            if quote_strings:
                df.to_csv(output, index=False, quoting=csv.QUOTE_ALL, sep=sep, encoding='utf8')
            else:
                df.to_csv(output, index=False, sep=sep, encoding='utf8')

            if open_file:
                os.startfile(output)

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
