import getpass
import pyodbc
import pymssql
from tqdm import tqdm
from typing import Optional, Union
import openpyxl
import json
import plotly.express as px
import configparser
import os
from .Config import write_config
import pyarrow.csv as pyarrowcsv
import numpy as np

write_config(confi_path=os.path.dirname(os.path.abspath(__file__)) + "\\config.cfg")
config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\config.cfg")

from .query import *
from .shapefile import *
from .geopackage import *
from .data_io import *
from .__init__ import __version__



# noinspection PyArgumentList
class DbConnect:
    """
    Database Connection class
    """

    def __init__(self, user=None, password=None, ldap=False, type=None, server=None, database=None, port=5432,
                 allow_temp_tables=False, use_native_driver=True, default=False, quiet=False,
                 inherits_from=None):
        # type: (DbConnect, str, str, bool, str, str, str, int, bool, bool, bool, bool, object) -> None
        """
        :params:
        user (string): default None
        password (string): default None
        ldap (bool): default None
        type (string): default None
        server (string): default None
        database (string): default None
        port (int): default 5432
        use_native_driver (bool): defaults to False
        default (bool): defaults to False; connects to ris db automatically
        quiet (bool): automatically performs all tasks quietly; defaults to False
        inherits_from (pysqldb3.DbConnect object): will take any input variable from other database connection
            not explicitly provided to self
        """
        # Explicitly in __init__ fn call
        self.user = user
        self.password = password
        self.LDAP = ldap
        if self.LDAP and not self.user:
            self.user = getpass.getuser()
        self.inherits_from = inherits_from
        self.type = type
        self.__set_type()
        self.server = get_unique_table_schema_string(server, self.type)
        self.database = database
        self.port = port
        self.allow_temp_tables = allow_temp_tables
        self.use_native_driver = use_native_driver
        self.default_connect = default
        self.quiet = quiet
        self.inherits_from = inherits_from

        # Other initialized variables
        self.params = dict()
        self.conn = None
        self.queries = list()
        self.internal_queries = list()
        self.connection_start = None
        self.tables_created = list()
        self.tables_dropped = list()
        self.data = None
        self.internal_data = None
        self.last_query = None
        self.default_schema = None
        self.connection_count = 0
        self.__set_type()

        # Connect and clean logs
        self.__get_credentials()
        self.default_schema = self.__get_default_schema(self.type)
        self.log_table = TEMP_LOG_TABLE.format(self.user)
        self.__cleanup_subroutine()

    def __str__(self):
        # type: (DbConnect) -> str
        """
        :return: string of database connection info
        """

        return 'Database connection ({typ}) to {db} on {srv} - user: {usr} \nConnection established {dt}, /' \
               '\n- ris version {v} - '.format(
                typ=self.type,
                db=self.database,
                srv=self.server,
                usr=self.user,
                dt=self.connection_start,
                v=__version__
                )

    def __get_most_recent_query_data(self, internal=False):
        # type: (DbConnect) -> list
        """
        Helper function to return the most recent query data
        :return:
        """
        if internal:
            return self.internal_queries[-1].data
        else:
            return self.queries[-1].data

    def __set_type(self):
        # type: (DbConnect) -> None
        """
        Sets standardized type of Db
        """
        if self.type and type(self.type) == str and self.type.upper() in POSTGRES_TYPES:
            self.type = PG

        if self.type and type(self.type) == str and self.type.upper() in SQL_SERVER_TYPES:
            self.type = MS

        if self.type and type(self.type) == str and self.type.upper() in AZURE_SERVER_TYPES:
            self.type = AZ

        if not self.type and self.inherits_from:
            self.type = self.inherits_from.type
            self.__set_type()


    def __get_default_schema(self, db_type):
        # type: (str) -> str
        """
        Gets default schema name depending on db type
        :param db_type: str of db type
        :return: str of default schema name
        """
        if db_type == MS:
            default_schema = self.dfquery('select schema_name()', internal=True)
            default_schema = default_schema.iloc[0, 0]#.encode('utf-8')
            if not default_schema: # fall back for missing default schema in some databases
                default_schema = 'dbo'
            return default_schema
        elif db_type == PG:
            return 'public'

    """
    Public and private helper functions for connecting, disconnecting
    """

    def __inherit_credentials(self):
        if not self.user:
            self.user = self.inherits_from.user
            # if inheriting user get pass too else assume differnet pass too
        if not self.password:
            self.password = self.inherits_from.password
        if not self.LDAP:
            self.LDAP = self.inherits_from.LDAP
        if not self.type:
            self.type = self.inherits_from.type
            self.__set_type()
        if not self.server:
            self.server = self.inherits_from.server
        if not self.database:
            self.database = self.inherits_from.database
        if not self.port:
            self.port = self.inherits_from.port

    def __get_credentials(self):
        # type: (DbConnect) -> None
        """
        Requests any missing credentials needed for db connection
        :return: None
        """
        if self.default_connect:
            self.type = config.get('DEFAULT DATABASE', 'type')
            self.__set_type()
            self.server = config.get('DEFAULT DATABASE', 'server')
            self.database = config.get('DEFAULT DATABASE', 'database')

        if self.inherits_from:
            self.__inherit_credentials()

        # Only prompts user if missing necessary information
        if self.type == AZ:
            # TODO find a better place for this
            self.LDAP = True
        if ( (self.LDAP and not all((self.database, self.server))) or
                (not self.LDAP and (not all((self.user, self.password, self.database, self.server))))):

            print('\nAdditional database connection details required:')

            # Prompts user input for each missing parameter
            if not self.type:
                self.type = input('Database type (MS/PG/AZ)').upper()
                self.__set_type()
            if not self.server:
                self.server = input('Server:')
            if not self.database:
                self.database = input('Database name:')
            if not self.user and not self.LDAP:
                self.user = input('User name ({}):'.format(self.database.lower()))
            if not self.password and not self.LDAP and self.type !=AZ:
                self.password = getpass.getpass('Password ({})'.format(self.database.lower()))

    def __connect_pg(self):
        # type: (DbConnect) -> None
        """
        Creates connection to pg db
        :return: None
        """
        self.params = {
            'dbname': self.database,
            'user': self.user,
            'password': self.password,
            'host': self.server,
            'port': self.port
        }
        self.conn = psycopg2.connect(**self.params)

    def __connect_ms(self):
        # type: (DbConnect) -> None
        """
        Creates connection to sql server db
        :return: None
        """

        if self.LDAP:
            self.params = {
                'database': self.database,
                'host': self.server
            }
        else:
            self.params = {
                'database': self.database,
                'host': self.server,
                'user': self.user,
                'password': self.password
            }

        try:
            self.conn = pymssql.connect(**self.params)
        except Exception as e:
            print(e)
            # Revert to SQL driver and show warning

            # deprecated
            # if self.use_native_driver:
            #     # Native client is required for correct handling of datetime2 types in SQL
            #     if self.connection_count == 0:
            #         print('Warning:\n\tMissing SQL Server Native Client 10.0 \
            #                           datetime2 will not be interpreted correctly\n')
            #
            #     self.conn = pymssql.connect(**self.params)
    def __connect_az(self):
        # type: (DbConnect) -> None
        """
        Creates connection to sql server db
        :return: None
        """

        self.LDAP = True

        self.params = {
            'database': self.database,
            'SERVER': self.server,
            'PORT' : '1433;DATABASE = '+self.database,
            'UID': self.user+'@dot.nyc.gov',
            'AUTHENTICATION' : 'ActiveDirectoryInteractive',
            'driver' :'{ODBC Driver 17 for SQL Server}'

        }
        try:
            self.conn = pyodbc.connect(**self.params)
        except Exception as e:
            print(e)
            # Revert to SQL driver and show warning
            if self.use_native_driver:
                # Native client is required for correct handling of datetime2 types in SQL
                if self.connection_count == 0:
                    print('Warning:\n\tMissing SQL Server Native Client 10.0 \
                                      datetime2 will not be interpreted correctly\n')

                self.conn = pymssql.connect(**self.params)

    def connect(self, quiet=False):
        # type: (DbConnect, bool) -> None
        """
        Connects to database
        Requires all connection parameters to be entered and connection type
        :param quiet: if true, does not output db as str
        :return:
        """
        # Document connection start time and current count
        self.connection_start = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if not quiet and not self.quiet:
            print(self)

        # Connect based on type of database
        if self.type == PG:
            self.__connect_pg()

        if self.type == MS:
            self.__connect_ms()

        if self.type == AZ:
            self.__connect_az()

        # Add successful connection
        self.connection_count += 1

    def disconnect(self, quiet=False):
        # type: (DbConnect, bool) -> None
        """
        Closes connection to db
        :param quiet: boolean to print out connection closing (defaults to false)
        :return:
        """
        try:
            self.conn.close()
            if not quiet and not self.quiet:
                print('Database connection ({typ}) to {db} on {srv} - user: {usr} \nConnection closed {dt}'.format(
                    typ=self.type,
                    db=self.database,
                    srv=self.server,
                    usr=self.user,
                    dt=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ))
        except Exception as e:
            print(e)
            return

    def check_conn(self):
        """
        Checks and reconnects to connection if need be
        """
        if not self.conn or (self.type == PG and self.conn and self.conn.closed == 1):
            self.connect(True)

        elif self.type == MS:
            try:
                self.conn._conn.connected
            except Exception as e:
                print(e)
                self.connect(True)
        else:
            if self.conn.closed:
                self.connect(True)

    """
    Private helper functions for log cleanup 
    """

    def __drop_expired_tables(self, schema):
        # type: (DbConnect, str) -> None
        """
        Deletes tables that have expired, as listed in the temp log
        :param schema: database schema name
        :return:
        """
        self.query("""
            SELECT table_schema, table_name FROM {s}.{l}
            WHERE expires < '{dt}'
            """.format(
            s=schema,
            l=self.log_table,
            dt=datetime.datetime.now().strftime('%Y-%m-%d')
        ), timeme=False, internal=True)

        to_clean = self.__get_most_recent_query_data(internal=True)
        cleaned = 0

        for sch, table in to_clean:
            drop_schema_name = get_query_table_schema_name(sch, self.type)
            drop_table_name = get_query_table_schema_name(table, self.type)
            self.drop_table(drop_schema_name, drop_table_name, strict=False, internal=True)
            cleaned += 1

        if cleaned > 0:
            print('Attempted to remove {} expired temp tables: {}'.format(cleaned, to_clean))

    def __remove_nonexistent_tables_from_logs(self):
        # type: (DbConnect) -> None
        """
        Removes from the log table any table that no longer exists in the database
        :return:
        """
        to_delete = list()

        # For each schema check if log file exists
        for s in self.get_schemas():
            if self.table_exists(self.log_table, schema=s, internal=True):

                # For each table in log file check if exists
                self.query("select table_name from {}.{}".format(s, self.log_table), timeme=False, internal=True)

                if self.__get_most_recent_query_data(internal=True):
                    for t in self.__get_most_recent_query_data(internal=True):
                        query_table_name = get_query_table_schema_name(str(t[0]), self.type)

                        # If table does not exist, add to the list of tables to delete
                        if not self.table_exists(query_table_name, schema=s, internal=True):
                            # Add the original version back to be deleted
                            to_delete.append(str(t[0]))

                # Remove stale table names
                if to_delete:
                    self.query(
                        "delete from {s}.{l} where table_name in ({tn})".format(
                            s=s, l=self.log_table, tn=str(to_delete)[1:-1]
                        ),
                        strict=False, timeme=False, internal=True
                    )

    def __cleanup_subroutine(self):
        # type: (DbConnect) -> None
        """
        Drops any tables that have expired (based on log table) and removes them from the log by:
        1. Calling  __drop_expired_tables to remove expired tables
        2. Calling  __remove_nonexistent_tables_from_logs to remove tables that don't exist anymore from logs
        And cleans up logs for tables that may have been dropped.
        :return:
        """
        if self.type == PG:
            self.query("SELECT schema_name FROM information_schema.schemata", timeme=False, internal=True)
        elif self.type == MS:
            self.query(MS_SCHEMA_FOR_LOG_CLEANUP_QUERY, internal=True)
        elif self.type == AZ:
            # todo: revist this, but for now its read only so should be ok
            return None

        for sch in self.__get_most_recent_query_data(internal=True):
            if self.table_exists(self.log_table, schema=sch[0], internal=True):
                self.__drop_expired_tables(sch[0])

        self.__remove_nonexistent_tables_from_logs()

    def __remove_dropped_tables_from_log(self, tables_dropped):
        # type: (DbConnect) -> None
        """
        Removes tables dropped in already-run Queries from log.
        :return:
        """
        for table_str in tables_dropped:
            server, database, schema, table = parse_table_string(table_str, self.default_schema, self.type)

            # Check if log table exists
            if self.table_exists(self.log_table, schema=schema, internal=True):
                # Delete from log to avoid dropping perm tables with same name
                self.query(
                    """DELETE FROM {s}."{tmp}" WHERE table_schema = '{s}' AND table_name = '{t}'""".format(
                        s=schema, t=table, tmp=self.log_table),
                    timeme=False, internal=True
                )

    def __run_table_logging(self, new_tables, days=7):
        """
        Logs new tables made in the query
        :param new_tables:
        :return:
        """

        for table in new_tables:
            server, database, sch, tbl = parse_table_string(table, self.default_schema, self.type)

            # All archive tables should default to permanent
            if sch != 'archive':
                self.log_temp_table(sch, tbl, self.user, database=database, server=server,
                                    expiration=datetime.datetime.now() + datetime.timedelta(days=days))

    def log_temp_table(self, schema, table, owner, server=None, database=None,
                       expiration=datetime.datetime.now() + datetime.timedelta(days=7)):
        # type: (DbConnect, str, str, str, str, str, datetime) -> None
        """
        Writes tables to temp log to be deleted after expiration date.
        :param schema: database schema name
        :param table: table name
        :param owner: userid for table owner
        :param server: database server path
        :param database: database name
        :param expiration: date after which the table can be deleted (defaults to 7 days)
        :return:
        """

        if table == self.log_table:
            return

        if server:
            ser = server + '.'
        else:
            ser = ''
        if database:
            db = database + '.'
        else:
            db = ''

        # Check if log exists; if not make one
        if not self.table_exists(self.log_table, schema=schema, server=server, database=database):
            if self.type == MS:
                self.query(MS_CREATE_LOG_TABLE_QUERY.format(s=schema, log=self.log_table, serv=ser, db=db),
                           timeme=False, temp=False, internal=True)

            elif self.type == PG:
                self.query(PG_CREATE_LOG_TABLE_QUERY.format(s=schema, log=self.log_table),
                           timeme=False, temp=False, internal=True)

        # Add new table to log
        if self.type == PG:
            self.query(PG_ADD_TABLE_TO_LOG_QUERY.format(
                s=schema,
                log=self.log_table,
                u=owner,
                t=table,
                dt=datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
                ex=expiration
            ), strict=False, timeme=False, internal=True)

        elif self.type == MS:
            self.query(MS_ADD_TABLE_TO_LOG_QUERY.format(
                s=schema,
                log=self.log_table,
                u=owner,
                t=table,
                dt=datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
                ex=expiration,
                ser=ser,
                db=db
            ), strict=False, timeme=False, internal=True)

    """
    User-facing functions
    """

    def check_logs(self, schema=None):
        """
        :param schema: schema to check; defaults to the default_schema
        :return: df
        """
        if not schema:
            schema = self.default_schema

        return self.dfquery("select * from {}.{}".format(schema, self.log_table))

    def check_table_in_log(self, table_name, schema=None):
        """
        :param table_name: name of table to check
        :param schema: schema to check; defaults to the default_schema
        :return: list
        """
        if not schema:
            schema = self.default_schema
        self.query("select * from {s}.{lt} where table_name = '{tn}'".format(
            s=schema, lt=self.log_table, tn=table_name))

        self.check_conn()
        return self.data

    def cleanup_new_tables(self):
        # type: (DbConnect) -> None
        """
        Drops all newly created tables from this DbConnect object
        :return: None
        """
        for tbl in self.tables_created:
            server, database, schema, table = parse_table_string(tbl, self.default_schema, self.type)
            self.drop_table(schema, table)

        print('Dropped %i tables' % len(self.tables_created))

        # Clean out list
        self.tables_created = list()

    def blocking_me(self):
        # type: (DbConnect) -> pd.DataFrame
        """
        Runs dfquery to find which queries or users are blocking the user defined in the connection. Postgres Only.
        :return: Pandas DataFrame of blocking queries
        """
        if self.type == MS:
            print('Aborting...attempting to run a Postgres-only command on a Sql Server DbConnect instance.')

        return self.dfquery(PG_BLOCKING_QUERY % self.user)

    def kill_blocks(self):
        # type: (DbConnect) -> None
        """
        Will kill any queries that are blocking, that the user (defined in the connection) owns. Postgres Only.
        :return: None
        """
        if self.type == MS:
            print('Aborting...attempting to run a Postgres-only command on a Sql Server DbConnect instance.')
            return

        self.query(PG_KILL_BLOCKS_QUERY % self.user, internal=True)

        pids_to_kill = [pid[0] for pid in self.__get_most_recent_query_data(internal=True)]

        if pids_to_kill:

            print('Killing %i connections' % len(pids_to_kill))

            for pid in tqdm(pids_to_kill):
                self.query("""SELECT pg_terminate_backend(%i);""" % pid)

    def my_tables(self, schema='public'):
        # type: (DbConnect, str) -> Optional[pd.DataFrame, None]
        """
        Get a list of tables for which you are the owner (PG only).
        :param schema: Schema to look in (defaults to public)
        :return: Pandas DataFrame of the table list
        """
        if self.type in (MS, AZ):
            print('Aborting...attempting to run a Postgres-only command on a SQL Server/Azure DbConnect instance.')
            return

        return self.dfquery(PG_MY_TABLES_QUERY.format(s=schema, u=self.user))

    def table_exists(self, table, **kwargs):
        # type: (DbConnect, str, **str) -> bool
        """
        Checks if table exists in the database
        :param table: table name
        :param kwargs:
                :schema: schema for check (defaults to default schema)
        :return: bool
        """
        schema = kwargs.get('schema', self.default_schema)
        server = kwargs.get('server', self.server)
        database = kwargs.get('database', self.database)
        internal = kwargs.get('internal', False)

        cleaned_server = get_unique_table_schema_string(server, self.type)
        cleaned_database = get_unique_table_schema_string(database, self.type)
        cleaned_schema = get_unique_table_schema_string(schema, self.type)
        cleaned_table = get_unique_table_schema_string(table, self.type)

        if self.type == PG:
            self.query(PG_TABLE_EXISTS_QUERY.format(s=cleaned_schema, t=cleaned_table), timeme=False, internal=internal)

            if self.__get_most_recent_query_data(internal=internal)[0][0]:
                return True
            else:
                return False

        elif self.type == MS:
            # todo: there is likely a more elegant way to do this
            # this is a catch for when server and db are passed in but dont have values
            # this is happening in some of the logging/clean up functions
            if not cleaned_server:
                cleaned_server = self.server
            if not cleaned_database:
                cleaned_database = self.database

            if cleaned_server == self.server and cleaned_database == get_unique_table_schema_string(self.database,
                                                                                                    self.type):
                self.query(MS_TABLE_EXISTS_QUERY.format(s=cleaned_schema, t=cleaned_table), timeme=False,
                           internal=internal)
                if self.__get_most_recent_query_data(internal=internal):
                    return True
                else:
                    return False
            else:
                # need to connect to other db to check for log
                # todo: there must be a better way to do this, but havent found it yet
                other_dbc = DbConnect(type=self.type, server=cleaned_server, database=cleaned_database,
                                      port=self.port,
                                      user=self.user, password=self.password, ldap=self.LDAP,
                                      default=self.default_connect, use_native_driver=self.use_native_driver)

                return other_dbc.table_exists(self.log_table, schema=cleaned_schema)
        else:
            return False

    def get_schemas(self):
        # type: (DbConnect) -> list
        """
        Gets a list of schemas available in the database
        :return: list of schemas
        """
        if self.type == MS:
            self.query(MS_GET_SCHEMAS_QUERY, timeme=False, internal=True)

        elif self.type == PG:
            self.query(PG_GET_SCHEMAS_QUERY, timeme=False, internal=True)

        return [schema_row[0] for schema_row in self.__get_most_recent_query_data(internal=True)]

    def get_table_columns(self, table, schema=None, full=False):
        if not schema:
            schema = self.default_schema
        if full:
            columns = '*'
        else:
            columns = "column_name, data_type"

        if self.type == PG:
            self.query("""
            SELECT {cols}
            FROM information_schema.columns
            WHERE table_schema = '{s}' 
                AND table_name = '{t}'
            ORDER BY ordinal_position;
            """.format(cols=columns, s=schema, t=table), timeme=False, internal=True)

        if self.type == MS:
            self.query("""
            SELECT {cols}
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema = '{s}' 
                AND table_name = '{t}'
            ORDER BY ORDINAL_POSITION;
            """.format(cols=columns, s=schema, t=table), timeme=False, internal=True)

        return self.__get_most_recent_query_data(internal=True)

    def query(self, query, strict=True, permission=True, temp=True, timeme=True, no_comment=False, comment='',
              lock_table=None, return_df=False, days=7, internal=False, no_print_out=False):
        # type: (str, bool, bool, bool, bool, bool, str, str, bool, int, bool) -> Optional[None, pd.DataFrame]
        """
        Runs Query object from input SQL string and adds query to queries
        :param query: String sql query to be run
        :param strict: If true will run sys.exit on failed query attempts
        :param permission:
        :param temp: if True any new tables will be logged for deletion at a future date
        :param timeme: Will print time of query
        :param no_comment: Will not comment on newly created tables
        :param comment: The itself; if the above is False
        :param lock_table:
        :param return_df: boolean that returns Pandas dataframe of data if true (defaults to false)
        :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
        :param internal: Boolean flag for internal processes
        :return:
        """
        self.check_conn()

        # Warn for unintended custom comment behavior
        if self.type == MS and comment:
            print('Comment functionality does not work with SQL Server databases. '
                  'Any inputted comments will not be recorded.')

        qry = Query(self, query, strict=strict, permission=permission, temp=temp, timeme=timeme,
                    no_comment=no_comment, comment=comment, lock_table=lock_table, internal=internal,
                    no_print_out=no_print_out)

        if not self.allow_temp_tables:
            self.disconnect(True)

        if internal:
            self.internal_queries.append(qry)
            self.internal_data = qry.data

        else:
            self.queries.append(qry)
            self.data = qry.data
            self.tables_created += [nt for nt in qry.new_tables]
            self.tables_dropped += [dt for dt in qry.dropped_tables]
            self.last_query = qry.query_string

            if qry.dropped_tables:
                self.__remove_dropped_tables_from_log(qry.dropped_tables)

            if qry.temp and qry.new_tables:
                self.__run_table_logging(qry.new_tables, days=days)
                self.__remove_nonexistent_tables_from_logs()

        if return_df:
            return qry.dfquery()

    def drop_table(self, schema, table, cascade=False, strict=True, server=None, database=None, internal=False):
        # type: (DbConnect, str, str, bool, bool, str, str, bool) -> None
        """
        Drops table from database and removes from the temp log table
        If a table uses "" or [] because of case, spaces, or periods, they (""/[]) must be inputted explicitly.
        :param schema: schema
        :param table: table name
        :param cascade: boolean if want to cascade
        :param strict: boolean; defaults to True to end on failure. Set to false during cleanup subroutine
        :param server: server path
        :param database: database name
        :param internal: boolean flag for internal processes
        :return:
        """
        if cascade:
            c = 'CASCADE'
        else:
            c = ''
        if server:
            ser = server + '.'
        else:
            ser = ''
        if database:
            db = database + '.'
        else:
            db = ''
        if self.type == PG:
            if '"' not in table:
                self.query('DROP TABLE IF EXISTS {}."{}" {}'.format(schema, table, c),
                           timeme=False, strict=strict, internal=internal)
            else:
                self.query('DROP TABLE IF EXISTS {}.{} {}'.format(schema, table, c),
                           timeme=False, strict=strict, internal=internal)
        elif self.type == MS:
            if self.table_exists(schema=schema, table=table):
                if '[' not in table:
                    self.query('DROP TABLE {}{}{}.[{}] {}'.format(ser, db, schema, table, c),
                               timeme=False, strict=strict, internal=internal)
                else:
                    self.query('DROP TABLE {}{}{}.{} {}'.format(ser, db, schema, table, c),
                               timeme=False, strict=strict, internal=internal)
            else:
                dropped_tables_list = Query.query_drops_table('DROP TABLE {}.{}'.format(schema, table))
                self.__remove_dropped_tables_from_log(dropped_tables_list)

    def rename_column(self, schema, table, old_column, new_column):
        # type: (DbConnect, str, str, str, str) -> None
        """
        Renames the column (old column) to the new column name on the specified table.
        :param schema: schema
        :param table: table name
        :param old_column: old column to be renamed
        :param new_column: new column name
        :return:
        """
        if not schema:
            schema = self.default_schema

        if self.type == PG:
            self.query("alter table {s}.{t} rename column {o} to {n}".format(s=schema, t=table, o=old_column,
                                                                             n=new_column))
        elif self.type == MS:
            self.query("EXEC sp_RENAME '{s}.{t}.{o}', '{n}', 'COLUMN'".format(s=schema, t=table, o=old_column,
                                                                              n=new_column))

    def dfquery(self, query, strict=False, permission=True, temp=True, timeme=False, no_comment=False, comment='',
                lock_table=None, days=7, internal=False):
        """
        Runs Query object from input SQL string and adds query to queries. Outputs as a dataframe.
        For dfquery, timeme and strict are default set to FALSE.
        :param query: String sql query to be run
        :param strict: If true will run sys.exit on failed query attempts
        :param permission:
        :param temp: if True any new tables will be logged for deletion at a future date
        :param timeme: Will print time of query
        :param no_comment: Will not comment on newly created tables
        :param comment: The itself; if the above is False
        :param lock_table:
        :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
        :param internal: boolean flag for internal processes
        :return:
        """
        return self.query(query, timeme=timeme, permission=permission, temp=temp, strict=strict, no_comment=no_comment,
                          comment=comment, lock_table=lock_table, return_df=True, days=days, internal=internal)

    def print_last_query(self):
        """
        Prints latst query run with basic formatting
        :return: None
        """
        Query.print_query(self.last_query)

    """
    IO Functions
    """

    def dataframe_to_table_schema(self, df, table, schema=None, overwrite=False, temp=True, allow_max_varchar=False,
                                  column_type_overrides=None, days=7):

        """
        Translates Pandas DataFrame into empty database table.
        :param df: Pandas DataFrame to be added to database
        :param table: Table name to be used in database
        :param schema: Database schema to use for destination in database (defaults database object's default schema)
        :param overwrite: If table exists in database will overwrite if True (defaults to False)
        :param temp: Optional flag to make table as not-temporary (defaults to True)
        :param allow_max_varchar: Boolean to allow unlimited/max varchar columns; defaults to False
        :param column_type_overrides: Dict of type key=column name, value=column type. Will manually set the
                raw column name as that type in the query, regardless of the pandas/postgres/sql server automatic
                detection.
        :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
        :return: Table schema that was created from DataFrame
        """
        if not schema:
            schema = self.default_schema

        input_schema = list()

        if allow_max_varchar:
            allowed_length = VARCHAR_MAX[self.type]
        else:
            allowed_length = 500

        # Parse df for schema
        for col in df.dtypes.items():
            col_name, col_type = col[0], type_decoder(col[1], varchar_length=allowed_length)

            # check if column is empty - if so force string
            s = df[col_name].value_counts()
            if s.empty:
                col_type = 'varchar ({})'.format(allowed_length)

            # autodetect date and force to text (common error)
            if 'date' in col_name.lower() and col_type in ('int', 'bigint', 'float'):
                col_type = 'varchar(500)'

            if column_type_overrides and col_name in column_type_overrides.keys():
                input_schema.append([clean_column(col_name), column_type_overrides[col_name]])
            else:
                input_schema.append([clean_column(col_name), col_type])

        if overwrite:
            self.drop_table(schema=schema, table=table, cascade=False)

        # Create table in database
        qry = """
                CREATE TABLE {s}.{t} (
                {cols}
                )
        """.format(s=schema, t=table,
                   cols=str(['"' + str(i[0]) + '" ' + i[1] for i in input_schema])[1:-1].replace("'", ""))

        self.query(qry.replace('\n', ' '), timeme=False, temp=temp, days=days)
        return input_schema

    def dataframe_to_table(self, df, table, table_schema=None, schema=None, overwrite=False, temp=True,
                           allow_max_varchar=False, column_type_overrides=None, days=7):
        """
        Adds data from Pandas DataFrame to existing table
        :param df: Pandas DataFrame to be added to database
        :param table: Table name to be used in database
        :param table_schema: schema of dataframe (returned from dataframe_to_table_schema)
        :param schema: Database schema to use for destination in database (defaults to db's default schema)
        :param overwrite: If table exists in database will overwrite if True (defaults to False)
        :param temp: Optional flag to make table temporary (defaults to True)
        :param allow_max_varchar: Boolean to allow unlimited/max varchar columns; defaults to False
        :param column_type_overrides: Dict of type key=column name, value=column type. Will manually set the
                raw column name as that type in the query, regardless of the pandas/postgres/sql server automatic
                detection. **Will not override a custom table_schema, if inputted**
        :param days: if temp=True and table schema needs to be created, the number of days that the temp table will be
                     kept. Defaults to 7.
        :return: None
        """

        if not schema:
            schema = self.default_schema

        if not table_schema:
            table_schema = self.dataframe_to_table_schema(df, table, overwrite=overwrite, schema=schema, temp=temp,
                                                          allow_max_varchar=allow_max_varchar,
                                                          column_type_overrides=column_type_overrides,
                                                          days=days)

        # Insert data
        print('Reading data into Database\n')

        for _, row in tqdm(df.iterrows()):
            # Clean up empty cells and prime for input into db
            row = row.replace({np.nan: None})
            row_values = ",".join([clean_cell(i) for i in row.values])
            row_values = row_values.replace('None', 'NULL')

            self.query(u"""
                INSERT INTO {s}.{t} ({cols})
                VALUES ({d})
            """.format(s=schema, t=table,
                       cols=str(['"' + str(i[0]) + '"' for i in table_schema])[1:-1].replace("'", ''),
                       d=row_values), strict=False, timeme=False)

        df = self.dfquery("SELECT COUNT(*) as cnt FROM {s}.{t}".format(s=schema, t=table), timeme=False)
        print('\n{c} rows added to {s}.{t}\n'.format(c=df.cnt.values[0], s=schema, t=table))

    def csv_to_table(self, input_file=None, overwrite=False, schema=None, table=None, temp=True, sep=',',
                     long_varchar_check=False, column_type_overrides=None, days=7, **kwargs):
        """
        Imports csv file to database. This uses pandas datatypes to generate the table schema.
        :param input_file: File path to csv file; if None, prompts user input
        :param overwrite: If table exists in database, will overwrite; defaults to False
        :param schema: Schema of table; if None, defaults to db's default schema
        :param table: Name for final database table; defaults to filename in path
        :param temp: Boolean for temporary table; defaults to True
        :param sep: Separator for csv file, defaults to comma (,)
        :param long_varchar_check: Boolean to allow unlimited/max varchar columns; defaults to False
        :param column_type_overrides: Dict of type key=column name, value=column type. Will manually set the
        raw column name as that type in the query, regardless of the pandas/postgres/sql server automatic
        detection. **Will not override a custom table_schema, if inputted**
        :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
        :param **kwargs: parameters to pass to pandas for read csv (ex. skiprows=1)
        :return:
        """

        def contains_long_columns(df2):
            for c in list(df2.columns):
                if df2[c].dtype in ('O','object', 'str'):
                    if df2[c].apply(lambda x: len(x) if x else 0).max() > 500:
                        print('Varchar column with length greater than 500 found; allowing max varchar length.')
                        return True

            return False

        if not schema:
            schema = self.default_schema

        if not input_file:
            input_file = file_loc('file')

        if not table:
            table = os.path.basename(input_file).split('.')[0]

        if not overwrite and self.table_exists(schema=schema, table=table):
            print('Must set overwrite=True; table already exists.')
            return

        # Use pandas to get existing data and schema
        # Check for varchar columns > 500 in length
        allow_max = False
        if os.path.getsize(input_file) > 1000000:
            data = pd.read_csv(input_file, iterator=True, chunksize=10 ** 15, sep=sep, **kwargs)
            df = data.get_chunk(1000)

            # Check for long column iteratively
            while df is not None and long_varchar_check:
                if contains_long_columns(df):
                    allow_max = True
                    break

                df = data.get_chunk(1000)
        else:
            df = pd.read_csv(input_file, sep=sep, **kwargs)
            allow_max = long_varchar_check and contains_long_columns(df)

        if 'ogc_fid' in df.columns:
            df = df.drop('ogc_fid', 1)

        # Calls dataframe_to_table_schema fn
        table_schema = self.dataframe_to_table_schema(df, table, overwrite=overwrite, schema=schema, temp=temp,
                                                      allow_max_varchar=allow_max,
                                                      column_type_overrides=column_type_overrides,
                                                      days=days)

        # For larger files use GDAL to import
        if df.shape[0] > 999:
            try:
                temp_file = os.path.dirname(input_file)+'\\'f'_temp_data_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}.csv'


                with pd.read_csv(input_file, chunksize=10 ** 15, sep=sep, **kwargs) as reader:
                    for chunk in reader:
                        chunk.to_csv(temp_file, mode='a', index=False, header=True)


                success = self._bulk_csv_to_table(input_file=temp_file, schema=schema, table=table,
                                                  table_schema=table_schema, days=days)
                os.remove(temp_file)

                if not success:
                    raise AssertionError('Bulk CSV loading failed.'.format(schema, table))

            except SystemExit:
                raise AssertionError(
                    'Bulk CSV loading failed.'.format(schema, table)
                )
            except Exception as e:
                print(e)
                raise AssertionError(
                    'Bulk CSV loading failed.'.format(schema, table)
                )

        else:
            # Calls dataframe_to_table fn
            self.dataframe_to_table(df, table, table_schema=table_schema, overwrite=overwrite, schema=schema,
                                    temp=temp, days=days)

    def _bulk_csv_to_table(self, input_file=None, schema=None, table=None, table_schema=None, print_cmd=False, days=7):
        """
        Shell for bulk_file_to_table. Routed to by csv_to_table when record count is >= 1,000.
        :param input_file: Source CSV filepath
        :param schema: Schema to write to; defaults to db's default schema
        :param table: Destination table name to write data to; defaults to user/date defined
        :param table_schema:
        :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
        :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
        :return:
        """

        return self._bulk_file_to_table(input_file=input_file, schema=schema, table=table,
                                        table_schema=table_schema, print_cmd=print_cmd, excel_header=False, days=days)

    def csv_to_table_pyarrow(self, input_file=None, overwrite=False, schema=None, table=None, temp=True, sep=',',
                     long_varchar_check=False, column_type_overrides=None, days=7, **kwargs):
        """
        Imports csv file to database. This uses pyarrow datatypes to generate the table schema.

        ** This is an alternative route for importing - faster and better type inference, but not as fully featured as pandas **

        :param input_file: File path to csv file; if None, prompts user input
        :param overwrite: If table exists in database, will overwrite; defaults to False
        :param schema: Schema of table; if None, defaults to db's default schema
        :param table: Name for final database table; defaults to filename in path
        :param temp: Boolean for temporary table; defaults to True
        :param sep: Separator for csv file, defaults to comma (,)
        :param long_varchar_check: Boolean to allow unlimited/max varchar columns; defaults to False
        :param column_type_overrides: Dict of type key=column name, value=column type. Will manually set the
        raw column name as that type in the query, regardless of the pandas/postgres/sql server automatic
        detection. **Will not override a custom table_schema, if inputted**
        :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
        :param **kwargs: parameters to pass to pandas for read csv (ex. skiprows=1)
        :return:
        """

        if not schema:
            schema = self.default_schema

        if not input_file:
            input_file = file_loc('file')

        if not table:
            table = os.path.basename(input_file).split('.')[0]

        if not overwrite and self.table_exists(schema=schema, table=table):
            print('Must set overwrite=True; table already exists.')
            return

        # Use pyarrow to get existing data and schema
        data = pyarrowcsv.read_csv(input_file, parse_options=pyarrowcsv.ParseOptions(delimiter=sep),
                                   read_options=pyarrowcsv.ReadOptions(**kwargs))
        if '' in [col.name for col in data.schema]:
            data = data.rename_columns([(lambda x: 'unnamed' if x == '' else x)(col.name) for col in data.schema])

        allow_max = False

        if 'ogc_fid' in [i.name for i in data.schema]:
            data = data.drop(['ogc_fid'])

        table_schema = self.dataframe_to_table_schema_pyarrow(data, table, overwrite=overwrite, schema=schema,
                                                              temp=temp,
                                                              allow_max_varchar=allow_max,
                                                              column_type_overrides=column_type_overrides,
                                                              days=days)
        # Default to bulk importer

        try:
            success = self._bulk_csv_to_table(input_file=input_file, schema=schema, table=table,
                                              table_schema=table_schema, days=days)

            if not success:
                raise AssertionError('Bulk CSV loading failed.'.format(schema, table))

        except Exception as e:
            print(e)
            # fall back to pandas dataframe to table
            # Calls dataframe_to_table fn
            self.dataframe_to_table(data.to_pandas(), table, table_schema=table_schema, overwrite=overwrite,
                                    schema=schema,
                                    temp=temp, days=days)

    def dataframe_to_table_schema_pyarrow(self, data, table, schema=None, overwrite=False, temp=True, allow_max_varchar=False,
                                          column_type_overrides=None, days=7):

        """
        Translates Pandas DataFrame into empty database table.
        :param df: Pandas DataFrame to be added to database
        :param table: Table name to be used in database
        :param schema: Database schema to use for destination in database (defaults database object's default schema)
        :param overwrite: If table exists in database will overwrite if True (defaults to False)
        :param temp: Optional flag to make table as not-temporary (defaults to True)
        :param allow_max_varchar: Boolean to allow unlimited/max varchar columns; defaults to False
        :param column_type_overrides: Dict of type key=column name, value=column type. Will manually set the
                raw column name as that type in the query, regardless of the pandas/postgres/sql server automatic
                detection.
        :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
        :return: Table schema that was created from DataFrame
        """
        if not schema:
            schema = self.default_schema

        input_schema = list()

        if allow_max_varchar:
            allowed_length = VARCHAR_MAX[self.type]
        else:
            allowed_length = 500

        # Parse df for schema
        for col in data.schema:
            col_name, col_type = col.name, type_decoder_pyarrow(col.type, varchar_length=allowed_length)


            if column_type_overrides and col_name in column_type_overrides.keys():
                input_schema.append([clean_column(col_name), column_type_overrides[col_name]])
            else:
                input_schema.append([clean_column(col_name), col_type])

        if overwrite:
            self.drop_table(schema=schema, table=table, cascade=False)

        # Create table in database
        qry = """
                CREATE TABLE {s}.{t} (
                {cols}
                )
        """.format(s=schema, t=table,
                   cols=str(['"' + str(i[0]) + '" ' + i[1] for i in input_schema])[1:-1].replace("'", ""))

        self.query(qry.replace('\n', ' '), timeme=False, temp=temp, days=days)
        return input_schema


    def _bulk_csv_to_table_pyarrow(self, input_file=None, schema=None, table=None, table_schema=None, print_cmd=False, days=7):
        """
        Shell for bulk_file_to_table. Routed to by csv_to_table when record count is >= 1,000.

        ** This is an alternative route for importing - faster and better type inference, but not as fully featured as pandas **

        
        :param input_file: Source CSV filepath
        :param schema: Schema to write to; defaults to db's default schema
        :param table: Destination table name to write data to; defaults to user/date defined
        :param table_schema:
        :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
        :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
        :return:
        """

        return self._bulk_file_to_table(input_file=input_file, schema=schema, table=table,
                                        table_schema=table_schema, print_cmd=print_cmd, excel_header=False, days=days)

    def _bulk_xlsx_to_table(self, input_file=None, schema=None, table=None, table_schema=None, print_cmd=False,
                            header=True, days=7):
        """
        Shell for bulk_file_to_table. Routed to by xls_to_table when record count is >= 1,000.
        :param input_file: Source XLSX filepath
        :param schema: Schema to write to; defaults to db's default schema
        :param table: Destination table name to write data to; defaults to user/date defined
        :param table_schema:
        :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
        :param header: defaults to True; true if the xlsx includes a header/column names
        :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
        :return:
        """
        return self._bulk_file_to_table(input_file=input_file, schema=schema, table=table,
                                        table_schema=table_schema, print_cmd=print_cmd, excel_header=header, days=days)
    @staticmethod
    def __double_cast_for_ints(i, column_names, column_type):
        """
        This will create the cast statements for insert query which is used by _bulk_file_to_table
        :param i: index position
        :param column_names: column name list
        :param column_type: column datatype
        :return: castting string for insert query
        """
        if column_type == 'bigint':
            return 'CAST(CAST("' + column_names[i] + '"as float) as bigint)'
        else:
            return 'CAST("' + column_names[i] + '" as ' + column_type + ')'

    def _bulk_file_to_table(self, input_file=None, schema=None, table=None, table_schema=None, print_cmd=False,
                            excel_header=True, days=7):
        """
        Uses GDAL to import CSV/XLSX files.
        Writes data to a staging table without inferred data types then transforms to final output table using
        data types from Pandas.
        :param input_file: Source CSV/XLS filepath
        :param schema: Schema to write to; defaults to db's default schema
        :param table: Destination table name to write data to; defaults to user/date defined
        :param table_schema: schema of dataframe (returned from dataframe_to_table_schema)
        :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
        :param excel_header: defaults to True; true if the csv/xls includes a header/column names
        :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
        :return:
        """

        print('Bulk loading data...')

        if not schema:
            schema = self.default_schema

        if not table:
            table = os.path.basename(input_file).split('.')[0]

        types = {PG: 'PostgreSQL', MS: 'MSSQLSpatial'}

        # Build staging table using GDAL to import data
        # This is needed because GDAL doesnt parse datatypes (all to varchar)
        if self.type == PG:
            cmd = PG_BULK_FILE_TO_TABLE_CMD.format(t=types[self.type],
                                                   server=self.server,
                                                   port=self.port,
                                                   user=self.user,
                                                   password=self.password,
                                                   db=self.database,
                                                   f=input_file,
                                                   schema=schema,
                                                   tbl=table
                                                   )
        else:
            cmd = MS_BULK_FILE_TO_TABLE_CMD.format(t=types[self.type],
                                                   server=self.server,
                                                   user=self.user,
                                                   password=self.password,
                                                   db=self.database,
                                                   f=input_file,
                                                   schema=schema,
                                                   tbl=table
                                                   )

        cmd_env = os.environ.copy()

        if excel_header:
            cmd_env['OGR_XLSX_HEADERS'] = 'FORCE'

        if print_cmd:
            print_cmd_string([self.password], cmd)

        try:
            ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT,
                                                   env=cmd_env)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print(f"Ogr2ogr Output:\n{e.output}")
            return False

        try:
            if table_schema:
                # Need to get staging field names, GDAL sanitizes differently
                if self.type == MS:
                    sq, p = 'TOP 1', ''
                else:
                    sq, p = '', 'LIMIT 1'

                # Query one row to get columns
                self.query("select {sq} * from {s}.stg_{t} {p}".format(s=schema, t=table, sq=sq, p=p), strict=False,
                           timeme=False)

                column_names = self.queries[-1].data_columns

                # check for null columns
                for c in column_names:
                    self.query('select count(case when "{c}" is not null then 1 else null end) nnulls from {s}.stg_{t}'.
                               format(s=schema, t=table, c=c),
                               strict=False, timeme=False, internal=True)
                    if self.internal_data[0][0] == 0:
                        self.query(
                            'alter table {s}.{t} alter "{c}" type varchar'.format(
                                s=schema, t=table, c=c),
                            strict=False, timeme=False, internal=True)

                # fix datatype issues
                table_schema = self.__sparse_data_types(schema, table, table_schema)


                # Drop ogc_fid
                if "ogc_fid" in column_names and "ogc_fid" not in [col_name for i, (col_name, col_type) in
                                                                   enumerate(table_schema)]:
                    column_names.remove("ogc_fid")

                # Drop ogr_fid
                if "ogr_fid" in column_names and "ogr_fid" not in [col_name for i, (col_name, col_type) in
                                                                   enumerate(table_schema)]:
                    column_names.remove("ogr_fid")

                # Cast all fields to new type to move from stg to final table
                # take staging field name from stg table


                cols = [self.__double_cast_for_ints(i, column_names, col_type) for i, (col_name, col_type) in
                        enumerate(table_schema)]
                # cols = ['CAST("' + column_names[i] + '" as ' + col_type + ')' for i, (col_name, col_type) in
                #         enumerate(table_schema)]
                # cols = [c.encode('utf-8') for c in cols]
                cols = str(cols).replace("'", "")[1:-1]
            else:
                # If not input_schema, use what GDAL created
                # cols = '*'
                _ = self.get_table_columns(f'stg_{table}', schema=schema)
                cols = []
                for c in _:
                    if len(set(c[0]) - {' ', ':', '.'}) != len(set(c[0])):
                        cols.append('"'+c[0]+'"'+' as '+c[0].strip().replace(' ', '_').replace('.', '_').replace(':', '_').replace('\n', '_'))
                    else:
                        cols.append(c[0])
                cols = str(cols).replace("'", "")[1:-1]



            if self.table_exists(schema=schema, table=table):
                # Move into final table from stg
                qry = """
                INSERT INTO {s}.{t}
                SELECT
                {cols}
                FROM {s}.stg_{t}
                """.format(
                    s=schema,
                    t=table,
                    cols=cols
                )

                self.query(qry, timeme=False, days=days)
            else:
                # Move into final table from stg
                qry = """
                SELECT {cols}
                INTO {s}.{t}
                FROM {s}.stg_{t}
                """.format(
                    s=schema,
                    t=table,
                    cols=cols
                )

                self.query(qry, timeme=False, days=days)

            # Drop stg table
            self.drop_table(schema=schema, table='stg_{}'.format(table))

            # Log rows added to table
            df = self.dfquery("SELECT COUNT(*) as cnt FROM {s}.{t}".format(s=schema, t=table), timeme=False)

            print("""
            {c} rows added to {s}.{t}. 
            The table name may include stg_. This will not change the end result. 
            """.format(c=df.cnt.values[0], s=schema, t=table))

        except SystemExit:
            # Drop stg table
            self.drop_table(schema=schema, table='stg_{}'.format(table))
            return False

        except Exception as e:
            print(e)
            # Drop stg table
            self.drop_table(schema=schema, table='stg_{}'.format(table))
            return False

        return True

    def __sparse_data_types(self, schema, table, table_schema):
        columns = self.get_table_columns(table, schema=schema)
        # get numeric types - 'bigint' 'double precision'
        numeric_cols = [_[0] for _ in columns if _[1] in ('bigint', 'double precision')]

        # need to unclean columns since stg_ hasnt been cleaned yet
        stg_columns = {}
        for c in self.get_table_columns('stg_'+table, schema=schema):
            # clean: unclean
            stg_columns[clean_column(c[0])] = c[0]
        # update in memory schema - used in casting into final table
        table_schema2 = []
        updated = []
        # try to cast field if fails convert to string
        for column in numeric_cols:
            try:
                self.query(f'select cast("{stg_columns[column]}" as float) from {schema}.stg_{table}',
                           strict=False, timeme=False, internal=True, no_print_out=True)
            except Exception as e:
                pass
            # check if no data then query failed and there is a non-numberic data type
            if not self.internal_data:
                if self.type == 'PG':
                    self.query(f"""
                        alter table {schema}.{table} alter "{stg_columns[column]}" type varchar 
                        using "{stg_columns[column]}"::varchar
                    """)
                else:
                    self.query(f"""
                        alter table {schema}.{table} alter "{stg_columns[column]}" type varchar 
                    """)
                updated.append(column)

        for c in table_schema:
            if not c[0] == updated:
                table_schema2.append(c)
            else:
                table_schema2.append([column, 'varchar (500)'])
        return table_schema2


    def xls_to_table(self, input_file=None, sheet_name=0, overwrite=False, schema=None, table=None, temp=True,
                     column_type_overrides=None, days=7, **kwargs):
        """
        Imports xls/x file to database. This uses pandas datatypes to generate the table schema.
        :param input_file: File path to csv file; if None, prompts user input
        :param sheet_name : str, int or None, defaults to the first sheet
        :param overwrite: If table exists in database, will overwrite; defaults to False
        :param schema: Schema of table; if None, defaults to db's default schema
        :param table: Name for final database table; defaults to filename in path
        :param temp: Boolean for temporary table; defaults to True
        :param column_type_overrides: Dict of type key=column name, value=column type. Will manually set the
        raw column name as that type in the query, regardless of the pandas/postgres/sql server automatic
        detection.
        :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
        :return:
        """

        # Add default schema
        if not schema:
            schema = self.default_schema

        if not overwrite and self.table_exists(schema=schema, table=table):
            print('Must set overwrite=True; table already exists.')
            return

        # Get the input file
        if not input_file:
            input_file = file_loc('file')

        # Get the table name if not provided
        if not table and sheet_name:
            table = os.path.basename(input_file).split('.')[0] + "_" + sheet_name
        elif not table and not sheet_name:
            table = os.path.basename(input_file).split('.')[0]

        # Ogr doesn't check overwriting; will append unless stopped.
        if self.table_exists(table=table, schema=schema) and not overwrite:
            print('{}.{} already exists. Use overwrite=True to replace.'.format(schema, table))
            return

        ef = pd.ExcelFile(input_file)
        multi_sheet = len(ef.sheet_names) > 1

        if multi_sheet:
            print("""
                Only the specified sheet (or 1st if not specified will be imported
            """)
        # if not too big use pandas


        try:
            df = pd.read_excel(input_file, sheet_name=sheet_name, **kwargs)

            # Match previous styles
            cols = []
            for c in df.columns:
                cols.append(c.strip().replace(' ', '_').replace('.', '_').replace(':', '_').replace('\n', '_'))
            df.columns= cols

            if 'ogc_fid' in df.columns:
                df = df.drop('ogc_fid', 1)

            if df.shape[0] > 100:
                try:
                    table_schema = self.dataframe_to_table_schema(df, table,
                                                                  schema=schema,
                                                                  overwrite=overwrite,
                                                                  temp=temp,
                                                                  column_type_overrides=column_type_overrides,
                                                                  days=days)
                    temp_file = os.path.dirname(
                        input_file) + '\\'f'_temp_data_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
                    df.to_csv(temp_file, index=False, header=True)

                    success = self._bulk_csv_to_table(input_file=temp_file, schema=schema, table=table,
                                                      table_schema=table_schema, days=days)
                    try:
                        os.remove(temp_file)
                    except Exception as e:
                        print(f'Could not remove temp file\n{e}')

                    if not success:
                        raise AssertionError('Bulk file loading failed.'.format(schema, table))

                except SystemExit:
                    raise AssertionError('Bulk file loading failed.'.format(schema, table))
                except Exception as e:
                    print(e)
                    raise AssertionError('Bulk file loading failed.'.format(schema, table))
            else:
                self.dataframe_to_table(df, table, schema=schema, overwrite=overwrite, temp=temp,
                                        column_type_overrides=column_type_overrides, days=days)
        except Exception as e:
            print(e)


    def query_to_csv(self, query, output_file=None, strict=True, open_file=False, sep=',', quote_strings=True,
                     quiet=False, overwrite=False):
        """
        Exports query results to a csv file.
        :param query: SQL query as string type
        :param output_file: File path for resulting csv file
        :param strict: If true will run sys.exit on failed query attempts
        :param open_file: If true will auto open the output csv file when done
        :param sep: Delimiter for csv; defaults to comma (,) -- see below
        :param quote_strings: Defaults to True; will always quote; if False, will quote as needed
        :param quiet: if true, does not output query metrics or output location
        :param overwrite: if True, will overwrite csv output file if already exists
        Separator must be one of the following:
                ',' (comma)
                ';' (semicolon)
                '\t' (tab)
                ' ' (space)
        :return:
        """
        # If no output specified, defaults to a generic data csv name with the date
        if not output_file:
            output_file = os.path.join(os.getcwd(),
                                       'data_{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d%H%M')))

        self.check_conn()

        # If just a filename, add to cwd. If dir specified but does not exist, create dir
        if not os.path.exists(os.path.dirname(output_file)) and os.path.dirname(output_file):
            os.makedirs(os.path.dirname(output_file))
        elif not os.path.exists(os.path.dirname(output_file)) and not os.path.dirname(output_file):
            output_file = os.path.join(os.getcwd(), output_file)

        if os.path.isfile(output_file) and not overwrite:
            raise RuntimeError(
                """ File already exists and overwrite was not allowed. Set overwrite=True or delete the file. """)
        elif os.path.isfile(output_file) and overwrite:
            os.remove(output_file)

        if not quiet:
            print('Writing to %s' % output_file)

        if sep == ',':
            OGR_SEPARATOR = 'COMMA'
        elif sep == ';':
            OGR_SEPARATOR = 'SEMICOLON'
        elif sep == '\t':
            OGR_SEPARATOR = 'TAB'
        elif sep == ' ':
            OGR_SEPARATOR = 'SPACE'
        else:
            raise RuntimeError(""" 
                Separator must be one of the following:

                ',' (comma) 
                ';' (semicolon)
                '\t' (tab)
                ' ' (space)
            """)

        if quote_strings:
            OGR_QUOTE_STRINGS = 'IF_AMBIGUOUS'
        else:
            OGR_QUOTE_STRINGS = 'IF_NEEDED'

        # escape double quote columns
        query = query.replace('"', '\\"')

        if self.type == PG:
            cmd = WRITE_CSV_CMD_PG.format(
                output_file=output_file,
                host=self.server,
                username=self.user,
                db=self.database,
                password=self.password,
                pg_sql_select=query,
                separator=OGR_SEPARATOR,
                string_quote=OGR_QUOTE_STRINGS
            )

        elif self.type == MS:
            cmd = WRITE_CSV_CMD_MS.format(
                output_file=output_file,
                host=self.server,
                username=self.user,
                db=self.database,
                password=self.password,
                ms_sql_select=query,
                separator=OGR_SEPARATOR,
                string_quote=OGR_QUOTE_STRINGS
            )

        try:
            ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)

            if strict:
                raise RuntimeError('Query_to_csv failed.')
            else:
                return False

        if open_file:
            os.startfile(output_file)

    def query_to_map(self, query, value_column, geom_column=None, id_column=None):
        """
        Function to output simple Plotly Choropleth Map
        If no geom_column is specified, and the results contain columns named precinct, nta, ntacode, boro, borough, or
        borocode, it will automatically link to precinct, NTA, or borough, respectively.
        :param query: a query to map
        :param value_column: the column with the value that is being mapped
        :param geom_column: the column with the geom that is being mapped;
            if not filled in, columns must contain one of the above
        :param id_column: the column that contains the id of the geography being mapped (ex. precinct, nta, boro);
            if not filled in, columns must contain one of the above
        :return:
        """

        def df_to_geojson(df):
            geojson = {'type': 'FeatureCollection', 'features': []}

            for _, row in df.iterrows():
                feature = {'type': 'Feature', 'properties': {}, 'geometry': json.loads(row['geojson_geometry']),
                           'id': row['id']}
                geojson['features'].append(feature)

            return geojson

        if self.type != PG:
            print('This is only available for Postgres right now.')
            return

        if (geom_column and not id_column) or (not geom_column and id_column):
            raise RuntimeError('Please input both geom and id columns or use the built-in precinct, nta, or borough.')

        if geom_column and id_column:
            query = "select *, {} as id, st_asgeojson(st_transform({}, 4326))  geojson_geometry from ({}) q".format(
                id_column, geom_column, query)

        query_df = self.dfquery(query)
        query_df[value_column] = query_df[value_column].astype('float')

        if not geom_column and not id_column:
            if 'precinct' in [c.lower() for c in list(query_df.columns)]:
                id_column = 'precinct'
                map_type = 'PRECINCT'

            if 'nta' in [c.lower() for c in list(query_df.columns)]:
                id_column = 'nta'
                map_type = 'NTA'

            if 'ntacode' in [c.lower() for c in list(query_df.columns)]:
                id_column = 'ntacode'
                map_type = 'NTA'

            if 'borough' in [c.lower() for c in list(query_df.columns)]:
                id_column = 'borough'
                map_type = 'BOROUGH'

            if 'boro' in [c.lower() for c in list(query_df.columns)]:
                id_column = 'boro'
                map_type = 'BOROUGH'

            if 'borocode' in [c.lower() for c in list(query_df.columns)]:
                id_column = 'borocode'
                map_type = 'BOROUGH'

        if id_column and not geom_column:
            if self.database.lower() == 'ris':
                geom_map = {
                    "PRECINCT": ("precinct", "districts_police_precincts"),
                    "NTA": ("ntacode", "districts_neighborhood_tabulation_areas"),
                }

            if self.database.lower() == 'crashdata':
                geom_map = {
                    "PRECINCT": ("precinct", "districts_police_precincts"),
                    "NTA": ("ntacode", "districts_nta"),
                    "BOROUGH": ("borocode", "districts_boroughs"),
                }

            g_df = self.dfquery("""
    
            select {} as id, st_asgeojson(st_transform(geom, 4326)) geojson_geometry
            from {}
            
            """.format(geom_map.get(map_type)[0], geom_map.get(map_type)[1]))
            geo_j = df_to_geojson(g_df)
        else:
            geo_j = df_to_geojson(query_df)

        fig = px.choropleth(query_df,
                            geojson=geo_j,
                            locations=id_column,
                            color=value_column,
                            color_continuous_scale="Viridis")
        fig.update_geos(fitbounds="locations", visible=False, projection_type='mercator')
        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        fig.show()
        return

    def query_to_shp(self, query, path=None, shp_name=None, cmd=None, gdal_data_loc=GDAL_DATA_LOC,
                     print_cmd=False, srid=2263):
        """
        Exports query results to a shp file.
        :param query: SQL query as string type
        :param path: folder path for output shp
        :param shp_name: filename for shape (should end in .shp)
        :param cmd: GDAL command to overwrite default
        :param gdal_data_loc: Path to gdal data, if not stored in system env correctly
        :param print_cmd: boolean to print ogr command (without password)
        :param srid: SRID to manually set output to; defaults to 2263
        :return:
        """
        # Temporarily sets temp flag to True
        original_temp_flag = self.allow_temp_tables
        self.allow_temp_tables = True

        # Makes a temp table name
        tmp_table_name = "tmp_query_to_shp_{}_{}".format(self.user,
                                                         str(datetime.datetime.now())[:16].replace('-', '_').replace(
                                                             ' ', '_').replace(':', ''))

        # Create temp table to get column types
        try:
            # Drop the temp table
            if self.type == PG:
                self.query("drop table {}".format(tmp_table_name), internal=True, strict=False)
            elif self.type == MS:
                self.query("drop table #{}".format(tmp_table_name), internal=True, strict=False)
        except Exception as e:
            print(e)
            pass

        if self.type == PG:
            self.query(u"""    
            create temp table {} as     
            select * 
            from ({}) q 
            limit 10
            """.format(tmp_table_name, query), internal=True)
        elif self.type == MS:
            self.query(u"""        
            select top 10 * 
            into #{}
            from ({}) q 
            """.format(tmp_table_name, query), internal=True)

        # Extract column names, including datetime/timestamp types, from results
        if self.type == PG:
            col_df = self.dfquery("""
            SELECT *
            FROM
            INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{}'
            """.format(tmp_table_name), internal=True)

            cols = ['\\"' + c + '\\"' for c in list(col_df['column_name'])]
            dt_col_names = ['\\"' + c + '\\"' for c in list(
                col_df[col_df['data_type'].str.contains('datetime') | col_df['data_type'].str.contains('timestamp')][
                    'column_name'])]

        elif self.type == MS:
            col_df = self.dfquery("""
            SELECT
                [column] = c.name,
                [type] = t.name, 
                c.max_length, 
                c.precision, 
                c.scale, 
                c.is_nullable
            FROM
                tempdb.sys.columns AS c
            LEFT JOIN
                tempdb.sys.types AS t
            ON
                c.system_type_id = t.system_type_id
                AND
                t.system_type_id = t.user_type_id
            WHERE
                [object_id] = OBJECT_ID(N'tempdb.dbo.#{}');
            """.format(tmp_table_name), internal=True)

            cols = ['[' + c + ']' for c in list(col_df['column'])]
            dt_col_names = ['[' + c + ']' for c in list(
                col_df[col_df['type'].str.contains('datetime') | col_df['type'].str.contains('timestamp')]['column'])]

        # Make string of columns to be returned by select statement
        return_cols = ' , '.join([c for c in cols if c not in dt_col_names])

        # If there are datetime/timestamp columns:
        if len(dt_col_names) > 0:
            if self.type == PG:
                print_cols = str([str(c[2:-2]) for c in dt_col_names])

            if self.type == MS:
                print_cols = str([str(c[1:-1]) for c in dt_col_names])

            print("""
            The following columns are of type datetime/timestamp: \n
            {}
            
            Shapefiles don't support datetime/timestamps with both the date and time. Each column will be split up
            into colname_dt (of type date) and colname_tm (of type **string/varchar**). 
            """.format(print_cols))

            # Add the date and time (casted as a string) to the output
            for col_name in dt_col_names:
                if self.type == PG:
                    shortened_col = col_name[2:-2][:7]
                    return_cols += ' , cast(\\"{col}\\" as date) \\"{short_col}_dt\\", ' \
                                   'cast(cast(\\"{col}\\" as time) as varchar) \\"{short_col}_tm\\" '.format(
                                    col=col_name[2:-2], short_col=shortened_col)
                elif self.type == MS:
                    shortened_col = col_name[1:-1][:7]
                    return_cols += " , cast([{col}] as date) [{short_col}_dt], cast(cast([{col}] as time) as varchar)" \
                                   " [{short_col}_tm] ".format(
                                    col=col_name[1:-1], short_col=shortened_col)

        # Wrap the original query and select the non-datetime/timestamp columns and the parsed out dates/times
        new_query = u"select {} from ({}) q ".format(return_cols, query)
        Query.query_to_shp(self, new_query, path=path, shp_name=shp_name, cmd=cmd, gdal_data_loc=gdal_data_loc,
                           print_cmd=print_cmd, srid=srid)

        # Drop the temp table
        if self.type == PG:
            self.query("drop table {}".format(tmp_table_name), internal=True)
        elif self.type == MS:
            self.query("drop table #{}".format(tmp_table_name), internal=True)

        # Reset the temp flag
        self.last_query = new_query
        self.allow_temp_tables = original_temp_flag

    def table_to_shp(self, table, schema=None, strict=True, path=None, shp_name=None, cmd=None,
                     gdal_data_loc=GDAL_DATA_LOC, print_cmd=False, srid=2263):
        """
        Exports table to a shp file. Generates query to query_to_shp.
        :param table: Database table name as string type
        :param schema: Database table's schema (defults to db default schema)
        :param strict: If True, will run sys.exit on failed query attempts; defaults to True
        :param path: folder path for output shp
        :param shp_name: filename for shape (should end in .shp)
        :param cmd: GDAL command to overwrite default
        :param gdal_data_loc: Path to gdal data, if not stored in system env correctly
        :param print_cmd: Boolean flag to print the OGR command
        :param srid: SRID to manually set output to; defaults to 2263
        :return:
        """
        if not schema:
            schema = self.default_schema

        path, shp_name = parse_shp_path(path, shp_name)

        return self.query_to_shp("select * from {}.{}".format(schema, table),
                                 path=path, shp_name=shp_name, cmd=cmd, gdal_data_loc=gdal_data_loc,
                                 print_cmd=print_cmd, srid=srid)

    def table_to_csv(self, table, schema=None, strict=True, output_file=None, open_file=False, sep=',',
                     quote_strings=True):
        """
        Writes table to csv
        :param table: table name
        :param schema: schema for table (defaults to default schema)
        :param strict: If True, will run sys.exit on failed query attempts; defaults to True
        :param output_file: String for csv output file location (defaults to current directory)
        :param open_file: Boolean flag to auto open output file; defaults to False
        :param sep: Separator for csv (defaults to ',')
        :param quote_strings: Boolean flag for adding quote strings to output (defaults to true, QUOTE_ALL)
        :return: None
        """
        # If no output_file, outputs to current directory with table as filename
        if not output_file:
            output_file = os.path.join(os.getcwd(), table + '.csv')

        if schema:
            if self.type == PG:
                if '"' not in table:
                    schema_table = '{}."{}"'.format(schema, table)
                else:
                    schema_table = '{}.{}'.format(schema, table)
            if self.type in (MS, AZ):
                if '[' not in table:
                    schema_table = '{}.[{}]'.format(schema, table)
                else:
                    schema_table = '{}.{}'.format(schema, table)
        else:
            schema_table = '{}'.format(table)


        query = """
        select *
        from {}
        """.format(schema_table)

        self.check_conn()

        self.query_to_csv(query, output_file=output_file, open_file=open_file, quote_strings=quote_strings, sep=sep,
                          strict=strict)

        if not self.allow_temp_tables:
            self.disconnect(True)

    def shp_to_table(self, path=None, table=None, schema=None, shp_name=None, cmd=None,
                     srid=2263, port=None, gdal_data_loc=GDAL_DATA_LOC, precision=False, private=False, temp=True,
                     shp_encoding=None, print_cmd=False, days=7, zip=False):
        """
        Imports shape file to database. This uses GDAL to generate the table.
        :param path: File path of the shapefile
        :param table: Table name to use in the database
        :param schema: Schema to use in the database (defaults to db's default schema)
        :param shp_name: Shapefile name (ends in .shp)
        :param cmd: Optional ogr2ogr command to overwrite default
        :param srid:  SRID to use (defaults to 2263)
        :param port:
        :param gdal_data_loc: File path fo the GDAL data (defaults to C:\\Program Files (x86)\\GDAL\\gdal-data)
        :param precision:  Sets precision flag in ogr (defaults to -lco precision=NO)
        :param private: Flag for permissions in database (Defaults to False - will only grant select to public)
        :param temp: If True any new tables will be logged for deletion at a future date; defaults to True
        :param shp_encoding: Defaults to None; if not None, sets the PG client encoding while uploading the shpfile.
        Options inlude LATIN1, UTF-8.
        :param print_cmd: Defaults to False; if True prints the cmd
        :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
        :param zip: Flag to use if importing from a sipped file (defaults to False)
        :return:
        """
        if not schema:
            schema = self.default_schema

        if not port:
            port = self.port

        path, shp = parse_shp_path(path, shp_name)
        if not shp_name:
            shp_name = shp

        if not all([path, shp_name]):
            filename = file_loc('file', 'Missing file info - Opening search dialog...')
            shp_name = os.path.basename(filename)
            path = os.path.dirname(filename)

        if not table:
            table = shp_name.replace('.shp', '').lower()

        table = table.lower()

        shp = Shapefile(dbo=self, path=path, table=table, schema=schema, shp_name=shp_name,
                        cmd=cmd, srid=srid, gdal_data_loc=gdal_data_loc, port=port)

        shp.read_shp(precision, private, shp_encoding, print_cmd, zip=zip)

        self.tables_created.append(f"{schema}.{table}")

        if temp:
            self.__run_table_logging([schema + "." + table], days=days)

    def feature_class_to_table(self, path, table, schema=None, shp_name=None, gdal_data_loc=GDAL_DATA_LOC,
                               srid=2263, private=False, temp=True, fc_encoding=None, print_cmd=False,
                               days=7, skip_failures=''):
        """
        Imports shape file feature class to database. This uses GDAL to generate the table.
        :param path: Filepath to the geodatabase
        :param table: Table name to use in the database
        :param schema: Schema to use in the database
        :param shp_name:  FeatureClass name
        :param gdal_data_loc: Filepath/location of GDAL on computer
        :param srid: SRID to use (defaults to 2263)
        :param private: If True any new tables will override defaut grant select permissions; defaults to False
        :param temp: If True any new tables will be logged for deletion at a future date; defaults to True
        :param fc_encoding: Defaults to None; if not None, sets the PG client encoding while uploading the feature class
        Options inlude LATIN1, UTF-8.
        :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
        :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
        :return:
        """
        if not schema:
            schema = self.default_schema

        if not all([path, shp_name]):
            filename = file_loc('file', 'Missing file info - Opening search dialog...')
            shp_name = os.path.basename(filename)
            path = os.path.dirname(filename)

        if not table:
            table = shp_name.replace('.shp', '').lower()

        table = table.lower()

        shp = Shapefile(dbo=self, path=path, table=table, schema=schema, query=None,
                        shp_name=shp_name, cmd=None, srid=srid, gdal_data_loc=gdal_data_loc,
                        skip_failures=skip_failures)

        shp.read_feature_class(private, fc_encoding=fc_encoding, print_cmd=print_cmd)

        if temp:
            self.__run_table_logging([schema + "." + table], days=days)


    def query_to_gpkg(self, query, path=None, gpkg_name=None, cmd=None, gdal_data_loc=GDAL_DATA_LOC,
                     print_cmd=False, srid=2263):
        """
        Exports query results to a geopackage (.gpkg) file.
        :param query: SQL query as string type
        :param path: folder path for output gpkg
        :param gpkg_name: filename for shape (should end in .gpkg)
        :param cmd: GDAL command to overwrite default
        :param gdal_data_loc: Path to gdal data, if not stored in system env correctly
        :param print_cmd: boolean to print ogr command (without password)
        :param srid: SRID to manually set output to; defaults to 2263
        :return:
        """
        # Temporarily sets temp flag to True
        original_temp_flag = self.allow_temp_tables
        self.allow_temp_tables = True

        # Makes a temp table name
        tmp_table_name = "tmp_query_to_gpkg_{}_{}".format(self.user,
                                                         str(datetime.datetime.now())[:16].replace('-', '_').replace(
                                                             ' ', '_').replace(':', ''))

        # Create temp table to get column types
        try:
            # Drop the temp table
            if self.type == PG:
                self.query("drop table {}".format(tmp_table_name), internal=True, strict=False)
            elif self.type == MS:
                self.query("drop table #{}".format(tmp_table_name), internal=True, strict=False)
        except Exception as e:
            print(e)
            pass

        if self.type == PG:
            self.query(u"""    
            create temp table {} as     
            select * 
            from ({}) q 
            limit 10
            """.format(tmp_table_name, query), internal=True)
        elif self.type == MS:
            self.query(u"""        
            select top 10 * 
            into #{}
            from ({}) q 
            """.format(tmp_table_name, query), internal=True)

        # Extract column names, including datetime/timestamp types, from results
        if self.type == PG:
            col_df = self.dfquery("""
            SELECT *
            FROM
            INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{}'
            """.format(tmp_table_name), internal=True)

            cols = ['\\"' + c + '\\"' for c in list(col_df['column_name'])]
            dt_col_names = ['\\"' + c + '\\"' for c in list(
                col_df[col_df['data_type'].str.contains('datetime') | col_df['data_type'].str.contains('timestamp')][
                    'column_name'])]

        elif self.type == MS:
            col_df = self.dfquery("""
            SELECT
                [column] = c.name,
                [type] = t.name, 
                c.max_length, 
                c.precision, 
                c.scale, 
                c.is_nullable
            FROM
                tempdb.sys.columns AS c
            LEFT JOIN
                tempdb.sys.types AS t
            ON
                c.system_type_id = t.system_type_id
                AND
                t.system_type_id = t.user_type_id
            WHERE
                [object_id] = OBJECT_ID(N'tempdb.dbo.#{}');
            """.format(tmp_table_name), internal=True)

            cols = ['[' + c + ']' for c in list(col_df['column'])]
            dt_col_names = ['[' + c + ']' for c in list(
                col_df[col_df['type'].str.contains('datetime') | col_df['type'].str.contains('timestamp')]['column'])]

        # Make string of columns to be returned by select statement
        return_cols = ' , '.join([c for c in cols if c not in dt_col_names])

        # If there are datetime/timestamp columns:
        if len(dt_col_names) > 0:
            if self.type == PG:
                print_cols = str([str(c[2:-2]) for c in dt_col_names])

            if self.type == MS:
                print_cols = str([str(c[1:-1]) for c in dt_col_names])

            # print("""
            # The following columns are of type datetime/timestamp: \n
            # {}
            
            # Geopackages don't support datetime/timestamps with both the date and time. Each column will be split up
            # into colname_dt (of type date) and colname_tm (of type **string/varchar**). 
            # """.format(print_cols))

            # Add the date and time (casted as a string) to the output
            for col_name in dt_col_names:
                if self.type == PG:
                    shortened_col = col_name[2:-2][:7]
                    return_cols += ' , cast(\\"{col}\\" as date) \\"{short_col}_dt\\", ' \
                                   'cast(cast(\\"{col}\\" as time) as varchar) \\"{short_col}_tm\\" '.format(
                                    col=col_name[2:-2], short_col=shortened_col)
                elif self.type == MS:
                    shortened_col = col_name[1:-1][:7]
                    return_cols += " , cast([{col}] as date) [{short_col}_dt], cast(cast([{col}] as time) as varchar)" \
                                   " [{short_col}_tm] ".format(
                                    col=col_name[1:-1], short_col=shortened_col)

        # Wrap the original query and select the non-datetime/timestamp columns and the parsed out dates/times
        new_query = u"select {} from ({}) q ".format(return_cols, query)
        Query.query_to_gpkg(self, new_query, path=path, gpkg_name=gpkg_name, cmd=cmd, gdal_data_loc=gdal_data_loc,
                           print_cmd=print_cmd, srid=srid)

        # Drop the temp table
        if self.type == PG:
            self.query("drop table {}".format(tmp_table_name), internal=True)
        elif self.type == MS:
            self.query("drop table #{}".format(tmp_table_name), internal=True)

        # Reset the temp flag
        self.last_query = new_query
        self.allow_temp_tables = original_temp_flag

    def table_to_gpkg(self, table, schema=None, strict=True, path=None, gpkg_name=None, cmd=None,
                     gdal_data_loc=GDAL_DATA_LOC, print_cmd=False, srid=2263):
        """
        Exports table to a geopackage file. Generates query to query_to_gpkg.
        :param table: Database table name as string type
        :param schema: Database table's schema (defults to db default schema)
        :param strict: If True, will run sys.exit on failed query attempts; defaults to True
        :param path: folder path for output geopackage
        :param geopackage_name: filename for geopackage (should end in .gpkg)
        :param cmd: GDAL command to overwrite default
        :param gdal_data_loc: Path to gdal data, if not stored in system env correctly
        :param print_cmd: Boolean flag to print the OGR command
        :param srid: SRID to manually set output to; defaults to 2263
        :return:
        """
        if not schema:
            schema = self.default_schema

        # check the file extension
        assert gpkg_name[-5:] == '.gpkg', "The input file should end with .gpkg . Please check your input."

        path, gpkg_name = parse_gpkg_path(path, gpkg_name)

        return self.query_to_gpkg(f"select * from {schema}.{table}",
                                 path=path, gpkg_name=gpkg_name, cmd=cmd, gdal_data_loc=gdal_data_loc,
                                 print_cmd=print_cmd, srid=srid)
    
    def shp_to_gpkg(self, path=None, shp_name=None, gpkg_name=None, gdal_data_loc=GDAL_DATA_LOC,
                     print_cmd=False, srid=2263):
        
        """
        Converts a Shapefile to Geopackage
        :param path: folder path for output geopackage
        :param gpkg_name: filename for geopackage (should end in .gpkg)
        :param shp_name: filename for shape file (should end in .shp)
        :param gdal_data_loc: Path to gdal data, if not stored in system env correctly
        :param print_cmd: Boolean flag to print the OGR command
        :param srid: SRID to manually set output to; defaults to 2263
        """
        
        assert gpkg_name[-5:] == '.gpkg', "The input file should end with .gpkg . Please check your input."
        assert shp_name[-4:] == '.shp', "The input file should end with .shp . Please check your input."

        path, shp = parse_shp_path(path, shp_name)
        if not shp_name:
            shp_name = shp

        if not all([path, shp_name]):
            filename = file_loc('file', 'Missing file info - Opening search dialog...')
            shp_name = os.path.basename(filename)
            path = os.path.dirname(filename)

        gpkg_output = Geopackage(dbo=self, path=path, shp_name=shp_name, cmd= WRITE_GPKG_CMD_SHP)

        
        return
    
    def gpkg_to_table(self, path=None, table=None, schema=None, gpkg_name=None, cmd=None,
                     srid=2263, port=None, gdal_data_loc=GDAL_DATA_LOC, precision=False, private=False, temp=True,
                     gpkg_encoding=None, print_cmd=False, days=7, zip=False):
        """
        Imports shape file to database. This uses GDAL to generate the table.
        :param path: File path of the shapefile
        :param table: Table name to use in the database
        :param schema: Schema to use in the database (defaults to db's default schema)
        :param gpkg_name: Geopackage name (ends in .gpkg)
        :param cmd: Optional ogr2ogr command to overwrite default
        :param srid:  SRID to use (defaults to 2263)
        :param port:
        :param gdal_data_loc: File path fo the GDAL data (defaults to C:\\Program Files (x86)\\GDAL\\gdal-data)
        :param precision:  Sets precision flag in ogr (defaults to -lco precision=NO)
        :param private: Flag for permissions in database (Defaults to False - will only grant select to public)
        :param temp: If True any new tables will be logged for deletion at a future date; defaults to True
        :param gpkg_encoding: Defaults to None; if not None, sets the PG client encoding while uploading the gpkgfile.
        Options inlude LATIN1, UTF-8.
        :param print_cmd: Defaults to False; if True prints the cmd
        :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
        :param zip: Flag to use if importing from a sipped file (defaults to False)
        :return:
        """

        assert gpkg_name[-5:] == '.gpkg', "The input file should end with .gpkg . Please check your input."

        if not schema:
            schema = self.default_schema

        if not port:
            port = self.port

        path, gpkg = parse_gpkg_path(path, gpkg_name)
        if not gpkg_name:
            gpkg_name = gpkg

        if not all([path, gpkg_name]):
            filename = file_loc('file', 'Missing file info - Opening search dialog...')
            gpkg_name = os.path.basename(filename)
            path = os.path.dirname(filename)

        if not table:
            table = gpkg_name.replace('.gpkg', '').lower()

        table = table.lower()

        gpkg = Geopackage(dbo=self, path=path, table=table, schema=schema, gpkg_name=gpkg_name,
                        cmd=cmd, srid=srid, gdal_data_loc=gdal_data_loc, port=port)

        gpkg.read_gpkg(precision, private, gpkg_encoding, print_cmd, zip=zip)

        self.tables_created.append(f"{schema}.{table}")

        if temp:
            self.__run_table_logging([schema + "." + table], days=days)
  