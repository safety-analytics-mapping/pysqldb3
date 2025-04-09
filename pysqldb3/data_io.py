import getpass
import os
import subprocess
import shlex
import pysqldb3

from .cmds import *
from .util import *

# TODO: standardize this into db to db (table, query, temp table) DRY up code and simplify

# PG to SQL ##########################################################################################################
def pg_to_sql(pg, ms, org_table, LDAP=False, spatial=True, org_schema=None, dest_schema=None, dest_table=None,
              print_cmd=False, temp=True):
    """
    Migrates tables from Postgres to SQL Server, generates spatial tables in MS if spatial in PG.

    :param pg: DbConnect instance connecting to PostgreSQL source database
    :param ms: DbConnect instance connecting to SQL Server destination database
    :param org_table: table name of table to migrate
    :param LDAP: Flag for using LDAP credentials (defaults to False)
    :param spatial: Flag for spatial table (defaults to True)
    :param org_schema: PostgreSQL schema for origin table (defaults to orig db's default schema)
    :param dest_schema: SQL Server schema for destination table (defaults to dest db's default schema)
    :param dest_table: Table name of final migrated table in SQL Server database
    :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
    :param temp: Flag for temporary table (defaults to True)
    :return:
    """
    if not org_schema:
        org_schema = pg.default_schema

    if not dest_schema:
        dest_schema = ms.default_schema

    if not dest_table:
        dest_table = org_table

    if spatial:
        spatial = ' -a_srs EPSG:2263 '
        nlt_spatial = ' '
    else:
        spatial = ' '
        nlt_spatial = '-nlt NONE'

    if LDAP:
        cmd = PG_TO_SQL_LDAP_CMD.format(
            ms_pass='',
            ms_user='',
            pg_pass=pg.password,
            pg_user=pg.user,
            ms_server=ms.server,
            ms_db=ms.database,
            pg_host=pg.server,
            pg_port=pg.port,
            pg_database=pg.database,
            pg_schema=org_schema,
            pg_table=org_table,
            ms_schema=dest_schema,
            spatial=spatial,
            dest_name=dest_table,
            nlt_spatial=nlt_spatial,
            gdal_data=GDAL_DATA_LOC
        )
    else:
        cmd = PG_TO_SQL_CMD.format(
            ms_pass=ms.password,
            ms_user=ms.user,
            pg_pass=pg.password,
            pg_user=pg.user,
            ms_server=ms.server,
            ms_db=ms.database,
            pg_host=pg.server,
            pg_port=pg.port,
            pg_database=pg.database,
            pg_schema=org_schema,
            pg_table=org_table,
            ms_schema=dest_schema,
            spatial=spatial,
            dest_name=dest_table,
            nlt_spatial=nlt_spatial,
            gdal_data=GDAL_DATA_LOC
        )

    if print_cmd:
        print(print_cmd_string([ms.password, pg.password], cmd))

    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([ms.password, pg.password], cmd), returncode=1)

    # tables created always has (server, db, schema, table), in pg server and db are not listed
    ms.tables_created.append((ms.server,ms.database, dest_schema, dest_table))

    if temp:
        ms.log_temp_table(dest_schema, dest_table, ms.user)

def pg_to_sql_qry(pg, ms, query, LDAP=False, spatial=True, dest_schema=None, dest_table=None,
              print_cmd=False, temp=True):
    """
    Migrates query from Postgres to SQL Server, generates spatial tables in MS if spatial in PG.
    :param pg: DbConnect instance connecting to PostgreSQL source database
    :param ms: DbConnect instance connecting to SQL Server destination database
    :param query: query in PG
    :param LDAP: Flag for using LDAP credentials (defaults to False)
    :param spatial: Flag for spatial table (defaults to True)
    :param dest_schema: SQL Server schema for destination table (defaults to dest db's default schema)
    :param dest_table: Table name of final migrated table in SQL Server database
    :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
    :param temp: Flag for temporary table (defaults to True)
    :return:
    """
    if not dest_schema:
        dest_schema = ms.default_schema

    if not dest_table:
        dest_table = '_{u}_{d}'.format(u=ms.user, d=datetime.datetime.now().strftime('%Y%m%d%H%M'))

    if spatial:
        spatial = ' -a_srs EPSG:2263 '
        nlt_spatial = ' '
    else:
        spatial = ' '
        nlt_spatial = '-nlt NONE'


    # apply regex to the query to filter out any dashed comments in the query
    # comments are defined by at least 2 dashes followed by a line break or the end of the query
    # comments with /* */ do not need to be filtered out from the query
    query = re.sub('(-){2,}.*(\n|$)', ' ', query)

    if LDAP:
        cmd = PG_TO_SQL_QRY_CMD.format(
            ms_pass='',
            ms_user='',
            pg_pass=pg.password,
            pg_user=pg.user,
            ms_server=ms.server,
            ms_db=ms.database,
            pg_host=pg.server,
            pg_port=pg.port,
            pg_database=pg.database,
            sql_select = query,
            ms_schema=dest_schema,
            spatial=spatial,
            dest_name=dest_table,
            nlt_spatial=nlt_spatial,
            gdal_data=GDAL_DATA_LOC
        )
    else:
        cmd = PG_TO_SQL_QRY_CMD.format(
            ms_pass=ms.password,
            ms_user=ms.user,
            pg_pass=pg.password,
            pg_user=pg.user,
            ms_server=ms.server,
            ms_db=ms.database,
            pg_host=pg.server,
            pg_port=pg.port,
            pg_database=pg.database,
            sql_select = query,
            ms_schema=dest_schema,
            spatial=spatial,
            dest_name=dest_table,
            nlt_spatial=nlt_spatial,
            gdal_data=GDAL_DATA_LOC
        )

    if print_cmd:
        print(print_cmd_string([ms.password, pg.password], cmd))

    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([ms.password, pg.password], cmd), returncode=1)

    ms.tables_created.append((ms.server, ms.database, dest_schema, dest_table))

    if temp:
        ms.log_temp_table(dest_schema, dest_table, ms.user)

def pg_to_sql_qry_temp_tbl(pg, ms, query, dest_table=None, print_cmd=False):
    """
    Migrates query from Postgres to SQL Server temp table.
    :param pg: DbConnect instance connecting to PostgreSQL source database
    :param ms: DbConnect instance connecting to SQL Server destination database
    :param query: query in PG
    :param dest_table: Table name of final migrated table in SQL Server database
    :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
    :return:
    """

    if not dest_table:
        dest_table = '_{u}_{d}'.format(u=ms.user, d=datetime.datetime.now().strftime('%Y%m%d%H%M'))

    # apply regex to the query to filter out any dashed comments in the query
    # comments are defined by at least 2 dashes followed by a line break or the end of the query
    # comments with /* */ do not need to be filtered out from the query
    query = re.sub('(-){2,}.*(\n|$)', ' ', query)

    # account for "table name"
    if '"' in query:
        query = query.replace('"', r'\"')

    # write data to local csv
    temp_csv = r'C:\Users\{}\Documents\temp_csv_{}.csv'.format(getpass.getuser(), datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
    cmd = PG_TO_CSV_CMD.format(
        gdal_data=GDAL_DATA_LOC,
        output_csv=temp_csv,
        from_pg_host=pg.server,
        from_pg_port=pg.port,
        from_pg_database=pg.database,
        from_pg_user=pg.user,
        from_pg_pass=pg.password,
        sql_select=query
    )
    if print_cmd:
        print(print_cmd_string([pg.password, pg.password], cmd))
    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([ms.password, pg.password], cmd), returncode=1)

    # import data to temp table
    ms.csv_to_table(input_file=temp_csv, table=f'{dest_table}', temp_table=True)

    _df = pd.read_csv(temp_csv)
    if 'WKT' in _df.columns:
        # add geom column
        ms.query(f"alter table [##{dest_table}] add [geom] [geometry];", internal=True, timeme=False)
        # update from wkt
        ms.query(f"update [##{dest_table}] set geom=geometry::STGeomFromText(wkt, 2263);", internal=True, timeme=False)
        # drop wkt col
        ms.query(f"alter table [##{dest_table}] drop column wkt", internal=True, timeme=False)

    # clean up csv
    os.remove(temp_csv)


def pg_to_sql_temp_tbl(pg, ms, table,  org_schema=None, dest_table=None, print_cmd=False):
    """
    Migrates table from Postgres to SQL Server temp table.
    :param pg: DbConnect instance connecting to PostgreSQL source database
    :param ms: DbConnect instance connecting to SQL Server destination database
    :param table: PG table to migrate
    :param org_schema: PostgreSQL schema for origin table (defaults to orig db's default schema)
    :param dest_table: Table name of final migrated table in SQL Server database
    :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
    :return:
    """

    if not dest_table:
        dest_table = table
    if org_schema:
        sch = f"{org_schema}."
    else:
        sch = ''
    query = f'select * from {sch}"{table}"'
    pg_to_sql_qry_temp_tbl(pg, ms, query, dest_table=dest_table, print_cmd=print_cmd)


# SQL to PG ##########################################################################################################
def sql_to_pg_qry(ms, pg, query, LDAP=False, spatial=True, dest_schema=None, print_cmd=False, temp=True,
                  dest_table=None, pg_encoding='UTF8', permission = True):
    """
    Migrates the result of a query from SQL Server database to PostgreSQL database, and generates spatial tables in
    PG if spatial in MS.

    :param ms: DbConnect instance connecting to SQL Server destination database
    :param pg: DbConnect instance connecting to PostgreSQL source database
    :param query: query in SQL
    :param LDAP: Flag for using LDAP credentials (defaults to False)
    :param spatial: Flag for spatial table (defaults to True)
    :param dest_schema: PostgreSQL schema for destination table (defaults to db's default schema)
    :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
    :param temp: flag, defaults to true, for temporary tables
    :param dest_table: destination table name
    :param pg_encoding: encoding to use for PG client (defaults to UTF-8)
    :param permission: set permission to Public on destination table
    :return:
    """
    if not dest_schema:
        dest_schema = pg.default_schema

    if not dest_table:
        dest_table = '_{u}_{d}'.format(u=pg.user, d=datetime.datetime.now().strftime('%Y%m%d%H%M'))

    if spatial:
        spatial = 'MSSQLSpatial'
        nlt_spatial = ' '
    else:
        spatial = 'MSSQL'
        nlt_spatial = '-nlt NONE'

    # apply regex to the query to filter out any dashed comments in the query
    # comments are defined by at least 2 dashes followed by a line break or the end of the query
    # comments with /* */ do not need to be filtered out from the query
    query = re.sub('(-){2,}.*(\n|$)', ' ', query)

    if LDAP:
        cmd = SQL_TO_PG_LDAP_QRY_CMD.format(
            ms_pass='',
            ms_user='',
            pg_pass=pg.password,
            pg_user=pg.user,
            ms_server=ms.server,
            ms_db=ms.database,
            pg_host=pg.server,
            pg_port=pg.port,
            ms_database=ms.database,
            pg_database=pg.database,
            pg_schema=dest_schema,
            sql_select=query,
            spatial=spatial,
            table_name=dest_table,
            nlt_spatial=nlt_spatial,
            gdal_data=GDAL_DATA_LOC
        )
    else:
        cmd = SQL_TO_PG_QRY_CMD.format(
            ms_pass=ms.password,
            ms_user=ms.user,
            pg_pass=pg.password,
            pg_user=pg.user,
            ms_server=ms.server,
            ms_db=ms.database,
            pg_host=pg.server,
            pg_port=pg.port,
            ms_database=ms.database,
            pg_database=pg.database,
            pg_schema=dest_schema,
            sql_select=query,
            spatial=spatial,
            table_name=dest_table,
            nlt_spatial=nlt_spatial,
            gdal_data=GDAL_DATA_LOC
        )

    if print_cmd:
        print(print_cmd_string([ms.password, pg.password], cmd))

    cmd_env = os.environ.copy()
    cmd_env['PGCLIENTENCODING'] = pg_encoding

    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT,
                                               env=cmd_env)
        if permission == True:
            pg.query(f"GRANT SELECT ON {dest_schema}.{dest_table} TO PUBLIC;", internal = True) 
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([ms.password, pg.password], cmd), returncode=1)

    clean_geom_column(pg, dest_table, dest_schema)

    # tables created always has (server, db, schema, table), in pg server and db are not listed
    pg.tables_created.append(('', '', dest_schema, dest_table))

    if temp:
        pg.log_temp_table(dest_schema, dest_table, pg.user)

def sql_to_pg(ms, pg, org_table, LDAP=False, spatial=True, org_schema=None, dest_schema=None, print_cmd=False,
              dest_table=None, temp=True, gdal_data_loc=GDAL_DATA_LOC, pg_encoding='UTF8', permission = True):
    """
    Migrates tables from SQL Server to PostgreSQL, generates spatial tables in PG if spatial in MS.

    :param ms: DbConnect instance connecting to SQL Server destination database
    :param pg: DbConnect instance connecting to PostgreSQL source database
    :param org_table: table name of table to migrate
    :param LDAP: Flag for using LDAP credentials (defaults to False)
    :param spatial: Flag for spatial table (defaults to True)
    :param org_schema: SQL Server schema for origin table (defaults to default schema function result)
    :param dest_schema: PostgreSQL schema for destination table (defaults to default schema function result)
    :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
    :param dest_table: Table name of final migrated table in PostgreSQL database
    :param temp: flag, defaults to true, for temporary tables
    :param gdal_data_loc: location of GDAL data
    :param pg_encoding: encoding to use for PG client (defaults to UTF-8)
    :param permission: set permission to Public on destination table
    :return:
    """
    if not org_schema:
        org_schema = ms.default_schema

    if not dest_schema:
        dest_schema = pg.default_schema

    if not dest_table:
        dest_table = org_table

    if spatial:
        spatial = 'MSSQLSpatial'
        nlt_spatial = ' '
    else:
        spatial = 'MSSQL'
        nlt_spatial = '-nlt NONE'

    if LDAP:
        cmd = SQL_TO_PG_LDAP_CMD.format(
            gdal_data=gdal_data_loc,
            ms_pass='',
            ms_user='',
            pg_pass=pg.password,
            pg_user=pg.user,
            ms_server=ms.server,
            ms_db=ms.database,
            pg_host=pg.server,
            pg_port=pg.port,
            ms_database=ms.database,
            pg_database=pg.database,
            pg_schema=dest_schema,
            pg_table=dest_table,
            ms_table=org_table,
            ms_schema=org_schema,
            spatial=spatial,
            to_pg_name=dest_table,
            nlt_spatial=nlt_spatial
        )
    else:
        cmd = SQL_TO_PG_CMD.format(
            gdal_data=gdal_data_loc,
            ms_pass=ms.password,
            ms_user=ms.user,
            pg_pass=pg.password,
            pg_user=pg.user,
            ms_server=ms.server,
            ms_db=ms.database,
            pg_host=pg.server,
            pg_port=pg.port,
            ms_database=ms.database,
            pg_database=pg.database,
            pg_schema=dest_schema,
            pg_table=dest_table,
            ms_table=org_table,
            ms_schema=org_schema,
            spatial=spatial,
            to_pg_name=dest_table,
            nlt_spatial=nlt_spatial
        )

    if print_cmd:
        print(print_cmd_string([ms.password, pg.password], cmd))

    cmd_env = os.environ.copy()
    cmd_env['PGCLIENTENCODING'] = pg_encoding

    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT,
                                               env=cmd_env)
        if permission == True:
            pg.query(f"GRANT SELECT ON {dest_schema}.{dest_table} TO PUBLIC;", internal = True) 
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([ms.password, pg.password], cmd), returncode=1)

    clean_geom_column(pg, dest_table, dest_schema)

    # tables created always has (server, db, schema, table), in pg server and db are not listed
    pg.tables_created.append(('', '', dest_schema, dest_table))

    if temp:
        pg.log_temp_table(dest_schema, dest_table, pg.user)

def sql_to_pg_qry_temp_tbl(ms, pg, query, dest_table=None, LDAP_from=False, print_cmd=False):
    """
        Migrates query from SQL Server to Postgres temp table.
        :param ms: DbConnect instance connecting to SQL Server source database
        :param pg: DbConnect instance connecting to PostgreSQL destination database
        :param query: query in SQL
        :param dest_table: Table name of final migrated table in PG database
        :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
        :return:
        """

    if not dest_table:
        dest_table = '_{u}_{d}'.format(u=pg.user, d=datetime.datetime.now().strftime('%Y%m%d%H%M'))

    # apply regex to the query to filter out any dashed comments in the query
    # comments are defined by at least 2 dashes followed by a line break or the end of the query
    # comments with /* */ do not need to be filtered out from the query
    query = re.sub('(-){2,}.*(\n|$)', ' ', query)

    # write data to local csv
    temp_csv = r'C:\Users\{}\Documents\temp_csv_{}.csv'.format(
        getpass.getuser(), datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
    if LDAP_from:
        from_user = ''
        from_password = ''
    else:
        from_user = ms.user
        from_password = ms.password

    cmd = SQL_TO_CSV_CMD.format(
        gdal_data=GDAL_DATA_LOC,
        output_csv=temp_csv,
        from_server=ms.server,
        from_database=ms.database,
        from_user=from_user,
        from_pass=from_password,
        sql_select=query
    )

    if print_cmd:
        print(print_cmd_string([ms.password, ms.password], cmd))
    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([ms.password, ms.password], cmd), returncode=1)

    # import data to temp table
    pg.csv_to_table(input_file=temp_csv, table=dest_table, temp_table=True)

    _df = pd.read_csv(temp_csv)
    if 'WKT' in _df.columns:
        if not _df.WKT.isnull().all():
            # add geom column
            pg.query(f'alter table "{dest_table}" add geom geometry;')
            # update from wkt
            pg.query(f'update "{dest_table}" set geom=st_setsrid(st_geomfromtext(wkt), 2263);')
        # drop wkt col
        pg.query(f'alter table "{dest_table}" drop column wkt')

    # clean up csv
    os.remove(temp_csv)


def sql_to_pg_temp_tbl(ms, pg, table, dest_table=None, org_schema=None, LDAP_from=False, print_cmd=False):
    """
        Migrates table from SQL Server to Postgres temp table.
        :param ms: DbConnect instance connecting to SQL Server source database
        :param pg: DbConnect instance connecting to PostgreSQL destination database
        :param table: SQL table to migrate
        :param org_schema: SQL Server schema for origin table (defaults to orig db's default schema)
        :param dest_table: Table name of final migrated table in PG database
        :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
        :return:
        """

    if not dest_table:
        dest_table = table
    if org_schema:
        sch = f"{org_schema}."
    else:
        sch = ''
    query = f"select * from {sch}[{table}]"

    sql_to_pg_qry_temp_tbl(ms, pg, query, dest_table=dest_table, LDAP_from=LDAP_from, print_cmd=print_cmd)

# SQL to SQL ##########################################################################################################
def sql_to_sql_qry(from_sql, to_sql, qry, LDAP_from=False, LDAP_to=False, spatial=True, org_schema=None, dest_schema=None,
                   print_cmd=False, dest_table=None, temp=True, gdal_data_loc=GDAL_DATA_LOC, pg_encoding='UTF8', permission = False):
    """
    Migrates tables from one SQL Server database to another SQL Server database.

    :param from_sql: DbConnect instance connecting to SQL Server Origin database
    :param to_sql: DbConnect instance connecting to SQL Server Destination database
    :param qry: Query String in SQL
    :param LDAP_from: Flag for using LDAP credentials (defaults to False) for source
    :param LDAP_to: Flag for using LDAP credentials (defaults to False) for destination
    :param spatial: Flag for spatial table (defaults to True)
    :param org_schema: SQL Server schema for origin table (defaults to default schema function result)
    :param dest_schema: SQL Server schema for destination table (defaults to default schema function result)
    :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
    :param dest_table: Table name of final migrated table in destination SQL Server database
    :param temp: flag, defaults to true, for temporary tables
    :param gdal_data_loc: location of GDAL data
    :param pg_encoding: encoding to use for PG client (defaults to UTF-8)
    :param permission: set permission to Public on destination table (defaults to False)
    :return:
    """

    if not org_schema:
        org_schema = from_sql.default_schema

    if not dest_schema:
        dest_schema = to_sql.default_schema

    if not dest_table:
        dest_table = '_{u}_{d}'.format(u=to_sql.user, d=datetime.datetime.now().strftime('%Y%m%d%H%M'))

    if spatial:
        spatial = 'MSSQLSpatial'
        nlt_spatial = ' '
    else:
        spatial = 'MSSQL'
        nlt_spatial = '-nlt NONE'

    if LDAP_from:
        from_user = ''
        from_password = ''
    else:
        from_user = from_sql.user
        from_password = from_sql.password
    if LDAP_to:
        to_user = ''
        to_password = ''
    else:
        to_user = to_sql.user
        to_password = to_sql.password
        
    cmd = SQL_TO_SQL_CMD.format(
        gdal_data=gdal_data_loc,
        from_server=from_sql.server,
        from_database=from_sql.database,
        from_user=from_user,
        from_pass=from_password,
        to_server=to_sql.server,
        to_database=to_sql.database,
        to_user=to_user,
        to_pass=to_password,
        to_schema=dest_schema,
        to_table=dest_table,
        qry=qry,

        spatial=spatial,
        nlt_spatial=nlt_spatial
    )

    if print_cmd:
        print(print_cmd_string([from_sql.password, to_sql.password], cmd))

    cmd_env = os.environ.copy()
    cmd_env['PGCLIENTENCODING'] = pg_encoding

    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT,
                                               env=cmd_env)
        if permission == True:
            to_sql.query(f"GRANT SELECT ON {dest_schema}.{dest_table} TO PUBLIC;", internal = True)
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([from_sql.password, to_sql.password], cmd),
                                            returncode=1)

    clean_geom_column(to_sql, dest_table, dest_schema)

    to_sql.tables_created.append((to_sql.server, to_sql.database, dest_schema, dest_table))

    if temp:
        to_sql.log_temp_table(dest_schema, dest_table, to_sql.user)


def sql_to_sql(from_sql, to_sql, org_table, LDAP_from=False, LDAP_to=False, spatial=True, org_schema=None, dest_schema=None,
               print_cmd=False, dest_table=None, temp=True, gdal_data_loc=GDAL_DATA_LOC, pg_encoding='UTF8', permission = False):
    """
    Migrates tables from one SQL Server database to another SQL Server database.

    :param from_sql: DbConnect instance connecting to SQL Server Origin database
    :param to_sql: DbConnect instance connecting to SQL Server Destination database
    :param org_table: Table name in source database to be migrated
    :param LDAP_from: Flag for using LDAP credentials (defaults to False) for source
    :param LDAP_to: Flag for using LDAP credentials (defaults to False) for destination
    :param spatial: Flag for spatial table (defaults to True)
    :param org_schema: SQL Server schema for origin table (defaults to default schema function result)
    :param dest_schema: SQL Server schema for destination table (defaults to default schema function result)
    :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
    :param dest_table: Table name of final migrated table in destination SQL Server database
    :param temp: flag, defaults to true, for temporary tables
    :param gdal_data_loc: location of GDAL data
    :param pg_encoding: encoding to use for PG client (defaults to UTF-8)
    :param permission: set permission to Public on destination table (defaults to False)
    :return:
    """

    if not dest_table:
        dest_table = org_table

    sql_to_sql_qry(from_sql, to_sql,
           f'select * from [{org_schema}].[{org_table}]', LDAP_from, LDAP_to, spatial, org_schema, dest_schema, print_cmd, dest_table, temp,
           gdal_data_loc, pg_encoding, permission)

    cols = from_sql.get_table_columns(org_table, schema=org_schema)
    cols = {c[0]:c[1] for c in cols}
    cols_to = to_sql.get_table_columns(dest_table, schema=dest_schema)
    cols_to = {c[0]: c[1] for c in cols_to}

    # enforce column data types in dest match the source
    for c in cols.keys():
        if '-' in c:
            # GDAL sanitizes `-` out of column names this will put them back so the start/end tables match
            to_sql.query(f"""EXEC sp_rename '{dest_schema}.{dest_table}.{c.replace('-', '_')}', '{c}', 'COLUMN';""", internal = True)
            cols_to = to_sql.get_table_columns(dest_table, schema=dest_schema)
            cols_to = {_[0]: _[1] for _ in cols_to}
        if cols[c] != cols_to[c]:
            if 'varchar' in cols[c] and 'nvarchar' in cols_to[c]:
                # allow upgrade to nvarchar
                pass
            else:
                to_sql.query(f"ALTER TABLE [{dest_schema}].[{dest_table}] ALTER COLUMN [{c}] {cols[c]}",
                         timeme=False, internal=True, strict=False)

    # tables created always has (server, db, schema, table), in pg server and db are not listed
    to_sql.tables_created.append((to_sql.server, to_sql.database, dest_schema, dest_table))


def sql_to_sql_qry_temp_tbl(from_sql, to_sql, query, dest_table=None, LDAP_from=False, print_cmd=False):
    """
           Migrates query from SQL Server to SQL Server temp table.
           :param from_sql: DbConnect instance connecting to SQL Server source database
           :param to_sql: DbConnect instance connecting to SQL Server destination database
           :param query: query in SQL Server
           :param dest_table: Table name of final migrated table in SQL Server database
           :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
           :return:
           """

    if not dest_table:
        dest_table = '_{u}_{d}'.format(u=to_sql.user, d=datetime.datetime.now().strftime('%Y%m%d%H%M'))

    # apply regex to the query to filter out any dashed comments in the query
    # comments are defined by at least 2 dashes followed by a line break or the end of the query
    # comments with /* */ do not need to be filtered out from the query
    query = re.sub('(-){2,}.*(\n|$)', ' ', query)

    # write data to local csv
    temp_csv = r'C:\Users\{}\Documents\temp_csv_{}.csv'.format(getpass.getuser(), datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
    if LDAP_from:
        from_user = ''
        from_password = ''
    else:
        from_user = from_sql.user
        from_password = from_sql.password
    cmd = SQL_TO_CSV_CMD.format(
        gdal_data=GDAL_DATA_LOC,
        output_csv=temp_csv,
        from_server=from_sql.server,
        from_database=from_sql.database,
        from_user=from_user,
        from_pass=from_password,
        sql_select=query
    )

    if print_cmd:
        print(print_cmd_string([from_sql.password, from_sql.password], cmd))
    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([from_sql.password, from_sql.password], cmd), returncode=1)

    # import data to temp table
    to_sql.csv_to_table(input_file=temp_csv, table=dest_table, temp_table=True)

    _df = pd.read_csv(temp_csv)
    if 'WKT' in _df.columns:
        if not _df.WKT.isnull().all():
            # add geom column
            to_sql.query(f"alter table [##{dest_table}] add [geom] [geometry];")
            # update from wkt
            to_sql.query(f"update [##{dest_table}] set geom=geometry::STGeomFromText(wkt, 2263);")
        # drop wkt col
        to_sql.query(f"alter table  [##{dest_table}] drop column wkt")

    # clean up csv
    os.remove(temp_csv)


def sql_to_sql_temp_tbl(from_sql, to_sql, table, dest_table=None, org_schema=None, LDAP_from=False, print_cmd=False):
    """
            Migrates table from SQL Server to SQL Server temp table.
            :param from_sql: DbConnect instance connecting to SQL Server source database
            :param to_sql: DbConnect instance connecting to SQL Server destination database
            :param table: SQL table to migrate
            :param org_schema: SQL Server schema for origin table (defaults to orig db's default schema)
            :param dest_table: Table name of final migrated table in PG database
            :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
            :return:
            """

    if not dest_table:
        dest_table = table
    if org_schema:
        sch = f"{org_schema}."
    else:
        sch = ''
    query = f"select * from {sch}{table}"

    sql_to_sql_qry_temp_tbl(from_sql, to_sql, query,dest_table=dest_table, LDAP_from=LDAP_from, print_cmd=print_cmd)


# PG to PG ##########################################################################################################
def pg_to_pg(from_pg, to_pg, org_table, org_schema=None, dest_schema=None, print_cmd=False, dest_table=None,
             spatial=True, temp=True, permission = True):
    """
    Migrates tables from one PostgreSQL database to another PostgreSQL.
    :param from_pg: Source database DbConnect object
    :param to_pg: Destination database DbConnect object
    :param org_table: Source table name
    :param org_schema: PostgreSQL schema for origin table (defaults to default schema)
    :param dest_schema: PostgreSQL schema for destination table (defaults to default schema)
    :param dest_table: New name for destination table if None will keep original
    :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
    :param spatial: Flag for spatial table (defaults to True)
    :param temp: temporary table, defaults to true
    :param permission: set permission to Public on destination table
    :return:
    """
    if not org_schema:
        org_schema = from_pg.default_schema

    if not dest_schema:
        dest_schema = to_pg.default_schema

    if not dest_table:
        dest_table = org_table

    if spatial:
        nlt_spatial = ' '
    if not spatial:
        nlt_spatial = '-nlt NONE'

    cmd = PG_TO_PG_CMD.format(
        from_pg_host=from_pg.server,
        from_pg_port=from_pg.port,
        from_pg_database=from_pg.database,
        from_pg_user=from_pg.user,
        from_pg_pass=from_pg.password,
        to_pg_host=to_pg.server,
        to_pg_port=to_pg.port,
        to_pg_database=to_pg.database,
        to_pg_user=to_pg.user,
        to_pg_pass=to_pg.password,
        from_pg_schema=org_schema,
        to_pg_schema=dest_schema,
        from_pg_table=org_table,
        to_pg_name=dest_table,
        nlt_spatial=nlt_spatial,
        gdal_data=GDAL_DATA_LOC
    )

    if print_cmd:
        print(print_cmd_string([from_pg.password, to_pg.password], cmd))

    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
        
        if permission == True:
            to_pg.query(f"GRANT SELECT ON {dest_schema}.{dest_table} TO PUBLIC;", internal = True)
        
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([from_pg.password, to_pg.password], cmd), returncode=1)

    clean_geom_column(to_pg, dest_table, dest_schema)

    # tables created always has (server, db, schema, table), in pg server and db are not listed
    to_pg.tables_created.append(('', '', dest_schema, dest_table))

    if temp:
        to_pg.log_temp_table(dest_schema, dest_table, to_pg.user)


def pg_to_pg_qry(from_pg, to_pg, query, dest_schema=None, print_cmd=False, dest_table=None,
             spatial=True, temp=True, permission = True):
    """
    Migrates query results  from one PostgreSQL database to another PostgreSQL.
    :param from_pg: Source database DbConnect object
    :param to_pg: Destination database DbConnect object
    :param query: query in SQL
    :param dest_schema: PostgreSQL schema for destination table (defaults to default schema)
    :param dest_table: New name for destination table if None will keep original
    :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
    :param spatial: Flag for spatial table (defaults to True)
    :param temp: temporary table, defaults to true
    :param permission: set permission to Public on destination table
    :return:
    """


    if not dest_schema:
        dest_schema = to_pg.default_schema

    if not dest_table:
        dest_table = '_{u}_{d}'.format(u=to_pg.user, d=datetime.datetime.now().strftime('%Y%m%d%H%M'))

    if spatial:
        nlt_spatial = ' '

    if not spatial:
        nlt_spatial = '-nlt NONE'

    # apply regex to the query to filter out any dashed comments in the query
    # comments are defined by at least 2 dashes followed by a line break or the end of the query
    # comments with /* */ do not need to be filtered out from the query
    query = re.sub('(-){2,}.*(\n|$)', ' ', query)

    cmd = PG_TO_PG_QRY_CMD.format(
        from_pg_host=from_pg.server,
        from_pg_port=from_pg.port,
        from_pg_database=from_pg.database,
        from_pg_user=from_pg.user,
        from_pg_pass=from_pg.password,
        to_pg_host=to_pg.server,
        to_pg_port=to_pg.port,
        to_pg_database=to_pg.database,
        to_pg_user=to_pg.user,
        to_pg_pass=to_pg.password,
        sql_select=query,
        to_pg_schema=dest_schema,
        to_pg_name=dest_table,
        nlt_spatial=nlt_spatial,
        gdal_data=GDAL_DATA_LOC
    )

    if print_cmd:
        print (print_cmd_string([from_pg.password, to_pg.password], cmd))

    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
        
        if permission:
            to_pg.query(f"GRANT SELECT ON {dest_schema}.{dest_table} TO PUBLIC;", internal = True)
        
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print ("Ogr2ogr Output:\n", e.output)
        print ('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([from_pg.password, to_pg.password], cmd), returncode=1)

    clean_geom_column(to_pg, dest_table, dest_schema)

    # tables created always has (server, db, schema, table), in pg server and db are not listed
    to_pg.tables_created.append(('', '', dest_schema, dest_table))

    if temp:
        to_pg.log_temp_table(dest_schema, dest_table, to_pg.user)


def pg_to_pg_qry_temp_tbl(from_pg, to_pg, query, dest_table=None, print_cmd=False):
    """
        Migrates query from Postgres to Postgres temp table.
        :param from_pg: DbConnect instance connecting to PostgreSQL source database
        :param to_pg: DbConnect instance connecting to PostgreSQL destination database
        :param query: query in PostgreSQL
        :param dest_table: Table name of final migrated table in PG database
        :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
        :return:
        """

    if not dest_table:
        dest_table = '_{u}_{d}'.format(u=to_pg.user, d=datetime.datetime.now().strftime('%Y%m%d%H%M'))

    # apply regex to the query to filter out any dashed comments in the query
    # comments are defined by at least 2 dashes followed by a line break or the end of the query
    # comments with /* */ do not need to be filtered out from the query
    query = re.sub('(-){2,}.*(\n|$)', ' ', query)
    query = query.replace('"', r'\"')

    # write data to local csv
    temp_csv = r'C:\Users\{}\Documents\temp_csv_{}.csv'.format(getpass.getuser(), datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
    cmd = PG_TO_CSV_CMD.format(
        gdal_data=GDAL_DATA_LOC,
        output_csv=temp_csv,
        from_pg_host=from_pg.server,
        from_pg_port=from_pg.port,
        from_pg_database=from_pg.database,
        from_pg_user=from_pg.user,
        from_pg_pass=from_pg.password,
        sql_select=query
    )
    if print_cmd:
        print(print_cmd_string([from_pg.password, from_pg.password], cmd))
    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([to_pg.password, from_pg.password], cmd), returncode=1)

    # import data to temp table
    to_pg.csv_to_table(input_file=temp_csv, table=dest_table, temp_table=True)

    _df = pd.read_csv(temp_csv)
    if 'WKT' in _df.columns:
        # add geom column
        to_pg.query(f'alter table "{dest_table}" add geom geometry;')
        # update from wkt
        to_pg.query(f'update "{dest_table}" set geom=st_setsrid(st_geomfromtext(wkt), 2263);')
        # drop wkt col
        to_pg.query(f'alter table "{dest_table}" drop column wkt')

    # clean up csv
    os.remove(temp_csv)


def pg_to_pg_temp_tbl(from_pg, to_pg, table, org_schema=None, dest_table=None, print_cmd=False):
    """
        Migrates table from Postgres to Postgres temp table.
        :param from_pg: DbConnect instance connecting to PostgreSQL source database
        :param to_pg: DbConnect instance connecting to PostgreSQL destination database
        :param table: PostgreSQL table to migrate
        :param org_schema: PostgreSQL schema for origin table (defaults to orig db's default schema)
        :param dest_table: Table name of final migrated table in PG database
        :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
        :return:
        """

    if not dest_table:
        dest_table = table
    if org_schema:
        sch = f"{org_schema}."
    else:
        sch = ''
    query = f'select * from {sch}"{table}"'
    pg_to_pg_qry_temp_tbl(from_pg, to_pg, query, dest_table=dest_table, print_cmd=print_cmd)
