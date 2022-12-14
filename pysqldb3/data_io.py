import subprocess
import shlex

from .cmds import *
from .util import *


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

    ms.tables_created.append(dest_schema + "." + dest_table)

    if temp:
        ms.log_temp_table(dest_schema, dest_table, ms.user)


def sql_to_pg_qry(ms, pg, query, LDAP=False, spatial=True, dest_schema=None, print_cmd=False, temp=True,
                  dest_table=None, pg_encoding='UTF8'):
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
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([ms.password, pg.password], cmd), returncode=1)

    clean_geom_column(pg, dest_table, dest_schema)

    pg.tables_created.append(dest_schema + "." + dest_table)

    if temp:
        pg.log_temp_table(dest_schema, dest_table, pg.user)


def sql_to_pg(ms, pg, org_table, LDAP=False, spatial=True, org_schema=None, dest_schema=None, print_cmd=False,
              dest_table=None, temp=True, gdal_data_loc=GDAL_DATA_LOC, pg_encoding='UTF8'):
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
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([ms.password, pg.password], cmd), returncode=1)

    clean_geom_column(pg, dest_table, dest_schema)

    pg.tables_created.append(dest_schema + "." + dest_table)

    if temp:
        pg.log_temp_table(dest_schema, dest_table, pg.user)


def pg_to_pg(from_pg, to_pg, org_table, org_schema=None, dest_schema=None, print_cmd=False, dest_table=None,
             spatial=True, temp=True):
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
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([from_pg.password, to_pg.password], cmd), returncode=1)

    clean_geom_column(to_pg, dest_table, dest_schema)

    to_pg.tables_created.append(dest_schema + "." + dest_table)

    if temp:
        to_pg.log_temp_table(dest_schema, dest_table, to_pg.user)


def pg_to_pg_qry(from_pg, to_pg, query, dest_schema=None, print_cmd=False, dest_table=None,
             spatial=True, temp=True):
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
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print ("Ogr2ogr Output:\n", e.output)
        print ('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([from_pg.password, to_pg.password], cmd), returncode=1)

    clean_geom_column(to_pg, dest_table, dest_schema)

    to_pg.tables_created.append(dest_schema + "." + dest_table)

    if temp:
        to_pg.log_temp_table(dest_schema, dest_table, to_pg.user)