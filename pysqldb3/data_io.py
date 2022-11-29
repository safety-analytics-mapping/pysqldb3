import subprocess
import shlex

from .cmds import *
from .util import *


def pg_to_sql(pg_dbconn, ms_dbconn, src_table_name, use_ldap=False, is_spatial=True, src_schema_name=None,
              dest_schema_name=None, dest_table_name=None, print_cmd=False, temp=True):
    """
    Migrates tables from Postgres to SQL Server, generates spatial tables in MS if spatial in PG.

    :param pg_dbconn: DbConnect instance connecting to PostgreSQL source database
    :param ms_dbconn: DbConnect instance connecting to SQL Server destination database
    :param src_table_name: table name of source table to migrate
    :param use_ldap: Flag for using LDAP credentials (defaults to False)
    :param is_spatial: Flag for spatial table (defaults to True)
    :param src_schema_name: PostgreSQL schema for source table (defaults to orig db's default schema)
    :param dest_schema_name: SQL Server schema for destination table (defaults to dest db's default schema)
    :param dest_table_name: Table name of final migrated table in SQL Server database
    :param print_cmd: Option to print ogr2ogr command line statement (defaults to False) - used for debugging
    :param temp: Flag for temporary table (defaults to True)
    :return:
    """
    if not src_schema_name:
        orig_schema_name = pg_dbconn.default_schema

    if not dest_schema_name:
        dest_schema_name = ms_dbconn.default_schema

    if not dest_table_name:
        dest_table_name = src_table_name

    if is_spatial:
        spatial_cmd = ' -a_srs EPSG:2263 '
        nlt_spatial_cmd = ' '
    else:
        spatial_cmd = ' '
        nlt_spatial_cmd = '-nlt NONE'

    if use_ldap:
        cmd = PG_TO_SQL_LDAP_CMD.format(
            ms_password='',
            ms_username='',
            pg_password=pg_dbconn.password,
            pg_username=pg_dbconn.username,
            ms_host=ms_dbconn.host,
            ms_dbname=ms_dbconn.db_name,
            pg_host=pg_dbconn.host,
            pg_port=pg_dbconn.port,
            pg_dbname=pg_dbconn.db_name,
            pg_schema_name=src_schema_name,
            pg_table_name=src_table_name,
            ms_schema_name=dest_schema_name,
            is_spatial=spatial_cmd,
            dest_name=dest_table_name,
            nlt_spatial=nlt_spatial_cmd,
            gdal_data=GDAL_DATA_LOC
        )
    else:
        cmd = PG_TO_SQL_CMD.format(
            ms_password=ms_dbconn.password,
            ms_username=ms_dbconn.username,
            pg_password=pg_dbconn.password,
            pg_username=pg_dbconn.user,
            ms_host=ms_dbconn.host,
            ms_dbname=ms_dbconn.database,
            pg_host=pg_dbconn.host,
            pg_port=pg_dbconn.port,
            pg_dbname=pg_dbconn.db_name,
            pg_schema_name=src_schema_name,
            pg_table_name=src_table_name,
            ms_schema_name=dest_schema_name,
            is_spatial=spatial_cmd,
            dest_name=dest_table_name,
            nlt_spatial=nlt_spatial_cmd,
            gdal_data=GDAL_DATA_LOC
        )

    if print_cmd:
        print(print_cmd_string([ms_dbconn.password, pg_dbconn.password], cmd))

    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([ms_dbconn.password, pg_dbconn.password], cmd), returncode=1)

    ms_dbconn.tables_created.append(dest_schema_name + "." + dest_table_name)

    if temp:
        ms_dbconn.log_temp_table(dest_schema_name, dest_table_name, ms_dbconn.user)


def sql_to_pg_qry(ms_dbconn, pg_dbconn, query, LDAP=False, is_spatial=True, dest_schema_name=None, print_cmd=False, temp=True,
                  dest_table_name=None, pg_encoding='UTF8'):
    """
    Migrates the result of a query from SQL Server database to PostgreSQL database, and generates spatial tables in
    PG if spatial in MS.

    :param ms_dbconn: DbConnect instance connecting to SQL Server destination database
    :param pg_dbconn: DbConnect instance connecting to PostgreSQL source database
    :param query: query in SQL
    :param LDAP: Flag for using LDAP credentials (defaults to False)
    :param is_spatial: Flag for spatial table (defaults to True)
    :param dest_schema_name: PostgreSQL schema for destination table (defaults to db's default schema)
    :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
    :param temp: flag, defaults to true, for temporary tables
    :param dest_table_name: destination table name
    :param pg_encoding: encoding to use for PG client (defaults to UTF-8)
    :return:
    """
    if not dest_schema_name:
        dest_schema_name = pg_dbconn.default_schema

    if not dest_table_name:
        dest_table_name = '_{u}_{d}'.format(u=pg_dbconn.user, d=datetime.datetime.now().strftime('%Y%m%d%H%M'))

    if is_spatial:
        spatial_cmd = 'MSSQLSpatial'
        nlt_spatial_cmd = ' '
    else:
        spatial_cmd = 'MSSQL'
        nlt_spatial_cmd = '-nlt NONE'

    if LDAP:
        cmd = SQL_TO_PG_LDAP_QRY_CMD.format(
            ms_password='',
            ms_username='',
            pg_password=pg_dbconn.password,
            pg_username=pg_dbconn.user,
            ms_host=ms_dbconn.host,
            pg_host=pg_dbconn.host,
            pg_port=pg_dbconn.port,
            ms_dbname=ms_dbconn.database,
            pg_dbname=pg_dbconn.database,
            pg_schema=dest_schema_name,
            sql_select=query,
            is_spatial=spatial_cmd,
            table_name=dest_table_name,
            nlt_spatial=nlt_spatial_cmd,
            gdal_data=GDAL_DATA_LOC
        )
    else:
        cmd = SQL_TO_PG_QRY_CMD.format(
            ms_password=ms_dbconn.password,
            ms_username=ms_dbconn.username,
            pg_password=pg_dbconn.password,
            pg_username=pg_dbconn.username,
            ms_host=ms_dbconn.host,
            pg_host=pg_dbconn.host,
            pg_port=pg_dbconn.port,
            ms_dbname=ms_dbconn.db_name,
            pg_dbname=pg_dbconn.db_name,
            pg_schema=dest_schema_name,
            sql_select=query,
            is_spatial=spatial_cmd,
            table_name=dest_table_name,
            nlt_spatial=nlt_spatial_cmd,
            gdal_data=GDAL_DATA_LOC
        )

    if print_cmd:
        print(print_cmd_string([ms_dbconn.password, pg_dbconn.password], cmd))

    cmd_env = os.environ.copy()
    cmd_env['PGCLIENTENCODING'] = pg_encoding

    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT,
                                               env=cmd_env)
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([ms_dbconn.password, pg_dbconn.password], cmd), returncode=1)

    clean_geom_column(pg_dbconn, dest_table_name, dest_schema_name)

    pg_dbconn.tables_created.append(dest_schema_name + "." + dest_table_name)

    if temp:
        pg_dbconn.log_temp_table(dest_schema_name, dest_table_name, pg_dbconn.username)


def sql_to_pg(ms_dbconn, pg_dbconn, src_ms_table_name, LDAP=False, is_spatial=True, src_ms_schema_name=None, dest_pg_schema_name=None, print_cmd=False,
              dest_pg_table_name=None, is_temp=True, gdal_data_loc=GDAL_DATA_LOC, pg_encoding='UTF8'):
    """
    Migrates tables from SQL Server to PostgreSQL, generates spatial tables in PG if spatial in MS.

    :param ms_dbconn: DbConnect instance connecting to SQL Server destination database
    :param pg_dbconn: DbConnect instance connecting to PostgreSQL source database
    :param src_ms_table_name: table name of table to migrate
    :param LDAP: Flag for using LDAP credentials (defaults to False)
    :param is_spatial: Flag for spatial table (defaults to True)
    :param src_ms_schema_name: SQL Server schema for origin table (defaults to default schema function result)
    :param dest_pg_schema_name: PostgreSQL schema for destination table (defaults to default schema function result)
    :param print_cmd: Option to print ogr2ogr command line statement (defaults to False) - used for debugging
    :param dest_pg_table_name: Table name of final migrated table in PostgreSQL database
    :param is_temp: flag, defaults to true, for temporary tables
    :param gdal_data_loc: location of GDAL data
    :param pg_encoding: encoding to use for PG client (defaults to UTF-8)
    :return:
    """
    if not src_ms_schema_name:
        src_ms_schema_name = ms_dbconn.default_schema

    if not dest_pg_schema_name:
        dest_pg_schema_name = pg_dbconn.default_schema

    # IF dest table name is blank, use same name as source
    if not dest_pg_table_name:
        dest_pg_table_name = src_ms_table_name

    if is_spatial:
        spatial = 'MSSQLSpatial'
        nlt_spatial = ' '
    else:
        spatial = 'MSSQL'
        nlt_spatial = '-nlt NONE'

    if LDAP:
        cmd = SQL_TO_PG_LDAP_CMD.format(
            gdal_data=gdal_data_loc,
            ms_password='',
            ms_username='',
            pg_password=pg_dbconn.password,
            pg_username=pg_dbconn.user,
            ms_host=ms_dbconn.host,
            ms_dbname=ms_dbconn.db_name,
            pg_host=pg_dbconn.host,
            pg_port=pg_dbconn.port,
            pg_dbname=pg_dbconn.db_name,
            pg_schema_name=dest_pg_schema_name,
            pg_table_name=dest_pg_table_name,
            ms_table_name=src_ms_table_name,
            ms_schema_name=src_ms_schema_name,
            spatial=spatial,
            dest_pg_table_name=dest_pg_table_name,
            nlt_spatial=nlt_spatial
        )
    else:
        cmd = SQL_TO_PG_CMD.format(
            gdal_data=gdal_data_loc,
            ms_pass=ms_dbconn.password,
            ms_user=ms_dbconn.username,
            pg_password=pg_dbconn.password,
            pg_username=pg_dbconn.user,
            ms_host=ms_dbconn.host,
            ms_dbname=ms_dbconn.db_name,
            pg_host=pg_dbconn.host,
            pg_port=pg_dbconn.port,
            pg_dbname=pg_dbconn.db_name,
            pg_schema_name=dest_pg_schema_name,
            pg_table_name=dest_pg_table_name,
            ms_table_name=src_ms_table_name,
            ms_schema_name=src_ms_schema_name,
            spatial=spatial,
            dest_pg_table_name=dest_pg_table_name,
            nlt_spatial=nlt_spatial
        )

    if print_cmd:
        print(print_cmd_string([ms_dbconn.password, pg_dbconn.password], cmd))

    cmd_env = os.environ.copy()
    cmd_env['PGCLIENTENCODING'] = pg_encoding

    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT,
                                               env=cmd_env)
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([ms_dbconn.password, pg_dbconn.password], cmd), returncode=1)

    clean_geom_column(pg_dbconn, dest_pg_table_name, dest_pg_schema_name)

    pg_dbconn.tables_created.append(dest_pg_schema_name + "." + dest_pg_table_name)

    if is_temp:
        pg_dbconn.log_temp_table(dest_pg_schema_name, dest_pg_table_name, pg_dbconn.user)


def pg_to_pg(src_dbconn, dest_dbconn, src_table_name, src_schema_name=None, dest_schema_name=None, print_cmd=False, dest_table_name=None,
             is_spatial=True, is_temp=True):
    """
    Migrates tables from one PostgreSQL database to another PostgreSQL.
    :param src_dbconn: Source database DbConnect object
    :param dest_dbconn: Destination database DbConnect object
    :param src_table_name: Source table name
    :param src_schema_name: PostgreSQL schema for origin table (defaults to default schema)
    :param dest_schema_name: PostgreSQL schema for destination table (defaults to default schema)
    :param dest_table_name: New name for destination table if None will keep original
    :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
    :param is_spatial: Flag for spatial table (defaults to True)
    :param is_temp: temporary table, defaults to true
    :return:
    """
    if not src_schema_name:
        src_schema_name = src_dbconn.default_schema

    if not dest_schema_name:
        dest_schema_name = dest_dbconn.default_schema

    if not dest_table_name:
        dest_table_name = src_table_name

    if is_spatial:
        nlt_spatial = ' '
    if not is_spatial:
        nlt_spatial = '-nlt NONE'

    cmd = PG_TO_PG_CMD.format(
        src_host=src_dbconn.host,
        src_port=src_dbconn.port,
        src_dbname=src_dbconn.db_name,
        src_username=src_dbconn.username,
        src_password=src_dbconn.password,
        dest_host=dest_dbconn.host,
        dest_port=dest_dbconn.port,
        dest_dbname=dest_dbconn.db_name,
        dest_username=dest_dbconn.username,
        dest_password=dest_dbconn.password,
        src_schema_name=src_schema_name,
        dest_schema_name=dest_schema_name,
        src_table_name=src_table_name,
        dest_table_name=dest_table_name,
        nlt_spatial=nlt_spatial,
        gdal_data=GDAL_DATA_LOC
    )

    if print_cmd:
        print(print_cmd_string([src_dbconn.password, dest_dbconn.password], cmd))

    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([src_dbconn.password, dest_dbconn.password], cmd), returncode=1)

    clean_geom_column(dest_dbconn, dest_table_name, dest_schema_name)

    dest_dbconn.tables_created.append(dest_schema_name + "." + dest_table_name)

    if is_temp:
        dest_dbconn.log_temp_table(dest_schema_name, dest_table_name, dest_dbconn.username)


def pg_to_pg_qry(src_dbconn, dest_dbconn, query, dest_schema_name=None, print_cmd=False, dest_table_name=None,
             is_spatial=True, is_temp=True):
    """
    Migrates query results  from one PostgreSQL database to another PostgreSQL.
    :param src_dbconn: Source database DbConnect object
    :param dest_dbconn: Destination database DbConnect object
    :param query: query in SQL
    :param dest_schema_name: PostgreSQL schema for destination table (defaults to default schema)
    :param dest_table_name: New name for destination table if None will keep original
    :param print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
    :param is_spatial: Flag for spatial table (defaults to True)
    :param is_temp: temporary table, defaults to true
    :return:
    """


    if not dest_schema_name:
        dest_schema_name = dest_dbconn.default_schema

    if not dest_table_name:
        dest_table_name = '_{u}_{d}'.format(u=dest_dbconn.username, d=datetime.datetime.now().strftime('%Y%m%d%H%M'))

    if is_spatial:
        nlt_spatial = ' '

    if not is_spatial:
        nlt_spatial = '-nlt NONE'

    cmd = PG_TO_PG_QRY_CMD.format(
        src_host=src_dbconn.host,
        src_port=src_dbconn.port,
        src_dbname=src_dbconn.db_name,
        src_username=src_dbconn.username,
        src_password=src_dbconn.password,
        dest_host=dest_dbconn.host,
        dest_port=dest_dbconn.port,
        dest_dbname=dest_dbconn.db_name,
        dest_username=dest_dbconn.username,
        dest_password=dest_dbconn.password,
        sql_select=query,
        dest_schema_name=dest_schema_name,
        dest_table_name=dest_table_name,
        nlt_spatial=nlt_spatial,
        gdal_data=GDAL_DATA_LOC
    )

    if print_cmd:
        print(print_cmd_string([src_dbconn.password, dest_dbconn.password], cmd))

    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([src_dbconn.password, dest_dbconn.password], cmd), returncode=1)

    clean_geom_column(dest_dbconn, dest_table_name, dest_schema_name)

    dest_dbconn.tables_created.append(dest_schema_name + "." + dest_table_name)

    if is_temp:
        dest_dbconn.log_temp_table(dest_schema_name, dest_table_name, dest_dbconn.username)