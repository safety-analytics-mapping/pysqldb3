import datetime
import decimal
import re
import os
import configparser
import pyarrow
import tempfile
import shutil
# import py7zr
import tarfile
# import rarfile
from pathlib import Path
from .cmds import *
from .sql import *
import shlex
import subprocess

import numpy as np
import pandas as pd
from shapely import wkb
from .Config import write_config
write_config(confi_path=os.path.dirname(os.path.abspath(__file__)) + "\\config.cfg")

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\config.cfg")

POSTGRES_TYPES = ['PG', 'POSTGRESQL', 'POSTGRES']
SQL_SERVER_TYPES = ['MS', 'SQL', 'MSSQL', 'SQLSERVER']
AZURE_SERVER_TYPES = ['AZ', 'AZURE', 'SYNAPSE']
TEMP_LOG_TABLE = '__temp_log_table_{}__'

GDAL_DATA_LOC = config.get('GDAL DATA', 'GDAL_DATA_LOC')

os.environ['GDAL_DATA'] = GDAL_DATA_LOC

UNICODE_REPLACEMENTS = {
    u'\xc4': 'A'
}
MS = "MS"
PG = "PG"
AZ = 'AZURE'

VARCHAR_MAX = {
    MS: 8000,
    PG: 65535
}


def clean_query_special_characters(query_string):
    # type(str) -> str
    """
    Cleans special characters
    :param query_string:
    :return: cleaned query string
    """
    query_string.replace('%', '%%')
    query_string = query_string.replace('-pct-', '%')
    query_string = query_string.replace('-qte-chr-', "''")
    return query_string


def clean_geom_column(db, table, schema):
    """
    Checks for column named wkb_geometry and renames to geom
    :param db: pysql.DbConect object
    :param table: table name
    :param schema: database schema name
    :return:
    """
    # Check if there is a geom column
    # Rename column to geom (only if wkb_geom or shape); otherwise could cause issues if more than 1 geom
    db.query("""SELECT COLUMN_NAME 
                FROM information_schema.COLUMNS 
                WHERE data_type='USER-DEFINED' 
                and lower(TABLE_NAME)=lower('{t}')
                and table_schema = '{s}'
            """.format(t=table, s=schema), timeme=False, internal=True)

    if db.internal_data:
        if db.internal_data[-1][0] == 'wkb_geometry':
            c = 'wkb_geometry'
            db.query("ALTER TABLE {s}.{t} RENAME COLUMN {c} to geom".format(c=c, t=table, s=schema),
                     timeme=False, internal=True)
        elif db.internal_data[-1][0] == 'shape':
            c = 'shape'
            db.query("ALTER TABLE {s}.{t} RENAME COLUMN {c} to geom".format(c=c, t=table, s=schema),
                     timeme=False, internal=True)
        elif db.internal_data[-1][0] == 'Shape':
            c = 'Shape'
            db.query('ALTER TABLE {s}.{t} RENAME COLUMN "{c}" to geom'.format(c=c, t=table, s=schema),
                     timeme=False, internal=True)


def get_unique_table_schema_string(tbl_str, db_type):
    """
    This takes a raw input for a PG/MS table and distills the name in the way the database stores it.

    This allows there to be one 'cleaned' version for multiple variations of the same table so they are not written into
    the log twice or erroneously /not/ removed.

    Ex. in PG: working.tbl, working.Tbl, working."tbl" --> all saved the same way by PG
    Ex. in MS: [dbo].[tbl], [dbo]."tbl", [dbo].tbl --> all saved the same way by MS

    :param tbl_str: Table or schema string
    :param db_type: Type of DB
    :return:
    """
    if not tbl_str:
        return None
    if db_type.upper() == PG:
        if '"' not in tbl_str:
            # If no "", lower case
            return tbl_str.lower()
        else:
            # If "", remove "" but keep case
            return tbl_str.replace('"', '')

    if db_type.upper() == MS:
        if not '"' in tbl_str and not '[' in tbl_str:
            tbl_str = tbl_str.lower()
        if '"' in tbl_str and '[' in tbl_str and ']' in tbl_str:
            # If "" and [], just remove []
            return tbl_str.replace('[', '').replace(']', '')

        if '"' in tbl_str and not ('[' in tbl_str and ']' in tbl_str):
            # If "" and not [], remove ""
            return tbl_str.replace('"', '')

        # If no "", still remove []
        return tbl_str.replace('[', '').replace(']', '')

    if db_type.upper() == AZ:
        return tbl_str.lower()




def get_query_table_schema_name(tbl_str, db_type):
    """
    The inverse of get_unique_table_schema_string. This takes a cleaned input from the log table and makes small
    changes to ensure MS/PG interpret it correctly.

    Ex. in PG: if stored in log as Table, then must be queried as "Table" to ensure capital letter.
    Ex. in MS: if stored in log as "table", then must be queried as ["table"] to ensure quotes.

    :param tbl_str: Table or schema string
    :param db_type: Type of DB
    :return:
    """
    if not tbl_str:
        return tbl_str
    if db_type == PG:
        if tbl_str.islower() and " " not in tbl_str:
            return tbl_str
        else:
            return '"' + tbl_str + '"'

    if db_type == MS:
        if tbl_str.islower() and " " not in tbl_str and '"' not in tbl_str:
            return tbl_str
        else:
            return '[' + tbl_str + ']'



def parse_table_string(tbl_str, default_schema, db_type):
    """
    Pareses extracts schema and table name from table references in query strings
    (ex. server.schema.table, schema.table, table)
    :param tbl_str: String of table reference
    :param default_schema: default schema
    :param db_type: db type (PG, MS, etc.)
    :return: schema name, table name
     
    """
    # Parse schema/table from table string
    if type(tbl_str) in (list, tuple):
        return tbl_str
    # names_arr = tbl_str.split('.')
    start = 0
    names_arr=list()
    if db_type == MS:
        regex = '\.(?=([^\[\]]*\[[^\[\]]*\])*[^\[\]]*$)'
    elif db_type == PG:
        regex = '\.(?=([^\"]*\"[^\"]*\")*[^\"]*$)'
    else:
        assert False, "Invalid Type"

    # Slices by the appropriate . found in the regex into schema, table, server...
    for r in re.finditer(regex, tbl_str):
        names_arr.append(tbl_str[start:r.start()])
        start = r.start() + 1
    names_arr.append(tbl_str[start:])

    server = None
    database = None

    # Assumes 2-4 .(dots) for MS and 0 - 1 for pg
    # > 4 accounts for servers with urls (ex.devpgserversql02.host.net)
    if len(names_arr) > 4:
        database, schema, table = names_arr[-3:]
        server = '.'.join(names_arr[:-3])
    if len(names_arr) == 4:
        server, database, schema, table = names_arr
    elif len(names_arr) == 3:
        database, schema, table = names_arr
    elif len(names_arr) == 2:
        schema, table = names_arr
    elif len(names_arr) == 1:
        schema = default_schema
        table = names_arr[0]
    else:
        schema, table = None, None

    # if not return_combined:
    if server:
        server = get_unique_table_schema_string(server, db_type)
    if database:
        database = get_unique_table_schema_string(database, db_type)
    return server, database, get_unique_table_schema_string(schema, db_type), \
           get_unique_table_schema_string(table, db_type)
    # else:
    #    return get_unique_table_schema_string(schema, db_type) + '.' + get_unique_table_schema_string(table, db_type)


def type_decoder(typ, varchar_length=500):
    """
    Lazy type decoding from pandas to SQL. There are problems assoicated with NaN values for numeric types when
    stored as Object dtypes.

    This does not try to optimize for smallest size datatype.

    :param typ: Numpy dtype for column
    :param varchar_length: Length for varchar columns
    :return: String representing data type
    """
    if typ == np.dtype('M'):
        return 'timestamp'
    elif typ == np.dtype('int64'):
        return 'bigint'
    elif typ == np.dtype('float64'):
        return 'float'
    else:
        return 'varchar ({})'.format(varchar_length)

def type_decoder_pyarrow(typ, varchar_length=500):
    """
    Lazy type decoding from pandas to SQL. There are problems assoicated with NaN values for numeric types when
    stored as Object dtypes.

    This does not try to optimize for smallest size datatype.

    :param typ: Numpy dtype for column
    :param varchar_length: Length for varchar columns
    :return: String representing data type
    """
    if typ in (pyarrow.int8(), pyarrow.int16(), pyarrow.int32(), pyarrow.int64()):
        return 'bigint'
    if typ in (pyarrow.float16(), pyarrow.float32(), pyarrow.float64()):
        return 'float'
    elif pyarrow.types.is_timestamp(typ):
        return 'timestamp'
    elif pyarrow.types.is_date(typ):
        return 'date'
    else:
        return 'varchar ({})'.format(varchar_length)

def clean_cell(x):
    """
    Formats csv cells for SQL to add to database

    :param x: Raw csv cell value
    :return: Formatted csv cell value as python object
    """
    if pd.isnull(x):
        return "None"
    elif type(x) == int:
        return str(int(x))
    elif type(x) == decimal.Decimal:
        return str(float(x))
    elif type(x) == str:
        x = x.replace("'", '-qte-chr-')

        # Try to first decode as utf-8; otherwise, try as latin1
        try:
            x = bytes(x, 'utf-8').decode('utf-8')
        except Exception as e:
            print(e)
            print('Decoding input string as utf-8 failed; trying as Latin1 ')

            try:
                x = bytes(x, 'utf-8').decode('latin1')
            except Exception as e:
                print(e)
                print('Decoding input string as Latin1 failed; leaving as str ')

    elif type(x) == datetime.date:
        x = x.strftime('%Y-%m-%d')
    elif type(x) == datetime.datetime:
        x = x.strftime('%Y-%m-%d %H:%M')
    elif type(x) == pd.Timestamp:
        x.to_pydatetime()
        x = x.strftime('%Y-%m-%d %H:%M')

    try:
        return "'" + str(x) + "'"
    except Exception as e:
        print(e)
        return "'" + x + "'"


def clean_column(x):
    """
    Reformats column names to for database
    :param x: column name
    :return: Reformatted column name with special characters replaced
    """
    if type(x) == int:
        x = str(x)

    try:
        x.strip().lower()
    except Exception as e:
        print('This dataframe has non-string column names (likely nulls). Please fix before uploading as a table.')
        assert e

    a = x.strip().lower()
    b = a.replace(' ', '_')
    c = b.replace('.', '')
    d = c.replace('(s)', '')
    e = d.replace(':', '_')
    return e


def convert_geom_col(df, geom_name="geom"):
    """
    df: DataFrame
    geom_name: column of geom to be converted, defaulted to "geom"
    """
    if geom_name in df.columns:
        df[geom_name] = df[geom_name].apply(lambda x: wkb.loads(x, hex=True).wkt if x else None)

    return df


def clean_df_before_output(df, geom_name="geom"):
    """
    A function that aggregates all data cleaning to be performed on pandas DataFrames before they're outputted

    Currently:
    1. Converts geom col
    2. Converts unicode errors

    :param df:
    :param geom_name:
    :return:
    """
    df = convert_geom_col(df, geom_name)
    return df


def file_loc(typ='file', print_message=None):
    if not print_message:
        print('File/folder search dialog...')
    else:
        print(print_message)
    from tkinter import Tk, filedialog as tkFileDialog
    # import tkMessageBox
    Tk().withdraw()
    if typ == 'file':
        # tkMessageBox.showinfo("Open file", "Please navigate to the file file you want to process")
        filename = tkFileDialog.askopenfilename(title="Select file")
        return filename
    elif typ == 'folder':
        folder = tkFileDialog.askdirectory(title="Select folder")
        return folder
    elif typ == 'save':
        output_file_name = tkFileDialog.asksaveasfilename(
            filetypes=(("Shapefile", "*.shp"), ("Geopackage", "*.gpkg"), ("All Files", "*.*")),
            defaultextension=".shp"
        )
        return output_file_name


def print_cmd_string(password_list, cmd_string):
    for p in password_list:
        if p:
            cmd_string = cmd_string.replace(p, '*' * len(p))
    return cmd_string

def parse_geospatial_file_path(path=None, file_name=None):
    """
    Standardizes extracting geospatial file name from path process, if file_name provided that will override anything in the path
    :param path: folder path with or without file name
    :param file_name: shapefile or geopackage name
    :return: path (without file), file_name
    """
    # type: (str, str)

    ## add an if statement if the zip folder is in the middle of / embedded in the path
    if '.zip' in path.lower():
        path = '/vsizip/' + path

    if file_name:
        return path, file_name

    file_name = os.path.basename(path)
    path = path.replace(file_name, '')

    return path, file_name

def rename_geom(db, schema, table):

    """
    Renames wkb_geometry to geom, along with index

    :param dbo: Database connection
    :param schema: Schema where geom is located
    :param table: Table where geom is located
    :return:
    """
    db.query(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = '{schema}'
                    AND table_name   = '{table}';
                """, timeme=False, internal=True)
    f = None

    if db.type == 'PG':

        # Get the column in question
        if 'wkb_geometry' in [i[0] for i in db.internal_queries[-1].data]:
            f = 'wkb_geometry'
        elif 'shape' in [i[0] for i in db.internal_queries[-1].data]:
            f = 'shape'

        if f:
            # Rename column
            db.rename_column(schema=schema, table=table, old_column=f, new_column='geom')

            # Rename index
            db.query(f"""
                ALTER INDEX IF EXISTS
                {schema}.{table}_{f}_geom_idx
                RENAME to {table}_geom_idx
            """, timeme=False, internal=True)

    elif db.type == 'MS':
        # Get the column in question
        if 'ogr_geometry' in [i[0] for i in db.internal_queries[-1].data]:
            f = 'ogr_geometry'
        elif 'Shape' in [i[0] for i in db.internal_queries[-1].data]:
            f = 'Shape'

        if f:
            # Rename column
            db.rename_column(schema=schema, table=table, old_column=f, new_column='geom')

            # Rename index if exists
            try:
                db.query(f"""
                    EXEC sp_rename N'{schema}.{table}.ogr_{schema}_{table}_{f}_sidx', N'{table}_geom_idx', N'INDEX';
                """, timeme=False, internal=True)
            except SystemExit as e:
                print(e)
                print('Warning - could not update index name after renaming geometry. It may not exist.')


def read_compressed(temp_dir, path = None, input_file = None):
    
    # unzip and read compressed file if applicable

    # if file has other compressed format
    # note: Since the compressed file may contain multiple different SHP files, this logic only applies when the path
    # includes the name of the compressed file and the input file is the name of the specific SHP file.
    compressed_exts = ['.tar', '.gz', '.tgz', '.7z', '.rar']

    if os.path.splitext(path)[1].lower() in compressed_exts:
        print("Importing Shp from compressed file")

        # Create a temporary directory to extract files
        temp_dir = tempfile.mkdtemp()
        suffix = Path(path).suffix.lower()

        # Extract compressed archive into temp_dir
        if suffix in ['.tar', '.gz', '.tgz', '.tar.gz']:
            with tarfile.open(path, 'r:*') as tar:
                tar.extractall(temp_dir)
        elif suffix == '.7z':
            with py7zr.SevenZipFile(path, mode='r') as archive:
                archive.extractall(path=temp_dir)
        elif suffix == '.rar':
            with rarfile.RarFile(path) as archive:
                archive.extractall(path=temp_dir)
        else:
            shutil.rmtree(temp_dir)
            raise ValueError(f"Unsupported compression format: {suffix}")

        # Look for a specific .shp file by name
        target_shp = Path(temp_dir).rglob(input_file)
        shp_path = next(target_shp, None)

        if not shp_path or not shp_path.exists():
            shutil.rmtree(temp_dir)
            raise FileNotFoundError(f"'{input_file}' not found in the archive.")

        # Return the folder path, .shp filename, and temp dir for later cleanup
        path, input_file = str(shp_path.parent), shp_path.name
        full_path = os.path.join(path, input_file)

    else:
        path, input_file = parse_geospatial_file_path(path, input_file)
        full_path = os.path.join(path, input_file)

    return full_path, path, input_file

def read_geospatial_command(dbo, input_file, gdal_data_loc, srid, full_path, schema,
                            table, precision, port, gpkg_tbl = None, feature_class = None, skip_failures = None):
    
    """"
    Generate the cmd text to read a Geospatial file into the database
    """

    if dbo.type == 'PG' and input_file.endswith('.gpkg'):
        cmd = READ_GPKG_CMD_PG.format(
            gdal_data=gdal_data_loc,
            srid=srid,
            host=dbo.server,
            dbname=dbo.database,
            user=dbo.user,
            password=dbo.password,
            gpkg_name = full_path,
            gpkg_tbl = gpkg_tbl,
            schema = schema,
            tbl_name = table,
            perc=precision,
            port=port
        )

    elif dbo.type == 'MS' and input_file.endswith('.gpkg'):
        if dbo.LDAP:
            cmd = READ_GPKG_CMD_MS.format(
                gdal_data=gdal_data_loc,
                srid=srid,
                host=dbo.server,
                dbname=dbo.database,
                gpkg_name=full_path,
                gpkg_tbl = gpkg_tbl,
                schema=schema,
                tbl_name='"' + table + '"',
                perc=precision,
                port=port
            )
            cmd.replace(";UID={user};PWD={password}", "")

        else:
            cmd = READ_GPKG_CMD_MS.format(
                gdal_data=gdal_data_loc,
                srid=srid,
                host=dbo.server,
                dbname=dbo.database,
                user=dbo.user,
                password=dbo.password,
                gpkg_name=full_path,
                gpkg_tbl = gpkg_tbl,
                schema=schema,
                tbl_name='"' + table + '"',
                perc=precision,
                port=port
            )

    elif dbo.type == 'PG' and input_file.endswith('.shp') and not feature_class:
        
            cmd = READ_SHP_CMD_PG.format(
                    gdal_data = gdal_data_loc,
                    srid = srid,
                    host = dbo.server,
                    dbname = dbo.database,
                    user = dbo.user,
                    password = dbo.password,
                    shp = full_path,
                    schema = schema,
                    tbl_name = table,
                    perc = precision,
                    port = port)

    elif dbo.type == 'MS' and input_file.endswith('.shp') and not feature_class:
        
        if dbo.LDAP:
            cmd = READ_SHP_CMD_MS.format(
                gdal_data = gdal_data_loc,
                srid = srid,
                host = dbo.server,
                dbname = dbo.database,
                shp = full_path,
                schema = schema,
                tbl_name = table,
                perc = precision,
                port = port
            )
            cmd.replace(";UID={user};PWD={password}", "")

        else:
            cmd = READ_SHP_CMD_MS.format(
                gdal_data = gdal_data_loc,
                srid = srid,
                host = dbo.server,
                dbname = dbo.database,
                user = dbo.user,
                password = dbo.password,
                shp = full_path,
                schema = schema,
                tbl_name = table,
                perc = precision,
                port = port
            )
    
    elif dbo.type == 'PG' and input_file.endswith('.gdb') and feature_class:
        
        cmd = READ_FEATURE_CMD.format(
            gdal_data = gdal_data_loc,
            srid = srid,
            host =  dbo.server,
            dbname = dbo.database,
            user = dbo.user,
            password= dbo.password,
            gdb = full_path,
            feature = feature_class,
            tbl_name = table,
            sch = schema
        )
    elif dbo.type == 'MS' and input_file.endswith('.gdb') and feature_class:
            # TODO: add LDAP version trusted_connection=yes
        cmd = READ_FEATURE_CMD_MS.format(
                gdal_data = gdal_data_loc,
                srid = srid,
                ms_server = dbo.server,
                ms_db = dbo.database,
                ms_user = dbo.user,
                ms_pass = dbo.password,
                gdb = full_path,
                feature= feature_class,
                tbl_name=table,
                sch= schema,
                sf=skip_failures
            )
    
    else:
        AssertionError('Please check your inputs.')

    return cmd

def comment_query(dbo, feature_class = None, schema = None,
                  table = None, path = None, input_file = None):
    
    """
    Add a comment to a shp table
    """
    
    if dbo.type == 'PG' and feature_class == True:
        dbo.query(FEATURE_COMMENT_QUERY.format(
            s = schema,
            t = table,
            u = dbo.user,
            d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        ), timeme=False, internal=True)
    
    elif dbo.type == 'PG' and input_file.endswith('.shp') and feature_class == False:
        dbo.query(SHP_COMMENT_QUERY.format(
                s=schema,
                t=table,
                u=dbo.user,
                p=path,
                shp=input_file,
                d=datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
            ), timeme=False, internal=True)
        
def encoding_changes(cmd_env, encoding = None):
    
    cmd_env = os.environ.copy()

    if encoding and encoding.upper() == 'LATIN1':
        cmd_env['PGCLIENTENCODING'] = 'LATIN1'

    if encoding and encoding.upper().replace('-', '') == 'UTF8':
        cmd_env['PGCLIENTENCODING'] = 'UTF8'

    return cmd_env

def retrieve_gpkg_tbl_names(dbo, full_path):

    """
    Parse through a geopackage and identify all the table names. Clean table names if necessary to make it compatible for upload.
    """
    # create empty dictionary
    gpkg_tbl_names = {}
    
    # count numbre of tables and parse through them
    try:
        count_cmd = COUNT_GPKG_LAYERS.format(full_path = full_path) 
        ogr_response = subprocess.check_output(shlex.split(count_cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)

        if full_path.endswith('.gpkg'):
            tables_in_gpkg = re.findall(r"\\n\d+:\s(.*?)(?=\\r|\s\(.*\))", str(ogr_response))
        # only allows tables names with underscores, numbers, and letters as the first character
        # excludes any dtype description that's also returned by the command line
        else: # .gdb
            tables_in_gpkg = re.findall(r"Layer:\s(.*?)(?=\\r|\s\(.*\))", str(ogr_response))

    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed. The Geopackage was not read in.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([dbo.password], count_cmd), returncode=1)

        # create a list of cleaned table names from the list that was generated
    for t_i_g in tables_in_gpkg:
        insert_val = re.sub(r'[^A-Za-z0-9_]+', r'_', t_i_g)
        gpkg_tbl_names[t_i_g] = insert_val # add the cleaned name

    # assert that the new cleaned names are unique. if not, we won't get the same dimensions
    assert len(gpkg_tbl_names) == len(tables_in_gpkg), "Clean the geopackage table names so they can be uploaded as tables (by removing special characters other than _) and make sure they are unique."

    return gpkg_tbl_names

def convert_cmd(input_full_path, output_full_path, gpkg_tbl = None, _update = None, _overwrite = None, feature_class = None):

    """
    Generate the cmd text to convert a geospatial file to another geospatial rformat
    """
    if input_full_path.endswith('shp') and output_full_path.endswith('gpkg'):
        cmd = WRITE_SHP_CMD_GPKG.format(shp_path = input_full_path,
                                        gpkg_path = output_full_path,
                                        _update = _update,
                                        _overwrite = _overwrite,
                                        gpkg_tbl = gpkg_tbl)
    
    elif input_full_path.endswith('.gpkg') and output_full_path.endswith('.shp'):
        cmd = WRITE_GPKG_CMD_SHP.format(    gpkg_path = input_full_path,
                                            gpkg_tbl = gpkg_tbl,
                                            shp_path=output_full_path
                                            )
    elif input_full_path.endswith('.gdb') and output_full_path.endswith('.gpkg'):
        cmd = WRITE_GDB_CMD_GPKG.format(    shp_path=input_full_path,
                                            feature_class = feature_class,
                                            gpkg_path = output_full_path,
                                            gpkg_tbl = gpkg_tbl,
                                            _update = _update,
                                            _overwrite = _overwrite,
                                            )
        
    return cmd
        
def write_cmd(dbo, full_path, gpkg_tbl = None, table = None,
                    _overwrite = None, _update = None, srid = None, gdal_data_loc = None, qry = None):
    
    """
    Generate the cmd text to write to a geospatial file
    """
    # set db connection string
    if dbo.type == PG:
        db_connect_str = f'"PG:host={dbo.server} user={dbo.user} dbname={dbo.database} password={dbo.password}"'
    elif dbo.type == MS:
        if dbo.LDAP:
            u=''
            p=''
        else:
            u=dbo.user
            p=dbo.password
        db_connect_str =  f"MSSQL:server={dbo.server};database={dbo.database};UID={u};PWD={p}"
    if full_path.endswith('.gpkg'):
        cmd = WRITE_GPKG_CMD.format(full_path=full_path,
                                    sql_select=qry,
                                    gpkg_tbl = gpkg_tbl,
                                    tbl_name = table,
                                    _overwrite = _overwrite,
                                    _update = _update,
                                    srid=srid,
                                    gdal_data=gdal_data_loc,
                                    db_connect_str=db_connect_str)
            
    elif full_path.endswith('.shp'):
        cmd = WRITE_SHP_CMD.format(full_path = full_path,
                                   db_connect_str=db_connect_str,
                                   sql_select = qry,
                                   srid = srid,
                                   gdal_data = gdal_data_loc)

    return cmd

def execute_cmd(cmd, dbo = None, feature_class = None, cmd_env = None):

    if cmd_env is None:
        try:
            ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print('Ogr2ogr command failed. The Geopackage/Shapefile/feature class was not written.')
            raise subprocess.CalledProcessError(cmd=print_cmd_string([dbo.password], cmd), returncode=1)
    

    else:
        try:
            ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT, env=cmd_env)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            if feature_class == True:
                (f'Ogr2ogr command failed. The feature class was not read in.')
            else:
                print(f'Ogr2ogr command failed. The file was not read in.')
            raise subprocess.CalledProcessError(cmd=print_cmd_string([dbo.password], cmd), returncode=1)
        
    return

def clean_table_name(table = None, gpkg_tbl = None, full_path = None, input_file = None):
    """
    Clean the table name for input_geospatial_bulk
    """

    if table:
        assert table == re.sub(r'[^A-Za-z0-9_]+', r'_', table) # make sure the name will load into the database
    elif not table and gpkg_tbl:
        table = gpkg_tbl.replace('.gpkg', '').replace('.shp', '').lower()
        # if the gpkg_table is left blank, we will populate the name using input_gpkg
    elif not table and full_path.endswith(('.shp')):
        table = input_file.replace('.shp', '').lower()

    return table

def bulk_upload_table_setup(dbo, full_path, input_file, table, feature_class = None, temp_dir = None, gpkg_tbl = None):

    """
    Sets up the dictionary for bulk uploading loop based on the type of geospatial input file
    """

    # create empty dictionary
    gpkg_tbl_names = {}

    if (not gpkg_tbl and full_path.endswith('.gpkg')) or (full_path.endswith('.gdb') and not feature_class):
        # retrieve all the table names from the geopackage
        gpkg_tbl_names = retrieve_gpkg_tbl_names(dbo, full_path)

    elif full_path.endswith('.shp') and not temp_dir:
        # exclude compressed files from this if statement
        gpkg_tbl_names[full_path.replace('.shp', '')] = table
        
    elif input_file.endswith('.shp') and '.zip' in full_path: # this is for zip files
        gpkg_tbl_names[input_file.replace('.shp', '')] = table

    elif full_path.endswith('.shp') and temp_dir:
        # compressed files if statement
        gpkg_tbl_names[input_file.replace('.shp', '')] = table
      
    elif full_path.endswith('.gdb') and feature_class:
        gpkg_tbl_names[feature_class.replace('.shp', '')] = table
   
    elif full_path.endswith('.gpkg') and gpkg_tbl:
        gpkg_tbl_names[gpkg_tbl] = table

    return gpkg_tbl_names