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

def add_zip_to_geo_path(path=None):
    """
    Unzip a zipped file path if required.
    :param path: folder path with or without file name
    :return: path (without file), file_name
    """

    ## add an if statement if the zip folder is in the middle of / embedded in the path
    if '.zip' in path.lower():
        path = '/vsizip/' + path

    return path

def parse_file_path(path = None):
    """
    Parse the file name to separate the path and file name.
    :param path: Full file path or file name

    :return: Clean file path
    """
    level1 = os.path.basename(path) # take base name
    level2 = os.path.dirname(path) # take directory
    folder_dir = os.path.dirname(level2) # take level above if needed

    if '.' in level1:
        # no table name
        file_name = level1 # .shp or .gpkg or .gdb
        table = ''
        folder_dir = level2 # directory
        # GDAL has an issue with reading this as os.path.join()
        full_path = folder_dir + '/' + file_name
    else:
        # table name exists
        table = level1 # table
        file_name = level2 # .gpkg or .gdb
        full_path = file_name

    return full_path, folder_dir, file_name, table

def rename_geom(db, schema, table):

    """
    Renames wkb_geometry to geom, along with index

    :param dbo: Database connection
    :param schema: Schema where geom is located
    :param table: Table where geom is located
    :return:
    """

    # remove double quotes for table names with special chars
    # as it is not recognized in the information_schema
    standard_table_name = re.sub('"', '', table)

    # if double quotes are in the table name, need to create special variable
    if re.search('"', table):
        double_quote = '"'
    else:
        double_quote = ''

    db.query(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = '{schema}'
                    AND table_name   = '{standard_table_name}';
                """, timeme=False, internal=True)
    f = None

    geom_set = {'wkb_geometry', 'shape', 'Shape', 'geometry', 'SHAPE', 'ogr_geometry'}
    comparison_set = {i[0] for i in db.internal_queries[-1].data}

    f = comparison_set.intersection(geom_set) # this finds the intersection of the 2 sets (the geometry field)

    # Get the column in question

    if f:
        # if f exists, take the string of f
        f = f.pop()

        # Rename column
        db.rename_column(schema=schema, table=table, old_column=f, new_column='geom')

        if db.type == 'PG':
            # Rename index
            db.query(f"""
                    ALTER INDEX IF EXISTS
                    {schema}.{double_quote}{standard_table_name}_{f}_geom_idx{double_quote}
                    RENAME to {double_quote}{standard_table_name}_geom_idx{double_quote}
                """, timeme=False, internal=True)

        elif db.type == 'MS':
            # Rename index if exists
            try:
                db.query(f"""
                    EXEC sp_rename N'{schema}.{table}.ogr_{schema}_{standard_table_name}_{f}_sidx', N'{standard_table_name}_geom_idx', N'INDEX';
                """, timeme=False, internal=True)
            except SystemExit as e:
                print(e)
                print('Warning - could not update index name after renaming geometry. It may not exist.')


def decompress_and_parse_file_path(path = None):
    
    """
    Unzip all Geospatial files or decompress SHP file if it's in a particular compressed format.

    If the file is not a compressed Shapefile, it can parse the file paths for all tables within
    the Geospatial file, such as multiple geopackage or geodatabase tables.

    :param path: (str) Full file path for file
    :returns: 
    """

    # note: Since the compressed file may contain multiple different SHP files, this logic only applies when the path
    # includes the name of the compressed file and the input file is the name of the specific SHP file.
    compressed_exts = ['.tar', '.gz', '.tgz', '.7z', '.rar']
    path_ = path # make a copy

    if any(c in path for c in compressed_exts):
        print("Importing Shp from compressed file")

        # Create a temporary directory to extract files
        temp_dir = tempfile.mkdtemp()
        
        while not any(path_.endswith(c) for c in compressed_exts):
            # in case there are many subfolders
            path_ = os.path.dirname(path_)
        
        suffix = Path(path_).suffix.lower()

        # Extract compressed archive into temp_dir
        if suffix in ['.tar', '.gz', '.tgz', '.tar.gz']:
            with tarfile.open(path_, 'r:*') as tar:
                tar.extractall(temp_dir)
        elif suffix == '.7z':
            with py7zr.SevenZipFile(path_, mode='r') as archive:
                archive.extractall(path=temp_dir)
        elif suffix == '.rar':
            with rarfile.RarFile(path_) as archive:
                archive.extractall(path=temp_dir)
        else:
            shutil.rmtree(temp_dir)
            raise ValueError(f"Unsupported compression format: {suffix}")

        # Look for a specific .shp file by name
        input_file = os.path.basename(path)
        target_shp = Path(temp_dir).rglob(input_file)
        shp_path = next(target_shp, None)

        if not shp_path or not shp_path.exists():
            shutil.rmtree(temp_dir)
            raise FileNotFoundError(f"'{input_file}' not found in the archive.")

        # Return the folder path, .shp filename, and temp dir for later cleanup
        path = os.path.join(str(shp_path.parent), shp_path.name)
        file_dir = shp_path.parent
        input_file = shp_path.name
        input_table = ''

    else:
        full_path = add_zip_to_geo_path(path) # this function only runs if applicable
        path, file_dir, input_file, input_table = parse_file_path(full_path)

    return path, file_dir, input_file, input_table

def read_geospatial_command(dbo, gdal_data_loc, srid, full_path, schema,
                            table, input_tbl, precision, port, skip_failures = None):
    
    """"
    Generate the cmd text to read a Geospatial file into the database
    """

    if dbo.type == 'PG' and '.gpkg' in full_path:
        cmd = READ_GPKG_CMD_PG.format(
            gdal_data=gdal_data_loc,
            srid=srid,
            host=dbo.server,
            dbname=dbo.database,
            user=dbo.user,
            password=dbo.password,
            gpkg_name = full_path,
            gpkg_tbl = input_tbl,
            schema = schema,
            tbl_name = table,
            perc=precision,
            port=port
        )

    elif dbo.type == 'MS' and '.gpkg' in full_path:
        if dbo.LDAP:
            cmd = READ_GPKG_CMD_MS.format(
                gdal_data=gdal_data_loc,
                srid=srid,
                host=dbo.server,
                dbname=dbo.database,
                gpkg_name=full_path,
                gpkg_tbl = input_tbl,
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
                gpkg_tbl = input_tbl,
                schema=schema,
                tbl_name='"' + table + '"',
                perc=precision,
                port=port
            )

    elif dbo.type == 'PG' and '.shp' in full_path:
        
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

    elif dbo.type == 'MS' and '.shp' in full_path:
        
        if dbo.LDAP:
            cmd = READ_SHP_CMD_MS.format(
                gdal_data = gdal_data_loc,
                srid = srid,
                host = dbo.server,
                dbname = dbo.database,
                shp = full_path,
                schema = schema,
                tbl_name = '"' + table + '"',
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
                tbl_name = '"' + table + '"',
                perc = precision,
                port = port
            )
    
    elif dbo.type == 'PG' and '.gdb' in full_path:
        
        cmd = READ_FEATURE_CMD.format(
            gdal_data = gdal_data_loc,
            srid = srid,
            host =  dbo.server,
            dbname = dbo.database,
            user = dbo.user,
            password= dbo.password,
            gdb = full_path.replace('.shp', ''),
            feature = input_tbl,
            tbl_name = table,
            sch = schema
        )
    elif dbo.type == 'MS' and '.gdb' in full_path:
            # TODO: add LDAP version trusted_connection=yes
        cmd = READ_FEATURE_CMD_MS.format(
                gdal_data = gdal_data_loc,
                srid = srid,
                ms_server = dbo.server,
                ms_db = dbo.database,
                ms_user = dbo.user,
                ms_pass = dbo.password,
                gdb = full_path.replace('.shp', ''),
                feature= input_tbl,
                tbl_name= '"' + table + '"',
                sch= schema,
                sf=skip_failures
            )
    
    else:
        raise ValueError('Please check your inputs.')

    return cmd

def comment_query(dbo, schema = None, table = None, path = None):
    
    """
    Add a comment to a shp table
    """
    
    if dbo.type == 'PG' and '.gdb' in path:
        # THIS IS FOR FEATURE CLASSES ONLY
        dbo.query(FEATURE_COMMENT_QUERY.format(
            s = schema,
            t = table,
            u = dbo.user,
            d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        ), timeme=False, internal=True)
    
    elif dbo.type == 'PG' and '.shp' in path:
        dbo.query(SHP_COMMENT_QUERY.format(
                s=schema,
                t=table,
                u=dbo.user,
                p=path,
                d=datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
            ), timeme=False, internal=True)
        
def encoding_changes(cmd_env, encoding = None):
    
    cmd_env = os.environ.copy()

    if encoding and encoding.upper() == 'LATIN1':
        cmd_env['PGCLIENTENCODING'] = 'LATIN1'

    if encoding and encoding.upper().replace('-', '') == 'UTF8':
        cmd_env['PGCLIENTENCODING'] = 'UTF8'

    return cmd_env

def retrieve_input_tbl_names(dbo, full_path):

    """
    Parse through a geopackage and identify all the table names.
    Clean table names if necessary to make it compatible for upload.

    :param dbo: Database connection
    :param full_path: Full file path
    :return: dict
    """

    # create empty dictionary
    gpkg_tbl_names = {}
    
    # count numbre of tables and parse through them
    try:
        if full_path.endswith('.gpkg'):
            count_cmd = COUNT_GEOSPATIAL_LAYERS.format(full_path = full_path) 
            ogr_response = subprocess.check_output(shlex.split(count_cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
            tables_in_gpkg = re.findall(r"\\n\d+:\s(.*?)(?=\\r|\s\(.*\))", str(ogr_response))
        # only allows tables names with underscores, numbers, and letters as the first character
        # excludes any dtype description that's also returned by the command line
        else: # .gdb
            count_cmd = COUNT_GEOSPATIAL_LAYERS.format(full_path = os.path.dirname(full_path)) 
            ogr_response = subprocess.check_output(shlex.split(count_cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
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
    if len(gpkg_tbl_names) != len(tables_in_gpkg):
        raise Exception("Clean geopackage table names for db upload and make sure they are unique.")

    return gpkg_tbl_names

def convert_cmd(input_full_path, output_full_path, _update = None, _overwrite = None, input_table = None, output_table = None):

    """
    Generate the cmd text to convert a geospatial file to another geospatial format

    :param input_full_path: Full path including file name for input
    :param output_full_path: Full path including file name for output
    :param _update: '-update' flag if adding a table to an existing gpkg
    :param _overwrite: '-overwrite' flag if overwriting output file
    :param input_table: Input table name if GPKG or GDB
    :param output_table: Input table name if GPKG
    """
    
    if '.shp' in input_full_path and '.gpkg' in output_full_path:
        cmd = WRITE_SHP_CMD_GPKG.format(shp_path = input_full_path,
                                        gpkg_path = output_full_path,
                                        _update = _update,
                                        _overwrite = _overwrite,
                                        gpkg_tbl = output_table)
    
    elif '.gpkg' in input_full_path and 'shp' in output_full_path:
        cmd = WRITE_GPKG_CMD_SHP.format(    gpkg_path = input_full_path,
                                            gpkg_tbl = input_table,
                                            shp_path=output_full_path
                                            )
    elif '.gdb' in input_full_path and '.gpkg' in output_full_path:
        cmd = WRITE_GDB_CMD_GPKG.format(    shp_path=input_full_path,
                                            feature_class = input_table,
                                            gpkg_path = output_full_path,
                                            gpkg_tbl = output_table,
                                            _update = _update,
                                            _overwrite = _overwrite,
                                            )
        
    return cmd
        
def write_cmd(dbo, full_path, input_tbl = None, table = None,
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
                                    gpkg_tbl = input_tbl,
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

def execute_cmd(cmd, dbo = None, cmd_env = None):

    if cmd_env is None:
        try:
            ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print('Ogr2ogr command failed. The Geopackage/Shapefile/feature class was not written.')

            if dbo:
                raise subprocess.CalledProcessError(cmd=print_cmd_string([dbo.password], cmd), returncode=1)
            else:
                # applies to the geospatial_convert 
                (f'Ogr2ogr command failed. Geospatial file was not converted.')

    else:
        try:
            ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT, env=cmd_env)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print(f'Ogr2ogr command failed. The file was not read in.')
            raise subprocess.CalledProcessError(cmd=print_cmd_string([dbo.password], cmd), returncode=1)
        
    return

def clean_table_name(table = None, input_tbl = None, full_path = None):
    """
    If a DB table name is not specified, name will be based on existing upload file
    or file's table name.

    :param table: Database table name
    :param input_tbl: Table of GPKG or GDB table
    :param full_path: Full file path
    :returns: Cleaned table name
    """

    if not table and input_tbl:
        table = input_tbl.replace('.gpkg', '').replace('.shp', '').lower()
        # if the gpkg_table is left blank, we will populate the name using input_gpkg
    elif not table and full_path.endswith(('.shp')):
        table = os.path.basename(full_path).replace('.shp', '').lower()

    return table

def set_up_geo_tbl_dict(dbo, full_path, table, input_tbl = None):

    """
    Sets up the dictionary for bulk uploading loop based on the type of geospatial input file

    :param dbo: Database connection where geospatial table will be uploaded
    :param full_path: Full file path
    :param table: Database table name to be uploaded
    :param input_tbl: If the file is a GPKG or GDB, this is the feature class or GPKG table name.
    :returns: Dictionary of table names to be uploaded
    """

    # create empty dictionary
    file_tbl_names = {}

    # take only the file name for cleaning
    file_name = os.path.basename(full_path)

    # if this ends up being a bulk upload, table gets overwritten anyway
    table = clean_table_name(table = table, input_tbl = input_tbl, full_path = file_name)

    ## BULK UPLOADING OPTION ##
    if not input_tbl and ('.gpkg' in full_path or '.gdb' in full_path):
        # retrieve all the table names from the geopackage
        # remove any feature class name that ends with .shp if applicable
        file_tbl_names = retrieve_input_tbl_names(dbo, full_path)

    elif file_name.endswith('.shp') and '.gdb' not in full_path:
        # exclude compressed files from this if statement
        file_tbl_names[file_name.replace('.shp', '')] = table
        
    elif file_name.endswith('.shp') and '.zip' in full_path and '.gdb' not in full_path:
        # this is for zip files containing sShps
        file_tbl_names[file_name.replace('.shp', '')] = table

    elif file_name.endswith('.shp') and '.gdb' not in full_path:
        # compressed files if statement
        file_tbl_names[file_name.replace('.shp', '')] = table
      
    elif '.gdb' in full_path and input_tbl:
        file_tbl_names[input_tbl.replace('.shp', '')] = table

    elif '.gpkg' in full_path and input_tbl:
        file_tbl_names[input_tbl] = table

    return file_tbl_names


def geospatial_assert_formats(path, output_file = None, bulk_optional = False):
    
    """
    Check the file types in the arguments for the geospatial functions
    :param path: Path argument from geospatial function
    :param output_file: Output file argument from geospatial_convert function, if applicable
    :param bulk_optional: (bool) .  Defaults to False.
    """

    # assert the INPUT file formats are correct
    if not (path.endswith('.shp') or any(x in path for x in ['.gpkg/', '.gpkg\\', '.gdb/', ''])): 
        raise ValueError("The input file must end with .shp. Or .gpkg or .gdb files must end with their table name")
    
    # only check this condition if no bulk upload involved
    if bulk_optional is False:
        if path.endswith(('.gpkg', '.gdb', '.gpkg/', '.gdb/', '.gdb\\', '.gpkg\\')):
            raise ValueError("Specify the gpkg or feature class table in the output name")
    
    # assert the OUTPUT file formats are correct
    if output_file:
        if not (output_file.endswith('.shp') or any(x in output_file for x in ['.gpkg/', '.gpkg\\'])):
            raise ValueError("The output file must end with .shp or .gpkg/[gpkg_tbl]. Cannot create new Geodatabase via GDAL")

    return