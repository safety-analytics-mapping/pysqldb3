import datetime
import decimal
import re
import os
import configparser
import pyarrow

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
    names_arr = []
    start = 0

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
            filetypes=(("Shapefile", "*.shp"), ("All Files", "*.*")),
            defaultextension=".shp"
        )
        return output_file_name


def print_cmd_string(password_list, cmd_string):
    for p in password_list:
        if p:
            cmd_string = cmd_string.replace(p, '*' * len(p))
    return cmd_string


def parse_shp_path(path=None, shp_name=None):
    """
    Standardizes extracting shpfile name from path process, if shp_name provided that will override anything in the path
    :param path: folder path with or without shp
    :param shp_name: shapefile name
    :return: path (without shp), shp_name
    """
    # type: (str, str)
    if not path:
        return path, shp_name

    if path.endswith('.shp'):
        shp = os.path.basename(path)
        path = path.replace(shp, '')

    if not shp_name:
        shp_name = shp

    return path, shp_name
