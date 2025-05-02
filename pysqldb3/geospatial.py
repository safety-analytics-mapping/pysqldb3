import shlex
import subprocess
import re
import os

from .cmds import *
from .sql import *
from .util import *
  
def list_gpkg_tables(file_name, path = None):

    """
    List all the tables contained in a geopackage file.

    :param file_name: Geopackage file name
    :param path: Optional file path
    """

    assert file_name.endswith('.gpkg'), "File name needs to end with .gpkg"

    if gpkg_exists(path = path, file_name = file_name):

        try:
            exists_cmd = f'ogrinfo {os.path.join(path, file_name)}'
            ogr_response = subprocess.check_output(exists_cmd, stderr=subprocess.STDOUT)

            # use regex to find all the tables in the geopackage
            tables_in_gpkg = re.findall(r"\\n\d+:\s(.*?)(?=\\r|\s\(.*\))", str(ogr_response))
            
            return tables_in_gpkg
            
        except subprocess.CalledProcessError as e:
            print(e)
    
    else:
            print("This geopackage file does not exist at this file location.")

def gpkg_exists(file_name, path = None):
    
    """
    Checks if a geopackage already exists at that location

    :param gpkg_name: Geopackage name, ends with .gpkg
    :param path: Optional file path
    """

    gpkg_exists = os.path.isfile(os.path.join(path, file_name))
                                
    return gpkg_exists

def gpkg_tbl_exists(gpkg_name, gpkg_tbl, path = None):
            
    """
    Checks if a geopackage table already exists, in case it isn't meant to be overwritten.
    :param gpkg_name: File name to check whether a certain table exists. Must end with .gpkg
    :param path: Optional file path
    """

    if gpkg_exists(path = path, file_name = gpkg_name):

        try:
            exists_cmd = f'ogrinfo {os.path.join(path, gpkg_name)}'
            ogr_response = subprocess.check_output(exists_cmd, stderr=subprocess.STDOUT)
            table_exists = re.findall(f"\b{gpkg_tbl}\b", str(ogr_response)) # only allows tables names with underscores, numbers, and letters

            if len(table_exists) == 0:
                gpkg_tbl_exists = False

            elif len(table_exists) > 0:
                gpkg_tbl_exists = True
            
        except:

            gpkg_tbl_exists = False

    else:

        gpkg_tbl_exists = False
        
    return gpkg_tbl_exists


def write_geospatial(dbo, path, output_file = None, table = None, schema = None, query = None, gpkg_tbl = None,
                        srid='2263', gdal_data_loc=GDAL_DATA_LOC, cmd = None, overwrite = False, print_cmd=False):
    
    """
    Converts a SQL or Postgresql query to a new Geospatial (Shapefile or GPKG) file.

    :param dbo: Database connection
    :param path (str): File path to the output file
    :param output_file (str): Optional name of the output file ending with .shp, .gdb, or .gpkg (if blank, use path)
    :param table (str): DB Table to be written to a GPKG
    :param schema (str): DB schema
    :param query (str): DB query whose output is to be written to a GPKG
    :param gpkg_tbl (str):  Name of the output table in the geopackage.
                            Required input if you write a query to gpkg.
                            Otherwise, optional if writing an existing db table to the gpkg; if blank, the table name
                            in the geopackage output will match the name of the input db table.
    :param srid (str): SRID for geometry. Defaults to 2263
    :param cmd (str): Command
    :param overwrite (bool): Overwrite the specific table in the geopackage; defaults to False
    :param print_cmd (bool): Optional flag to print the GDAL command being used; defaults to False
    :return:
    """

    ## INPUT CHECKS ##    
    # assert that a valid file format was input
    if output_file:
        assert output_file.endswith('.gpkg') or output_file.endswith('.shp') or output_file.endswith('.gdb'), "Output file needs to be .gpkg, .shp, or .gdb format"
        assert path, "Fill in the file path to the output file"
    else:
        assert path.endswith('.gpkg') or path.endswith('.shp') or path.endswith('.gdb'), "Output path needs to end with .gpkg, .shp, or .gdb if no file name is supplied"

    path, output_file = parse_geospatial_file_path(path, output_file)
    full_path = os.path.join(path, output_file)

    original_temp_flag = dbo.allow_temp_tables
    dbo.allow_temp_tables = True
    
    if not query and not table:
        # this would only happen if query_to_geospatial() wasn't run and instead, the user runs write_geospatial_file(), since query is required
        raise Exception('You must specify the db table to be written.')
    
    if query and not gpkg_tbl and output_file.endswith('.gpkg'):
        raise Exception ('You must specify a gpkg_tbl name in the function for the output table if you are writing a db query to a geopackage.')
    
    if table:
        qry = f"SELECT * FROM {schema}.{table}"
    elif not table and not query:
        raise Exception('Please specify the table to be written to the Geospatial file')
    else:
        # this logic is for queries

        # Makes a temp table name
        tmp_table_name = f"tmp_query_to_shp_{dbo.user}_{str(datetime.datetime.now())[:16].replace('-', '_').replace(' ', '_').replace(':', '')}"

        # Create temp table to get column types
        try:
            # Drop the temp table
            if dbo.type == PG:
                dbo.query(f"drop table {tmp_table_name}", internal=True, strict=False)
            elif dbo.type == MS:
                dbo.query(f"drop table #{tmp_table_name}", internal=True, strict=False)
        except Exception as e:
            print(e)
            pass    
        
        if dbo.type == PG:
            dbo.query(f"""    
            create temp table {tmp_table_name} as     
            select * 
            from ({query}) q 
            limit 10
            """, internal=True)
        elif dbo.type == MS:
            dbo.query(f"""        
            select top 10 * 
            into #{tmp_table_name}
            from ({query}) q 
            """, internal=True)

        # Extract column names, including datetime/timestamp types, from results
        if dbo.type == PG:
            col_df = dbo.dfquery(f"""
            SELECT *
            FROM
            INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{tmp_table_name}'
            """, internal = True)

            cols = ['\\"' + c + '\\"' for c in list(col_df['column_name'])]
            dt_col_names = ['\\"' + c + '\\"' for c in list(
                col_df[col_df['data_type'].str.contains('datetime') | col_df['data_type'].str.contains('timestamp')][
                    'column_name'])]

        elif dbo.type == MS:
            col_df = dbo.dfquery(f"""
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
                [object_id] = OBJECT_ID(N'tempdb.dbo.#{tmp_table_name}');
            """, internal=True)

            cols = ['[' + c + ']' for c in list(col_df['column'])]
            dt_col_names = ['[' + c + ']' for c in list(
                col_df[col_df['type'].str.contains('datetime') | col_df['type'].str.contains('timestamp')]['column'])]

        # Make string of columns to be returned by select statement
        return_cols = ' , '.join([c for c in cols if c not in dt_col_names])

        # If there are datetime/timestamp columns:
        if len(dt_col_names) > 0:
            if dbo.type == PG:
                print_cols = str([str(c[2:-2]) for c in dt_col_names])

            if dbo.type == MS:
                print_cols = str([str(c[1:-1]) for c in dt_col_names])

            print(f"""
            The following columns are of type datetime/timestamp: \n
            {print_cols}
            
            Shapefiles don't support datetime/timestamps with both the date and time. Each column will be split up
            into colname_dt (of type date) and colname_tm (of type **string/varchar**). 
            """)

            # Add the date and time (casted as a string) to the output
            for col_name in dt_col_names:
                if dbo.type == PG:
                    shortened_col = col_name[2:-2][:7]
                    return_cols += ' , cast(\\"{col}\\" as date) \\"{short_col}_dt\\", ' \
                                   'cast(cast(\\"{col}\\" as time) as varchar) \\"{short_col}_tm\\" '.format(
                                    col=col_name[2:-2], short_col=shortened_col)
                elif dbo.type == MS:
                    shortened_col = col_name[1:-1][:7]
                    return_cols += " , cast([{col}] as date) [{short_col}_dt], cast(cast([{col}] as time) as varchar)" \
                                   " [{short_col}_tm] ".format(
                                    col=col_name[1:-1], short_col=shortened_col)

        # Wrap the original query and select the non-datetime/timestamp columns and the parsed out dates/times
        qry = f"select {return_cols} from ({query}) q "

    if output_file.endswith('.gpkg') and "." in output_file[:-5]:
        output_file = output_file[:-5].replace(".", "_") + ".gpkg"
        print(' The "." character is not allowed in output gpkg file names. Any "." have been removed.')
    elif not output_file.endswith(".gpkg") and "." in output_file[-5]:
        output_file = output_file[:-5].replace(".", "_") + '.gpkg'
        print(' The "." character is not allowed in output gpkg file names. Any "." have been removed.')
    elif (output_file.endswith(".shp") or output_file.endswith(".gdb")) and "." in output_file[:-4]:
        output_file = output_file[:-4].replace(".", "_") + ".shp"
        print(' The "." character is not allowed in output shp file names. Any "." have been removed.')
    elif not (output_file.endswith(".shp") or output_file.endswith(".gdb"))and "." in output_file[-4]:
        output_file = output_file[:-4].replace(".", "_") + '.shp'
        print(' The "." character is not allowed in output shp file names. Any "." have been removed.')

    if not gpkg_tbl:
        gpkg_tbl = table

    if not schema:
            schema = dbo.default_schema

    # overwrite vs update vs an issue has arisen
    if overwrite:
        # if explict overwrite, then create command line as directed
        _update = ''
        _overwrite = '-overwrite'
    
    elif not overwrite and not gpkg_exists(path = path, file_name = output_file):
        # this geopackage does not exist so create as if new
        _update = ''
        _overwrite = ''
    
    elif not overwrite and gpkg_exists(path = path, file_name = output_file): # check if the geopackage already exists
        
        table_exists = gpkg_tbl_exists(path = path, gpkg_name = output_file, gpkg_tbl = gpkg_tbl)
        
        if table_exists == True:
            print("The table name to be exported already exists in the geopackage. Either change to Overwrite = True or check the name of the table to be copied.")
            exit
    
    # update allows you to add an extra table into an existing geopackage
        _update = '-update'
        _overwrite = ''

    # run the final command
    if not cmd:
        if dbo.type == 'PG' and output_file.endswith('.gpkg'):
            cmd = WRITE_GPKG_CMD_PG.format(     full_path=full_path,
                                                host=dbo.server,
                                                username=dbo.user,
                                                db=dbo.database,
                                                password=dbo.password,
                                                pg_sql_select=qry,
                                                gpkg_tbl = gpkg_tbl,
                                                tbl_name = table,
                                                _overwrite = _overwrite,
                                                _update = _update,
                                                srid=srid,
                                                gdal_data=gdal_data_loc)
            
        elif dbo.type == 'MS' and output_file.endswith('.gpkg'):
            if dbo.LDAP:
                cmd = WRITE_GPKG_CMD_MS.replace(";UID={username};PWD={password}", "").format(
                    full_path = full_path,
                    host=dbo.server,
                    db=dbo.database,
                    ms_sql_select=qry,
                    gpkg_tbl = gpkg_tbl,
                    _overwrite = _overwrite,
                    _update = _update,
                    tbl_name = table,
                    srid=srid,
                    gdal_data=gdal_data_loc
                )
            else:
                cmd = WRITE_GPKG_CMD_MS.format(     full_path = full_path,
                                                    host=dbo.server,
                                                    username=dbo.user,
                                                    db=dbo.database,
                                                    password=dbo.password,
                                                    ms_sql_select=qry,
                                                    gpkg_tbl = gpkg_tbl,
                                                    _overwrite = _overwrite,
                                                    _update = _update,
                                                    tbl_name = table,
                                                    srid=srid,
                                                    gdal_data=gdal_data_loc)
                
        elif dbo.type == 'PG' and (output_file.endswith('.shp') or output_file.endswith('.gdb')):

            cmd = WRITE_SHP_CMD_PG.format(  full_path = full_path,
                                            host = dbo.server,
                                            username = dbo.user,
                                            db = dbo.database,
                                            password = dbo.password,
                                            pg_sql_select = qry,
                                            srid = srid,
                                            gdal_data = gdal_data_loc)
            
        elif dbo.type == 'MS' and (output_file.endswith('.shp') or output_file.endswith('.gdb')):

            if dbo.LDAP:
                cmd = WRITE_SHP_CMD_MS.replace(";UID={username};PWD={password}", "").format(
                    full_path = full_path,
                    host = dbo.server,
                    db = dbo.database,
                    ms_sql_select = qry,
                    srid = srid,
                    gdal_data = gdal_data_loc
                )

            else:
                cmd = WRITE_SHP_CMD_MS.format(      full_path = full_path,
                                                    host = dbo.server,
                                                    username = dbo.user,
                                                    db = dbo.database,
                                                    password = dbo.password,
                                                    ms_sql_select=qry,
                                                    srid = srid,
                                                    gdal_data = gdal_data_loc)

    if print_cmd:
        print(print_cmd_string([dbo.password], cmd))

    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed. The Geopackage/Shapefile/feature class was not written.')
        raise subprocess.CalledProcessError(cmd=print_cmd_string([dbo.password], cmd), returncode=1)

    if table:
        print(f'{output_file} \nwritten to: {path}\ngenerated from: {table}')
    else:
        print(f'{output_file} geospatial file \nwritten to: {path}\ngenerated from: {query}')

    # Reset the temp flag
    dbo.last_query = qry
    dbo.allow_temp_tables = original_temp_flag
        
def geospatial_convert(input_path, input_file = None, export_path = None, output_file = None, gpkg_tbl = None, overwrite = False, print_cmd = False):
    
    """
    Converts a single Geospatial file or table to another Geospatial format.
    Please use convert_geospatial_bulk() if you want to convert an entire Geopackage file to multiple Shapefiles.

    :param input_path: Path for the geospatial input (required if no input_file)
    :param input_file(str): File name for input (ends with .shp, .gdb, or .gpkg). Optional if input_path has full directory.
    :param export_path: Folder directory to place the geospatial output.
                        You cannot specify the shapefiles' names as they are copied from the table names within the geopackage.
                        Will default to input_path if no export_path supplied
    :param output_file (str): File name for ouput (ends with .shp, .gdb, or .gpkg). Optional if export_path has full directory.
    :param gpkg_tbl (str):  If the input format is a SHP, this will be the output table name in the Geopackage.
                            Leave blank if you want the output table name to be the input .shp file's name.
                            If the input format is a GPKG, this is the single GPKG table that will be converted.
    :param overwrite (bool): Boolean; defaults to False. Overwrite table in the geopackage if the table name already exists in the file.
    :param print_cmd (bool): Print command
    """

    # assert that file formats were input correctly
    if input_file:
        assert input_file.endswith('.shp') or input_file.endswith('.gpkg') or input_file.endswith('.gdb'), "The input file must end with .shp or .gpkg or .gdb"
    else:
        assert input_path.endswith('.shp') or input_path.endswith('.gpkg') or input_path.endswith('.gdb'), "The input path must end with .shp or .gpkg or .gdb if no input_file supplied."

    if output_file:
        assert output_file.endswith('.shp') or output_file.endswith('.gpkg') or output_file.endswith('.gdb'), "The output file must end with .shp or .gpkg or .gdb"
    elif export_path:
        assert export_path.endswith('.shp') or export_path.endswith('.gpkg') or export_path.endswith('.gdb'), "The output path must end with .shp or .gpkg or .gdb if no output_file supplied."
    
    # set up the correct file paths
    input_path, input_file = parse_geospatial_file_path(input_path, input_file)
    export_path, output_file = parse_geospatial_file_path(export_path, output_file)

    # set an export path to the geospatial file if it is not manually set up
    if not export_path:
        export_path = input_path

    # create full paths from these outputs
    input_full_path = os.path.join(input_path, input_file)
    output_full_path = os.path.join(export_path, output_file)


    assert input_full_path[:-4] != output_full_path[:-4], "This function does not allow you to convert a file to the same format"

    # if no gpkg_tbl name given and we convert a shp file, name the table consistent with the shapefile
    if not gpkg_tbl and (input_file.endswith('.shp') or input_file.endswith('.gdb')):
        gpkg_tbl = input_file.replace('.shp', '')
        gpkg_tbl = gpkg_tbl.replace('.gdb', '')

    # set variables
    _overwrite = ''
    _update = ''

    # if the output file is a gpkg, do these additional checks
    if output_file.endswith('.gpkg'):
        # if gpkg exists and overwrite is explicityly written
        if overwrite == True and gpkg_exists(path = export_path, file_name = output_file): 
            _overwrite = '-overwrite'
    
        # if gpkg exists and overwrite was not explicitly called
        if gpkg_exists(path = export_path, file_name = output_file) and not gpkg_tbl_exists(path = export_path, gpkg_name = output_file, gpkg_tbl = gpkg_tbl) and overwrite == False:
            _update = '-update' # then add the table to the gpkg
    
        # if the gpkg and table exists but no overwrite was called
        if gpkg_exists(path = export_path, file_name = output_file) and gpkg_tbl_exists(path = export_path, gpkg_name = output_file, gpkg_tbl = gpkg_tbl) and overwrite == False:
            print("The table name to be copied to the geopackage already exists. Either change to Overwrite = True or check the name of the table to be copied.")
            exit # stop process so user can fix

    # update the commandexport_path
    if (input_file.endswith('shp') or input_file.endswith('.gdb')) and output_file.endswith('gpkg'):
        cmd = WRITE_SHP_CMD_GPKG.format(shp_path = input_full_path,
                                        gpkg_path = output_full_path,
                                        _update = _update,
                                        _overwrite = _overwrite,
                                        gpkg_tbl = gpkg_tbl)
    
    else:
        cmd = WRITE_GPKG_CMD_SHP.format(    gpkg_path = input_full_path,
                                            gpkg_tbl = gpkg_tbl,
                                            shp_path=output_full_path
                                            )

    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed. The Geopackage/feature class was not written.')
    
    if print_cmd:
        print(cmd)
    return
    

def gpkg_to_shp_bulk(   input_path,
                        input_file = None,
                        export_path = None,
                        print_cmd = False):
    """
    Converts an entire Geopackage (all tables) to a Shapefile.
    The output Shapefile name will match the name of the geopackage table to be copied.

    :param input_path: str File path to geopackage input.
    :param input_file(str): File name for input (ends with .gpkg). Optional if input_path includes file name.
    :param export_path: str The folder directory to place the shapefiles output.
                        You cannot specify the shapefiles' names as they are copied from the table names within the geopackage.
    :param print_cmd (bool): Print command
    """

    assert input_file.endswith('.gpkg'), "The input file must end with .gpkg and the output file with .shp"

    try:
        count_cmd = COUNT_GPKG_LAYERS.format(full_path = os.path.join(input_path, input_file))
        ogr_response = subprocess.check_output(shlex.split(count_cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
        tables_in_gpkg = re.findall(r"\\n\d+:\s(.*?)(?=\\r|\s\(.*\))", str(ogr_response)) 

        for t_i_g in tables_in_gpkg:
            geospatial_convert(gpkg_tbl = t_i_g, input_path = input_path, export_path = export_path,
                               input_file = input_file, output_file = t_i_g +'.shp', print_cmd = print_cmd)
    
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed. The Geopackage was not read in.')
        raise subprocess.CalledProcessError(count_cmd, returncode=1)

    return


def input_geospatial_file(dbo, path, input_file = None, schema = None, table = None, feature_class = None, gpkg_tbl = None, port = 5432,
                            srid = '2263', gdal_data_loc=GDAL_DATA_LOC, precision=False, private=False, encoding=None, skip_failures = '',
                            temp = True, days = 7, print_cmd=False):
    """
    Imports single Geopackage table, Geodatabase feature class, or Shp to database. This uses GDAL to generate the table.

    :param dbo: Database connection
    :param path: Input file path
    :param input_file (str): Optional file name for input (ends with .shp, .gdb, .gpkg); if none, fill in path
    :param schema (str): Schema that the imported geospatial data will be found
    :param table (str): (Optional) name for the uploaded db table. If blank, it will default to the gpkg_tbl or shp file name.
    :param feature_class (str): (Optional) If importing a Geodatabase, enter the feature class name (ends with .shp)
    :param gpkg_tbl (str): (Optional) If the input file is a Geopackage, list the specific gpkg table to upload.
    :param srid (str): SRID for geometry. Defaults to 2263
    :param gdal_data_loc: File path fo the GDAL data (defaults to C:\\Program Files (x86)\\GDAL\\gdal-data)
    :param precision: Sets precision flag in ogr (defaults to -lco precision=NO)
    :param private: Flag for permissions in database (Defaults to False - will only grant select to public)
    :param encoding: encoding of data within the geospatial file
    :param skip_failures (str): Defualts to ''
    :param temp: If True any new tables will be logged for deletion at a future date; defaults to True
    :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
    :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
    :return:
    """

    if input_file:
        assert input_file.endswith('.shp') or input_file.endswith('.gpkg') or input_file.endswith('.gdb'), "The input file should end with .gpkg, .shp, or .gdb"
        assert path, "Fill in the file path to the input file"
    else:
        assert path.endswith('.shp') or path.endswith('.gpkg') or path.endswith('.gdb'), "The path should end with .gpkg, .shp, or .gdb"

    path, input_file = parse_geospatial_file_path(path, input_file)

    # Use default schema from db object
    if not schema:
        schema = dbo.default_schema

    if precision:
        precision = '-lco precision=NO'
    else:
        precision = ''

    if path.endswith('.zip') or input_file.endswith('.zip'):
        path = '/vsizip/' + path
        full_path = path
    else:
        full_path = os.path.join(path, input_file)

    if feature_class:
        assert feature_class.endswith('.shp') and input_file.endswith('.gdb'), "input_file must end with .gdb & feature_class must end with .shp"

    if table:
        assert table == re.sub(r'[^A-Za-z0-9_]+', r'', table) # make sure the name will load into the database
    elif not table and input_file.endswith('.gpkg'):
        # clean the geopackage table name
        table = re.sub(r'[^A-Za-z0-9_]+', r'', gpkg_tbl) # clean the table name in case there are special characters
    elif not table and gpkg_tbl:
        table = gpkg_tbl.replace('.gpkg', '').replace('.shp', '').lower()
        # if the gpkg_table is left blank, we will populate the name using input_gpkg
    elif not table and (input_file.endswith('.shp') or input_file.endswith('.gdb')):
        table = input_file.replace('.shp', '').lower()
        table = table.replace('.gdb', '').lower()
    else:
        table = input_file[:-4].lower()

    table = table.lower()

    if dbo.table_exists(table = table, schema = schema):

        if input_file.endswith('.shp') or input_file.endswith('.gdb'):
            del_indexes(dbo, schema, table)
    
        print(f'Deleting existing table {schema}.{table}')
        
        if dbo.type == 'MS':
            dbo.drop_table(schema=schema, table=table)
        else:
            dbo.drop_table(schema, table, cascade = True)

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

    elif dbo.type == 'PG' and (input_file.endswith('.shp') or input_file.endswith('.gdb')) and not feature_class:
    
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

    elif dbo.type == 'MS' and (input_file.endswith('.shp') or input_file.endswith('.gdb')) and not feature_class:
        
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

    cmd_env = os.environ.copy()

    if encoding and encoding.upper() == 'LATIN1':
        cmd_env['PGCLIENTENCODING'] = 'LATIN1'

    if encoding and encoding.upper().replace('-', '') == 'UTF8':
        cmd_env['PGCLIENTENCODING'] = 'UTF8'

    if print_cmd:
        print(print_cmd_string([dbo.password], cmd))

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

    # needs to be rewritten since this is not possible in geospatial.py
    if not private:
        try:
            dbo.query(f'grant select on {schema}."{table}" to public;',
                timeme=False, internal=True, strict=True)
        except:
            pass

    rename_geom(dbo, schema, table)
    dbo.tables_created.append((dbo.server, dbo.database, schema,  table))
    
    if temp:
        dbo.run_table_logging([schema + "." + table], days=days)


def input_geospatial_bulk(path, dbo, input_file = None, schema = None, port = 5432, srid = '2263', gdal_data_loc=GDAL_DATA_LOC,
                precision=False, private=False, encoding=None, print_cmd=False, temp = True, days = 7):

    """
    Reads all tables within a Geopackage/Geodatabase file into SQL or Postgresql as tables.
    Function is NOT applicable to Shapefiles.

    :param path: Input file path for geopackage
    :param dbo: Database connection
    :param input_file(str): Optional file name for input (must end with .gpkg or .gdb)
    :param schema (str): Schema that the imported geopackage data will be found
    :param port (int): Optional port
    :param srid (str): SRID for geometry. Defaults to 2263
    :param gdal_data_loc:
    :param precision: Default to False
    :param private: Default to False
    :param encoding: encoding of data within Geopackage
    :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
    :param temp: If True any new tables will be logged for deletion at a future date; defaults to True
    :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
    :return:
    """
    if input_file:
        assert input_file.endswith('.gpkg') or input_file.endswith('.gdb'), "You cannot bulk upload Shapefiles, you can only bulk upload tables within a Geopackage/database"
    else:
        assert path.endswith('.gpkg'), "Your path must end with .gpkg (Geopackage only) if there is no input_file"
    
    # set the full path
    path, input_file = parse_geospatial_file_path(path, input_file)
    full_path = os.path.join(path, input_file)
    
    # retrieve all the table names from the geopackage
    
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

    # create empty dictoinary
    gpkg_tbl_names = {}

    # create a list of cleaned table names from the list that was generated
    for t_i_g in tables_in_gpkg:
        insert_val = re.sub(r'[^A-Za-z0-9_]+', r'', t_i_g)
        gpkg_tbl_names[t_i_g] = insert_val # add the cleaned name

    # assert that the new cleaned names are unique. if not, we won't get the same dimensions
    assert len(gpkg_tbl_names) == len(tables_in_gpkg), "Clean the geopackage table names so they can be uploaded as tables (by removing special characters other than _) and make sure they are unique."

    for gpkg_tbl, table in gpkg_tbl_names.items():

        if full_path.endswith('.gpkg'):
            input_geospatial_file(dbo = dbo, input_file = input_file, table = table, path = path, gpkg_tbl = gpkg_tbl, schema = schema, port = port, srid = srid, gdal_data_loc=gdal_data_loc,
                                precision=precision, private=private, encoding=encoding, print_cmd=print_cmd, temp = temp, days = days)
            
        else:
            input_geospatial_file(dbo = dbo, input_file = input_file, table = table, path = path, schema = schema, port = port, srid = srid, gdal_data_loc=gdal_data_loc,
                                    feature_class = table + '.shp', precision=precision, private=private, encoding=encoding, print_cmd=print_cmd, temp = temp, days = days)


def del_indexes(dbo, schema, table):
    """
    Drops indexes
    :dbo: Database connection
    :param schema: Schema for table whose index will be deleted
    :param table: Table name whose index will be deleted
    """
    if dbo.type == 'PG':
        dbo.query(SHP_DEL_INDICES_QUERY_PG.format(s=schema, t=table), internal=True)
        indexes_to_delete = dbo.internal_data

        for _ in list(indexes_to_delete):
            table_name, schema_name, index_name, column_name = _
            if 'pkey' not in index_name and 'PK' not in index_name:
                dbo.query(f'DROP INDEX {schema}.{index_name}',
                                strict=False, internal=True)
    else:
        dbo.query(SHP_DEL_INDICES_QUERY_MS.format(s=schema, t=table), internal=True)
        indexes_to_delete = dbo.internal_data

        for _ in list(indexes_to_delete):
            table_name, index_name, column_name, idx_typ = _
            if 'pkey' not in index_name and 'PK' not in index_name:
                dbo.query(f'DROP INDEX {table}.{index_name}', strict=False, internal=True)