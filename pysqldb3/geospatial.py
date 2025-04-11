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


def write_geospatial(dbo, 
                        output_file, path=None, table = None, schema = None, query = None, gpkg_tbl = None,
                        srid='2263', gdal_data_loc=GDAL_DATA_LOC, cmd = None, overwrite = False, print_cmd=False):
    
    """
    Converts a SQL or Postgresql query to a new Geospatial (Shapefile or GPKG) file.

    :param dbo: Database connection
    :param output_file (str): The name of the output file ending with .shp, .dbf, or .gpkg
    :param path (str): Optional file path to the output file
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
    
    # assert that a valid file format was input
    assert output_file.endswith('.gpkg') or output_file.endswith('.shp') or output_file.endswith('.dbf'), "Output file needs to be .gpkg, .shp, or .dbf format"

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
        qry = f"SELECT * FROM ({query}) x"

    if output_file.endswith('.gpkg') and "." in output_file[:-5]:
        output_file = output_file[:-5].replace(".", "_") + ".gpkg"
        print(' The "." character is not allowed in output gpkg file names. Any "." have been removed.')
    elif not output_file.endswith(".gpkg") and "." in output_file[-5]:
        output_file = output_file[:-5].replace(".", "_") + '.gpkg'
        print(' The "." character is not allowed in output gpkg file names. Any "." have been removed.')
    elif (output_file.endswith(".shp") or output_file.endswith(".dbf")) and "." in output_file[:-4]:
        output_file = output_file[:-4].replace(".", "_") + ".shp"
        print(' The "." character is not allowed in output shp file names. Any "." have been removed.')
    elif not (output_file.endswith(".shp") or output_file.endswith(".dbf"))and "." in output_file[-4]:
        output_file = output_file[:-4].replace(".", "_") + '.shp'
        print(' The "." character is not allowed in output shp file names. Any "." have been removed.')

    if not gpkg_tbl:
        gpkg_tbl = table
        if gpkg_tbl:
            print('The gpkg_tbl argument in write_geospatial() overrides the class input for gpkg_tbl.')

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

    if not cmd:
        if dbo.type == 'PG' and output_file.endswith('.gpkg'):
            cmd = WRITE_GPKG_CMD_PG.format(export_path=path,
                                                gpkg_name=output_file,
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
                    export_path=path,
                    gpkg_name=output_file,
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
                cmd = WRITE_GPKG_CMD_MS.format(export_path=path,
                                                    gpkg_name=output_file,
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
                
        elif dbo.type == 'PG' and (output_file.endswith('.shp') or output_file.endswith('.dbf')):

            cmd = WRITE_SHP_CMD_PG.format(export_path=path,
                                            shpname = output_file,
                                            host = dbo.server,
                                            username = dbo.user,
                                            db = dbo.database,
                                            password = dbo.password,
                                            pg_sql_select = qry,
                                            srid = srid,
                                            gdal_data = gdal_data_loc)
            
        elif dbo.type == 'MS' and (output_file.endswith('.shp') or output_file.endswith('.dbf')):

            if dbo.LDAP:
                cmd = WRITE_SHP_CMD_MS.replace(";UID={username};PWD={password}", "").format(
                    export_path = path,
                    shpname = output_file,
                    host = dbo.server,
                    db = dbo.database,
                    ms_sql_select = qry,
                    srid = srid,
                    gdal_data = gdal_data_loc
                )

            else:
                cmd = WRITE_SHP_CMD_MS.format(export_path=path,
                                                    shpname=output_file,
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
        
def geospatial_convert(input_file, output_file, input_path = None, export_path = None, gpkg_tbl = None, overwrite = False, print_cmd = False):
    
    """
    Converts a single Geospatial file or table to another Geospatial format.
    Please use convert_geospatial_bulk() if you want to convert an entire Geopackage file to multiple Shapefiles.

    :param input_file(str): File name for input (ends with .shp, .dbf, or .gpkg)
    :param output_file (str): File name for ouput (ends with .shp, .dbf, or .gpkg)
    :param input_path: Optional folder director for the geospatial input.
    :param export_path: Optional folder directory to place the geospatial output.
                        You cannot specify the shapefiles' names as they are copied from the table names within the geopackage.
    :param gpkg_tbl (str):  If the input format is a SHP, this will be the output table name in the Geopackage.
                            Leave blank if you want the output table name to be the input .shp file's name.
                            If the input format is a GPKG, this is the single GPKG table that will be converted.
    :param overwrite (bool): Boolean; defaults to False. Overwrite table in the geopackage if the table name already exists in the file.
    :param print_cmd (bool): Print command
    """

    # assert that file formats were input correctly
    assert input_file.endswith('.shp') or input_file.endswith('.gpkg') or input_file.endswith('.dbf'), "The input file must end with .shp or .gpkg"
    assert output_file.endswith('.shp') or output_file.endswith('.gpkg') or output_file.endswith('.dbf'), "The output file must end with .shp or .gpkg"
    assert input_file[:-4] != output_file[:-4], "This function does not allow you to convert a file to the same format"
    
    if not input_path:
        input_path = file_loc('folder')

    # set an export path to the geospatial file if it is not manually set up
    if not export_path:
        export_path = input_path

    # if no gpkg_tbl name given and we convert a shp file, name the table consistent with the shapefile
    if not gpkg_tbl and (input_file.endswith('.shp') or input_file.endswith('.dbf')):
        gpkg_tbl = input_file.replace('.shp', '')
        gpkg_tbl = input_file.replace('.dbf', '')

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
    if (input_file.endswith('shp') or input_file.endswith('dbf')) and output_file.endswith('gpkg'):
        cmd = WRITE_SHP_CMD_GPKG.format(shp_name = input_file,
                                        gpkg_name = output_file,
                                        full_path = input_path,
                                        _update = _update,
                                        _overwrite = _overwrite,
                                        gpkg_tbl = gpkg_tbl)
    
    else:
        cmd = WRITE_GPKG_CMD_SHP.format(    full_path = input_path,
                                            gpkg_name = input_file,
                                            gpkg_tbl = gpkg_tbl,
                                            export_path=export_path,
                                            shp_name=output_file
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
    

def gpkg_to_shp_bulk(   input_file,
                        input_path = None,
                        export_path = None,
                        print_cmd = False):
    """
    Converts an entire Geopackage (all tables) to a Shapefile.
    The output Shapefile name will match the name of the geopackage table to be copied.

    :param gpkg_name (str): file name for geopackage input (should end in .gpkg)
    :param input_path: str Optional file path to geopackage input.
    :param export_path: str The folder directory to place the shapefiles output.
                        You cannot specify the shapefiles' names as they are copied from the table names within the geopackage.
    :param input_file(str): File name for input (ends with .gpkg)
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


def input_geospatial_file(input_file, dbo, schema = None, table = None, feature_class = False, path = None, gpkg_tbl = None, port = 5432,
                            srid = '2263', gdal_data_loc=GDAL_DATA_LOC, precision=False, private=False, encoding=None, zip = False, skip_failures = '',
                            temp = True, days = 7, print_cmd=False):
    """
    Imports single Geopackage table, Shp feature class, or Shp to database. This uses GDAL to generate the table.

    :param input_file(str): File name for input (ends with .shp, .dbf, .gpkg)
    :param dbo: Database connection
    :param schema (str): Schema that the imported geospatial data will be found
    :param table (str): (Optional) name for the uploaded db table. If blank, it will default to the gpkg_tbl or shp file name.
    :param feature_class (bool): Import only 1 feature class (input_file must end with .shp or .dbf)
    :param path: Optional file path
    :param gpkg_tbl (str): (Optional) If the input file is a Geopackage, list the specific gpkg table to upload.
    :param srid (str): SRID for geometry. Defaults to 2263
    :param gdal_data_loc: File path fo the GDAL data (defaults to C:\\Program Files (x86)\\GDAL\\gdal-data)
    :param precision: Sets precision flag in ogr (defaults to -lco precision=NO)
    :param private: Flag for permissions in database (Defaults to False - will only grant select to public)
    :param encoding: encoding of data within the geospatial file
    :param zip: Optional flag needed if reading from a zipped file; defaults to False
    :param skip_failures (str): Defualts to ''
    :param temp: If True any new tables will be logged for deletion at a future date; defaults to True
    :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
    :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
    :return:
    """

    assert input_file.endswith('.shp') or input_file.endswith('.gpkg') or input_file.endswith('.dbf'), "The input file should end with .gpkg, .shp, or .dbf"

    # Use default schema from db object
    if not schema:
        schema = dbo.default_schema

    if precision:
        precision = '-lco precision=NO'
    else:
        precision = ''

    if not all([path, input_file]):
        input_file = file_loc('file', 'Missing file info - Opening search dialog...')
        input_file = os.path.basename(input_file)
        path = os.path.dirname(input_file)

    if zip:
        path = '/vsizip/' + path
        full_path = path
    else:
        path = path
        full_path = os.path.join(path, input_file)

    if feature_class == True:
        assert input_file.endswith('.shp') or input_file.endswith('.dbf'), "You cannot select feature_class = True if the input_file does not end with .shp or .dbf"

    if table:
        assert table == re.sub(r'[^A-Za-z0-9_]+', r'', table) # make sure the name will load into the database
    elif not table and input_file.endswith('.gpkg'):
        # clean the geopackage table name
        table = re.sub(r'[^A-Za-z0-9_]+', r'', gpkg_tbl) # clean the table name in case there are special characters
    elif not table and gpkg_tbl:
        table = gpkg_tbl.replace('.gpkg', '').replace('.shp', '').lower()
        # if the gpkg_table is left blank, we will populate the name using input_gpkg
    elif not table and (input_file.endswith('.shp') or input_file.endswith('.dbf')):
        table = input_file.replace('.shp', '').lower()
        table = input_file.replace('.dbf', '').lower()
    else:
        table = input_file[:-4].lower()

    table = table.lower()

    if dbo.table_exists(table = table, schema = schema):

        if input_file.endswith('.shp') or input_file.endswith('.dbf'):
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

    elif dbo.type == 'PG' and (input_file.endswith('.shp') or input_file.endswith('.dbf')) and feature_class == False:
    
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

    elif dbo.type == 'MS' and (input_file.endswith('.shp') or input_file.endswith('.dbf'))and feature_class == False:
        
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
    
    elif dbo.type == 'PG' and (input_file.endswith('.shp') or input_file.endswith('.dbf'))and feature_class == True:
        
        cmd = READ_FEATURE_CMD.format(
            gdal_data = gdal_data_loc,
            srid = srid,
            host =  dbo.server,
            dbname = dbo.database,
            user = dbo.user,
            password= dbo.password,
            gdb = path,
            feature = input_file[:-4],
            tbl_name = table,
            sch = schema
        )
    elif dbo.type == 'MS' and (input_file.endswith('.shp') or input_file.endswith('.dbf')) and feature_class == True:
            # TODO: add LDAP version trusted_connection=yes
        cmd = READ_FEATURE_CMD_MS.format(
                gdal_data = gdal_data_loc,
                srid = srid,
                ms_server = dbo.server,
                ms_db = dbo.database,
                ms_user = dbo.user,
                ms_pass = dbo.password,
                gdb = path,
                feature= input_file[:-4],
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


def input_gpkg_bulk(input_file, dbo, schema = None, port = 5432, srid = '2263', gdal_data_loc=GDAL_DATA_LOC,
                precision=False, private=False, encoding=None, zip = False, path = None, print_cmd=False, temp = True, days = 7):

    """
    Reads all tables within a GEOPACKAGE file into SQL or Postgresql as tables.
    Function is NOT applicable to Shapefiles.

    :param input_file(str): File name for input (must end with .gpkg)
    :param dbo: Database connection
    :param schema (str): Schema that the imported geopackage data will be found
    :param port (int): Optional port
    :param srid (str): SRID for geometry. Defaults to 2263
    :param gdal_data_loc:
    :param precision: Default to False
    :param private: Default to False
    :param encoding: encoding of data within Geopackage
    :param path: Optional file path
    :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
    :param temp: If True any new tables will be logged for deletion at a future date; defaults to True
    :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
    :return:
    """

    assert input_file.endswith('.gpkg'), "You cannot bulk upload Shapefiles, you can only bulk upload tables within a Geopackage"

    if not path:
        input_file = file_loc('file', 'Missing file info - Opening search dialog...')
        input_file = os.path.basename(input_file)
        path = os.path.dirname(input_file)

    # set the full path
    full_path = os.path.join(path, input_file)
    
    # retrieve all the table names from the geopackage
    
    try:
        count_cmd = COUNT_GPKG_LAYERS.format(full_path = full_path) 
        ogr_response = subprocess.check_output(shlex.split(count_cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
        tables_in_gpkg = re.findall(r"\\n\d+:\s(.*?)(?=\\r|\s\(.*\))", str(ogr_response))
        # only allows tables names with underscores, numbers, and letters as the first character
        # excludes any dtype description that's also returned by the command line

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

        input_geospatial_file(dbo = dbo, input_file = input_file, table = table, path = path, gpkg_tbl = gpkg_tbl, schema = schema, port = port, srid = srid, gdal_data_loc=gdal_data_loc,
                        precision=precision, private=private, encoding=encoding, zip = zip, print_cmd=print_cmd)


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