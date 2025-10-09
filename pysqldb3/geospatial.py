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

    if geospatial_exists(path):

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

def geospatial_exists(path):
    
    """
    Checks if a geospatial file already exists at that file location

    :param file_name: Geospatial file name
    :param path: Optional file path
    """

    geospatial_exists = os.path.isfile(path)
                                
    return geospatial_exists

def geospatial_tbl_exists(geospatial_tbl, path):
            
    """
    Checks if a table or Shapefile already exists within a Geospatial package database.
    Helpful function to check if a table is to be overwritten.
    :param file_name: File name to check whether a certain table exists. Must end with .gpkg or .gdb
    :param path: Optional file path
    """

    if geospatial_exists(path):

        try:
            exists_cmd = f'ogrinfo {path}'
            ogr_response = subprocess.check_output(exists_cmd, stderr=subprocess.STDOUT)
            table_exists = re.findall(f"\b{geospatial_tbl}\b", str(ogr_response)) # only allows tables names with underscores, numbers, and letters

            if len(table_exists) == 0:
                geo_tbl_exists = False

            elif len(table_exists) > 0:
                geo_tbl_exists = True
            
        except:

            geo_tbl_exists = False

    else:

        geo_tbl_exists = False
        
    return geo_tbl_exists


def write_geospatial(dbo, path,  table = None, schema = None, query = None, gpkg_tbl = None,
                        srid='2263', gdal_data_loc=GDAL_DATA_LOC, cmd = None, overwrite = False, print_cmd=False):
    
    """
    Converts a SQL or Postgresql query to a new Geospatial (Shapefile, GPKG) file. Cannot write to a GDB.

    :param dbo: Database connection
    :param path (str): File path to the output file

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
    assert path.endswith(('.gpkg', '.shp')), "Output path needs to end with .gpkg or .shp if no file name is supplied"

    original_temp_flag = dbo.allow_temp_tables
    dbo.allow_temp_tables = True

    if not query and not table:
        # this would only happen if query_to_geospatial() wasn't run and instead, the user runs write_geospatial_file(), since query is required
        raise Exception('You must specify the db table to be written.')
    
    if query and not gpkg_tbl and path.endswith('.gpkg'):
        raise Exception ('You must specify a gpkg_tbl name in the function for the output table if you are writing a db query to a geopackage.')
    
    if table:
        qry = f"SELECT * FROM {schema}.{table}"
    elif not table and not query:
        raise Exception('Please specify the table to be written to the Geospatial file')
    else:
        # create query
        qry = create_query(dbo, query)

    # clean up the output file name
    # need 2 statements because of the difference in characters
    if path.endswith('.gpkg') and "." in path[:-5]:
        path = path[:-5].replace(".", "_") + ".gpkg"
        print(' The "." character is not allowed in output gpkg file names. Any "." have been replaced with "_".')
    elif path.endswith(".shp") and "." in path[:-4]:
        path = path[:-4].replace(".", "_") + ".shp"
        print(' The "." character is not allowed in output shp file names. Any "." have been replaced with "_".')

    if not gpkg_tbl:
        gpkg_tbl = table

    if not schema:
            schema = dbo.default_schema

    # overwrite vs update vs an issue has arisen
    if overwrite:
        # if explict overwrite, then create command line as directed
        _update = ''
        _overwrite = '-overwrite'
    
    elif not overwrite and not geospatial_exists(path = path):
        # this geopackage does not exist so create as if new
        _update = ''
        _overwrite = ''
    
    elif not overwrite and geospatial_exists(path): # check if the geopackage already exists
        
        table_exists = geospatial_tbl_exists(path, geospatial_tbl = gpkg_tbl)
        
        if table_exists == True:
            print("The table name to be exported already exists in the geopackage. Either change to Overwrite = True or check the name of the table to be copied.")
            exit
    
    # update allows you to add an extra table into an existing geopackage
        _update = '-update'
        _overwrite = ''

    # run the final command
    if not cmd:
        cmd = write_cmd(dbo, path, gpkg_tbl, table, _overwrite, _update, srid, gdal_data_loc, qry)

    if print_cmd:
        print(print_cmd_string([dbo.password], cmd))

    # execute the cmd
    execute_cmd(cmd = cmd, dbo = dbo)

    if table:
        print(f'{os.path.basename(path)} \nwritten to: {path}\ngenerated from: {table}')
    else:
        print(f'{os.path.basename(path)} geospatial file \nwritten to: {path}\ngenerated from: {query}')

    # Reset the temp flag
    dbo.last_query = qry
    dbo.allow_temp_tables = original_temp_flag
        
def geospatial_convert(input_path, input_file = None, output_file = None, gpkg_tbl = None, feature_class = None,
                        overwrite = False, print_cmd = False):
    
    """
    Converts a single Geospatial file or table to another Geospatial format.
    Possible converions: shp -> gpkg, gpkg -> shp, gdb -> gpkg
    Please use convert_geospatial_bulk() if you want to convert an entire Geopackage file to multiple Shapefiles.

    :param input_path: Path for the geospatial input AND export
    :param input_file(str): File name for input (ends with .shp, .gdb, or .gpkg). Optional if input_path has full directory.
    :param output_file (str): File name for ouput (ends with .shp or .gpkg).
    :param gpkg_tbl (str):  Optional argument if Geopackage involved.
                            If the input format is a SHP, this will be the output table name in the Geopackage.
                            Leave blank if you want the output table name to be the input .shp file's name.
                            If the input format is a GPKG, this is the single GPKG table that will be converted.
    :param feature_class (str): Optional argument if a Geodatabase input is involved.
    :param overwrite (bool): Boolean; defaults to False. Overwrite table in the geopackage if the table name already exists in the file.
    :param print_cmd (bool): Print command

    Example:
    geospatial_convert( input_path = 'C:/Documents/files/',
                        input_file = 'test1.shp',
                        output_file = 'my_new_gpkg.gpkg',
                        gpkg_tbl = 'resulting_table')
    """

    # assert that file formats were input correctly
    if input_file:
        assert input_file.endswith(('.shp', '.gpkg', '.gdb')), "The input file must end with .shp or .gpkg or .gdb"
    else:
        assert input_path.endswith(('.shp', '.gpkg', '.gdb')), "The input path must end with .shp or .gpkg or .gdb if no input_file supplied."

    if output_file:
        assert output_file.endswith(('.shp', '.gpkg')), "The output file must end with .shp or .gpkg"
    
    # set up the correct file paths
    input_path, input_file = parse_geospatial_file_path(input_path, input_file)
    export_path, output_file = parse_geospatial_file_path(input_path, output_file)

    # create full paths from these outputs
    input_full_path = os.path.join(input_path, input_file)
    output_full_path = os.path.join(export_path, output_file)

    assert input_full_path[:-4] != output_full_path[:-4], "This function does not allow you to convert a file to the same format"

    # check feature class argument is filled in if gdb
    if input_full_path.endswith('.gdb'):
        assert feature_class, "feature_class arg needs to be filled in if the input is a geodatabase"
        feature_class = feature_class.replace('.shp', '') # clean up name if needed

    # if no gpkg_tbl name given and we convert a shp file, name the table consistent with the shapefile
    if not gpkg_tbl and input_full_path.endswith('.shp'):
        gpkg_tbl = input_file.replace('.shp', '')

    # set variables
    _overwrite = ''
    _update = ''

    # if the output file is a gpkg, do these additional checks
    if output_full_path.endswith('.gpkg'):
        # if gpkg exists and overwrite is explicityly written
        if overwrite == True and geospatial_exists(export_path):
            _overwrite = '-overwrite'
    
        # if gpkg exists and overwrite was not explicitly called
        if geospatial_exists(path = export_path) and not geospatial_tbl_exists(path = export_path, geospatial_tbl = gpkg_tbl) and overwrite == False:
            _update = '-update' # then add the table to the gpkg
    
        # if the gpkg and table exists but no overwrite was called
        if geospatial_exists(path = export_path) and geospatial_tbl_exists(path = export_path, geospatial_tbl = gpkg_tbl) and overwrite == False:
            print("The table name to be copied to the geopackage already exists. Either change to Overwrite = True or check the name of the table to be copied.")
            exit # stop process so user can fix


    # update the command
    cmd = convert_cmd(input_full_path, output_full_path, gpkg_tbl, _update, _overwrite, feature_class)

    # execute the cmd
    execute_cmd(cmd = cmd)
    
    if print_cmd:
        print(cmd)
    return
    

def gpkg_to_shp_bulk(   input_path,
                        input_file = None,
                        print_cmd = False):
    """
    Converts an entire Geopackage (all tables) to a Shapefile.
    The output Shapefile name will match the name of the geopackage table to be copied.

    :param input_path: str File path to geopackage input. Shp output will also be located here.
    :param input_file(str): File name for input (ends with .gpkg). Optional if input_path includes file name.
    :param print_cmd (bool): Print command
    """

    assert input_file.endswith('.gpkg'), "The input file must end with .gpkg and the output file with .shp"

    try:
        count_cmd = COUNT_GPKG_LAYERS.format(full_path = os.path.join(input_path, input_file))
        ogr_response = subprocess.check_output(shlex.split(count_cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
        tables_in_gpkg = re.findall(r"\\n\d+:\s(.*?)(?=\\r|\s\(.*\))", str(ogr_response)) 

        for t_i_g in tables_in_gpkg:
            geospatial_convert(gpkg_tbl = t_i_g, input_path = input_path,
                               input_file = input_file, output_file = t_i_g +'.shp', print_cmd = print_cmd)
    
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
        print('Ogr2ogr command failed. The Geopackage was not read in.')
        raise subprocess.CalledProcessError(count_cmd, returncode=1)

    return

def upload_geospatial(dbo, path, input_file = None, schema = None, table = None, gpkg_tbl = None, feature_class = None, port = 5432,
                                srid = '2263', gdal_data_loc=GDAL_DATA_LOC, precision=False, private=False, encoding=None, print_cmd=False, 
                                skip_failures = '', temp = True, days = 7, extra_cmd = None):

    """
    Reads all tables within a Geopackage/Geodatabase file into SQL or Postgresql as tables.
    Function is NOT applicable to Shapefiles.

    :param path: Input file path for geopackage
    :param dbo: Database connection
    :param input_file(str): Optional file name for input (must end with .gpkg or .gdb)
    :param schema (str): Schema that the imported geopackage data will be found
    :param table (str): SINGLE TABLE EXPORT ONLY. Name of table in db.
    :param gpkg_tbl (str): SINGLE TABLE EXPORT ONLY. Name of geopackage table for input to db.
    :param feature_class (str): SINGLE TABLE EXPORT ONLY. Name of feature class in .gdb.
    :param port (int): Optional port
    :param srid (str): SRID for geometry. Defaults to 2263
    :param gdal_data_loc:
    :param precision: Default to False
    :param private: Default to False
    :param encoding: encoding of data within Geopackage
    :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
    :param temp: If True any new tables will be logged for deletion at a future date; defaults to True
    :param days: if temp=True, the number of days that the temp table will be kept. Defaults to 7.
    :param extra_cmd: allows user to pass any additional flag/paramters to OGR2OGR.
    :return:
    """

    temp_dir = None
    
    # check input file path
    if input_file:
        assert input_file.endswith(('.shp', '.gpkg', '.gdb')), "The input file should end with .gpkg, .shp or .gdb"
        assert path, "Fill in the file path to the input file"
    else:
        assert path.endswith(('.shp', '.gpkg', '.gdb')), "The path should end with .gpkg, .shp, .gdb"

    full_path, path, input_file = read_compressed(temp_dir, path, input_file)
    
    # if shapefile is selected, you can't have feature class filled in since it will not take that argument
    if full_path.endswith('.shp',):
        assert not feature_class, "feature_class input will not be considered if the input file is .shp"

    # Use default schema from db object
    if not schema:
        schema = dbo.default_schema

    if precision:
        precision = '-lco precision=NO'
    else:
        precision = ''

    if feature_class:
        if feature_class.endswith('.shp'):
            feature_class = feature_class[:-4]

    # clean table name if it's a single input   
    table = clean_table_name(table, gpkg_tbl, full_path, input_file)

    # if the inputs suggest bulk uploading
    gpkg_tbl_names = bulk_upload_table_setup(dbo, full_path, input_file, table, feature_class, temp_dir, gpkg_tbl)

    # start of loop
    for gpkg_tbl, table in gpkg_tbl_names.items():
        table = table.lower()

        if dbo.table_exists(table = table, schema = schema):

            del_indexes(dbo, schema, table)
            print(f'Deleting existing table {schema}.{table}')
            
            if dbo.type == 'MS':
                dbo.drop_table(schema=schema, table=table)
            else:
                dbo.drop_table(schema, table, cascade = True)

        # produce command
        cmd = read_geospatial_command(dbo, input_file, gdal_data_loc, srid, full_path, schema,
                            table, precision, port, gpkg_tbl, feature_class, skip_failures)

        if extra_cmd:
            cmd = cmd + f' {extra_cmd}'

        if print_cmd:
            print(print_cmd_string([dbo.password], cmd))

        cmd_env = encoding_changes(encoding)

        execute_cmd(dbo = dbo, cmd = cmd, feature_class = feature_class, cmd_env = cmd_env)

        # add a comment to the query
        comment_query(dbo, feature_class, schema, table, path, input_file)

        if not private and dbo.type == 'PG':
            # can only grant select to public in PG
            dbo.query(f'grant select on {schema}."{table}" to public;', timeme=False, internal=True, strict=True)

        rename_geom(dbo, schema, table)
        dbo.tables_created.append((dbo.server, dbo.database, schema,  table))
        
        if temp:
            dbo._run_table_logging([schema + "." + table], days=days)

        # remove temp folders of any decompressed files
        if temp_dir:
            shutil.rmtree(temp_dir)


def del_indexes(dbo, schema, table):
    """
    Drops indices
    :dbo: Database connection
    :param schema: Schema for table whose index will be deleted
    :param table: Table name whose index will be deleted
    """
    if dbo.type == 'PG':
        dbo.query(DEL_INDICES_QUERY_PG.format(s=schema, t=table), internal=True)
        indexes_to_delete = dbo.internal_data

        for _ in list(indexes_to_delete):
            table_name, schema_name, index_name, column_name = _
            if 'pkey' not in index_name and 'PK' not in index_name:
                dbo.query(f'DROP INDEX {schema}.{index_name}',
                                strict=False, internal=True)
    else:
        dbo.query(DEL_INDICES_QUERY_MS.format(s=schema, t=table), internal=True)
        indexes_to_delete = dbo.internal_data

        for _ in list(indexes_to_delete):
            table_name, index_name, column_name, idx_typ = _
            if 'pkey' not in index_name and 'PK' not in index_name:
                dbo.query(f'DROP INDEX {table}.{index_name}', strict=False, internal=True)