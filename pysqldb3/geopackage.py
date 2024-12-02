import shlex
import subprocess
import re
import os

from .cmds import *
from .sql import *
from .util import *


class Geopackage:
    def __str__(self):
        pass

    def __init__(self, gpkg_name, path=None, gpkg_tbl = None, skip_failures=''):
        self.path = path
        self.gpkg_name = gpkg_name
        self.gpkg_tbl = gpkg_tbl
        self.skip_failures=skip_failures

    @staticmethod
    def name_extension(name):
        """
        Adds .gpkg to name,  if not there

        :param name:
        :return:
        """
        if '.gpkg' in name:
            return name
        else:
            return name + '.gpkg'
        
    def list_gpkg_tables(self):
        
        """
        List all the tables contained in a geopackage file.
        """

        if self.gpkg_exists():

            try:
                exists_cmd = f'ogrinfo {os.path.join(self.path, self.gpkg_name)}'
                ogr_response = subprocess.check_output(exists_cmd, stderr=subprocess.STDOUT)

                # use regex to find all the tables in the geopackage
                tables_in_gpkg = re.findall(r"\\n\d+:\s(.*?)(?=\\r|\s\(.*\))", str(ogr_response)) # only allows tables names with underscores, numbers, and letters
                
                return tables_in_gpkg
                
            except subprocess.CalledProcessError as e:
                print(e)
        
        else:
             print("This geopackage file does not exist at this file location.")
    
    def gpkg_exists(self, gpkg_name = None):
        
        """
        Checks if a geopackage already exists at that location
        """
        
        if not gpkg_name:
            gpkg_name = self.gpkg_name

        gpkg_exists = os.path.isfile(os.path.join(self.path, gpkg_name))
                                  
        return gpkg_exists
    
    def gpkg_tbl_exists(self, gpkg_tbl):
               
        """
        Checks if a geopackage table already exists, in case it isn't meant to be overwritten.
        """
        
        if self.gpkg_exists():
            try:
                exists_cmd = f'ogrinfo {os.path.join(self.path, self.gpkg_name)}'
                ogr_response = subprocess.check_output(exists_cmd, stderr=subprocess.STDOUT)
                table_exists = re.findall(f"{gpkg_tbl}", str(ogr_response)) # only allows tables names with underscores, numbers, and letters

                if len(table_exists) == 0:
                    gpkg_tbl_exists = False

                elif len(table_exists) > 0:
                    gpkg_tbl_exists = True
                
            except:

                gpkg_tbl_exists = None

        return gpkg_tbl_exists


    def write_gpkg(self, dbo, table = None, schema = None, query = None, gpkg_tbl = None, srid='2263', gdal_data_loc=GDAL_DATA_LOC, cmd = None, overwrite = False, print_cmd=False):
        
        """
        Converts a SQL or Postgresql query to a new geopackage.

        :param dbo: Database connection
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

        if not query and not table:
            # this would only happen if query_to_gpkg() wasn't run and instead, the user runs write_gpkg(), since query is required
            raise Exception('You must specify the db table to be written.')
        
        if query and not gpkg_tbl:
            raise Exception ('You must specify a gpkg_tbl name in the function for the output table if you are writing a db query to a geopackage.')
        
        if table:
            qry = f"SELECT * FROM {schema}.{table}"
        elif not table and not query:
            raise Exception('Please specify the table to be written to the Geopackage.')
        else:
            qry = f"SELECT * FROM ({query}) x"

        self.path, gpkg = parse_gpkg_path(self.path, self.gpkg_name)

        if not self.gpkg_name:
            if gpkg:
                self.gpkg_name = gpkg
            else:
                output_file_name = file_loc('save')
                self.gpkg_name = os.path.basename(output_file_name)
                self.path = os.path.dirname(output_file_name)

        if self.gpkg_name[-5:] == ".gpkg" and "." in self.gpkg_name[:-5]:
            self.gpkg_name = self.gpkg_name[:-4].replace(".", "_") + ".gpkg"
            print(' The "." character is not allowed in output gpkg file names. Any "." have been removed.')
        elif self.gpkg_name[-5:] != ".gpkg" and "." in self.gpkg_name:
            self.gpkg_name = self.gpkg_name.replace(".", "_")
            print(' The "." character is not allowed in output gpkg file names. Any "." have been removed.')

        if not self.path:
            self.path = file_loc('folder')

        if not gpkg_tbl:
            gpkg_tbl = table
            if self.gpkg_tbl:
                print('The gpkg_tbl argument in write_gpkg() overrides the class input for gpkg_tbl.')

        # overwrite vs update vs an issue has arisen
        if overwrite:
            # if explict overwrite, then create command line as directed
            _update = ''
            _overwrite = '-overwrite'
        
        elif not overwrite and not self.gpkg_exists():
            # this geopackage does not exist so create as if new
            _update = ''
            _overwrite = ''
        
        elif not overwrite and self.gpkg_exists(): # check if the geopackage already exists
            
            table_exists = self.gpkg_tbl_exists(gpkg_tbl)
            
            if table_exists == True:
                print("The table name to be exported already exists in the geopackage. Either change to Overwrite = True or check the name of the table to be copied.")
                exit 
        
        # update allows you to add an extra table into an existing geopackage
            _update = '-update'
            _overwrite = ''

        if not cmd:
            if dbo.type == 'PG':
                cmd = WRITE_GPKG_CMD_PG.format(export_path=self.path,
                                                   gpkg_name=self.name_extension(self.gpkg_name),
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
            elif dbo.type == 'MS':
                if dbo.LDAP:
                    cmd = WRITE_GPKG_CMD_MS.replace(";UID={username};PWD={password}", "").format(
                        export_path=self.path,
                        gpkg_name=self.name_extension(self.gpkg_name),
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
                    cmd = WRITE_GPKG_CMD_MS.format(export_path=self.path,
                                                       gpkg_name=self.name_extension(self.gpkg_name),
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

        if print_cmd:
            print(print_cmd_string([dbo.password], cmd))

        try:
            ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print('Ogr2ogr command failed. The Geopackage/feature class was not written.')
            raise subprocess.CalledProcessError(cmd=print_cmd_string([dbo.password], cmd), returncode=1)

        if table:
            print('{t} geopackage \nwritten to: {p}\ngenerated from: {q}'.format(t=self.name_extension(self.gpkg_name),
                                                                                p=self.path,
                                                                                q=table))
        else:
            print(u'{t} geopackage \nwritten to: {p}\ngenerated from: {q}'.format(
                t=self.name_extension(self.gpkg_name),
                p=self.path,
                q=query))
            
    def shp_to_gpkg(self, shp_name, gpkg_tbl = None, overwrite = False, print_cmd = False):
        
        """
        Converts a Shapefile to a Geopackage file in the same file location as the original Shapefile.
        :param shp_name (str): file name for shape file input (should end in .shp)
        :param gpkg_name (str): file name for gpkg ouput (should end with .gpkg)
        :param gpkg_tbl (str): OPTIONAL table name in the new geopackage; leave blank if you want the table name the same as the gpkg_name
        :param overwrite (bool): Boolean; defaults to False. Overwrite table in the geopackage if the table name already exists in the file.
        :param print_cmd (bool): Print command
        """
        
        assert self.gpkg_name[-5:] == '.gpkg', "The input file should end with .gpkg . Please check your input."
        assert shp_name[-4:] == '.shp', "The input file should end with .shp . Please check your input."

        # same function as shape file
        self.path, shp_name = parse_shp_path(self.path, shp_name)
        
        if not self.path:
            self.path = file_loc('folder')

        if not gpkg_tbl:
            # if no gpkg_tbl name given, name the table consistente with the shape file
            gpkg_tbl = shp_name.replace('.shp', '')

        # set variables
        _overwrite = ''
        _update = ''

        # check if the gpkg already exists. if so, we add another layer to the gpkg
        if not self.gpkg_exists(self.gpkg_name):
            _update = ''
        elif overwrite == True: # if gpkg exists and overwrite is explicityly written
            _overwrite = '-overwrite'
        elif not self.gpkg_tbl_exists(gpkg_tbl): # if gpkg exists and overwrite was not explicitly called
            _update = '-update' # then add the table to the gpkg
        elif self.gpkg_tbl_exists(gpkg_tbl): # if the gpkg and table exists but no overwrite was called
            print("The table name to be copied to the geopackage already exists. Either change to Overwrite = True or check the name of the table to be copied.")
            exit 

        # update the command
        cmd = WRITE_SHP_CMD_GPKG.format(gpkg_name = self.gpkg_name,
                                                  full_path = self.path,
                                                shp_name = shp_name,
                                                _update = _update,
                                                _overwrite = _overwrite,
                                                gpkg_tbl = gpkg_tbl)

        try:
            ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print('Ogr2ogr command failed. The Geopackage/feature class was not written.')
        
        if print_cmd:
            print(cmd)
        return
        

    def gpkg_to_shp(self,
                    gpkg_tbl:str,
                    export_path = None,
                    print_cmd = False,
                    bulk_upload = False):
        """
        Converts a Geopackage to a Shapefile.
        The output Shapefile name will match the name of the geopackage table to be copied.

        :param gpkg_name (str): file name for geopackage input (should end in .gpkg)
        :param gpkg_tbl (list): Specific table within the geopackage to convert to a Shapefile. Use gpkg_to_shp_bulk() if you want all tables in a gpkg file.
        :param export_path: str The folder directory to place the shapefiles output.
                            You cannot specify the shapefiles' names as they are copied from the table names within the geopackage.'
        :param print_cmd (bool): Print command
        :param bulk_upload: Default to False. Param used for gpkg_to_shp_bulk() function.
        """
        
        assert self.gpkg_name[-5:] == '.gpkg', "The input file should end with .gpkg . Please check your input."

        self.path, gpkg = parse_gpkg_path(self.path, self.gpkg_name)

        # set a default self.path for the gpkg input if it exists
        if not self.path:
            self.path = file_loc('folder')

        # set an export path to the gpkg if it is not manually set up
        if not export_path:
            export_path = self.path

        if bulk_upload == True:
            ### bulk upload capabilities ### 

            try:
                count_cmd = COUNT_GPKG_LAYERS.format(full_path = os.path.join(self.path, self.gpkg_name))
                ogr_response = subprocess.check_output(shlex.split(count_cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
                tables_in_gpkg = re.findall(r"\\n\d+:\s(.*?)(?=\\r|\s\(.*\))", str(ogr_response)) # only allows tables names with underscores, numbers, and letters
                tbl_count = len(tables_in_gpkg)
            
            except subprocess.CalledProcessError as e:
                print("Ogr2ogr Output:\n", e.output)
                print('Ogr2ogr command failed. The Geopackage was not read in.')
                raise subprocess.CalledProcessError(count_cmd, returncode=1)
            
            # create a list of cleaned gpkg table names
            gpkg_tbl_names = {} # create empty dictionary
 
            for t_i_g in tables_in_gpkg:
                
                cmd = WRITE_GPKG_CMD_SHP.format(   full_path = self.path,
                                                        gpkg_name = self.gpkg_name,
                                                        gpkg_tbl = t_i_g,
                                                        export_path=export_path)
                
                if print_cmd:
                    print(cmd)
                
                try:
                    ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
                    print(ogr_response)

                except subprocess.CalledProcessError as e:
                    print("Ogr2ogr Output:\n", e.output)
                    print('Ogr2ogr command failed. The Geopackage/feature class was not written.')
            
        else:
        
            cmd = WRITE_GPKG_CMD_SHP.format(   full_path = self.path,
                                                gpkg_name = self.gpkg_name,
                                                gpkg_tbl = gpkg_tbl,
                                                export_path=export_path)

            if print_cmd:
                print(cmd)

            try:
                ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
                print(ogr_response)

            except subprocess.CalledProcessError as e:
                print("Ogr2ogr Output:\n", e.output)
                print('Ogr2ogr command failed. The Geopackage/feature class was not written.')
        
        return
    
    def gpkg_to_shp_bulk(   self,
                            export_path = None,
                            print_cmd = False):
        """
        Converts an entire Geopackage (all tables) to a Shapefile.
        The output Shapefile name will match the name of the geopackage table to be copied.

        :param gpkg_name (str): file name for geopackage input (should end in .gpkg)
        :param export_path: str The folder directory to place the shapefiles output.
                            You cannot specify the shapefiles' names as they are copied from the table names within the geopackage.'
        :param print_cmd (bool): Print command
        """

        self.gpkg_to_shp(gpkg_tbl = '', export_path = export_path, print_cmd = print_cmd, bulk_upload = True)
        
        return

    def read_gpkg(self, dbo, table = None, gpkg_tbl = None, schema = None, port = 5432, srid = '2263', gdal_data_loc=GDAL_DATA_LOC,
                    precision=False, private=False, gpkg_encoding=None, print_cmd=False, bulk_upload = False):
        """
        Reads a single geopackage table into SQL or Postgresql as a table

        :param dbo: Database connection
        :param schema (str): Schema that the imported geopackage data will be found
        :param table (str): (Optional) name for the uploaded db table. If blank, it will default to the gpkg_tbl name.
        :param gpkg_tbl (str): (Optional) Specific table in the geopackage to upload. If blank, it will default to the gpkg_tbl name defined in the class.
        :param port (int): Optional port
        :param srid (str): SRID for geometry. Defaults to 2263
        :param precision:
        :param private:
        :param gpkg_encoding: encoding of data within Geopackage
        :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
        :param bulk_upload: Default to False. Do not set to True; please use read_gpkg_bulk() to upload all gpkg tables to db.
        :return:
        """
        # Use default schema from db object
        if not schema:
            schema = dbo.default_schema

        if precision:
            precision = '-lco precision=NO'
        else:
            precision = ''

        if not all([self.path, self.gpkg_name]):
            filename = file_loc('file', 'Missing file info - Opening search dialog...')
            self.gpkg_name = os.path.basename(filename)
            self.path = os.path.dirname(filename)

        path = self.path
        full_path = os.path.join(path, self.gpkg_name)

        if bulk_upload == False:

            if not gpkg_tbl:
                gpkg_tbl = self.gpkg_tbl

            if table:
                assert table == re.sub(r'[^A-Za-z0-9_]+', r'', table) # make sure the name will load into the database
            else:
                # clean the geopackage table name
                table = re.sub(r'[^A-Za-z0-9_]+', r'', gpkg_tbl) # clean the table name in case there are special characters

            table = table.lower()

            if dbo.table_exists(schema = schema, table = table):

                print(f'Deleting existing table {schema}.{table}')
                dbo.drop_table(schema=schema, table=table)

            if dbo.type == 'PG':
                    cmd = READ_GPKG_CMD_PG.format(
                        gdal_data=gdal_data_loc,
                        srid=srid,
                        host=dbo.server,
                        dbname=dbo.database,
                        user=dbo.user,
                        password=dbo.password,
                        gpkg_name = full_path,
                        gpkg_tbl = gpkg_tbl,
                        schema=schema,
                        tbl_name=table,
                        perc=precision,
                        port=port
                    )
            elif dbo.type == 'MS':
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
                        tbl_name= '"' + table + '"',
                        perc=precision,
                        port=port
                    )

            cmd_env = os.environ.copy()

            if gpkg_encoding and gpkg_encoding.upper() == 'LATIN1':
                cmd_env['PGCLIENTENCODING'] = 'LATIN1'

            if gpkg_encoding and gpkg_encoding.upper().replace('-', '') == 'UTF8':
                cmd_env['PGCLIENTENCODING'] = 'UTF8'

            if print_cmd:
                print(print_cmd_string([dbo.password], cmd))

            try:
                ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT, env=cmd_env)
                print(ogr_response)
            except subprocess.CalledProcessError as e:
                print("Ogr2ogr Output:\n", e.output)
                print('Ogr2ogr command failed. The Geopackage was not read in.')
                raise subprocess.CalledProcessError(cmd=print_cmd_string([dbo.password], cmd), returncode=1)

            if dbo.type == 'PG':
                dbo.query(FEATURE_COMMENT_QUERY.format(
                    s=schema,
                    t=table,
                    u=dbo.user,
                    p=self.path,
                    gpkg=self.gpkg_name,
                    d=datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
                ), timeme=False, internal=True)

            if not private:
                try:
                    dbo.query(f'grant select on {schema}."{table}" to public;',
                        timeme=False, internal=True, strict=True)
                except:
                    pass
            self.rename_geom(dbo, schema, table)

        else:
            
            # bulk upload == True

            gpkg_tbl_names = self.read_gpkg_bulk_function(dbo, table, gpkg_tbl, schema, port, srid, gdal_data_loc, precision, private, gpkg_encoding, print_cmd, bulk_upload)

            return gpkg_tbl_names

    def read_gpkg_bulk(self, dbo, schema = None, port = 5432, srid = '2263', gdal_data_loc=GDAL_DATA_LOC,
                    precision=False, private=False, gpkg_encoding=None, print_cmd=False):

        """
        Reads all tables within a geopackage file into SQL or Postgresql as tables

        :param dbo: Database connection
        :param schema (str): Schema that the imported geopackage data will be found
        :param port (int): Optional port
        :param srid (str): SRID for geometry. Defaults to 2263
        :param precision:
        :param private:
        :param gpkg_encoding: encoding of data within Geopackage
        :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
        :return:
        """

        gpkg_tbl_names = self.read_gpkg(dbo = dbo, table = '', gpkg_tbl = '', schema = schema, port = port, srid = srid, gdal_data_loc=gdal_data_loc,
                    precision=precision, private=private, gpkg_encoding=gpkg_encoding, print_cmd=print_cmd, bulk_upload = True)
        
        return gpkg_tbl_names

    def read_gpkg_bulk_function(self, dbo, table, gpkg_tbl, schema, port, srid, gdal_data_loc, precision, private, gpkg_encoding, print_cmd, bulk_upload):

        """
        Not called by end user. This is a sub-function for bulk uploading since there is a lot of code.
        """
        # Use default schema from db object
        if not schema:
            schema = dbo.default_schema

        if precision:
            precision = '-lco precision=NO'
        else:
            precision = ''

        if not all([self.path, self.gpkg_name]):
            filename = file_loc('file', 'Missing file info - Opening search dialog...')
            self.gpkg_name = os.path.basename(filename)
            self.path = os.path.dirname(filename)

        path = self.path
        full_path = os.path.join(path, self.gpkg_name)

        ####### BULK UPLOAD ########
        
        count_cmd = COUNT_GPKG_LAYERS.format(full_path = full_path) 

        try:
            ogr_response = subprocess.check_output(shlex.split(count_cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
            tables_in_gpkg = re.findall(r"\\n\d+:\s(.*?)(?=\\r|\s\(.*\))", str(ogr_response))
            # only allows tables names with underscores, numbers, and letters as the first character
            # excludes any dtype description that's also returned by the command line
        
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print('Ogr2ogr command failed. The Geopackage was not read in.')
            raise subprocess.CalledProcessError(cmd=print_cmd_string([dbo.password], count_cmd), returncode=1)
        

        # create a list of cleaned gpkg table names
        gpkg_tbl_names = {} # create empty dictionary
        for t_i_g in tables_in_gpkg:
            insert_val = re.sub(r'[^A-Za-z0-9_]+', r'', t_i_g)
            gpkg_tbl_names[insert_val] = t_i_g # add the cleaned name

        # assert that the new cleaned names are unique. if not, we won't get the same dimensions
        assert len(gpkg_tbl_names) == len(tables_in_gpkg), "Clean the geopackage table names so they can be uploaded as tables (by removing special characters other than _) and make sure they are unique."

        # loop through the tables     
        for output_name, input_name in gpkg_tbl_names.items():
        
            if dbo.table_exists(table = output_name, schema = schema):

                print(f'Deleting existing table {schema}.{output_name}')
                dbo.drop_table(schema=schema, table=output_name)

            if dbo.type == 'PG':
                cmd = READ_GPKG_CMD_PG.format(
                    gdal_data=gdal_data_loc,
                    srid=srid,
                    host=dbo.server,
                    dbname=dbo.database,
                    user=dbo.user,
                    password=dbo.password,
                    gpkg_name = full_path,
                    gpkg_tbl = input_name,
                    schema=schema,
                    tbl_name=output_name,
                    perc=precision,
                    port=port
                )
            elif dbo.type == 'MS':
                if dbo.LDAP:
                    cmd = READ_GPKG_CMD_MS.format(
                        gdal_data=gdal_data_loc,
                        srid=srid,
                        host=dbo.server,
                        dbname=dbo.database,
                        gpkg_name=full_path,
                        gpkg_tbl = input_name,
                        schema=schema,
                        tbl_name='"' + output_name + '"',
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
                        gpkg_tbl = input_name,
                        schema=schema,
                        tbl_name='"' + output_name + '"',
                        perc=precision,
                        port=port
                    )

            cmd_env = os.environ.copy()

            if gpkg_encoding and gpkg_encoding.upper() == 'LATIN1':
                cmd_env['PGCLIENTENCODING'] = 'LATIN1'

            if gpkg_encoding and gpkg_encoding.upper().replace('-', '') == 'UTF8':
                cmd_env['PGCLIENTENCODING'] = 'UTF8'

            if print_cmd:
                print(print_cmd_string([dbo.password], cmd))

            try:
                ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT, env=cmd_env)
                print(ogr_response)
            except subprocess.CalledProcessError as e:
                print("Ogr2ogr Output:\n", e.output)
                print('Ogr2ogr command failed. The Geopackage was not read in.')
                raise subprocess.CalledProcessError(cmd=print_cmd_string([dbo.password], cmd), returncode=1)

            if dbo.type == 'PG':
                dbo.query(FEATURE_COMMENT_QUERY.format(
                    s=schema,
                    t=output_name,
                    u=dbo.user,
                    p=self.path,
                    gpkg=self.gpkg_name,
                    d=datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
                ), timeme=False, internal=True)

            if not private:
                try:
                    dbo.query(f'grant select on {schema}."{output_name}" to public;',
                        timeme=False, internal=True, strict=True)
                except:
                    pass
            self.rename_geom(dbo, schema, output_name)
        
        return gpkg_tbl_names

    def rename_geom(self, dbo = None, schema = None, table = None, port=5432):
        """
        Renames wkb_geometry to geom, along with index

        :param dbo: Database connection
        :param schema: Schema where geom is located
        :param table: Table where geom is located
        :return:
        """
        dbo.query("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = '{s}'
                        AND table_name   = '{t}';
                    """.format(s=schema, t=table), timeme=False, internal=True)
        f = None

        if dbo.type == 'PG':

            # Get the column in question
            if 'wkb_geometry' in [i[0] for i in dbo.internal_queries[-1].data]:
                f = 'wkb_geometry'
            elif 'shape' in [i[0] for i in dbo.internal_queries[-1].data]:
                f = 'shape'

            if f:
                # Rename column
                dbo.rename_column(schema=schema, table=table, old_column=f, new_column='geom')

                # Rename index
                dbo.query("""
                    ALTER INDEX IF EXISTS
                    {s}.{t}_{f}_geom_idx
                    RENAME to {t}_geom_idx
                """.format(s=schema, t=table, f=f), timeme=False, internal=True)

        elif dbo.type == 'MS':
            # Get the column in question
            if 'ogr_geometry' in [i[0] for i in dbo.internal_queries[-1].data]:
                f = 'ogr_geometry'
            elif 'Shape' in [i[0] for i in dbo.internal_queries[-1].data]:
                f = 'Shape'

            if f:
                # Rename column
                dbo.rename_column(schema=schema, table=table, old_column=f, new_column='geom')

                # Rename index if exists
                try:
                    dbo.query(f"""
                        EXEC sp_rename N'{schema}.{table}.ogr_{schema}_{table}_{f}_sidx', N'{table}_geom_idx', N'INDEX';
                    """, timeme=False, internal=True)
                except SystemExit as e:
                    print(e)
                    print('Warning - could not update index name after renaming geometry. It may not exist.')
