import shlex
import subprocess
import re
import os

from .cmds import *
from .sql import *
from .util import *


class Geospatial:
    def __str__(self):
        pass

    def __init__(self, gpkg_or_shp, file_name, path=None, gpkg_tbl = None, skip_failures=''):

        self.path = path
        self.gpkg_or_shp = gpkg_or_shp
        self.file_name = file_name
        self.gpkg_tbl = gpkg_tbl
        self.skip_failures=skip_failures

    @staticmethod
    def name_extension(gpkg_or_shp, name):

        """
        Adds .gpkg or .shp to name,  if not there

        :param name:
        :return:
        """
        if '.gpkg' or '.shp' in name:
            return name
        else:
            if gpkg_or_shp == 'gpkg':
                return name + '.gpkg'
            else:
                return name + '.shp'
        
    def list_gpkg_tables(self):
        
        """
        List all the tables contained in a geopackage file.
        """

        if self.gpkg_exists():

            try:
                exists_cmd = f'ogrinfo {os.path.join(self.path, self.file_name)}'
                ogr_response = subprocess.check_output(exists_cmd, stderr=subprocess.STDOUT)

                # use regex to find all the tables in the geopackage
                tables_in_gpkg = re.findall(r"\\n\d+:\s(.*?)(?=\\r|\s\(.*\))", str(ogr_response))
                
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
            gpkg_name = self.file_name

        gpkg_exists = os.path.isfile(os.path.join(self.path, gpkg_name))
                                  
        return gpkg_exists
    
    def gpkg_tbl_exists(self, gpkg_tbl):
               
        """
        Checks if a geopackage table already exists, in case it isn't meant to be overwritten.
        """
        
        if self.gpkg_exists():
            try:
                exists_cmd = f'ogrinfo {os.path.join(self.path, self.file_name)}'
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


    def write_geospatial(self, dbo, table = None, schema = None, query = None, gpkg_tbl = None, srid='2263', gdal_data_loc=GDAL_DATA_LOC, cmd = None, overwrite = False, print_cmd=False):
        
        """
        Converts a SQL or Postgresql query to a new Geospatial (Shapefile or GPKG) file.

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
        
        # assert that a valid file format was input
        gpkg_or_shp = self.gpkg_or_shp.lower()
        assert self.gpkg_or_shp == 'shp' or self.gpkg_or_shp == 'gpkg', "Please input a valid gpkg_or_shp input. Either type 'GPKG' or 'SHP'"

        if not query and not table:
            # this would only happen if query_to_geospatial() wasn't run and instead, the user runs write_geospatial_file(), since query is required
            raise Exception('You must specify the db table to be written.')
        
        if query and not gpkg_tbl:
            raise Exception ('You must specify a gpkg_tbl name in the function for the output table if you are writing a db query to a geopackage.')
        
        if table:
            qry = f"SELECT * FROM {schema}.{table}"
        elif not table and not query:
            raise Exception('Please specify the table to be written to the Geopackage.')
        else:
            qry = f"SELECT * FROM ({query}) x"

        self.path, geospatial = parse_geospatial_file_path(self.path, self.name_extension(gpkg_or_shp, self.file_name))

        if not self.file_name:
            if geospatial:
                self.file_name = geospatial
            else:
                output_file_name = file_loc('save')
                self.file_name = os.path.basename(output_file_name)
                self.path = os.path.dirname(output_file_name)

        if self.file_name[-5:] == ".gpkg" and "." in self.file_name[:-5]:
            self.file_name = self.file_name[:-4].replace(".", "_") + ".gpkg"
            print(' The "." character is not allowed in output gpkg file names. Any "." have been removed.')
        elif self.file_name[-5:] != ".gpkg" and "." in self.file_name:
            self.file_name = self.file_name.replace(".", "_")
            print(' The "." character is not allowed in output gpkg file names. Any "." have been removed.')
        elif self.file_name[-4:] == ".shp" and "." in self.file_name[:-4]:
            self.file_name = self.file_name[:-4].replace(".", "_") + ".shp"
            print(' The "." character is not allowed in output shp file names. Any "." have been removed.')
        elif self.file_name[-4:] != ".shp" and "." in self.file_name:
            self.file_name = self.file_name.replace(".", "_")
            print(' The "." character is not allowed in output shp file names. Any "." have been removed.')

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
            if dbo.type == 'PG' and gpkg_or_shp == 'gpkg':
                cmd = WRITE_GPKG_CMD_PG.format(export_path=self.path,
                                                   gpkg_name=self.name_extension('gpkg', self.file_name),
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
                
            elif dbo.type == 'MS' and gpkg_or_shp == 'gpkg':
                if dbo.LDAP:
                    cmd = WRITE_GPKG_CMD_MS.replace(";UID={username};PWD={password}", "").format(
                        export_path=self.path,
                        gpkg_name=self.name_extension('gpkg', self.file_name),
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
                                                       gpkg_name=self.name_extension('gpkg', self.file_name),
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
                    
            elif self.dbo.type == 'PG' and gpkg_or_shp == 'shp':

                self.cmd = WRITE_SHP_CMD_PG.format(export_path=self.path,
                                                   shpname = self.name_extension('shp', self.file_name),
                                                   host = dbo.server,
                                                   username = dbo.user,
                                                   db = dbo.database,
                                                   password = dbo.password,
                                                   pg_sql_select = qry,
                                                   srid = srid,
                                                   gdal_data = gdal_data_loc)
                
            elif self.dbo.type == 'MS' and gpkg_or_shp == 'shp':

                if self.dbo.LDAP:
                    self.cmd = WRITE_SHP_CMD_MS.replace(";UID={username};PWD={password}", "").format(
                        export_path = self.path,
                        shpname = self.name_extension('shp', self.file_name),
                        host = dbo.server,
                        db = dbo.database,
                        ms_sql_select = qry,
                        srid = srid,
                        gdal_data = gdal_data_loc
                    )

                else:
                    self.cmd = WRITE_SHP_CMD_MS.format(export_path=self.path,
                                                       shpname=self.name_extension('shp', self.file_name),
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
            print(f'{self.name_extension(gpkg_or_shp, self.file_name)} {gpkg_or_shp} \nwritten to: {self.path}\ngenerated from: {table}')
        else:
            print(f'{self.name_extension(gpkg_or_shp, self.file_name)} geopackage \nwritten to: {self.path}\ngenerated from: {query}')
            
    def shp_to_gpkg(self, shp_name, gpkg_tbl = None, overwrite = False, print_cmd = False):
        
        """
        Converts a Shapefile to a Geopackage file in the same file location as the original Shapefile.
        :param shp_name (str): file name for shape file input (should end in .shp)
        :param gpkg_name (str): file name for gpkg ouput (should end with .gpkg)
        :param gpkg_tbl (str): OPTIONAL table name in the new geopackage; leave blank if you want the table name the same as the gpkg_name
        :param overwrite (bool): Boolean; defaults to False. Overwrite table in the geopackage if the table name already exists in the file.
        :param print_cmd (bool): Print command
        """
        
        assert self.file_name[-5:] == '.gpkg', "The input file should end with .gpkg . Please check your input."
        assert shp_name[-4:] == '.shp', "The input file should end with .shp . Please check your input."

        # same function as shape file
        self.path, shp_name = parse_geospatial_file_path(self.path, shp_name)
        
        if not self.path:
            self.path = file_loc('folder')

        if not gpkg_tbl:
            # if no gpkg_tbl name given, name the table consistente with the shape file
            gpkg_tbl = shp_name.replace('.shp', '')

        # set variables
        _overwrite = ''
        _update = ''

        # if gpkg exists and overwrite is explicityly written
        if overwrite == True and self.gpkg_exists(self.file_name): 
            _overwrite = '-overwrite'
        
        # if gpkg exists and overwrite was not explicitly called
        if self.gpkg_exists(self.file_name) and not self.gpkg_tbl_exists(gpkg_tbl) and overwrite == False: 
            _update = '-update' # then add the table to the gpkg
        
        # if the gpkg and table exists but no overwrite was called
        if self.gpkg_exists(self.file_name) and self.gpkg_tbl_exists(gpkg_tbl) and overwrite == False:
            print("The table name to be copied to the geopackage already exists. Either change to Overwrite = True or check the name of the table to be copied.")
            exit # stop process so user can fix

        # update the command
        cmd = WRITE_SHP_CMD_GPKG.format(gpkg_name = self.file_name,
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
                    print_cmd = False):
        """
        Converts a Geopackage to a Shapefile.
        The output Shapefile name will match the name of the geopackage table to be copied.

        :param gpkg_name (str): file name for geopackage input (should end in .gpkg)
        :param gpkg_tbl (list): Specific table within the geopackage to convert to a Shapefile. Use gpkg_to_shp_bulk() if you want all tables in a gpkg file.
        :param export_path: str The folder directory to place the shapefiles output.
                            You cannot specify the shapefiles' names as they are copied from the table names within the geopackage.'
        :param print_cmd (bool): Print command
        """
        
        assert self.file_name[-5:] == '.gpkg', "The input file should end with .gpkg . Please check your input."

        self.path, gpkg = parse_geospatial_file_path(self.path, self.name_extension('gpkg', self.file_name))

        # set a default self.path for the gpkg input if it exists
        if not self.path:
            self.path = file_loc('folder')

        # set an export path to the gpkg if it is not manually set up
        if not export_path:
            export_path = self.path
            
        cmd = WRITE_GPKG_CMD_SHP.format(        full_path = self.path,
                                                gpkg_name = self.file_name,
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

        try:
            count_cmd = COUNT_GPKG_LAYERS.format(full_path = os.path.join(self.path, self.file_name))
            ogr_response = subprocess.check_output(shlex.split(count_cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
            tables_in_gpkg = re.findall(r"\\n\d+:\s(.*?)(?=\\r|\s\(.*\))", str(ogr_response)) 

            for t_i_g in tables_in_gpkg:
            
               self.gpkg_to_shp(gpkg_tbl = t_i_g, export_path = export_path, print_cmd = print_cmd)
        
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print('Ogr2ogr command failed. The Geopackage was not read in.')
            raise subprocess.CalledProcessError(count_cmd, returncode=1)

        return

    def input_geospatial_file(self, dbo, table = None, gpkg_tbl = None, schema = None, port = 5432, srid = '2263', gdal_data_loc=GDAL_DATA_LOC,
                    precision=False, private=False, encoding=None, zip = False, print_cmd=False):
        """
        Reads a single Geopackage table or Shapefile into SQL or Postgresql as a table

        :param dbo: Database connection
        :param schema (str): Schema that the imported geopackage data will be found
        :param table (str): (Optional) name for the uploaded db table. If blank, it will default to the gpkg_tbl name.
        :param gpkg_tbl (str): (Optional) Specific table in the geopackage to upload. If blank, it will default to the gpkg_tbl name defined in the class.
        :param port (int): Optional port
        :param srid (str): SRID for geometry. Defaults to 2263
        :param precision:
        :param private:
        :param encoding: encoding of data within the geospatial file
        :param zip: Optional flag needed if reading from a zipped file; defaults to False
        :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
        :return:
        """
        # Use default schema from db object
        if not schema:
            schema = dbo.default_schema

        if precision:
            precision = '-lco precision=NO'
        else:
            precision = ''

        if not all([self.path, self.file_name]):
            filename = file_loc('file', 'Missing file info - Opening search dialog...')
            self.file_name = os.path.basename(filename)
            self.path = os.path.dirname(filename)

        if zip:
            path = '/vsizip/' + self.path
            full_path = path
        else:
            path = self.path
            full_path = os.path.join(path, self.file_name)

        if not gpkg_tbl:
            gpkg_tbl = self.gpkg_tbl

        if table:
            assert table == re.sub(r'[^A-Za-z0-9_]+', r'', table) # make sure the name will load into the database
        else:
            # clean the geopackage table name
            table = re.sub(r'[^A-Za-z0-9_]+', r'', gpkg_tbl) # clean the table name in case there are special characters

        table = table.lower()

        # format gpkg_or_shp
        gpkg_or_shp = self.gpkg_or_shp.lower() # gpkg or shp
        assert gpkg_or_shp == 'gpkg' or gpkg_or_shp == 'shp', "Your Geospatial class's gpkg_or_shape input should be either 'GPKG' or 'SHP'"

        if dbo.table_exists(table = table, schema = schema):

            if gpkg_or_shp == 'shp':
                self.del_indexes()
        
            print(f'Deleting existing table {schema}.{table}')
            dbo.drop_table(schema=schema, table=table)

        if dbo.type == 'PG' and gpkg_or_shp == 'gpkg':
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
        elif dbo.type == 'MS' and gpkg_or_shp == 'gpkg':
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

        elif dbo.type == 'PG' and gpkg_or_shp == 'shp':
        
            cmd = READ_SHP_CMD_PG.format(
                    gdal_data = gdal_data_loc,
                    srid = srid,
                    host = dbo.server,
                    dbname = dbo.database,
                    user = dbo.user,
                    password = dbo.password,
                    shp = full_path, #.lower(),
                    schema = schema,
                    tbl_name = table,
                    perc = precision,
                    port = port)

        elif dbo.type == 'MS' and gpkg_or_shp == 'shp':
            
            if dbo.LDAP:
                cmd = READ_SHP_CMD_MS.format(
                    gdal_data = gdal_data_loc,
                    srid = srid,
                    host = dbo.server,
                    dbname = dbo.database,
                    shp = full_path, # .lower(),
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
                    shp = full_path, #.lower(),
                    schema = schema,
                    tbl_name = table,
                    perc = precision,
                    port = port
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
            print(f'Ogr2ogr command failed. The .{gpkg_or_shp} file was not read in.')
            raise subprocess.CalledProcessError(cmd=print_cmd_string([dbo.password], cmd), returncode=1)

        if dbo.type == 'PG':
            dbo.query(FEATURE_COMMENT_QUERY.format(
                s=schema,
                t=table,
                u=dbo.user,
                p=self.path,
                gpkg=self.file_name,
                d=datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
            ), timeme=False, internal=True)

        if not private:
            try:
                dbo.query(f'grant select on {schema}."{table}" to public;',
                    timeme=False, internal=True, strict=True)
            except:
                pass
        rename_geom(dbo, schema, table)


    def input_gpkg_bulk(self, dbo, schema = None, port = 5432, srid = '2263', gdal_data_loc=GDAL_DATA_LOC,
                    precision=False, private=False, encoding=None, zip = False, print_cmd=False):

        """
        Reads all tables within a GEOPACKAGE file into SQL or Postgresql as tables.
        Function is NOT applicable to Shapefiles.

        :param dbo: Database connection
        :param schema (str): Schema that the imported geopackage data will be found
        :param port (int): Optional port
        :param srid (str): SRID for geometry. Defaults to 2263
        :param gdal_data_loc:
        :param precision: Default to False
        :param private: Default to False
        :param encoding: encoding of data within Geopackage
        :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
        :return:
        """

        # set the full path
        path = self.path
        full_path = os.path.join(path, self.file_name)
        
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

            self.input_geospatial_file(dbo = dbo, table = table, gpkg_tbl = gpkg_tbl, schema = schema, port = port, srid = srid, gdal_data_loc=gdal_data_loc,
                            precision=precision, private=private, encoding=encoding, zip_ = zip, print_cmd=print_cmd)
    
        return gpkg_tbl_names


    
    def del_indexes(self, dbo, schema, table):
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
                    
    def read_feature_class(self, dbo, schema, table, srid = '2263', fc_encoding = None, gdal_data_loc=GDAL_DATA_LOC, private=False, print_cmd=False):
        """
        Reads a feature of a shapefile in as a table
        :param schema: Schema for the output table in the database
        :param table: Output table name in database
        :param srid (str): SRID for geometry. Defaults to 2263
        :param fc_encoding: Optional encoding of data within feature class
        :param gdal_data_loc:d
        :param private:
        :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False

        :return:
        """
        
        file_name = self.name_extension('shp', self.file_name)

        if not all([self.path, file_name]):
            return 'Missing path and/or file_name'

        if not self.table:
            self.table = file_name.lower()

        if self.table_exists():
            # clean up spatial index
            self.del_indexes()
            print(f'Deleting existing table {schema}.{table}')
            if dbo.type == 'MS':
                dbo.drop_table(schema, table)
            else:
                dbo.drop_table(schema, table, cascade=True)

        if dbo.type == 'PG':
            cmd = READ_FEATURE_CMD.format(
                gdal_data = gdal_data_loc,
                srid = srid,
                host =  dbo.server,
                dbname = dbo.database,
                user = dbo.user,
                password= dbo.password,
                gdb = self.path,
                feature = file_name,
                tbl_name = table,
                sch = schema
            )
        else:
            # TODO: add LDAP version trusted_connection=yes
            cmd = READ_FEATURE_CMD_MS.format(
                gdal_data = gdal_data_loc,
                srid = srid,
                ms_server = dbo.server,
                ms_db = dbo.database,
                ms_user = dbo.user,
                ms_pass = dbo.password,
                gdb = self.path,
                feature= file_name,
                tbl_name=self.table,
                sch= schema,
                sf=self.skip_failures
            )

        cmd_env = os.environ.copy()
        if fc_encoding and fc_encoding.upper() == 'LATIN1':
            cmd_env['PGCLIENTENCODING'] = 'LATIN1'

        if fc_encoding and fc_encoding.upper().replace('-', '') == 'UTF8':
            cmd_env['PGCLIENTENCODING'] = 'UTF8'

        if print_cmd:
            print(print_cmd_string([dbo.password], cmd))

        try:
            ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print('Ogr2ogr command failed. The feature class was not read in.')
            raise subprocess.CalledProcessError(cmd=print_cmd_string([dbo.password], cmd), returncode=1)

        if dbo.type == 'PG':
            dbo.query(FEATURE_COMMENT_QUERY.format(
                s = schema,
                t = table,
                u = dbo.user,
                d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
            ), timeme=False, internal=True)

        if not private:
            # some SQL DBs we dont have grant permssions on
            try:
                dbo.query(f'grant select on {schema}."{table}" to public;',
                    timeme=False, internal=True, strict=True)
            except:
                pass
        rename_geom(dbo, schema, table)
        dbo.tables_created.append(schema + "." + table)