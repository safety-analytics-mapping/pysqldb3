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

    def __init__(self, path=None, gpkg_name=None, gpkg_tbl = None, skip_failures='', shp_name = None):
        self.path = path
        self.gpkg_name = gpkg_name
        self.gpkg_tbl = gpkg_tbl
        self.skip_failures=skip_failures
        self.shp_name = shp_name

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

    def write_gpkg(self, dbo = None, table = None, schema  = None, query = None, srid='2263', gdal_data_loc=GDAL_DATA_LOC, cmd = None, overwrite = False, print_cmd=False):
        
        """
        Converts a SQL or Postgresql query to a new geopackage.
        Rename the geopackage table using the argument gpkg_tbl.
        Otherwise, the table name in the geopackage output will match the name of the input db table.
        
        :param dbo: Database connection
        :param table (str): DB Table
        :param schema (str): DB schema
        :param query (str): DB query whose output is to be written to a GPKG
        :param srid (str): SRID for geometry. Defaults to 2263
        :param cmd (str): Command
        :param overwrite (bool): Overwrite the specific table in the geopackage; defaults to False
        :param print_cmd (bool): Optional flag to print the GDAL command being used; defaults to False
        :return:
        """
        
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

        if not self.gpkg_tbl:
            self.gpkg_tbl = table

        # overwrite vs update vs an issue has arisen
        if overwrite:
            # if explict overwrite, then create command line as directed
            _update = ''
            _overwrite = '-overwrite'
        
        elif not overwrite and not os.path.isfile(os.path.join(self.path, self.gpkg_name)):
            # this geopackage does not exist so create as if new
            _update = ''
            _overwrite = ''
        
        elif not overwrite and os.path.isfile(os.path.join(self.path, self.gpkg_name)): # check if the geopackage already exists
            
            try:
                exists_cmd = f'ogrinfo {os.path.join(self.path, self.gpkg_name)}'
                ogr_response = subprocess.check_output(exists_cmd, stderr=subprocess.STDOUT)
                table_exists = re.findall(f"{self.gpkg_tbl}", str(ogr_response)) # only allows tables names with underscores, numbers, and letters
                
            except Exception:
                if len(table_exists) == 0:
                    "The table name to be exported already exists in the geopackage. Either change to Overwrite = True or check the name of the table to be copied."

            finally:
            # update variables if this case is successful
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
                                                   gpkg_tbl = self.gpkg_tbl,
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
                        gpkg_tbl = self.gpkg_tbl,
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
                                                       gpkg_tbl = self.gpkg_tbl,
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
            
    def shp_to_gpkg(self, shp_name, gpkg_tbl = None, cmd = None):
        
        """
        Converts a Shapefile to a Geopackage file in the same file location as the original Shapefile.
        :param shp_name (str): file name for shape file input (should end in .shp)
        :param gpkg_name (str): file name for gpkg ouput (should end with .gpkg)
        :param gpkg_tbl (str): OPTIONAL table name in the new geopackage; leave blank if you want the table name the same as the gpkg_name
        :param print_cmd (bool): Print command line query
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

        # check if the gpkg already exists. if so, we add another layer to the gpkg
        if not os.path.isfile(os.path.join(self.path, self.gpkg_name)):

            cmd = WRITE_SHP_CMD_GPKG.format(gpkg_name = self.gpkg_name,
                                                  full_path = self.path,
                                                shp_name = shp_name,
                                                _update = '',
                                                gpkg_tbl = gpkg_tbl)
        
        else:

            cmd = WRITE_SHP_CMD_GPKG.format(gpkg_name = self.gpkg_name,
                                                  full_path = self.path,
                                                shp_name = shp_name,
                                                _update = '-update',
                                                gpkg_tbl = gpkg_tbl)

        try:
            ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print('Ogr2ogr command failed. The Geopackage/feature class was not written.')
        return

    def gpkg_to_shp(self, gpkg_tbl = None, export_path = None, cmd = None):
        """
        Converts a Geopackage to a Shapefile.
        The output Shapefile name will match the name of the geopackage table to be copied.

        :param gpkg_name (str): file name for geopackage input (should end in .gpkg)
        :param gpkg_tbl (str): OPTIONAL specific table within the geopackage to convert to a Shapefile
        :param export_path: str The folder directory to place the shapefiles output.
                            You cannot specify the shapefiles' names as they are copied from the table names within the geopackage.
        :param print_cmd (bool): Print command line query
        """
        
        assert self.gpkg_name[-5:] == '.gpkg', "The input file should end with .gpkg . Please check your input."

        self.path, gpkg = parse_gpkg_path(self.path, self.gpkg_name)

        # set a default self.path for the gpkg input if it exists
        if not self.path:
            self.path = file_loc('folder')

        # set an export path to the gpkg if it is not manually set up
        if not export_path:
            export_path = self.path

        ### bulk upload capabilities ### 
        
        if not gpkg_tbl:

            #######
            # if no table name listed in argument, then we assume bulk upload
            # this prompts us to count the number of tables within the geopackage to confirm bulk upload
            
            # confirm number of tables within the geotable
            count_cmd = COUNT_GPKG_LAYERS.format(full_path = os.path.join(self.path, self.gpkg_name)) 

            try:
                ogr_response = subprocess.check_output(shlex.split(count_cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
                tables_in_gpkg = re.findall(r"\\n\d+:\s(?:[a-z0-9A-Z_]+)", str(ogr_response)) # only allows tables names with underscores, numbers, and letters
                tbl_count = len(tables_in_gpkg)
            
            except subprocess.CalledProcessError as e:
                print("Ogr2ogr Output:\n", e.output)
                print('Ogr2ogr command failed. The Geopackage was not read in.')
                raise subprocess.CalledProcessError(count_cmd, returncode=1)
            
            if tbl_count > 1:

                assert not gpkg_tbl, """Since the geopackage has more than 1 layer to be read, you cannot specify one output table name to apply to all the tables. \n
                                    Either run the function for each table with customized output table names separately, \n
                                    or remove the 'db_table' or 'table' argument for bulk upload.
                                    """

            # create a list of cleaned gpkg table names
                gpkg_tbl_names = {} # create empty dictionary
                for t_i_g in tables_in_gpkg:
                    insert_val = re.search(r'([a-z0-9A-Z_]+)$', t_i_g).group()
                    gpkg_tbl_names[insert_val] = insert_val # add the cleaned name

            # if the bulk upload involves only 1 table and no db table name was specified, we name the table the same thing as the original gpkg table
            elif tbl_count == 1:            
                gpkg_tbl_names = {re.search(r'([a-z0-9A-Z_]+)$', tables_in_gpkg[0]).group(): gpkg_tbl}

        else:
            
            gpkg_tbl = self.gpkg_tbl.replace('.gpkg', '').lower()

            # create empty list of table names that will get read into the db
            gpkg_tbl_names = {self.gpkg_tbl: gpkg_tbl}
        

        for input_name, output_name in gpkg_tbl_names.items():
            
            cmd = WRITE_GPKG_CMD_SHP.format(   full_path = self.path,
                                                    gpkg_name = self.gpkg_name,
                                                    gpkg_tbl = input_name,
                                                    export_path=export_path)
            
            try:
                ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
                print(ogr_response)
            except subprocess.CalledProcessError as e:
                print("Ogr2ogr Output:\n", e.output)
                print('Ogr2ogr command failed. The Geopackage/feature class was not written.')
        
        return

    def table_exists(self, dbo = None, schema = None, table = None):
        """
        Wrapper for DbConnect table_exists function.

        :param table: table name
        :param schema: schema name
        :return: boolean if gpkg name exists as a table in the schema
        """

        return dbo.table_exists(schema = schema, table = table)

    def del_indexes(self, dbo, schema = None, table = None):
        """
        Drops indexes
        :param dbo: database connection
        :param schema: Schema for deleting index
        :param table: Table for deleting index
        :return:
        """
        
        if dbo.type == 'PG':
            dbo.query(SHP_DEL_INDICES_QUERY_PG.format(s=schema, t=table), internal=True)
            indexes_to_delete = dbo.internal_data

            for _ in list(indexes_to_delete):
                table_name, schema_name, index_name, column_name = _
                if 'pkey' not in index_name and 'PK' not in index_name:
                    dbo.query('DROP INDEX {s}.{i}'.format(s=schema, i=index_name),
                                   strict=False, internal=True)
        else:
            dbo.query(SHP_DEL_INDICES_QUERY_MS.format(s=schema, t=table), internal=True)
            indexes_to_delete = dbo.internal_data

            for _ in list(indexes_to_delete):
                table_name, index_name, column_name, idx_typ = _
                if 'pkey' not in index_name and 'PK' not in index_name:
                    dbo.query('DROP INDEX {t}.{i}'.format(t=table, i=index_name), strict=False, internal=True)

    def read_gpkg(self, dbo = None, schema = None, table = None, port = 5432, srid = '2263', gdal_data_loc=GDAL_DATA_LOC, precision=False, private=True, gpkg_encoding=None, print_cmd=False):
        """
        Reads a geopackage into SQL or Postgresql as a table

        :param dbo: Database connection
        :param schema (str): Schema that the imported geopackage data will be found
        :param table (str): Name for uploaded table
        :param port (int): Optional port
        :param srid (str): SRID for geometry. Defaults to 2263
        :param precision:
        :param private:
        :param gpkg_encoding: encoding of data within Geopackage
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

        if not all([self.path, self.gpkg_name]):
            filename = file_loc('file', 'Missing file info - Opening search dialog...')
            self.gpkg_name = os.path.basename(filename)
            self.path = os.path.dirname(filename)

        if table:
            table = table.lower()

        if dbo.table_exists(schema = schema, table = table):
            # Clean up spatial index
            self.del_indexes(dbo = dbo, schema = schema, table = table)

            print('Deleting existing table {s}.{t}'.format(s=schema, t=table))
            dbo.drop_table(schema=schema, table=table)

        path = self.path
        full_path = os.path.join(path, self.gpkg_name)
    
        # based on whether gpkg_table name is filled in, bulk upload or load specific table
        if not self.gpkg_tbl:

            #######
            # if no table name listed in argument, then we assume bulk upload
            # this prompts us to count the number of tables within the geopackage to confirm bulk upload
            
            # confirm number of tables within the geotable
            count_cmd = COUNT_GPKG_LAYERS.format(full_path = full_path) 

            try:
                ogr_response = subprocess.check_output(shlex.split(count_cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
                tables_in_gpkg = re.findall(r"\\n\d+:\s(?:[a-z0-9A-Z_]+)", str(ogr_response)) # only allows tables names with underscores, numbers, and letters
                tbl_count = len(tables_in_gpkg)
            
            except subprocess.CalledProcessError as e:
                print("Ogr2ogr Output:\n", e.output)
                print('Ogr2ogr command failed. The Geopackage was not read in.')
                raise subprocess.CalledProcessError(cmd=print_cmd_string([dbo.password], count_cmd), returncode=1)
            
            if tbl_count > 1:

                assert not table, """Since the geopackage has more than 1 layer to be read, you cannot specify one output table name to apply to all the tables. \n
                                    Either run the function for each table with customized output table names separately, \n
                                    or remove the 'db_table' or 'table' argument for bulk upload.
                                    """

            # create a list of cleaned gpkg table names
                gpkg_tbl_names = {} # create empty dictionary
                for t_i_g in tables_in_gpkg:
                    insert_val = re.search(r'([a-z0-9A-Z_]+)$', t_i_g).group()
                    gpkg_tbl_names[insert_val] = insert_val # add the cleaned name

            # if the bulk upload involves only 1 table and no db table name was specified, we name the table the same thing as the original gpkg table
            elif tbl_count == 1:            
                gpkg_tbl_names = {re.search(r'([a-z0-9A-Z_]+)$', tables_in_gpkg[0]).group(): table}

        else:

            if not table:
                table = self.gpkg_tbl.replace('.gpkg', '').lower()

            # create empty list of table names that will get read into the db
            gpkg_tbl_names = {self.gpkg_tbl: table}


        # loop through the tables     
        for input_name, output_name in gpkg_tbl_names.items():
            
            if dbo.table_exists(table = table, schema = schema):
            # Clean up spatial index
                self.del_indexes(dbo = dbo, table = table, schema = schema)

                print('Deleting existing table {s}.{t}'.format(s=schema, t=output_name))
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
                        tbl_name=output_name,
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
                        tbl_name=output_name,
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
                    dbo.query('grant select on {s}."{t}" to public;'.format(
                        s=schema, t=output_name),
                        timeme=False, internal=True, strict=True)
                except:
                    pass
            self.rename_geom(dbo, schema, table)

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
                    dbo.query("""
                        EXEC sp_rename N'{s}.{t}.ogr_{s}_{t}_{f}_sidx', N'{t}_geom_idx', N'INDEX';
                    """.format(s=schema, t=table, f=f), timeme=False, internal=True)
                except SystemExit as e:
                    print(e)
                    print('Warning - could not update index name after renaming geometry. It may not exist.')
