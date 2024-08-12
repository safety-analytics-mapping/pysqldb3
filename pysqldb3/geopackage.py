import shlex
import subprocess

from .cmds import *
from .sql import *
from .util import *


class Geopackage:
    def __str__(self):
        pass

    def __init__(self, dbo=None, path=None, table=None, schema=None, query=None, gpkg_name=None, cmd=None,
                 srid='2263', port=5432, gdal_data_loc=GDAL_DATA_LOC, skip_failures='', shp_name = None):
        self.dbo = dbo
        self.path = path
        self.table = table
        self.schema = schema
        self.query = query
        self.gpkg_name = gpkg_name
        self.cmd = cmd
        self.srid = srid
        self.port = port
        self.gdal_data_loc = gdal_data_loc
        self.skip_failures=skip_failures
        self.shp_name = shp_name

        # Use default schema from db object
        if not self.schema:
            self.schema = dbo.default_schema

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

    def write_gpkg(self, tbl_name = None, print_cmd=False):
        """
        Converts a SQL or Postgresql query to a geopackage output.

        :param tbl_name: Name of the table in the Geopackage output (it will default to 'SELECT' as the table name otherwise)
        :param print_cmd: Optional flag to print the GDAL command being used; defaults to False
        :return:
        """
        if self.table:
            qry = f"SELECT * FROM {self.schema}.{self.table}"
        else:
            qry = f"SELECT * FROM ({self.query}) x"

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

        if not self.table:
            self.table = tbl_name

        if not self.cmd:
            if self.dbo.type == 'PG':
                self.cmd = WRITE_GPKG_CMD_PG.format(export_path=self.path,
                                                   gpkg_name=self.name_extension(self.gpkg_name),
                                                   host=self.dbo.server,
                                                   username=self.dbo.user,
                                                   db=self.dbo.database,
                                                   password=self.dbo.password,
                                                   pg_sql_select=qry,
                                                   tbl_name = self.table,
                                                   srid=self.srid,
                                                   gdal_data=self.gdal_data_loc)
            elif self.dbo.type == 'MS':
                if self.dbo.LDAP:
                    self.cmd = WRITE_GPKG_CMD_MS.replace(";UID={username};PWD={password}", "").format(
                        export_path=self.path,
                        gpkg_name=self.name_extension(self.gpkg_name),
                        host=self.dbo.server,
                        db=self.dbo.database,
                        ms_sql_select=qry,
                        tbl_name = self.table,
                        srid=self.srid,
                        gdal_data=self.gdal_data_loc
                    )
                else:
                    self.cmd = WRITE_GPKG_CMD_MS.format(export_path=self.path,
                                                       gpkg_name=self.name_extension(self.gpkg_name),
                                                       host=self.dbo.server,
                                                       username=self.dbo.user,
                                                       db=self.dbo.database,
                                                       password=self.dbo.password,
                                                       ms_sql_select=qry,
                                                       tbl_name = self.table,
                                                       srid=self.srid,
                                                       gdal_data=self.gdal_data_loc)

        if print_cmd:
            print(print_cmd_string([self.dbo.password], self.cmd))

        try:
            ogr_response = subprocess.check_output(shlex.split(self.cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print('Ogr2ogr command failed. The Geopackage/feature class was not written.')
            raise subprocess.CalledProcessError(cmd=print_cmd_string([self.dbo.password], self.cmd), returncode=1)

        if self.table:
            print('{t} geopackage \nwritten to: {p}\ngenerated from: {q}'.format(t=self.name_extension(self.gpkg_name),
                                                                                p=self.path,
                                                                                q=self.table))
        else:
            print(u'{t} geopackage \nwritten to: {p}\ngenerated from: {q}'.format(
                t=self.name_extension(self.gpkg_name),
                p=self.path,
                q=self.query))
            
    def shp_to_gpkg(self, shp_name=None, gpkg_name=None, tbl_name=None, print_cmd=False):
        
        """
        Converts a Shapefile to a Geopackage file
        :param gpkg_name: filename for geopackage (should end in .gpkg)
        :param shp_name: filename for shape file (should end in .shp)
        :param print_cmd: Print command line query
        """
        
        assert gpkg_name[-5:] == '.gpkg', "The input file should end with .gpkg . Please check your input."
        assert shp_name[-4:] == '.shp', "The input file should end with .shp . Please check your input."

        # same function as shape file
        self.path, shp_name = parse_shp_path(self.path, shp_name)

        if not self.path:
            self.path = file_loc('folder')

        self.cmd = WRITE_SHP_CMD_GPKG.format(gpkg_name = gpkg_name,
                                                shp_name = shp_name,
                                                tbl_name = self.table,
                                                export_path=self.path)
        
        if print_cmd:
            print(print_cmd_string([self.dbo.password], self.cmd))

        try:
            ogr_response = subprocess.check_output(shlex.split(self.cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print('Ogr2ogr command failed. The Geopackage/feature class was not written.')
            raise subprocess.CalledProcessError(cmd=print_cmd_string([self.dbo.password], self.cmd), returncode=1)

        return

    def gpkg_to_shp(self, export_path = None, gpkg_name=None, print_cmd=False):
        
        """
        Converts a Geopackage to a Shapefile
        :param gpkg_name: filename for geopackage (should end in .gpkg)
        :param export_path: The folder directory to place the shapefiles output.
                            You cannot specify the shapefiles' names as they are copied from the table names within the geopackage.
        :param tbl_name: Name of the first table in the geopackage whose name will be transferred to the Shapefile.
                         If there are more than 1 table, the subsequent names will default.
        :param print_cmd: Print command line query
        """
        
        assert gpkg_name[-5:] == '.gpkg', "The input file should end with .gpkg . Please check your input."

        self.path, gpkg = parse_gpkg_path(self.path, self.gpkg_name)

        if not self.path:
            self.path = file_loc('folder')

        self.cmd = WRITE_GPKG_CMD_SHP.format(   gpkg_name = gpkg_name,
                                                export_path=self.path)

        if print_cmd:
            print(print_cmd_string([self.dbo.password], self.cmd))

        try:
            ogr_response = subprocess.check_output(shlex.split(self.cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print('Ogr2ogr command failed. The Geopackage/feature class was not written.')
            raise subprocess.CalledProcessError(cmd=print_cmd_string([self.dbo.password], self.cmd), returncode=1)
        
        return

    def table_exists(self):
        """
        Wrapper for DbConnect table_exists function.

        :return: boolean if gpkg name exists as a table in the schema
        """

        return self.dbo.table_exists(table=self.table, schema=self.schema)

    def del_indexes(self):
        """
        Drops indexes
        :return:
        """
        if self.dbo.type == 'PG':
            self.dbo.query(GPKG_DEL_INDICES_QUERY_PG.format(s=self.schema, t=self.table), internal=True)
            indexes_to_delete = self.dbo.internal_data

            for _ in list(indexes_to_delete):
                table_name, schema_name, index_name, column_name = _
                if 'pkey' not in index_name and 'PK' not in index_name:
                    self.dbo.query('DROP INDEX {s}.{i}'.format(s=self.schema, i=index_name),
                                   strict=False, internal=True)
        else:
            self.dbo.query(GPKG_DEL_INDICES_QUERY_MS.format(s=self.schema, t=self.table), internal=True)
            indexes_to_delete = self.dbo.internal_data

            for _ in list(indexes_to_delete):
                table_name, index_name, column_name, idx_typ = _
                if 'pkey' not in index_name and 'PK' not in index_name:
                    self.dbo.query('DROP INDEX {t}.{i}'.format(t=self.table, i=index_name), strict=False, internal=True)

    def read_gpkg(self, precision=False, private=True, gpkg_encoding=None, print_cmd=False, zip=False):
        """
        Reads a geopackage into SQL or Postgresql as a table

        :param precision:
        :param private:
        :param gpkg_encoding: encoding of data within Geopackage
        :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
        :param zip: Optional flag needed if reading from a zipped file; defaults to False
        :return:
        """
        port = self.port

        if precision:
            precision = '-lco precision=NO'
        else:
            precision = ''

        if not all([self.path, self.gpkg_name]):
            filename = file_loc('file', 'Missing file info - Opening search dialog...')
            self.gpkg_name = os.path.basename(filename)
            self.path = os.path.dirname(filename)

        if not self.table:
            self.table = self.gpkg_name.replace('.gpkg', '').lower()

        self.table = self.table.lower()
        if self.table_exists():
            # Clean up spatial index
            self.del_indexes()

            print('Deleting existing table {s}.{t}'.format(s=self.schema, t=self.table))
            self.dbo.drop_table(schema=self.schema, table=self.table)

        path = self.path
        full_path = os.path.join(path, self.gpkg_name)

        if self.dbo.type == 'PG':
            cmd = READ_GPKG_CMD_PG.format(
                gdal_data=self.gdal_data_loc,
                srid=self.srid,
                host=self.dbo.server,
                dbname=self.dbo.database,
                user=self.dbo.user,
                password=self.dbo.password,
                gpkg_name = full_path,
                schema=self.schema,
                tbl_name=self.table,
                perc=precision,
                port=port
            )
        elif self.dbo.type == 'MS':
            if self.dbo.LDAP:
                cmd = READ_GPKG_CMD_MS.format(
                    gdal_data=self.gdal_data_loc,
                    srid=self.srid,
                    host=self.dbo.server,
                    dbname=self.dbo.database,
                    gpkg_name=full_path,
                    schema=self.schema,
                    tbl_name=self.table,
                    perc=precision,
                    port=port
                )
                cmd.replace(";UID={user};PWD={password}", "")

            else:
                cmd = READ_GPKG_CMD_MS.format(
                    gdal_data=self.gdal_data_loc,
                    srid=self.srid,
                    host=self.dbo.server,
                    dbname=self.dbo.database,
                    user=self.dbo.user,
                    password=self.dbo.password,
                    gpkg_name=full_path,
                    schema=self.schema,
                    tbl_name=self.table,
                    perc=precision,
                    port=port
                )

        cmd_env = os.environ.copy()

        if gpkg_encoding and gpkg_encoding.upper() == 'LATIN1':
            cmd_env['PGCLIENTENCODING'] = 'LATIN1'

        if gpkg_encoding and gpkg_encoding.upper().replace('-', '') == 'UTF8':
            cmd_env['PGCLIENTENCODING'] = 'UTF8'

        if print_cmd:
            print(print_cmd_string([self.dbo.password], cmd))

        try:
            ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT, env=cmd_env)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print('Ogr2ogr command failed. The Geopackage was not read in.')
            raise subprocess.CalledProcessError(cmd=print_cmd_string([self.dbo.password], cmd), returncode=1)

        if self.dbo.type == 'PG':
            self.dbo.query(GPKG_COMMENT_QUERY.format(
                s=self.schema,
                t=self.table,
                u=self.dbo.user,
                p=self.path,
                gpkg=self.gpkg_name,
                d=datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
            ), timeme=False, internal=True)

        if not private:
            try:
                self.dbo.query('grant select on {s}."{t}" to public;'.format(
                    s=self.schema, t=self.table),
                    timeme=False, internal=True, strict=True)
            except:
                pass
        self.rename_geom()

    def read_feature_class(self, private=False, print_cmd=False, fc_encoding=None):
        """
        Reads a feature of a Geopackage in as a table
        :param private:
        :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
        :param fc_encoding: Optional encoding of data within feature class
        :return:
        """
        if not all([self.path, self.gpkg_name]):
            return 'Missing path and/or gpkg_name'

        if not self.table:
            self.table = self.gpkg_name.lower()

        if self.table_exists():
            # clean up spatial index
            self.del_indexes()
            print('Deleting existing table {s}.{t}'.format(s=self.schema, t=self.table))
            if self.dbo.type == 'MS':
                self.dbo.drop_table(self.schema, self.table)
            else:
                self.dbo.drop_table(self.schema, self.table, cascade=True)

        if self.dbo.type == 'PG':
            cmd = READ_GPKG_FEATURE_CMD.format(
                gdal_data=self.gdal_data_loc,
                srid=self.srid,
                host=self.dbo.server,
                dbname=self.dbo.database,
                user=self.dbo.user,
                password=self.dbo.password,
                gdb=self.path,
                feature=self.gpkg_name,
                tbl_name=self.table,
                sch=self.schema
            )
        else:
            # TODO: add LDAP version trusted_connection=yes
            cmd = READ_GPKG_FEATURE_CMD_MS.format(
                gdal_data=self.gdal_data_loc,
                srid=self.srid,
                ms_server=self.dbo.server,
                ms_db=self.dbo.database,
                ms_user=self.dbo.user,
                ms_pass=self.dbo.password,
                gdb=self.path,
                feature=self.gpkg_name,
                tbl_name=self.table,
                sch=self.schema,
                sf=self.skip_failures
            )

        cmd_env = os.environ.copy()
        if fc_encoding and fc_encoding.upper() == 'LATIN1':
            cmd_env['PGCLIENTENCODING'] = 'LATIN1'

        if fc_encoding and fc_encoding.upper().replace('-', '') == 'UTF8':
            cmd_env['PGCLIENTENCODING'] = 'UTF8'

        if print_cmd:
            print(print_cmd_string([self.dbo.password], cmd))

        try:
            ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print('Ogr2ogr command failed. The feature class was not read in.')
            raise subprocess.CalledProcessError(cmd=print_cmd_string([self.dbo.password], cmd), returncode=1)

        if self.dbo.type == 'PG':
            self.dbo.query(FEATURE_COMMENT_QUERY.format(
                s=self.schema,
                t=self.table,
                u=self.dbo.user,
                d=datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
            ), timeme=False, internal=True)

        if not private:
            # some SQL DBs we dont have grant permssions on
            try:
                self.dbo.query('grant select on {s}."{t}" to public;'.format(
                    s=self.schema, t=self.table),
                    timeme=False, internal=True, strict=True)
            except:
                pass
        self.rename_geom()
        self.dbo.tables_created.append(self.schema + "." + self.table)

    def rename_geom(self):
        """
        Renames wkb_geometry to geom, along with index

        :return:
        """
        self.dbo.query("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = '{s}'
                        AND table_name   = '{t}';
                    """.format(s=self.schema, t=self.table), timeme=False, internal=True)
        f = None

        if self.dbo.type == 'PG':

            # Get the column in question
            if 'wkb_geometry' in [i[0] for i in self.dbo.internal_queries[-1].data]:
                f = 'wkb_geometry'
            elif 'shape' in [i[0] for i in self.dbo.internal_queries[-1].data]:
                f = 'shape'

            if f:
                # Rename column
                self.dbo.rename_column(schema=self.schema, table=self.table, old_column=f, new_column='geom')

                # Rename index
                self.dbo.query("""
                    ALTER INDEX IF EXISTS
                    {s}.{t}_{f}_geom_idx
                    RENAME to {t}_geom_idx
                """.format(s=self.schema, t=self.table, f=f), timeme=False, internal=True)

        elif self.dbo.type == 'MS':
            # Get the column in question
            if 'ogr_geometry' in [i[0] for i in self.dbo.internal_queries[-1].data]:
                f = 'ogr_geometry'
            elif 'Shape' in [i[0] for i in self.dbo.internal_queries[-1].data]:
                f = 'Shape'

            if f:
                # Rename column
                self.dbo.rename_column(schema=self.schema, table=self.table, old_column=f, new_column='geom')

                # Rename index if exists
                try:
                    self.dbo.query("""
                        EXEC sp_rename N'{s}.{t}.ogr_{s}_{t}_{f}_sidx', N'{t}_geom_idx', N'INDEX';
                    """.format(s=self.schema, t=self.table, f=f), timeme=False, internal=True)
                except SystemExit as e:
                    print(e)
                    print('Warning - could not update index name after renaming geometry. It may not exist.')
