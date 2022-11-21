import shlex
import subprocess

from .cmds import *
from .sql import *
from .util import *


class Shapefile:
    def __str__(self):
        pass

    def __init__(self, dbconn=None, path=None, table_name=None, schema_name=None, query=None, shpfile_name=None, command=None,
                 srid='2263', port=5432, gdal_data_loc=GDAL_DATA_LOC, skip_failures=''):
        self.dbconn = dbconn
        self.path = path
        self.table_name = table_name
        self.schema_name = schema_name
        self.query = query
        self.shpfile_name = shpfile_name
        self.command = command
        self.srid = srid
        self.port = port
        self.gdal_data_loc = gdal_data_loc
        self.skip_failures=skip_failures

        # Use default schema from db object
        if not self.schema_name:
            self.schema_name = dbconn.default_schema

    @staticmethod
    def name_extension(name):
        """
        Adds .shp to name,  if not there

        :param name:
        :return:
        """
        if '.shp' in name:
            return name
        else:
            return name + '.shp'

    def write_shp(self, print_cmd=False):
        """
        :param print_cmd: Optional flag to print the GDAL command being used; defaults to False
        :return:
        """
        if self.table_name:
            query = "SELECT * FROM {schema}.{table}".format(schema=self.schema_name, table=self.table_name)
        else:
            query = u"SELECT * FROM ({query}) x".format(query=self.query)

        self.path, shpfile = parse_shp_path(self.path, self.shpfile_name)

        if not self.shpfile_name:
            if shpfile:
                self.shpfile_name = shpfile
            else:
                output_file_name = file_loc('save')
                self.shpfile_name = os.path.basename(output_file_name)
                self.path = os.path.dirname(output_file_name)

        if self.shpfile_name[-4:] == ".shp" and "." in self.shpfile_name[:-4]:
            self.shpfile_name = self.shpfile_name[:-4].replace(".", "_") + ".shp"
            print(' The "." character is not allowed in output shp file names. Any "." have been removed.')
        elif self.shpfile_name[-4:] != ".shp" and "." in self.shpfile_name:
            self.shpfile_name = self.shpfile_name.replace(".", "_")
            print(' The "." character is not allowed in output shp file names. Any "." have been removed.')

        if not self.path:
            self.path = file_loc('folder')

        if not self.command:
            if self.dbconn.type == 'PG':
                self.command = WRITE_SHP_CMD_PG.format(export_path=self.path,
                                                   shpfile=self.name_extension(self.shpfile_name),
                                                   host=self.dbconn.host,
                                                   username=self.dbconn.username,
                                                   db=self.dbconn.db_name,
                                                   password=self.dbconn.password,
                                                   pg_sql_select=query,
                                                   srid=self.srid,
                                                   gdal_data=self.gdal_data_loc)
            elif self.dbconn.type == 'MS':
                if self.dbconn.use_ldap:
                    self.command = WRITE_SHP_CMD_MS.replace(";UID={username};PWD={password}", "").format(
                        export_path=self.path,
                        shpfile=self.name_extension(self.shpfile_name),
                        host=self.dbconn.host,
                        db=self.dbconn.db_name,
                        ms_sql_select=query,
                        srid=self.srid,
                        gdal_data=self.gdal_data_loc
                    )
                else:
                    self.command = WRITE_SHP_CMD_MS.format(export_path=self.path,
                                                       shpfile=self.name_extension(self.shpfile_name),
                                                       host=self.dbconn.server,
                                                       username=self.dbconn.user,
                                                       db=self.dbconn.database,
                                                       password=self.dbconn.password,
                                                       ms_sql_select=query,
                                                       srid=self.srid,
                                                       gdal_data=self.gdal_data_loc)

        if print_cmd:
            print(print_cmd_string([self.dbconn.password], self.command))

        try:
            ogr_response = subprocess.check_output(shlex.split(self.command.replace('\n', ' ')), stderr=subprocess.STDOUT)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print('Ogr2ogr command failed. The shapefile/feature class was not written.')
            raise subprocess.CalledProcessError(cmd=print_cmd_string([self.dbconn.password], self.command), returncode=1)

        if self.table_name:
            print('{shp} shapefile \nwritten to: {path}\ngenerated from: {table}'.format(
                shp=self.name_extension(self.shpfile_name), path=self.path, table=self.table_name))
        else:
            print(u'{shp} shapefile \nwritten to: {path}\ngenerated from: {table}'.format(
                shp=self.name_extension(self.shpfile_name), path=self.path, table=self.query))

    def table_exists(self):
        """
        Wrapper for DbConnect table_exists function.

        :return: boolean if shp name exists as a table in the schema
        """

        return self.dbconn.table_exists(table_name=self.table_name, schema_name=self.schema_name)

    def del_indexes(self):
        """
        Drops indexes
        :return:
        """
        if self.dbconn.type == 'PG':
            self.dbconn.query(SHP_DEL_INDICES_QUERY_PG.format(
                schema_name=self.schema_name, table_name=self.table_name), internal=True)
            indexes_to_delete = self.dbconn.internal_data

            for _ in list(indexes_to_delete):
                table_name, schema_name, index_name, column_name = _
                if 'pkey' not in index_name and 'PK' not in index_name:
                    self.dbconn.query('DROP INDEX {schema}.{index}'.format(schema=self.schema_name, index=index_name),
                        strict=False, internal=True)
        else:
            self.dbconn.query(SHP_DEL_INDICES_QUERY_MS.format(schema_name=self.schema_name, table_name=self.table_name), internal=True)
            indexes_to_delete = self.dbconn.internal_data

            for _ in list(indexes_to_delete):
                table_name, index_name, column_name, idx_typ = _
                if 'pkey' not in index_name and 'PK' not in index_name:
                    self.dbconn.query('DROP INDEX {table}.{index}'.format(table=self.table_name, index=index_name),
                        strict=False, internal=True)

    def read_shp(self, precision=False, private=False, shp_encoding=None, print_cmd=False):
        """
        Reads a shapefile into a dbconnect as a table

        :param precision:
        :param private:
        :param shp_encoding: encoding of data within Shapefile
        :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
        :return:
        """
        port = self.port

        if precision:
            precision = '-lco precision=NO'
        else:
            precision = ''

        if not all([self.path, self.shpfile_name]):
            filename = file_loc('file', 'Missing file info - Opening search dialog...')
            self.shpfile_name = os.path.basename(filename)
            self.path = os.path.dirname(filename)

        if not self.table_name:
            self.table_name = self.shpfile_name.replace('.shp', '').lower()

        self.table_name = self.table_name.lower()
        if self.table_exists():
            # Clean up spatial index
            self.del_indexes()

            print('Deleting existing table {schema}.{table}'.format(schema=self.schema_name, table=self.table_name))
            self.dbconn.drop_table(schema_name=self.schema_name, table_name=self.table_name)

        if self.dbconn.type == 'PG':
            command = READ_SHP_CMD_PG.format(
                gdal_data=self.gdal_data_loc,
                srid=self.srid,
                host=self.dbconn.host,
                db=self.dbconn.db_name,
                username=self.dbconn.username,
                password=self.dbconn.password,
                shpfile=os.path.join(self.path, self.shpfile_name).lower(),
                schema=self.schema_name,
                table=self.table_name,
                precision=precision,
                port=port
            )
        elif self.dbconn.type == 'MS':
            if self.dbconn.use_ldap:
                command = READ_SHP_CMD_MS.format(
                    gdal_data=self.gdal_data_loc,
                    srid=self.srid,
                    host=self.dbconn.host,
                    db=self.dbconn.db_name,
                    shpfile=os.path.join(self.path, self.shpfile_name).lower(),
                    schema=self.schema_name,
                    table=self.table_name,
                    precision=precision,
                    port=port
                )
                command.replace(";UID={user};PWD={password}", "")

            else:
                command = READ_SHP_CMD_MS.format(
                    gdal_data=self.gdal_data_loc,
                    srid=self.srid,
                    host=self.dbconn.host,
                    db=self.dbconn.db_name,
                    username=self.dbconn.username,
                    password=self.dbconn.password,
                    shpfile=os.path.join(self.path, self.shpfile_name).lower(),
                    schema=self.schema_name,
                    table=self.table_name,
                    precision=precision,
                    port=port
                )

        command_env = os.environ.copy()

        if shp_encoding and shp_encoding.upper() == 'LATIN1':
            command_env['PGCLIENTENCODING'] = 'LATIN1'

        if shp_encoding and shp_encoding.upper().replace('-', '') == 'UTF8':
            command_env['PGCLIENTENCODING'] = 'UTF8'

        if print_cmd:
            print(print_cmd_string([self.dbconn.password], command))

        try:
            ogr_response = subprocess.check_output(shlex.split(command), stderr=subprocess.STDOUT, env=command_env)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print('Ogr2ogr command failed. The shapefile was not read in.')
            raise subprocess.CalledProcessError(cmd=print_cmd_string([self.dbconn.password], command), returncode=1)

        if self.dbconn.type == 'PG':
            self.dbconn.query(SHP_COMMENT_QUERY.format(
                schema=self.schema_name,
                table=self.table_name,
                username=self.dbconn.username,
                path=self.path,
                shpfile=self.shpfile_name,
                dt=datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
            ), timeme=False, internal=True)

        if not private:
            self.dbconn.query('grant select on {schema}."{table}" to public;'.format(
                schema=self.schema_name, table=self.table_name), timeme=False, internal=True)

        self.rename_geom()

    def read_feature_class(self, private=False, print_cmd=False, fc_encoding=None):
        """
        Reads a feature of a shapefile in as a table
        :param private:
        :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
        :param fc_encoding: Optional encoding of data within feature class
        :return:
        """
        if not all([self.path, self.shpfile_name]):
            return 'Missing path and/or shp_name'

        if not self.table_name:
            self.table_name = self.shpfile_name.lower()

        if self.table_exists():
            # clean up spatial index
            self.del_indexes()
            print('Deleting existing table {schema}.{table}'.format(schema=self.schema_name, table=self.table_name))
            if self.dbconn.type == 'MS':
                self.dbconn.drop_table(self.schema_name, self.table_name)
            else:
                self.dbconn.drop_table(self.schema_name, self.table_name, cascade=True)

        if self.dbconn.type == 'PG':
            cmd = READ_FEATURE_CMD.format(
                gdal_data=self.gdal_data_loc,
                srid=self.srid,
                host=self.dbconn.host,
                dbname=self.dbconn.database,
                username=self.dbconn.username,
                password=self.dbconn.password,
                gdb=self.path,
                feature=self.shpfile_name,
                table_name=self.table_name,
                schema_name=self.schema_name
            )
        else:
            # TODO: add LDAP version trusted_connection=yes
            cmd = READ_FEATURE_CMD_MS.format(
                gdal_data=self.gdal_data_loc,
                srid=self.srid,
                ms_host=self.dbconn.host,
                ms_dbname=self.dbconn.db_name,
                ms_username=self.dbconn.username,
                ms_password=self.dbconn.password,
                gdb=self.path,
                feature=self.shpfile_name,
                table_name=self.table_name,
                schema_name=self.schema_name,
                skip_failures=self.skip_failures
            )

        cmd_env = os.environ.copy()
        if fc_encoding and fc_encoding.upper() == 'LATIN1':
            cmd_env['PGCLIENTENCODING'] = 'LATIN1'

        if fc_encoding and fc_encoding.upper().replace('-', '') == 'UTF8':
            cmd_env['PGCLIENTENCODING'] = 'UTF8'

        if print_cmd:
            print(print_cmd_string([self.dbconn.password], cmd))

        try:
            ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print('Ogr2ogr command failed. The feature class was not read in.')
            raise subprocess.CalledProcessError(cmd=print_cmd_string([self.dbconn.password], cmd), returncode=1)

        if self.dbconn.type == 'PG':
            self.dbconn.query(FEATURE_COMMENT_QUERY.format(
                schema_name=self.schema_name,
                table_name=self.table_name,
                username=self.dbconn.username,
                dt=datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
            ), timeme=False, internal=True)

        if not private:
            self.dbconn.query('grant select on {schema}."{table}" to public;'.format(
                schema=self.schema_name, table=self.table_name), timeme=False, internal=True)

        self.rename_geom()

    def rename_geom(self):
        """
        Renames wkb_geometry to geom, along with index

        :return:
        """
        self.dbconn.query("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = '{schema}'
                        AND table_name   = '{table}';
                    """.format(schema=self.schema_name, table=self.table_name), timeme=False, internal=True)
        f = None

        if self.dbconn.type == 'PG':

            # Get the column in question
            if 'wkb_geometry' in [i[0] for i in self.dbconn.internal_queries[-1].data]:
                f = 'wkb_geometry'
            elif 'shape' in [i[0] for i in self.dbconn.internal_queries[-1].data]:
                f = 'shape'

            if f:
                # Rename column
                self.dbconn.rename_column(schema_name=self.schema_name, table_name=self.table_name, old_column=f, new_column='geom')

                # Rename index
                self.dbconn.query("""
                    ALTER INDEX IF EXISTS
                    {schema}.{table}_{f}_geom_idx
                    RENAME to {table}_geom_idx
                """.format(schema=self.schema_name, table=self.table_name, f=f), timeme=False, internal=True)

        elif self.dbconn.type == 'MS':
            # Get the column in question
            if 'ogr_geometry' in [i[0] for i in self.dbconn.internal_queries[-1].data]:
                f = 'ogr_geometry'
            elif 'Shape' in [i[0] for i in self.dbconn.internal_queries[-1].data]:
                f = 'Shape'

            if f:
                # Rename column
                self.dbconn.rename_column(schema_name=self.schema_name, table_name=self.table_name, old_column=f, new_column='geom')

                # Rename index if exists
                try:
                    self.dbconn.query("""
                        EXEC sp_rename N'{schema}.{table}.ogr_{schema}_{table}_{f}_sidx', N'{table}_geom_idx', N'INDEX';
                    """.format(schema=self.schema_name, table=self.table_name, f=f), timeme=False, internal=True)
                except SystemExit as e:
                    print(e)
                    print('Warning - could not update index name after renaming geometry. It may not exist.')
