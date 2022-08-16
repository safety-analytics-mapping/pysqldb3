import shlex
import subprocess

from cmds import *
from sql import *
from util import *


class Shapefile:
    def __str__(self):
        pass

    def __init__(self, dbo=None, path=None, table=None, schema=None, query=None, shp_name=None, cmd=None,
                 srid='2263', port=5432, gdal_data_loc=GDAL_DATA_LOC, skip_failures=''):
        self.dbo = dbo
        self.path = path
        self.table = table
        self.schema = schema
        self.query = query
        self.shp_name = shp_name
        self.cmd = cmd
        self.srid = srid
        self.port = port
        self.gdal_data_loc = gdal_data_loc
        self.skip_failures=skip_failures

        # Use default schema from db object
        if not self.schema:
            self.schema = dbo.default_schema

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
        if self.table:
            qry = f"SELECT * FROM {self.schema}.{self.table}"
        else:
            qry = u"SELECT * FROM ({self.query}) x"

        self.path, shp = parse_shp_path(self.path, self.shp_name)

        if not self.shp_name:
            if shp:
                self.shp_name = shp
            else:
                output_file_name = file_loc('save')
                self.shp_name = os.path.basename(output_file_name)
                self.path = os.path.dirname(output_file_name)

        if self.shp_name[-4:] == ".shp" and "." in self.shp_name[:-4]:
            self.shp_name = self.shp_name[:-4].replace(".", "_") + ".shp"
            print(' The "." character is not allowed in output shp file names. Any "." have been removed.')
        elif self.shp_name[-4:] != ".shp" and "." in self.shp_name:
            self.shp_name = self.shp_name.replace(".", "_")
            print(' The "." character is not allowed in output shp file names. Any "." have been removed.')

        if not self.path:
            self.path = file_loc('folder')

        if not self.cmd:
            if self.dbo.type == 'PG':
                self.cmd = WRITE_SHP_CMD_PG.format(export_path=self.path,
                                                   shpname=self.name_extension(self.shp_name),
                                                   host=self.dbo.server,
                                                   username=self.dbo.user,
                                                   db=self.dbo.database,
                                                   password=self.dbo.password,
                                                   pg_sql_select=qry,
                                                   srid=self.srid,
                                                   gdal_data=self.gdal_data_loc)
            elif self.dbo.type == 'MS':
                if self.dbo.LDAP:
                    self.cmd = WRITE_SHP_CMD_MS.replace(";UID={username};PWD={password}", "").format(
                        export_path=self.path,
                        shpname=self.name_extension(self.shp_name),
                        host=self.dbo.server,
                        db=self.dbo.database,
                        ms_sql_select=qry,
                        srid=self.srid,
                        gdal_data=self.gdal_data_loc
                    )
                else:
                    self.cmd = WRITE_SHP_CMD_MS.format(export_path=self.path,
                                                       shpname=self.name_extension(self.shp_name),
                                                       host=self.dbo.server,
                                                       username=self.dbo.user,
                                                       db=self.dbo.database,
                                                       password=self.dbo.password,
                                                       ms_sql_select=qry,
                                                       srid=self.srid,
                                                       gdal_data=self.gdal_data_loc)

        if print_cmd:
            print(print_cmd_string([self.dbo.password], self.cmd))

        try:
            ogr_response = subprocess.check_output(shlex.split(self.cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print('Ogr2ogr command failed. The shapefile/feature class was not written.')
            raise subprocess.CalledProcessError(cmd=print_cmd_string([self.dbo.password], self.cmd), returncode=1)

        if self.table:
            print(f'{self.name_extension(self.shp_name)} shapefile \nwritten to: {self.path}\ngenerated from: {self.table}')
        else:
            print(f'{self.name_extension(self.shp_name)} shapefile \nwritten to: {self.path}\ngenerated from: {self.query}')

    def table_exists(self):
        """
        Wrapper for DbConnect table_exists function.

        :return: boolean if shp name exists as a table in the schema
        """

        return self.dbo.table_exists(table=self.table, schema=self.schema)

    def del_indexes(self):
        """
        Drops indexes
        :return:
        """
        if self.dbo.type == 'PG':
            self.dbo.query(SHP_DEL_INDICES_QUERY_PG.format(s=self.schema, t=self.table), internal=True)
            indexes_to_delete = self.dbo.internal_data

            for _ in list(indexes_to_delete):
                table_name, schema_name, index_name, column_name = _
                if 'pkey' not in index_name and 'PK' not in index_name:
                    self.dbo.query(f'DROP INDEX {self.schema}.{index_name}', strict=False, internal=True)
        else:
            self.dbo.query(SHP_DEL_INDICES_QUERY_MS.format(s=self.schema, t=self.table), internal=True)
            indexes_to_delete = self.dbo.internal_data

            for _ in list(indexes_to_delete):
                table_name, index_name, column_name, idx_typ = _
                if 'pkey' not in index_name and 'PK' not in index_name:
                    self.dbo.query(f'DROP INDEX {self.schema}.{index_name}', strict=False, internal=True)

    def read_shp(self, precision=False, private=False, shp_encoding=None, print_cmd=False):
        """
        Reads a shapefile in as a table

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

        if not all([self.path, self.shp_name]):
            filename = file_loc('file', 'Missing file info - Opening search dialog...')
            self.shp_name = os.path.basename(filename)
            self.path = os.path.dirname(filename)

        if not self.table:
            self.table = self.shp_name.replace('.shp', '').lower()

        self.table = self.table.lower()
        if self.table_exists():
            # Clean up spatial index
            self.del_indexes()

            print(f'Deleting existing table {self.schema}.{self.table}')
            self.dbo.drop_table(schema=self.schema, table=self.table)

        if self.dbo.type == 'PG':
            cmd = READ_SHP_CMD_PG.format(
                gdal_data=self.gdal_data_loc,
                srid=self.srid,
                host=self.dbo.server,
                dbname=self.dbo.database,
                user=self.dbo.user,
                password=self.dbo.password,
                shp=os.path.join(self.path, self.shp_name).lower(),
                schema=self.schema,
                tbl_name=self.table,
                perc=precision,
                port=port
            )
        elif self.dbo.type == 'MS':
            if self.dbo.LDAP:
                cmd = READ_SHP_CMD_MS.format(
                    gdal_data=self.gdal_data_loc,
                    srid=self.srid,
                    host=self.dbo.server,
                    dbname=self.dbo.database,
                    shp=os.path.join(self.path, self.shp_name).lower(),
                    schema=self.schema,
                    tbl_name=self.table,
                    perc=precision,
                    port=port
                )
                cmd.replace(";UID={user};PWD={password}", "")

            else:
                cmd = READ_SHP_CMD_MS.format(
                    gdal_data=self.gdal_data_loc,
                    srid=self.srid,
                    host=self.dbo.server,
                    dbname=self.dbo.database,
                    user=self.dbo.user,
                    password=self.dbo.password,
                    shp=os.path.join(self.path, self.shp_name).lower(),
                    schema=self.schema,
                    tbl_name=self.table,
                    perc=precision,
                    port=port
                )

        cmd_env = os.environ.copy()

        if shp_encoding and shp_encoding.upper() == 'LATIN1':
            cmd_env['PGCLIENTENCODING'] = 'LATIN1'

        if shp_encoding and shp_encoding.upper().replace('-', '') == 'UTF8':
            cmd_env['PGCLIENTENCODING'] = 'UTF8'

        if print_cmd:
            print(print_cmd_string([self.dbo.password], cmd))

        try:
            ogr_response = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT, env=cmd_env)
            print(ogr_response)
        except subprocess.CalledProcessError as e:
            print("Ogr2ogr Output:\n", e.output)
            print('Ogr2ogr command failed. The shapefile was not read in.')
            raise subprocess.CalledProcessError(cmd=print_cmd_string([self.dbo.password], cmd), returncode=1)

        if self.dbo.type == 'PG':
            self.dbo.query(SHP_COMMENT_QUERY.format(
                s=self.schema,
                t=self.table,
                u=self.dbo.user,
                p=self.path,
                shp=self.shp_name,
                d=datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
            ), timeme=False, internal=True)

        if not private:
            self.dbo.query(f'GRANT SELECT ON {self.schema}."{self.table}" TO public;', timeme=False, internal=True)

        self.rename_geom()

    def read_feature_class(self, private=False, print_cmd=False, fc_encoding=None):
        """
        Reads a feature of a shapefile in as a table
        :param private:
        :param print_cmd: Optional flag to print the GDAL command that is being used; defaults to False
        :param fc_encoding: Optional encoding of data within feature class
        :return:
        """
        if not all([self.path, self.shp_name]):
            return 'Missing path and/or shp_name'

        if not self.table:
            self.table = self.shp_name.lower()

        if self.table_exists():
            # clean up spatial index
            self.del_indexes()
            print(f'Deleting existing table {self.schema}.{self.table}')
            if self.dbo.type == 'MS':
                self.dbo.drop_table(self.schema, self.table)
            else:
                self.dbo.drop_table(self.schema, self.table, cascade=True)

        if self.dbo.type == 'PG':
            cmd = READ_FEATURE_CMD.format(
                gdal_data=self.gdal_data_loc,
                srid=self.srid,
                host=self.dbo.server,
                dbname=self.dbo.database,
                user=self.dbo.user,
                password=self.dbo.password,
                gdb=self.path,
                feature=self.shp_name,
                tbl_name=self.table,
                sch=self.schema
            )
        else:
            # TODO: add LDAP version trusted_connection=yes
            cmd = READ_FEATURE_CMD_MS.format(
                gdal_data=self.gdal_data_loc,
                srid=self.srid,
                ms_server=self.dbo.server,
                ms_db=self.dbo.database,
                ms_user=self.dbo.user,
                ms_pass=self.dbo.password,
                gdb=self.path,
                feature=self.shp_name,
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
            self.dbo.query(f'GRANT SELECT ON {self.schema}."{self.table}" TO public;', timeme=False, internal=True)

        self.rename_geom()

    def rename_geom(self):
        """
        Renames wkb_geometry to geom, along with index

        :return:
        """
        self.dbo.query(f"SELECT column_name FROM information_schema.columns" \
                        "WHERE table_schema = '{self.schema}' AND table_name = '{self.table}';",
                        timeme=False, internal=True)
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
                self.dbo.query(f"ALTER INDEX IF EXISTS {self.schema}.{self.table}_{f}_geom_idx RENAME to {self.table}_geom_idx", timeme=False, internal=True)

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
                    self.dbo.query(f"EXEC sp_rename N'{self.schema}.{self.table}.ogr_{self.schema}_{self.table}_{f}_sidx'," \
                    "N'{self.table}_geom_idx', N'INDEX';", timeme=False, internal=True)
                except SystemExit as e:
                    print(e)
                    print('Warning - could not update index name after renaming geometry. It may not exist.')
