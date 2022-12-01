import os

import configparser

from .. import pysqldb3 as pysqldb

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

pg_dbconn = pysqldb.DbConnect(db_type=config.get('PG_DB', 'TYPE'),
                              host=config.get('PG_DB', 'SERVER'),
                              db_name=config.get('PG_DB', 'DB_NAME'),
                              username=config.get('PG_DB', 'DB_USER'),
                              password=config.get('PG_DB', 'DB_PASSWORD'))

ms_dbconn = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                              host=config.get('SQL_DB', 'SERVER'),
                              db_name=config.get('SQL_DB', 'DB_NAME'),
                              username=config.get('SQL_DB', 'DB_USER'),
                              password=config.get('SQL_DB', 'DB_PASSWORD'))

test_table = '___test_rename_index_org_tbl_{table}__'.format(table=pg_dbconn.username)
new_test_table = '___test_rename_index_new_tbl_{table}__'.format(table=pg_dbconn.username)

"""
Rename_index is in Query.
It gets called when a table is renamed so index names stay up to date with table names
prevents standard index name conflicts if a table is renamed
"""


class TestRenameIndexPG:
    def get_indexes(self, table, schema):
        pg_dbconn.query("""
          SELECT indexname
          FROM pg_indexes
          WHERE tablename = '{table}'
           AND schemaname='{schema}';
        """.format(table=table.replace('"', ''), schema=schema), timeme=False)
        return pg_dbconn.data

    def test_rename_index_basic(self):
        schema = 'working'
        pg_dbconn.drop_table(table_name=test_table, schema_name=schema)
        pg_dbconn.drop_table(table_name=new_test_table, schema_name=schema)
        assert not pg_dbconn.table_exists(table_name=test_table, schema_name=schema)

        # create a basic table - no indexes
        pg_dbconn.query("create table {schema}.{table} (id int, txt text, geo geometry)".format(schema=schema, table=test_table))
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0

        # add index with org table name in idx name
        pg_dbconn.query("CREATE UNIQUE INDEX idx_id_{table} ON {schema}.{table} (id);".format(table=test_table, schema=schema))

        # check index on org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 1

        # rename table
        pg_dbconn.query("alter table {schema}.{table} rename to {new_table}".format(
            schema=schema, table=test_table, new_table=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0
        assert len(self.get_indexes(table=new_test_table, schema=schema)) == 1

        # check old table name not referenced in index after rename
        assert test_table not in self.get_indexes(table=new_test_table, schema=schema)[0][0]
        assert new_test_table in self.get_indexes(table=new_test_table, schema=schema)[0][0]

        pg_dbconn.drop_table(schema_name=schema, table_name=test_table)
        pg_dbconn.drop_table(schema_name=schema, table_name=new_test_table)

    def test_rename_index_basic_quotes(self):
        schema = 'working'
        table = '"' + test_table + '"'
        new_table = '"' + new_test_table + '"'

        pg_dbconn.drop_table(table_name=table, schema_name=schema)
        pg_dbconn.drop_table(table_name=new_table, schema_name=schema)

        assert not pg_dbconn.table_exists(table_name=table, schema=schema)
        assert not pg_dbconn.table_exists(table_name=new_table, schema=schema)

        # create a basic table - no indexes
        pg_dbconn.query("create table {schema}.{table} (id int, txt text, geo geometry)".format(schema=schema, table=table))
        assert len(self.get_indexes(table=table, schema=schema)) == 0

        # add index with org table name in idx name
        pg_dbconn.query(
            'CREATE UNIQUE INDEX "idx_id_{t1}" ON {schema}.{table} (id);'.format(t1=table.replace('"', ''), table=table, schema=schema))

        # check index on org table
        assert len(self.get_indexes(table=table, schema=schema)) == 1

        # rename table
        pg_dbconn.query('alter table {schema}.{table} rename to {new_table}'.format(
            schema=schema, table=table, new_table=new_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(table=table, schema=schema)) == 0
        assert len(self.get_indexes(table=new_table, schema=schema)) == 1

        # check old table name not referenced in index after rename
        assert table.replace('"', '') not in self.get_indexes(new_table, schema)[0][0]
        assert new_table.replace('"', '') in self.get_indexes(new_table, schema)[0][0]

        pg_dbconn.drop_table(schema_name=schema, table_name=table)
        pg_dbconn.drop_table(schema_name=schema, table_name=new_table)

    def test_rename_index_basic_auto_idx(self):
        schema = 'working'
        pg_dbconn.drop_table(table_name=test_table, schema_name=schema)
        assert not pg_dbconn.table_exists(table_name=test_table, schema_name=schema)

        # create a basic table - no indexes
        pg_dbconn.query("create table {schema}.{table} (id serial PRIMARY KEY, txt text, geo geometry)".format(schema=schema, table=test_table))
        assert len(self.get_indexes(table=test_table, schema=schema)) == 1

        # rename table
        pg_dbconn.query("alter table {schema}.{table} rename to {new_table}".format(
            schema=schema, table=test_table, new_table=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0
        assert len(self.get_indexes(table=new_test_table, schema=schema)) == 1

        # check old table name not referenced in index after rename
        assert test_table not in self.get_indexes(table=new_test_table, schema=schema)[0][0]
        assert new_test_table in self.get_indexes(table=new_test_table, schema=schema)[0][0]

        pg_dbconn.drop_table(schema_name=schema, table_name=test_table)
        pg_dbconn.drop_table(schema_name=schema, table_name=new_test_table)

    def test_rename_index_multiple_indexes(self):
        schema = 'working'
        pg_dbconn.drop_table(table_name=test_table, schema_name=schema)
        pg_dbconn.drop_table(table_name=new_test_table, schema_name=schema)

        assert not pg_dbconn.table_exists(table_name=test_table, schema=schema)
        assert not pg_dbconn.table_exists(table_name=new_test_table, schema=schema)

        # create a basic table - no indexes
        pg_dbconn.query("create table {schema}.{table} (id int, txt text, geom geometry)".format(schema=schema, table=test_table))
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0

        # add index with org table name in idx name
        pg_dbconn.query("CREATE UNIQUE INDEX idx_id_{table} ON {schema}.{table} (id);".format(table=test_table, schema=schema))
        pg_dbconn.query("CREATE INDEX {table}__geom_idx ON {schema}.{table} USING gist (geom)".format(table=test_table, schema=schema))

        # check index on org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 2

        # rename table
        pg_dbconn.query("alter table {schema}.{table} rename to {new_table}".format(
            schema=schema, table=test_table, new_table=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0
        assert len(self.get_indexes(table=new_test_table, schema=schema)) == 2

        # check old table name not referenced in index after rename
        assert {test_table in i[0] for i in self.get_indexes(new_test_table, schema)} == {False}
        assert {new_test_table in i[0] for i in self.get_indexes(new_test_table, schema)} == {True}

        pg_dbconn.drop_table(table_name=test_table, schema_name=schema)
        pg_dbconn.drop_table(table_name=new_test_table, schema_name=schema)

    def test_rename_index_imported_shp(self):
        schema = 'working'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        pg_dbconn.drop_table(table_name=test_table, schema_name=schema)
        pg_dbconn.drop_table(table_name=new_test_table, schema_name=schema)

        assert not pg_dbconn.table_exists(table_name=test_table, schema=schema)
        assert not pg_dbconn.table_exists(table_name=new_test_table, schema=schema)

        # create a shapefile to import
        q = """
            select 1 id, 'test text' txt, st_setsrid(st_makepoint(1015329.1, 213793.1),2263) geom
        """
        pg_dbconn.query_to_shp(q, path=fldr, shpfile_name=test_table + '.shp')
        pg_dbconn.shp_to_table(path=fldr, table_name=test_table, schema_name=schema, shpfile_name=test_table + '.shp')

        # check index on org table
        print(self.get_indexes(table=test_table, schema=schema))
        assert len(self.get_indexes(table=test_table, schema=schema)) == 2

        # rename table
        pg_dbconn.query("alter table {schema}.{table} rename to {new_table}".format(
            schema=schema, table=test_table, new_table=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0
        assert len(self.get_indexes(table=new_test_table, schema=schema)) == 2

        # check old table name not referenced in index after rename
        assert {test_table in i[0] for i in self.get_indexes(table=new_test_table, schema=schema)} == {False}
        assert {new_test_table in i[0] for i in self.get_indexes(table=new_test_table, schema=schema)} == {True}

        pg_dbconn.drop_table(schema_name=schema, table_name=test_table)
        pg_dbconn.drop_table(schema_name=schema, table_name=new_test_table)

        for ext in ('.dbf', '.prj', '.shx', '.shp'):
            try:
                os.remove(os.path.join(fldr, test_table + ext))
            except:
                pass

    def test_rename_index_tbl_name_not_in_index(self):
        schema = 'working'
        pg_dbconn.drop_table(table_name=test_table, schema_name=schema)
        pg_dbconn.drop_table(table_name=new_test_table, schema_name=schema)

        assert not pg_dbconn.table_exists(table_name=test_table, schema_name=schema)
        assert not pg_dbconn.table_exists(table_name=new_test_table, schema_name=schema)

        # create a basic table - no indexes
        pg_dbconn.query("create table {schema}.{table} (id int, txt text, geo geometry)".format(schema=schema, table=test_table))
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0

        # add index with org table name in idx name
        pg_dbconn.query("CREATE UNIQUE INDEX idx_id_test ON {schema}.{table} (id);".format(table=test_table, schema=schema))

        # check index on org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 1

        # rename table
        pg_dbconn.query("alter table {schema}.{table} rename to {new_table}".format(
            schema=schema, table=test_table, new_table=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0
        assert len(self.get_indexes(table=new_test_table, schema=schema)) == 1

        # check old table name not referenced in index after rename
        assert {test_table in i[0] for i in self.get_indexes(table=new_test_table, schema=schema)} == {False}
        assert {new_test_table in i[0] for i in self.get_indexes(table=new_test_table, schema=schema)} == {False}

        pg_dbconn.drop_table(schema_name=schema, table_name=test_table)
        pg_dbconn.drop_table(schema_name=schema, table_name=new_test_table)

    def test_rename_index_no_indexes(self):
        schema = 'working'
        pg_dbconn.drop_table(table_name=test_table, schema_name=schema)
        pg_dbconn.drop_table(table_name=new_test_table, schema_name=schema)

        assert not pg_dbconn.table_exists(table_name=test_table, schema_name=schema)
        assert not pg_dbconn.table_exists(table_name=new_test_table, schema_name=schema)

        # create a basic table - no indexes
        pg_dbconn.query("create table {schema}.{table} (id int, txt text, geo geometry)".format(schema=schema, table=test_table))
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0

        # rename table
        pg_dbconn.query("alter table {schema}.{table} rename to {new_table}".format(
            schema=schema, table=test_table, new_table=new_test_table
        ))
        # check index on renamed table no longer references org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0
        assert len(self.get_indexes(table=new_test_table, schema=schema)) == 0

        # check old table name not referenced in index after rename
        assert {test_table in i[0] for i in self.get_indexes(table=new_test_table, schema=schema)} == set()
        assert {new_test_table in i[0] for i in self.get_indexes(table=new_test_table, schema=schema)} == set()

        pg_dbconn.drop_table(schema_name=schema, table_name=test_table)
        pg_dbconn.drop_table(schema_name=schema, table_name=new_test_table)


class TestRenameIndexMS:
    def get_indexes(self, table, schema):
        ms_dbconn.query("""
            SELECT
                a.name AS Index_Name,
                type_desc
            FROM
                sys.indexes AS a
            INNER JOIN
                sys.index_columns AS b
                ON a.object_id = b.object_id AND a.index_id = b.index_id
            WHERE
                a.is_hypothetical = 0
                AND a.object_id = OBJECT_ID('{schema}.{table}')
        """.format(table=table, schema=schema), timeme=False)
        return ms_dbconn.data

    def test_rename_index_basic(self):
        schema = 'dbo'
        ms_dbconn.drop_table(table_name=test_table, schema_name=schema)
        ms_dbconn.drop_table(table_name=new_test_table, schema_name=schema)
        assert ms_dbconn.table_exists(table_name=test_table, schema=schema) is False
        assert ms_dbconn.table_exists(table_name=new_test_table, schema=schema) is False

        # create a basic table - no indexes
        ms_dbconn.query("create table {schema}.{table} (id int, txt text, geo geometry)".format(schema=schema, table=test_table))
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0

        # add index with org table name in idx name
        ms_dbconn.query("CREATE UNIQUE INDEX idx_id_{table} ON {schema}.{table} (id);".format(table=test_table, schema=schema))

        # check index on org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 1

        # rename table
        ms_dbconn.query("EXEC sp_rename '{schema}.{table}', '{new_table}'".format(
            schema=schema, table=test_table, new_table=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0
        assert len(self.get_indexes(table=new_test_table, schema=schema)) == 1

        # check old table name not referenced in index after rename
        assert test_table not in self.get_indexes(table=new_test_table, schema=schema)[0][0]
        assert new_test_table in self.get_indexes(table=new_test_table, schema=schema)[0][0]

        ms_dbconn.drop_table(schema_name=schema, table_name=test_table)
        ms_dbconn.drop_table(schema_name=schema, table_name=new_test_table)

    def test_rename_index_basic_brackets(self):
        schema = 'dbo'
        table = '[' + test_table + ']'
        new_table = '[' + new_test_table + ']'
        ms_dbconn.drop_table(table_name=table, schema_name=schema)
        assert not ms_dbconn.table_exists(table_name=table, schema=schema)

        # create a basic table - no indexes
        ms_dbconn.query("create table {schema}.{table} (id int, txt text, geo geometry)".format(schema=schema, table=table))
        assert len(self.get_indexes(table=table, schema=schema)) == 0

        # add index with org table name in idx name
        ms_dbconn.query(
            "CREATE UNIQUE INDEX [idx_id_{t1}] ON {schema}.{table} (id);".format(t1=table.replace("[", '').replace("]", ''),
                                                                        table=table, schema=schema))
        # check index on org table
        assert len(self.get_indexes(table=table, schema=schema)) == 1

        # rename table
        ms_dbconn.query("EXEC sp_rename '{schema}.{table}', '{new_table}'".format(
            schema=schema, table=table.replace("[", '').replace("]", ''), new_table=new_table.replace("[", '').replace("]", '')
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0
        assert len(self.get_indexes(table=new_test_table, schema=schema)) == 1

        # check old table name not referenced in index after rename
        assert table.replace("[", '').replace("]", '') not in self.get_indexes(table=new_table, schema=schema)[0][0]
        assert new_table.replace("[", '').replace("]", '') in self.get_indexes(table=new_table, schema=schema)[0][0]

        ms_dbconn.drop_table(schema_name=schema, table_name=test_table)
        ms_dbconn.drop_table(schema_name=schema, table_name=new_test_table)

    def test_rename_index_basic_auto_idx(self):
        schema = 'dbo'
        ms_dbconn.drop_table(table_name=test_table, schema_name=schema)
        ms_dbconn.drop_table(table_name=new_test_table, schema_name=schema)
        assert ms_dbconn.table_exists(table_name=test_table, schema=schema) is False
        assert ms_dbconn.table_exists(table_name=new_test_table, schema=schema) is False

        # create a basic table - with default index
        ms_dbconn.query(
            "create table {schema}.{table} (id int IDENTITY(1,1) PRIMARY KEY, txt text, geo geometry)".format(
                schema=schema, table=test_table))
        assert len(self.get_indexes(table=test_table, schema=schema)) == 1

        # rename table
        ms_dbconn.query("EXEC sp_rename '{schema}.{table}', '{new_table}'".format(
            schema=schema, table=test_table, new_table=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0
        assert len(self.get_indexes(table=new_test_table, schema=schema)) == 1

        # check old table name not referenced in index after rename
        assert test_table not in self.get_indexes(table=new_test_table, schema=schema)[0][0]
        assert new_test_table not in self.get_indexes(table=new_test_table, schema=schema)[0][0]

        ms_dbconn.drop_table(schema_name=schema, table_name=test_table)
        ms_dbconn.drop_table(schema_name=schema, table_name=new_test_table)

    def test_rename_index_multiple_indexes(self):
        schema = 'dbo'
        ms_dbconn.drop_table(table_name=test_table, schema_name=schema)
        assert ms_dbconn.table_exists(table_name=test_table, schema=schema) is False

        # create a basic table - no indexes
        ms_dbconn.query("create table {schema}.{table} (id int IDENTITY(1,1) PRIMARY KEY, txt text, geom geometry)".format(
            schema=schema, table=test_table))
        assert len(self.get_indexes(table=test_table, schema=schema)) == 1

        # add index with org table name in idx name
        ms_dbconn.query("CREATE UNIQUE INDEX idx_id_{table} ON {schema}.{table} (id);".format(table=test_table, schema=schema))
        ms_dbconn.query("CREATE SPATIAL INDEX {table}__geom_idx ON \
        {schema}.{table}(geom) WITH ( BOUNDING_BOX = ( 0, 0, 500, 200 ) ); ".format(table=test_table, schema=schema))

        # check index on org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 3

        # rename table
        ms_dbconn.query("EXEC sp_rename '{schema}.{table}', '{new_table}'".format(
            schema=schema, table=test_table, new_table=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0
        assert len(self.get_indexes(table=new_test_table, schema=schema)) == 3

        # check old table name not referenced in index after rename
        assert {test_table in i[0] for i in self.get_indexes(new_test_table, schema)} == {False}
        assert {new_test_table in i[0] for i in self.get_indexes(new_test_table, schema) if 'PK' not in i[0]} == {True}

        ms_dbconn.drop_table(schema_name=schema, table_name=test_table)
        ms_dbconn.drop_table(schema_name=schema, table_name=new_test_table)

    def test_rename_index_imported_shp(self):
        schema = 'dbo'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        ms_dbconn.drop_table(table_name=test_table, schema_name=schema)
        assert not ms_dbconn.table_exists(table_name=test_table, schema=schema)

        # create a shapefile to import
        q = """
            select 1 id, 'test text' txt, geometry::STGeomFromText('POINT(1015329.34900 213793.65100)', 2263) geom
        """
        ms_dbconn.query_to_shp(q, path=fldr, shpfile_name=test_table + '.shp')
        ms_dbconn.shp_to_table(path=fldr, table_name=test_table, schema_name=schema, shpfile_name=test_table + '.shp', private=True)

        # check index on org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 1

        # rename table
        ms_dbconn.query("EXEC sp_rename '{schema}.{table}', '{new_table}'".format(
            schema=schema, table=test_table, new_table=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0
        assert len(self.get_indexes(table=new_test_table, schema=schema)) == 1

        # check old table name not referenced in index after rename
        assert {test_table in i[0] for i in self.get_indexes(new_test_table, schema)} == {False}
        assert {new_test_table in i[0] for i in self.get_indexes(new_test_table, schema)} == {True}

        ms_dbconn.drop_table(schema_name=schema, table_name=test_table)
        ms_dbconn.drop_table(schema_name=schema, table_name=new_test_table)
        for ext in ('.dbf', '.prj', '.shx', '.shp'):
            try:
                os.remove(os.path.join(fldr, test_table + ext))
            except:
                pass

    def test_rename_index_tbl_name_not_in_index(self):
        schema = 'dbo'
        ms_dbconn.drop_table(table_name=test_table, schema_name=schema)
        assert ms_dbconn.table_exists(table_name=test_table, schema=schema) is False

        # create a basic table - no indexes
        ms_dbconn.query("create table {schema}.{table} (id int, txt text, geo geometry)".format(schema=schema, table=test_table))
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0

        # add index with org table name in idx name
        ms_dbconn.query("CREATE UNIQUE INDEX idx_id_test ON {schema}.{table} (id);".format(table=test_table, schema=schema))

        # check index on org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 1

        # rename table
        ms_dbconn.query("EXEC sp_rename '{schema}.{table}', '{new_table}'".format(
            schema=schema, table=test_table, new_table=new_test_table))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0
        assert len(self.get_indexes(table=new_test_table, schema=schema)) == 1

        # check old table name not referenced in index after rename
        assert test_table not in self.get_indexes(table=new_test_table, schema=schema)[0][0]
        assert new_test_table not in self.get_indexes(table=new_test_table, schema=schema)[0][0]

        ms_dbconn.drop_table(schema_name=schema, table_name=test_table)
        ms_dbconn.drop_table(schema_name=schema, table_name=new_test_table)

    def test_rename_index_no_indexes(self):
        schema = 'dbo'
        ms_dbconn.drop_table(table_name=test_table, schema_name=schema)
        assert ms_dbconn.table_exists(table_name=test_table, schema_name=schema) is False

        # create a basic table - no indexes
        ms_dbconn.query("create table {schema}.{table} (id int, txt text, geo geometry)".format(schema=schema, table=test_table))
        assert len(self.get_indexes(table=test_table, schema=schema)) == 0

        # rename table
        ms_dbconn.query("EXEC sp_rename '{schema}.{table}', '{new_table}'".format(
            schema=schema, table=test_table, new_table=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(table=new_test_table, schema=schema)) == 0

        # check old table name not referenced in index after rename
        assert {test_table in i[0] for i in self.get_indexes(table=new_test_table, schema=schema)} == set()
        assert {new_test_table in i[0] for i in self.get_indexes(table=new_test_table, schema=schema)} == set()

        ms_dbconn.drop_table(schema_name=schema, table_name=test_table)
        ms_dbconn.drop_table(schema_name=schema, table_name=new_test_table)
