import os

import configparser

from .. import pysqldb3 as pysqldb

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                       server=config.get('PG_DB', 'SERVER'),
                       database=config.get('PG_DB', 'DB_NAME'),
                       user=config.get('PG_DB', 'DB_USER'),
                       password=config.get('PG_DB', 'DB_PASSWORD'))

sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                        server=config.get('SQL_DB', 'SERVER'),
                        database=config.get('SQL_DB', 'DB_NAME'),
                        user=config.get('SQL_DB', 'DB_USER'),
                        password=config.get('SQL_DB', 'DB_PASSWORD'))

test_table = '___test_rename_index_org_tbl_{}__'.format(db.user)
new_test_table = '___test_rename_index_new_tbl_{}__'.format(db.user)

"""
Rename_index is in Query.
It gets called when a table is renamed so index names stay up to date with table names
prevents standard index name conflicts if a table is renamed
"""


class TestRenameIndexPG:
    def get_indexes(self, table, schema):
        db.query("""
          SELECT indexname
          FROM pg_indexes
          WHERE tablename = '{t}'
           AND schemaname='{s}';
        """.format(t=table.replace('"', ''), s=schema), timeme=False)
        return db.data

    def test_rename_index_basic(self):
        schema = 'working'
        db.drop_table(table=test_table, schema=schema)
        db.drop_table(table=new_test_table, schema=schema)
        assert not db.table_exists(test_table, schema=schema)

        # create a basic table - no indexes
        db.query("create table {s}.{t} (id int, txt text, geo geometry)".format(s=schema, t=test_table))
        assert len(self.get_indexes(test_table, schema)) == 0

        # add index with org table name in idx name
        db.query("CREATE UNIQUE INDEX idx_id_{t} ON {s}.{t} (id);".format(t=test_table, s=schema))

        # check index on org table
        assert len(self.get_indexes(test_table, schema)) == 1

        # rename table
        db.query("alter table {s}.{t} rename to {nt}".format(
            s=schema, t=test_table, nt=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(test_table, schema)) == 0
        assert len(self.get_indexes(new_test_table, schema)) == 1

        # check old table name not referenced in index after rename
        assert test_table not in self.get_indexes(new_test_table, schema)[0][0]
        assert new_test_table in self.get_indexes(new_test_table, schema)[0][0]

        db.drop_table(schema, test_table)
        db.drop_table(schema, new_test_table)

    def test_rename_index_basic_quotes(self):
        schema = 'working'
        table = '"' + test_table + '"'
        new_table = '"' + new_test_table + '"'

        db.drop_table(table=table, schema=schema)
        db.drop_table(table=new_table, schema=schema)

        assert not db.table_exists(table, schema=schema)
        assert not db.table_exists(new_table, schema=schema)

        # create a basic table - no indexes
        db.query("create table {s}.{t} (id int, txt text, geo geometry)".format(s=schema, t=table))
        assert len(self.get_indexes(table, schema)) == 0

        # add index with org table name in idx name
        db.query(
            'CREATE UNIQUE INDEX "idx_id_{t1}" ON {s}.{t} (id);'.format(t1=table.replace('"', ''), t=table, s=schema))

        # check index on org table
        assert len(self.get_indexes(table, schema)) == 1

        # rename table
        db.query('alter table {s}.{t} rename to {nt}'.format(
            s=schema, t=table, nt=new_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(table, schema)) == 0
        assert len(self.get_indexes(new_table, schema)) == 1

        # check old table name not referenced in index after rename
        assert table.replace('"', '') not in self.get_indexes(new_table, schema)[0][0]
        assert new_table.replace('"', '') in self.get_indexes(new_table, schema)[0][0]

        db.drop_table(schema, table)
        db.drop_table(schema, new_table)

    def test_rename_index_basic_auto_idx(self):
        schema = 'working'
        db.drop_table(table=test_table, schema=schema)
        assert not db.table_exists(test_table, schema=schema)

        # create a basic table - no indexes
        db.query("create table {s}.{t} (id serial PRIMARY KEY, txt text, geo geometry)".format(s=schema, t=test_table))
        assert len(self.get_indexes(test_table, schema)) == 1

        # rename table
        db.query("alter table {s}.{t} rename to {nt}".format(
            s=schema, t=test_table, nt=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(test_table, schema)) == 0
        assert len(self.get_indexes(new_test_table, schema)) == 1

        # check old table name not referenced in index after rename
        assert test_table not in self.get_indexes(new_test_table, schema)[0][0]
        assert new_test_table in self.get_indexes(new_test_table, schema)[0][0]

        db.drop_table(schema, test_table)
        db.drop_table(schema, new_test_table)

    def test_rename_index_multiple_indexes(self):
        schema = 'working'
        db.drop_table(table=test_table, schema=schema)
        db.drop_table(table=new_test_table, schema=schema)

        assert not db.table_exists(test_table, schema=schema)
        assert not db.table_exists(new_test_table, schema=schema)

        # create a basic table - no indexes
        db.query("create table {s}.{t} (id int, txt text, geom geometry)".format(s=schema, t=test_table))
        assert len(self.get_indexes(test_table, schema)) == 0

        # add index with org table name in idx name
        db.query("CREATE UNIQUE INDEX idx_id_{t} ON {s}.{t} (id);".format(t=test_table, s=schema))
        db.query("CREATE INDEX {t}__geom_idx ON {s}.{t} USING gist (geom)".format(t=test_table, s=schema))

        # check index on org table
        assert len(self.get_indexes(test_table, schema)) == 2

        # rename table
        db.query("alter table {s}.{t} rename to {nt}".format(
            s=schema, t=test_table, nt=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(test_table, schema)) == 0
        assert len(self.get_indexes(new_test_table, schema)) == 2

        # check old table name not referenced in index after rename
        assert {test_table in i[0] for i in self.get_indexes(new_test_table, schema)} == {False}
        assert {new_test_table in i[0] for i in self.get_indexes(new_test_table, schema)} == {True}

        db.drop_table(schema, test_table)
        db.drop_table(schema, new_test_table)

    def test_rename_index_imported_shp(self):
        schema = 'working'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

        db.drop_table(table=test_table, schema=schema)
        db.drop_table(table=new_test_table, schema=schema)

        assert not db.table_exists(test_table, schema=schema)
        assert not db.table_exists(new_test_table, schema=schema)

        # create a shapefile to import
        q = """
            select 1 id, 'test text' txt, st_setsrid(st_makepoint(1015329.1, 213793.1),2263) geom
        """
        db.query_to_shp(q, path=fldr, shp_name=test_table + '.shp')
        db.shp_to_table(path=fldr, table=test_table, schema=schema, shp_name=test_table + '.shp')

        # check index on org table
        print(self.get_indexes(test_table, schema))
        assert len(self.get_indexes(test_table, schema)) == 2

        # rename table
        db.query("alter table {s}.{t} rename to {nt}".format(
            s=schema, t=test_table, nt=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(test_table, schema)) == 0
        assert len(self.get_indexes(new_test_table, schema)) == 2

        # check old table name not referenced in index after rename
        assert {test_table in i[0] for i in self.get_indexes(new_test_table, schema)} == {False}
        assert {new_test_table in i[0] for i in self.get_indexes(new_test_table, schema)} == {True}

        db.drop_table(schema, test_table)
        db.drop_table(schema, new_test_table)

        for ext in ('.dbf', '.prj', '.shx', '.shp'):
            try:
                os.remove(os.path.join(fldr, test_table + ext))
            except:
                pass

    def test_rename_index_tbl_name_not_in_index(self):
        schema = 'working'
        db.drop_table(table=test_table, schema=schema)
        db.drop_table(table=new_test_table, schema=schema)

        assert not db.table_exists(test_table, schema=schema)
        assert not db.table_exists(new_test_table, schema=schema)

        # create a basic table - no indexes
        db.query("create table {s}.{t} (id int, txt text, geo geometry)".format(s=schema, t=test_table))
        assert len(self.get_indexes(test_table, schema)) == 0

        # add index with org table name in idx name
        db.query("CREATE UNIQUE INDEX idx_id_test ON {s}.{t} (id);".format(t=test_table, s=schema))

        # check index on org table
        assert len(self.get_indexes(test_table, schema)) == 1

        # rename table
        db.query("alter table {s}.{t} rename to {nt}".format(
            s=schema, t=test_table, nt=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(test_table, schema)) == 0
        assert len(self.get_indexes(new_test_table, schema)) == 1

        # check old table name not referenced in index after rename
        assert {test_table in i[0] for i in self.get_indexes(new_test_table, schema)} == {False}
        assert {new_test_table in i[0] for i in self.get_indexes(new_test_table, schema)} == {False}

        db.drop_table(schema, test_table)
        db.drop_table(schema, new_test_table)

    def test_rename_index_no_indexes(self):
        schema = 'working'
        db.drop_table(table=test_table, schema=schema)
        db.drop_table(table=new_test_table, schema=schema)

        assert not db.table_exists(test_table, schema=schema)
        assert not db.table_exists(new_test_table, schema=schema)

        # create a basic table - no indexes
        db.query("create table {s}.{t} (id int, txt text, geo geometry)".format(s=schema, t=test_table))
        assert len(self.get_indexes(test_table, schema)) == 0

        # rename table
        db.query("alter table {s}.{t} rename to {nt}".format(
            s=schema, t=test_table, nt=new_test_table
        ))
        # check index on renamed table no longer references org table
        assert len(self.get_indexes(test_table, schema)) == 0
        assert len(self.get_indexes(new_test_table, schema)) == 0

        # check old table name not referenced in index after rename
        assert {test_table in i[0] for i in self.get_indexes(new_test_table, schema)} == set()
        assert {new_test_table in i[0] for i in self.get_indexes(new_test_table, schema)} == set()

        db.drop_table(schema, test_table)
        db.drop_table(schema, new_test_table)


class TestRenameIndexMS:
    def get_indexes(self, table, schema):
        sql.query("""
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
                AND a.object_id = OBJECT_ID('{s}.{t}')
        """.format(t=table, s=schema), timeme=False)
        return sql.data

    def test_rename_index_basic(self):
        schema = 'dbo'
        sql.drop_table(table=test_table, schema=schema)
        sql.drop_table(table=new_test_table, schema=schema)
        assert sql.table_exists(test_table, schema=schema) is False
        assert sql.table_exists(new_test_table, schema=schema) is False

        # create a basic table - no indexes
        sql.query("create table {s}.{t} (id int, txt text, geo geometry)".format(s=schema, t=test_table))
        assert len(self.get_indexes(test_table, schema)) == 0

        # add index with org table name in idx name
        sql.query("CREATE UNIQUE INDEX idx_id_{t} ON {s}.{t} (id);".format(t=test_table, s=schema))

        # check index on org table
        assert len(self.get_indexes(test_table, schema)) == 1

        # rename table
        sql.query("EXEC sp_rename '{s}.{t}', '{nt}'".format(
            s=schema, t=test_table, nt=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(test_table, schema)) == 0
        assert len(self.get_indexes(new_test_table, schema)) == 1

        # check old table name not referenced in index after rename
        assert test_table not in self.get_indexes(new_test_table, schema)[0][0]
        assert new_test_table in self.get_indexes(new_test_table, schema)[0][0]

        sql.drop_table(schema, test_table)
        sql.drop_table(schema, new_test_table)

    def test_rename_index_basic_brackets(self):
        schema = 'dbo'
        table = '[' + test_table + ']'
        new_table = '[' + new_test_table + ']'
        sql.drop_table(table=table, schema=schema)
        assert not sql.table_exists(table, schema=schema)

        # create a basic table - no indexes
        sql.query("create table {s}.{t} (id int, txt text, geo geometry)".format(s=schema, t=table))
        assert len(self.get_indexes(table, schema)) == 0

        # add index with org table name in idx name
        sql.query(
            "CREATE UNIQUE INDEX [idx_id_{t1}] ON {s}.{t} (id);".format(t1=table.replace("[", '').replace("]", ''),
                                                                        t=table, s=schema))
        # check index on org table
        assert len(self.get_indexes(table, schema)) == 1

        # rename table
        sql.query("EXEC sp_rename '{s}.{t}', '{nt}'".format(
            s=schema, t=table.replace("[", '').replace("]", ''), nt=new_table.replace("[", '').replace("]", '')
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(table, schema)) == 0
        assert len(self.get_indexes(new_table, schema)) == 1

        # check old table name not referenced in index after rename
        assert table.replace("[", '').replace("]", '') not in self.get_indexes(new_table, schema)[0][0]
        assert new_table.replace("[", '').replace("]", '') in self.get_indexes(new_table, schema)[0][0]

        sql.drop_table(schema, table)
        sql.drop_table(schema, new_table)

    def test_rename_index_basic_auto_idx(self):
        schema = 'dbo'
        sql.drop_table(table=test_table, schema=schema)
        sql.drop_table(table=new_test_table, schema=schema)
        assert sql.table_exists(test_table, schema=schema) is False
        assert sql.table_exists(new_test_table, schema=schema) is False

        # create a basic table - with default index
        sql.query(
            "create table {s}.{t} (id int IDENTITY(1,1) PRIMARY KEY, txt text, geo geometry)".format(s=schema, t=test_table))
        assert len(self.get_indexes(test_table, schema)) == 1

        # rename table
        sql.query("EXEC sp_rename '{s}.{t}', '{nt}'".format(
            s=schema, t=test_table, nt=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(test_table, schema)) == 0
        assert len(self.get_indexes(new_test_table, schema)) == 1

        # check old table name not referenced in index after rename
        assert test_table not in self.get_indexes(new_test_table, schema)[0][0]
        assert new_test_table not in self.get_indexes(new_test_table, schema)[0][0]

        sql.drop_table(schema, test_table)
        sql.drop_table(schema, new_test_table)

    def test_rename_index_multiple_indexes(self):
        schema = 'dbo'
        sql.drop_table(table=test_table, schema=schema)
        assert sql.table_exists(test_table, schema=schema) is False

        # create a basic table - no indexes
        sql.query("create table {s}.{t} (id int IDENTITY(1,1) PRIMARY KEY, txt text, geom geometry)".format(
            s=schema, t=test_table))
        assert len(self.get_indexes(test_table, schema)) == 1

        # add index with org table name in idx name
        sql.query("CREATE UNIQUE INDEX idx_id_{t} ON {s}.{t} (id);".format(t=test_table, s=schema))
        sql.query("CREATE SPATIAL INDEX {t}__geom_idx ON \
        {s}.{t}(geom) WITH ( BOUNDING_BOX = ( 0, 0, 500, 200 ) ); ".format(t=test_table, s=schema))

        # check index on org table
        assert len(self.get_indexes(test_table, schema)) == 3

        # rename table
        sql.query("EXEC sp_rename '{s}.{t}', '{nt}'".format(
            s=schema, t=test_table, nt=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(test_table, schema)) == 0
        assert len(self.get_indexes(new_test_table, schema)) == 3

        # check old table name not referenced in index after rename
        assert {test_table in i[0] for i in self.get_indexes(new_test_table, schema)} == {False}
        assert {new_test_table in i[0] for i in self.get_indexes(new_test_table, schema) if 'PK' not in i[0]} == {True}

        sql.drop_table(schema, test_table)
        sql.drop_table(schema, new_test_table)

    def test_rename_index_imported_shp(self):
        schema = 'dbo'
        fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
        sql.drop_table(table=test_table, schema=schema)
        assert not sql.table_exists(test_table, schema=schema)

        # create a shapefile to import
        q = """
            select 1 id, 'test text' txt, geometry::STGeomFromText('POINT(1015329.34900 213793.65100)', 2263) geom
        """
        sql.query_to_shp(q, path=fldr, shp_name=test_table + '.shp')
        sql.shp_to_table(path=fldr, table=test_table, schema=schema, shp_name=test_table + '.shp', private=True)

        # check spatial index on org table
        # assert str((f'{test_table}_geom_idx', 'SPATIAL')) in [str(i) for i in self.get_indexes(test_table, schema)]
        # spatial index is not being added by default now, but not the issue for this test
        idx = self.get_indexes(test_table, schema)[0][0]

        # rename table
        sql.query("EXEC sp_rename '{s}.{t}', '{nt}'".format(
            s=schema, t=test_table, nt=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(test_table, schema)) == 0
        # check spatial index is now mapped to new table name
        # assert str((f'{new_test_table}_geom_idx', 'SPATIAL')) in [str(i) for i in self.get_indexes(new_test_table, schema)]
        # check index is renamed and mapped to the new table name
        assert idx.replace(test_table, new_test_table) in [i[0] for i in self.get_indexes(new_test_table, schema)]

        # check old table name not referenced in index after rename
        assert {test_table in i[0] for i in self.get_indexes(new_test_table, schema)} == {False}
        assert {new_test_table in i[0] for i in self.get_indexes(new_test_table, schema)} == {True}

        sql.drop_table(schema, test_table)
        sql.drop_table(schema, new_test_table)
        for ext in ('.dbf', '.prj', '.shx', '.shp'):
            try:
                os.remove(os.path.join(fldr, test_table + ext))
            except:
                pass

    def test_rename_index_tbl_name_not_in_index(self):
        schema = 'dbo'
        sql.drop_table(table=test_table, schema=schema)
        assert sql.table_exists(test_table, schema=schema) is False

        # create a basic table - no indexes
        sql.query("create table {s}.{t} (id int, txt text, geo geometry)".format(s=schema, t=test_table))
        assert len(self.get_indexes(test_table, schema)) == 0

        # add index with org table name in idx name
        sql.query("CREATE UNIQUE INDEX idx_id_test ON {s}.{t} (id);".format(t=test_table, s=schema))

        # check index on org table
        assert len(self.get_indexes(test_table, schema)) == 1

        # rename table
        sql.query("EXEC sp_rename '{s}.{t}', '{nt}'".format(
            s=schema, t=test_table, nt=new_test_table))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(test_table, schema)) == 0
        assert len(self.get_indexes(new_test_table, schema)) == 1

        # check old table name not referenced in index after rename
        assert test_table not in self.get_indexes(new_test_table, schema)[0][0]
        assert new_test_table not in self.get_indexes(new_test_table, schema)[0][0]

        sql.drop_table(schema, test_table)
        sql.drop_table(schema, new_test_table)

    def test_rename_index_no_indexes(self):
        schema = 'dbo'
        sql.drop_table(table=test_table, schema=schema)
        assert sql.table_exists(test_table, schema=schema) is False

        # create a basic table - no indexes
        sql.query("create table {s}.{t} (id int, txt text, geo geometry)".format(s=schema, t=test_table))
        assert len(self.get_indexes(test_table, schema)) == 0

        # rename table
        sql.query("EXEC sp_rename '{s}.{t}', '{nt}'".format(
            s=schema, t=test_table, nt=new_test_table
        ))

        # check index on renamed table no longer references org table
        assert len(self.get_indexes(new_test_table, schema)) == 0

        # check old table name not referenced in index after rename
        assert {test_table in i[0] for i in self.get_indexes(new_test_table, schema)} == set()
        assert {new_test_table in i[0] for i in self.get_indexes(new_test_table, schema)} == set()

        sql.drop_table(schema, test_table)
        sql.drop_table(schema, new_test_table)
