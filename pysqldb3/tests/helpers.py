import random
import os
import pandas as pd
#from .. import shapefile
#import subprocess
import requests
import zipfile
from xlrd import open_workbook
from xlutils.copy import copy
from ..Config import write_config
write_config(config_path=os.path.dirname(os.path.abspath(__file__)).replace('\\tests', '') + "\\config.cfg")

DIR = os.path.join(os.path.dirname(os.path.abspath(__file__))) + '\\test_data'


def set_up_test_csv():
    data = {'id': {0: 1, 1: 2, 2: 3, 3: 4, 4: 5},
            'living space (sq ft)': {0: 2222, 1: 1628, 2: 3824, 3: 1137, 4: 3560},
            'beds': {0: 3, 1: 3, 2: 5, 3: 3, 4: 6},
            'baths': {0: 3, 1: 2.5, 2: 4.0, 3: 2.0, 4: 4.0},
            'zip': {0: 32312, 1: 32308, 2: 32312, 3: 32309, 4: 32309},
            'year': {0: 1981, 1: 2009, 2: 1954, 3: 1993, 4: 1973},
            'list price': {0: 250000, 1: 185000, 2: 399000, 3: 150000, 4: 315000},
            'neighborhood': {0: 'Corona', 1: 'Kensington', 2: 'Morris Heights', 3: 'Bayside', 4: 'Inwood'},
            'sale date': {0: '1/1/2015', 1: '2/25/2012', 2: '7/9/2018', 3: '12/2/2021', 4: '11/13/1995'}}
    df = pd.DataFrame(data)
    df.to_csv(os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test.csv", index=False)
    df.to_csv(os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test2.csv", index=False, sep='|')
    df.to_csv(os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\test3.csv", index=False, header=False)

    data['neighborhood'][0] = data['neighborhood'][0] * 500
    df.to_csv(os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\varchar.csv", index=False, header=False)


def set_up_test_table_sql(ms_dbconn, schema='dbo'):
    """
    Creates one test table for testing

    Uses random to make randomly generated inputs.
    """
    table_name = 'sql_test_table_{user}'.format(user=ms_dbconn.username)

    if ms_dbconn.table_exists(table_name=table_name, schema=schema):
        return

    ms_dbconn.query("""
    create table {schema}.{table} (test_col1 int, test_col2 int, geom geometry);
    insert into {schema}.{table} VALUES(1, 2, geometry::Point(985831.79200444, 203371.60461367, 2263));
    insert into {schema}.{table} VALUES(3, 4, geometry::Point(985831.79200444, 203371.60461367, 2263));
    """.format(schema=schema, table=table_name))


def clean_up_test_table_sql(ms_dbconn, schema='dbo'):
    table_name = 'sql_test_table_{user}'.format(user=ms_dbconn.username)
    ms_dbconn.drop_table(table_name=table_name, schema_name=schema)


def set_up_test_table_pg(pg_dbconn, schema='working'):
    """
    Creates one test table for testing

    Uses random to make randomly generated inputs.
    """
    table_name = 'pg_test_table_{user}'.format(user=pg_dbconn.username)

    if pg_dbconn.table_exists(table_name=table_name, schema=schema):
        return

    pg_dbconn.query("""
    create table {schema}.{table}(id int, test_col1 int, test_col2 int, geom geometry);
    """.format(schema=schema, table=table_name))

    for i in range(0, 1000):
        c1 = random.randint(0, 10000)
        c2 = random.randint(0, 10000)

        dec_lat = random.random() / 100
        dec_lon = random.random() / 100

        lat = 40.7 + dec_lat
        lon = -74 + dec_lon

        pg_dbconn.query("""
        INSERT INTO {schema}.{table} values
        ({i}, {c1}, {c2}, ST_SetSRID(ST_MakePoint({lat}, {lon}), 4326))
        """.format(schema=schema, table=table_name, i=i, c1=c1, c2=c2, lat=lat, lon=lon))


def clean_up_test_table_pg(pg_dbconn):
    table_name = 'pg_test_table_{user}'.format(user=pg_dbconn.username)
    pg_dbconn.drop_table(table_name=table_name, schema_name='working')


def set_up_two_test_tables_pg(pg_dbconn):
    """
    Creates two test tables for testing

    Uses random to make randomly generated inputs.
    """
    table_name = 'pg_test_table_{user}'.format(user=pg_dbconn.username)
    table_name2 = 'pg_test_table_{user}_2'.format(user=pg_dbconn.username)

    if pg_dbconn.table_exists(table_name=table_name, schema_name='working') and \
            pg_dbconn.table_exists(table_name=table_name2, schema_name='working'):
        return
    else:
        pg_dbconn.drop_table(table_name=table_name, schema_name='working')
        pg_dbconn.drop_table(table_name=table_name2, schema_name='working')

    pg_dbconn.query("""
    create table working.{table}(id int, test_col1 int, test_col2 int, geom geometry);
    """.format(table=table_name))

    pg_dbconn.query("""
    create table working.{table}(id int, test_col1 int, test_col2 int, geom geometry);
    """.format(table=table_name2))

    for i in range(0, 10000):
        c1 = random.randint(0, 10000)
        c2 = random.randint(0, 10000)

        dec_lat = random.random() / 100
        dec_lon = random.random() / 100

        lat = 40.7 + dec_lat
        lon = -74 + dec_lon

        dec_lat2 = random.random() / 100
        dec_lon2 = random.random() / 100

        lat2 = 40.7 + dec_lat2
        lon2 = -74 + dec_lon2

        pg_dbconn.query("""
        INSERT INTO working.{table} values
        ({i}, {c1}, {c2}, ST_SetSRID(ST_MakePoint({lat}, {lon}), 4326))
        """.format(table=table_name, i=i, c1=c1, c2=c2, lat=lat, lon=lon))

        pg_dbconn.query("""
        INSERT INTO working.{table} values
        ({i}, {c1}, {c2}, ST_SetSRID(ST_MakePoint({lat2}, {lon2}), 4326))
        """.format(table=table_name2, i=i, c1=c1, c2=c2, lat2=lat2, lon2=lon2))


def clean_up_two_test_tables_pg(pg_dbconn):
    table_name = 'pg_test_table_{user}'.format(user=pg_dbconn.username)
    table_name2 = 'pg_test_table_{user}_2'.format(user=pg_dbconn.username)

    pg_dbconn.drop_table(table_name=table_name, schema_name='working')
    pg_dbconn.drop_table(table_name=table_name2, schema_name='working')


def set_up_feature_class():
    """
    Builds file gdb with a feature class with sample data
    :return:
    """
    zip_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data/nyclion_21d.zip')
    if not os.path.isfile(zip_path):

        download_url = r'https://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/nyclion_21d.zip'
        req = requests.get(download_url)

        with open(zip_path, 'wb') as _file:
            _file.write(req.content)
    gdb = os.path.join(os.path.dirname(zip_path), 'lion/lion.gdb')
    if not os.path.isfile(gdb):
        print ('extracting\n\n')
        print(os.path.dirname(zip_path))
        print('\n\n')
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(os.path.dirname(zip_path))


def clean_up_feature_class():
    zip_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data/nyclion_21d.zip')
    _dir = os.path.dirname(zip_path)+'/lion'
    gdb = "lion.gdb"
    print('Deleting any existing gdb')
    # os.remove(os.path.join(fldr, gdb))
    os.rmdir(_dir)

# def set_up_fc_and_shapefile():
#     """
#     Builds file gdb with a feature class with sample data
#     Builds a shapefile for import
#     :return:
#     """
#     fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
#     set_up_feature_class()
#
#     print 'Deleting any existing shp'
#     shp = 'test_feature_class.shp'
#     Delete_management(os.path.join(fldr, shp))
#
#     print 'Building shapefile'
#     FeatureClassToShapefile_conversion(["test_feature_class"], fldr)
#
#
# def clean_up_fc_and_shapefile():
#     fldr = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')
#     gdb = "fGDB.gdb"
#     shp = 'test_feature_class.shp'
#
#     Delete_management(os.path.join(fldr, gdb))
#     Delete_management(os.path.join(fldr, shp))
#     print 'Deleted ESRI sample files'
#
#
def set_up_shapefile():
    data = {
        'gid': {0: 1, 1: 2},
        'WKT': {0: 'POINT(-73.88782477721676 40.75343453961836)', 1: 'POINT(-73.88747073046778 40.75149365677327)'},
        'some_value': {0: 'test1', 1: 'test2'}
    }
    df = pd.DataFrame(data)
    df.to_csv(os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\sample.csv", index=False)
    _file = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\sample.csv"

    _dir = os.path.dirname(os.path.abspath(__file__)) + "\\test_data\\"

    cmd = f'''ogr2ogr -f "ESRI Shapefile" {_dir}test.shp -dialect sqlite -sql 
    "SELECT gid, GeomFromText(WKT, 4326), some_value FROM sample" {_file}'''
    os.system(cmd.replace('\n', ' '))
    print('Sample shapefile ready...')


def clean_up_shapefile():
    _dir = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    #shp = "test.shp"
    for ext in ('shp', 'dbf', 'shx'):
        os.remove(f'{_dir}\\test_data\\test.{ext}')

    print('Deleting any existing shp')
    # Delete_management(os.path.join(fldr, shp))


def set_up_schema(db, ms_schema='dbo', pg_schema='working'):
    if db.type == 'MS':
        db.query(f"""
            IF NOT EXISTS (
                SELECT  schema_name
                FROM    information_schema.schemata
                WHERE   schema_name = '{ms_schema}' 
            ) 
             
            BEGIN
            EXEC sp_executesql N'CREATE SCHEMA {ms_schema}'
            END
        """)
    if db.type == 'PG':
        db.query(f"""
            create schema if not exists {pg_schema};
        """)


def clean_up_schema(db, schema):
    if db.type == 'PG':
        cascade = ' cascade'
    else:
        cascade = ''
    db.query("DROP SCHEMA IF EXISTS {schema}{cascade};".format(schema=schema, cascade=cascade))

def clean_up_shp(file_path):
    for ext in ('.shp', '.dbf', '.shx', '.prj'):
        clean_up_file(file_path.replace('.shp', ext))


def clean_up_file(file_path):
    if os.path.isfile(file_path):
        os.remove(file_path)
        print('%s file removed\n' % os.path.basename(file_path))


def set_up_xls():
    xls_file1 = os.path.join(DIR, 'test_xls.xls')
    if os.path.isfile(xls_file1):
        clean_up_file(xls_file1)

    test_df1 = pd.DataFrame({'a': {0: 1, 1: 2}, 'b': {0: 3, 1: 4}, 'Unnamed: 0': {0: 0, 1: 1}})
    test_df1.to_excel(os.path.join(DIR, 'test_xls.xls'))
    print('%s created\n' % os.path.basename(xls_file1))

    xls_file2 = os.path.join(DIR, 'test_xls_with_sheet.xls')
    if os.path.isfile(xls_file2):
        clean_up_file(xls_file2)

    test_df2 = pd.DataFrame({'a': {0: 1, 1: 2}, 'b': {0: 3, 1: 4}, 'Unnamed: 0': {0: 0, 1: 1}})

    test_df2.to_excel(os.path.join(DIR, 'test_xls_with_sheet.xls'), sheet_name='AnotherSheet')
    w = copy(open_workbook(xls_file2))
    sheet2 = w.add_sheet('Sheet2')
    col, row = 0, 0
    sheet2.write(row, col, '')
    row += 1
    for i in range(len(test_df2)):
        sheet2.write(row, col, i)
        row += 1
    col, row = 1, 0
    for c in test_df2.columns:
        sheet2.write(row, col, c)
        row += 1
        for r in test_df2[c]:
            sheet2.write(row, col, r)
            row += 1
        col += 1
        row = 0
    w.save(xls_file2)
    print('%s created\n' % os.path.basename(xls_file2))