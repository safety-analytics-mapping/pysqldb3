"""
DbConnect IO
"""

PG_BULK_FILE_TO_TABLE_CMD = """
ogr2ogr -f "{t}" 
PG:"host={server} 
user={user} 
dbname={db} 
password={password}
port={port}"
"{f}" 
-oo EMPTY_STRING_AS_NULL=YES
-nln "{schema}.stg_{tbl}" 
-overwrite
""".replace('\n', ' ')

MS_BULK_FILE_TO_TABLE_CMD = """
ogr2ogr -f "{t}" "MSSQL:server={server}; 
UID={user}; database={db}; PWD={password}"
-nln "{schema}.stg_{tbl}" 
"{f}" 
-oo EMPTY_STRING_AS_NULL=YES
--config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO 
--config MSSQLSPATIAL_LIST_ALL_TABLES YES
-overwrite
""".replace('\n', ' ')

WRITE_CSV_CMD_PG = """
ogr2ogr -overwrite -f "CSV" "{output_file}"
PG:"host={host} user={username} dbname={db} password={password}" -sql "{pg_sql_select}"
-lco SEPARATOR={separator} -lco STRING_QUOTING={string_quote} -lco GEOMETRY=AS_WKT -nln none 
""".replace('\n', ' ')

WRITE_CSV_CMD_MS = """
ogr2ogr -overwrite -f "CSV" "{output_file}"
"MSSQL:server={host};database={db};UID={username};PWD={password}" -sql "{ms_sql_select}" 
--config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO --config MSSQLSPATIAL_LIST_ALL_TABLES YES
-lco SEPARATOR={separator} -lco STRING_QUOTING={string_quote} -lco GEOMETRY=AS_WKT -nln none 
""".replace('\n', ' ')


"""
Shapefiles 
"""

WRITE_SHP_CMD_PG = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -overwrite -f "ESRI Shapefile" "{export_path}\{shpname}"  -a_srs "EPSG:{srid}"
PG:"host={host} user={username} dbname={db} password={password}" -sql "{pg_sql_select}"
""".replace('\n', ' ')

WRITE_SHP_CMD_MS = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -overwrite -f "ESRI Shapefile" "{export_path}\{shpname}"  -a_srs "EPSG:{srid}"
"MSSQL:server={host};database={db};UID={username};PWD={password}" -sql "{ms_sql_select}"
""".replace('\n', ' ')

READ_SHP_CMD_PG = r"""ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs 
"EPSG:{srid}" -progress -f "PostgreSQL" PG:"host={host} port={port} dbname={dbname} 
user={user} password={password}" "{shp}" -nln {schema}.{tbl_name} {perc}
""".replace('\n', ' ')

READ_SHP_CMD_MS = r"""ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs 
"EPSG:{srid}" -progress -f MSSQLSpatial "MSSQL:server={host};database={dbname};UID={user};PWD={password}"
 "{shp}" -nln {schema}.{tbl_name} {perc} --config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO
""".replace('\n', ' ')

READ_FEATURE_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs 
"EPSG:{srid}" -f "PostgreSQL" PG:"host={host} user={user} dbname={dbname} 
password={password}" "{gdb}" "{feature}" -nln {sch}.{tbl_name} -progress 
""".replace('\n', ' ')

READ_FEATURE_CMD_MS = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs 
"EPSG:{srid}" -f MSSQLSpatial "MSSQL:server={ms_server};database={ms_db};UID={ms_user};PWD={ms_pass}"
 "{gdb}" "{feature}" -nln {sch}.{tbl_name} -progress --config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO {sf}
""".replace('\n', ' ')

"""
Geopackage
"""

# when writing data to geopackage, gpkg output table will match input table name
WRITE_GPKG_CMD_PG = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" {_overwrite} -f "GPKG" {_update} "{export_path}\{gpkg_name}" -nln {gpkg_tbl} -a_srs "EPSG:{srid}"
PG:"host={host} user={username} dbname={db} password={password}" -sql "{pg_sql_select}"
""".replace('\n', ' ')

WRITE_GPKG_CMD_MS = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" {_overwrite} -f "GPKG" {_update} "{export_path}\{gpkg_name}" -nln {gpkg_tbl} -a_srs "EPSG:{srid}"
"MSSQL:server={host};database={db};UID={username};PWD={password}" -sql "{ms_sql_select}"
""".replace('\n', ' ')

READ_GPKG_CMD_PG = r"""ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -lco OVERWRITE=YES -overwrite -a_srs 
"EPSG:{srid}" -progress -f "PostgreSQL" PG:"host={host} port={port} dbname={dbname} 
user={user} password={password}" "{gpkg_name}" '{gpkg_tbl}' -nln {schema}.{tbl_name} {perc}
""".replace('\n', ' ')

READ_GPKG_CMD_MS = r"""ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -lco OVERWRITE=YES -overwrite -a_srs 
"EPSG:{srid}" -progress -f MSSQLSpatial "MSSQL:server={host};database={dbname};UID={user};PWD={password}"
 "{gpkg_name}" {gpkg_tbl} -nln {schema}.{tbl_name} {perc} --config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO
""".replace('\n', ' ')

WRITE_SHP_CMD_GPKG = r'ogr2ogr -f GPKG {_update} "{full_path}\{gpkg_name}" "{full_path}\{shp_name}" -nln {gpkg_tbl}'

WRITE_GPKG_CMD_SHP = r'ogr2ogr -f "ESRI Shapefile" "{export_path}\{shp_name}" "{full_path}\{gpkg_name}" {gpkg_tbl}'

COUNT_GPKG_LAYERS = r'ogrinfo "{full_path}"' # command used for reading in all geopackage tables

"""
Db to Db IO 
"""

PG_TO_SQL_LDAP_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}"  -overwrite -f MSSQLSpatial 
"MSSQL:server={ms_server};database={ms_db};UID={ms_user};PWD={ms_pass}" 
PG:"host={pg_host} port={pg_port} dbname={pg_database} user={pg_user} password={pg_pass}" 
{pg_schema}.{pg_table} -lco OVERWRITE=yes -nln {ms_schema}.{dest_name} {spatial} {nlt_spatial} -progress 
--config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO
""".replace('\n', ' ')

PG_TO_SQL_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}"  -overwrite -f MSSQLSpatial 
"MSSQL:server={ms_server};database={ms_db};UID={ms_user};PWD={ms_pass}" 
PG:"host={pg_host} port={pg_port} dbname={pg_database} user={pg_user} password={pg_pass}" 
{pg_schema}.{pg_table} -lco OVERWRITE=yes -nln {ms_schema}.{dest_name} {spatial} {nlt_spatial} -progress 
--config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO
""".replace('\n', ' ')

PG_TO_SQL_QRY_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}"  -overwrite -f MSSQLSpatial 
"MSSQL:server={ms_server};database={ms_db};UID={ms_user};PWD={ms_pass}" 
PG:"host={pg_host} port={pg_port} dbname={pg_database} user={pg_user} password={pg_pass}" 
-sql "{sql_select}" -lco OVERWRITE=yes -nln {ms_schema}.{dest_name} {spatial} {nlt_spatial} -progress 
--config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO
""".replace('\n', ' ')

SQL_TO_PG_LDAP_QRY_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -overwrite -f "PostgreSQL" PG:"host={pg_host} 
port={pg_port} dbname={pg_database} user={pg_user} password={pg_pass}" -f {spatial} 
"MSSQL:server={ms_server};database={ms_database}; UID={ms_user};PWD={ms_pass}" -sql "{sql_select}" -lco OVERWRITE=yes 
-nln {pg_schema}.{table_name} {nlt_spatial} -progress --config MSSQLSPATIAL_LIST_ALL_TABLES YES
""".replace('\n', ' ')


SQL_TO_PG_QRY_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -overwrite -f "PostgreSQL" PG:"host={pg_host} port={pg_port} 
dbname={pg_database} user={pg_user} password={pg_pass}" -f {spatial} "MSSQL:server={ms_server};database={ms_database};
UID={ms_user};PWD={ms_pass}" -sql "{sql_select}" -lco OVERWRITE=yes -nln {pg_schema}.{table_name} {nlt_spatial} 
-progress --config MSSQLSPATIAL_LIST_ALL_TABLES YES
""".replace('\n', ' ')

SQL_TO_PG_LDAP_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -overwrite -f "PostgreSQL" PG:"host={pg_host} port={pg_port} 
dbname={pg_database} 
user={pg_user} password={pg_pass}" -f {spatial} "MSSQL:server={ms_server};database={ms_database};" 
{ms_schema}.{ms_table} -nln {pg_schema}.{to_pg_name} {nlt_spatial} -progress 
--config MSSQLSPATIAL_LIST_ALL_TABLES YES
""".replace('\n', ' ')

SQL_TO_PG_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -overwrite -f "PostgreSQL" PG:"host={pg_host} port={pg_port} 
dbname={pg_database} user={pg_user} password={pg_pass}" 
-f {spatial} "MSSQL:server={ms_server};database={ms_database};
UID={ms_user};PWD={ms_pass}" {ms_schema}.{ms_table} -lco OVERWRITE=yes 
-nln {pg_schema}.{to_pg_name} {nlt_spatial} -progress 
--config MSSQLSPATIAL_LIST_ALL_TABLES YES
""".replace('\n', ' ')

SQL_TO_SQL_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" 
-f "MSSQLSpatial" "MSSQL:server={to_server};database={to_database}; UID={to_user};PWD={to_pass}" 
-overwrite "MSSQL:server={from_server};database={from_database};UID={from_user};PWD={from_pass}" 
-sql "{qry}" -lco OVERWRITE=yes -nln {to_schema}.{to_table} {nlt_spatial} -progress 
--config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO
""".replace('\n', ' ')

PG_TO_PG_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -overwrite -f "PostgreSQL" PG:"host={to_pg_host} port={to_pg_port} 
dbname={to_pg_database} user={to_pg_user} password={to_pg_pass}" PG:"host={from_pg_host} port={from_pg_port} 
dbname={from_pg_database}  user={from_pg_user} password={from_pg_pass}" {from_pg_schema}."{from_pg_table}" 
-lco OVERWRITE=yes -nln {to_pg_schema}.{to_pg_name} {nlt_spatial} -progress
""".replace('\n', ' ')

PG_TO_PG_QRY_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -overwrite -f "PostgreSQL" PG:"host={to_pg_host} port={to_pg_port} 
dbname={to_pg_database} user={to_pg_user} password={to_pg_pass}" PG:"host={from_pg_host} port={from_pg_port} 
dbname={from_pg_database}  user={from_pg_user} password={from_pg_pass}"  -sql "{sql_select}"
-lco OVERWRITE=yes -nln {to_pg_schema}.{to_pg_name} {nlt_spatial} -progress
""".replace('\n', ' ')

PG_TO_CSV_CMD = """
ogr2ogr --config GDAL_DATA "{gdal_data}"
-f CSV "{output_csv}" 
PG:"host={from_pg_host} port={from_pg_port} dbname={from_pg_database} user={from_pg_user} password={from_pg_pass}" 
-sql "{sql_select}"
-lco GEOMETRY=AS_WKT
""".replace('\n', ' ')

SQL_TO_CSV_CMD = """
ogr2ogr --config GDAL_DATA "{gdal_data}"
--config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO 
-f CSV "{output_csv}" 
"MSSQL:server={from_server};database={from_database};UID={from_user};PWD={from_pass}" "MSSQLSpatial"
-sql "{sql_select}" 
-lco GEOMETRY=AS_WKT
""".replace('\n', ' ')