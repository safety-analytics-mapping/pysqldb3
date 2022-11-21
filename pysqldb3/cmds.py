"""
DbConnect IO
"""

PG_BULK_FILE_TO_TABLE_CMD = """
ogr2ogr -f "{t}" 
PG:"host={server} 
user={user} 
dbname={db_name} 
password={password}
port={port}"
"{f}" 
-oo EMPTY_STRING_AS_NULL=YES
-nln "{schema}.stg_{tbl}" 
-overwrite
""".replace('\n', ' ')

MS_BULK_FILE_TO_TABLE_CMD = """
ogr2ogr -f "{t}" "MSSQL:server={server}; 
UID={user}; database={db_name}; PWD={password}"
-nln "{schema}.stg_{tbl}" 
"{f}" 
-oo EMPTY_STRING_AS_NULL=YES
--config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO 
--config MSSQLSPATIAL_LIST_ALL_TABLES YES
-overwrite
""".replace('\n', ' ')

"""
Shapefiles 
"""

WRITE_SHP_CMD_PG = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -overwrite -f "ESRI Shapefile" "{export_path}\{shpfile_name}"  -a_srs "EPSG:{srid}"
PG:"host={host} user={username} dbname={db_name} password={password}" -sql "{pg_sql_select}"
""".replace('\n', ' ')

WRITE_SHP_CMD_MS = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -overwrite -f "ESRI Shapefile" "{export_path}\{shpfile_name}"  -a_srs "EPSG:{srid}"
"MSSQL:server={host}; database={db_name}; UID={username}; PWD={password}" -sql "{ms_sql_select}"
""".replace('\n', ' ')

READ_SHP_CMD_PG = r"""ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs 
"EPSG:{srid}" -progress -f "PostgreSQL" PG:"host={host} port={port} dbname={db_name} 
user={username} password={password}" "{shpfile_name}" -nln {schema_name}.{table_name} {perc}
""".replace('\n', ' ')

READ_SHP_CMD_MS = r"""ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs 
"EPSG:{srid}" -progress -f MSSQLSpatial "MSSQL:server={host}; database={db_name}; UID={username}; PWD={password}"
 "{shpfile_name}" -nln {schema_name}.{table_name} {perc} --config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO
""".replace('\n', ' ')

READ_FEATURE_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs 
"EPSG:{srid}" -f "PostgreSQL" PG:"host={host} user={username} dbname={db_name} 
password={password}" "{gdb}" "{feature}" -nln {schema_name}.{table_name} -progress 
""".replace('\n', ' ')

READ_FEATURE_CMD_MS = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs 
"EPSG:{srid}" -f MSSQLSpatial "MSSQL:server={host}; database={db_name}; UID={username}; PWD={password}"
 "{gdb}" "{feature}" -nln {schema_name}.{table_name} -progress --config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO {shpfile_name}
""".replace('\n', ' ')

"""
Db to Db IO 
"""

PG_TO_SQL_LDAP_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -overwrite -f MSSQLSpatial 
"MSSQL:server={ms_host};database={ms_dbname};UID={ms_username};PWD={ms_password}" 
PG:"host={pg_host} port={pg_port} dbname={pg_dbname} user={pg_username} password={pg_password}" 
{pg_schema_name}.{pg_table_name} -lco OVERWRITE=yes -nln {ms_schema_name}.{ms_table_name} {spatial} {nlt_spatial} -progress 
--config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO
""".replace('\n', ' ')

PG_TO_SQL_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}"  -overwrite -f MSSQLSpatial 
"MSSQL:server={ms_server};database={ms_dbname};UID={ms_username};PWD={ms_password}" 
PG:"host={pg_host} port={pg_port} dbname={pg_dbname} user={pg_username} password={pg_password}" 
{pg_schema}.{pg_table} -lco OVERWRITE=yes -nln {ms_schema}.{dest_name} {spatial} {nlt_spatial} -progress 
--config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO
""".replace('\n', ' ')

SQL_TO_PG_LDAP_QRY_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -overwrite -f "PostgreSQL" PG:"host={pg_host} 
port={pg_port} dbname={pg_dbname} user={pg_username} password={pg_password}" -f {spatial} 
"MSSQL:server={ms_host};database={ms_dbname}; UID={ms_username};PWD={ms_password}" -sql "{sql_select}" -lco OVERWRITE=yes 
-nln {pg_schema_name}.{pg_table_name} {nlt_spatial} -progress --config MSSQLSPATIAL_LIST_ALL_TABLES YES
""".replace('\n', ' ')


SQL_TO_PG_QRY_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -overwrite -f "PostgreSQL" PG:"host={pg_host} port={pg_port} 
dbname={pg_dbname} user={pg_username} password={pg_password}" -f {spatial} "MSSQL:server={ms_host}; database={ms_dbname};
UID={ms_username}; PWD={ms_password}" -sql "{sql_select}" -lco OVERWRITE=yes -nln {pg_schema}.{table_name} {nlt_spatial} 
-progress --config MSSQLSPATIAL_LIST_ALL_TABLES YES
""".replace('\n', ' ')

SQL_TO_PG_LDAP_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -overwrite -f "PostgreSQL" PG:"host={pg_host} port={pg_port} 
dbname={pg_dbname} user={pg_username} password={pg_password}" -f {spatial} "MSSQL:server={ms_host}; database={ms_dbname};" 
{ms_schema_name}.{ms_table_name} -nln {pg_schema_name}.{pg_table_name} {nlt_spatial} -progress 
--config MSSQLSPATIAL_LIST_ALL_TABLES YES
""".replace('\n', ' ')

SQL_TO_PG_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -overwrite -f "PostgreSQL" PG:"host={pg_host} port={pg_port} 
dbname={pg_dbname} user={pg_username} password={pg_password}" 
-f {spatial} "MSSQL:server={ms_host};database={ms_dbname};
UID={ms_username};PWD={ms_password}" {ms_schema_name}.{ms_table_name} -lco OVERWRITE=yes 
-nln {pg_schema_name}.{pg_table_name} {nlt_spatial} -progress 
--config MSSQLSPATIAL_LIST_ALL_TABLES YES
""".replace('\n', ' ')

PG_TO_PG_CMD = r"""
ogr2ogr --config GDAL_DATA "{gdal_data}" -overwrite -f "PostgreSQL" PG:"host={dest_host} port={dest_port}  
dbname={dest_dbname} user={dest_username} password={dest_password}" PG:"host={from_pg_host} port={src_port} 
dbname={src_dbname}  user={src_username} password={src_password}" {src_schema_name}.{src_table_name} 
-lco OVERWRITE=yes -nln {dest_schema_name}.{dest_table_name} {nlt_spatial} -progress
""".replace('\n', ' ')

PG_TO_PG_QRY_CMD = """
ogr2ogr --config GDAL_DATA "{gdal_data}" -overwrite -f "PostgreSQL" PG:"host={dest_host} port={dest_port} 
dbname={dest_dbname} user={dest_username} password={dest_password}" PG:"host={src_host} port={src_port} 
dbname={src_dbname}  user={src_username} password={src_password}"  -sql "{sql_select}"
-lco OVERWRITE=yes -nln {dest_schema_name}.{dest_table_name} {nlt_spatial} -progress
""".replace('\n', ' ')