MS_SCHEMA_FOR_LOG_CLEANUP_QUERY = r"""
SELECT s.name AS schema_name, 
    s.schema_id,
    u.name AS schema_owner
FROM sys.schemas s
    INNER JOIN sys.sysusers u
        ON u.uid = s.principal_id
WHERE u.name NOT LIKE 'db[_]%' 
    AND u.name != 'INFORMATION_SCHEMA' 
    AND u.name != 'sys'
    AND u.name != 'guest'
ORDER BY s.schema_id
"""

MS_CREATE_LOG_TABLE_QUERY = r"""
CREATE TABLE {serv}{db}{s}.{log} (
    tbl_id int IDENTITY(1,1) PRIMARY KEY,
    table_owner varchar(255),
    table_schema varchar(255),
    table_name varchar(255),
    created_on datetime, 
    expires date
)
"""

MS_ADD_TABLE_TO_LOG_QUERY = r"""
MERGE {ser}{db}{s}.__temp_log_table_{u}__ AS [Target] 
USING (
    SELECT 
        '{u} 'as table_owner,
        '{s}' as table_schema,
        '{t}' as table_name,
        '{dt}' as created_on , 
        '{ex}' as expires
) AS [Source] ON [Target].table_schema = [Source].table_schema 
    AND [Target].table_name = [Source].table_name 
WHEN MATCHED THEN UPDATE 
    SET [Target].created_on = [Source].created_on,
    [Target].expires = [Source].expires
WHEN NOT MATCHED THEN INSERT (table_owner, table_schema, table_name, created_on, expires) 
    VALUES ([Source].table_owner, [Source].table_schema, [Source].table_name, 
        [Source].created_on, [Source].expires);
"""

PG_CREATE_LOG_TABLE_QUERY = r"""
CREATE TABLE {s}.{log}  (
    tbl_id SERIAL,
    table_owner varchar,
    table_schema varchar,
    table_name varchar,
    created_on timestamp, 
    expires date, 
    primary key (table_schema, table_name)
    )
"""

PG_ADD_TABLE_TO_LOG_QUERY = r"""
INSERT INTO {s}.{log} (
    table_owner,
    table_schema,
    table_name,
    created_on , 
    expires
)
VALUES ('{u}', '{s}', '{t}', '{dt}', '{ex}')
ON CONFLICT (table_schema, table_name) DO 
UPDATE SET expires = EXCLUDED.expires, created_on=EXCLUDED.created_on
"""

PG_BLOCKING_QUERY = r"""
SELECT blocked_locks.pid     AS blocked_pid,
     blocked_activity.usename  AS blocked_user,
     blocking_locks.pid     AS blocking_pid,
     blocking_activity.usename AS blocking_user,
     blocked_activity.query    AS blocked_statement,
     blocking_activity.query   AS current_statement_in_blocking_process
FROM  pg_catalog.pg_locks         blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks         blocking_locks 
    ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
    AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
    AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
    AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
    AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
    AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
    AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
    AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
    AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.GRANTED
AND blocked_activity.usename = '%s'
ORDER BY blocking_activity.usename;
"""

PG_KILL_BLOCKS_QUERY = r"""
SELECT blocking_locks.pid AS blocking_pid
FROM  pg_catalog.pg_locks         blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks         blocking_locks 
    ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
    AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
    AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
    AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
    AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
    AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
    AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
    AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
    AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.GRANTED
and blocking_activity.usename = '%s'
"""

PG_MY_TABLES_QUERY = r"""
SELECT
    tablename, tableowner
FROM
    pg_catalog.pg_tables
WHERE
    schemaname ='{s}'
    AND tableowner='{u}'
ORDER BY 
    tablename
"""

PG_TABLE_EXISTS_QUERY = r"""
SELECT EXISTS (
SELECT 1
FROM pg_catalog.pg_tables
WHERE schemaname = '{s}'
AND tablename = '{t}'
)        
"""

MS_TABLE_EXISTS_QUERY = r"""
SELECT * 
FROM sys.tables t 
JOIN sys.schemas s 
ON t.schema_id = s.schema_id
WHERE LOWER(s.name) = '{s}' AND LOWER(t.name) = '{t}'
"""

MS_GET_SCHEMAS_QUERY = r"""
SELECT s.name 
FROM sys.schemas s
JOIN sys.sysusers u
ON u.uid = s.principal_id
"""

PG_GET_SCHEMAS_QUERY = r"""
SELECT schema_name FROM information_schema.schemata
"""

"""
Shapefile
"""

SHP_DEL_INDICES_QUERY_PG = r"""
SELECT t.relname AS table_name, n.nspname AS schema_name, i.relname AS index_name, a.attname AS column_name
FROM pg_class t
JOIN pg_index ix 
ON t.oid = ix.indrelid
JOIN pg_class i 
ON i.oid = ix.indexrelid
JOIN pg_attribute a 
ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) 
JOIN pg_catalog.pg_namespace n 
ON t.relnamespace = n.oid 
WHERE
    t.relkind = 'r'
    AND n.nspname != 'pg_catalog'
    AND t.relname = '{t}'
    AND n.nspname = '{s}'
"""

SHP_DEL_INDICES_QUERY_MS = r"""
    SELECT
        '{t}' as table_name,
        a.name AS index_name,
        COL_NAME(b.object_id,b.column_id) AS Column_Name,
        type_desc
    FROM
        sys.indexes AS a
    INNER JOIN
        sys.index_columns AS b
        ON a.object_id = b.object_id AND a.index_id = b.index_id
    WHERE
        a.is_hypothetical = 0 
        AND a.object_id = OBJECT_ID('{s}.{t}')
        AND type_desc !='SPATIAL' -- taken care of by GDAL
"""

SHP_COMMENT_QUERY = r"""
comment on table {s}.{t} is '{t} created by {u} on {d}
shp source: {p}\{shp}
- imported using pysql module -'
"""

FEATURE_COMMENT_QUERY = """
comment on table {s}.{t} is '{t} created by {u} on {d}
- imported using pysql module -'
"""
