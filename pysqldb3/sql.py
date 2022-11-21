MS_SCHEMA_FOR_LOG_CLEANUP_QUERY = r"""
select s.name as schema_name, 
    s.schema_id,
    u.name as schema_owner
from sys.schemas s
    inner join sys.sysusers u
        on u.uid = s.principal_id
where u.name not like 'db[_]%' 
    and u.name != 'INFORMATION_SCHEMA' 
    and u.name != 'sys'
    and u.name != 'guest'
order by s.schema_id
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
    and [Target].table_name = [Source].table_name 
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
select s.name 
from sys.schemas s
join sys.sysusers u
on u.uid = s.principal_id
"""

PG_GET_SCHEMAS_QUERY = r"""
SELECT schema_name FROM information_schema.schemata
"""

"""
Shapefile
"""

SHP_DEL_INDICES_QUERY_PG = r"""
select t.relname as table_name, n.nspname as schema_name, i.relname as index_name, a.attname as column_name
from pg_class t
join pg_index ix
on t.oid = ix.indrelid
join pg_class i
on i.oid = ix.indexrelid
join pg_attribute a
on a.attrelid = t.oid and a.attnum = ANY(ix.indkey) 
join pg_catalog.pg_namespace n 
on  t.relnamespace = n.oid 
where
    t.relkind = 'r'
    and n.nspname != 'pg_catalog'
    and t.relname = '{t}'
    and n.nspname = '{s}'
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
comment on table {schema}.{table} is '{table} created by {username} on {dt}
shp source: {path}\{shpfile}
- imported using pysql module -'
"""

FEATURE_COMMENT_QUERY = """
comment on table {schema}.{table} is '{table} created by {username} on {dt}
- imported using pysql module -'
"""
