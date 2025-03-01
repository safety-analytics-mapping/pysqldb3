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
    ); 
    grant select on {s}.{log} to public;
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

PG_PYSQLDB_USERS_QUERY = r"""
SELECT DISTINCT 
	tableowner
FROM
    pg_catalog.pg_tables
WHERE
    schemaname ='{s}'
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

PG_USER_TABLES_QUERY = r"""
{b}
SELECT
    t.tablename, t.tableowner, u.created_on, u.expires
FROM
    pg_catalog.pg_tables t
LEFT OUTER JOIN 
	user_tables u
USING
	(tablename, tableowner)
WHERE
    t.schemaname ='{s}'
ORDER BY 
    t.tableowner, t.tablename
	
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
WHERE s.name = '{s}' AND t.name = '{t}'
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
comment on table {s}.{t} is '{t} created by {u} on {d}
shp source: {p}\{shp}
- imported using pysql module -'
"""

FEATURE_COMMENT_QUERY = """
comment on table {s}.{t} is '{t} created by {u} on {d}
- imported using pysql module -'
"""


GET_MS_INDEX_QUERY = """
WITH CTE_Indexes (SchemaName, ObjectID, TableName, IndexID, IndexName, 
                  ColumnID, column_index_id, ColumnNames, IncludeColumns, 
                  NumberOfColumns, IndexType, Is_Unique,Is_Primary_Key,Is_Unique_Constraint)
AS
(
SELECT s.name, t.object_id, t.name, i.index_id, i.name, c.column_id, ic.index_column_id,
        CASE ic.is_included_column WHEN 0 THEN CAST(c.name AS VARCHAR(5000)) ELSE '' END, 
        CASE ic.is_included_column WHEN 1 THEN CAST(c.name AS VARCHAR(5000)) ELSE '' END, 
        1, i.type_desc,I.is_unique,i.Is_Primary_Key,i.Is_Unique_Constraint 
    FROM  sys.schemas AS s
        JOIN sys.tables AS t ON s.schema_id = t.schema_id
            JOIN sys.indexes AS i ON i.object_id = t.object_id
                JOIN sys.index_columns AS ic 
                  ON ic.index_id = i.index_id 
                 AND ic.object_id = i.object_id
                    JOIN sys.columns AS c 
                      ON c.column_id = ic.column_id 
                     AND c.object_id = ic.object_id
                     AND ic.index_column_id = 1
UNION ALL
SELECT s.name, t.object_id, t.name, i.index_id, i.name, c.column_id, ic.index_column_id,
        CASE ic.is_included_column WHEN 0 THEN CAST(cte.ColumnNames + ', ' + c.name AS VARCHAR(5000))  
                                          ELSE cte.ColumnNames END, 
        CASE  
            WHEN ic.is_included_column = 1 AND cte.IncludeColumns != '' 
                THEN CAST(cte.IncludeColumns + ', ' + c.name AS VARCHAR(5000))
            WHEN ic.is_included_column =1 AND cte.IncludeColumns = '' 
                THEN CAST(c.name AS VARCHAR(5000)) 
            ELSE '' 
        END,
        cte.NumberOfColumns + 1, i.type_desc,I.is_unique,I.Is_Primary_Key,i.Is_Unique_Constraint 
    FROM  sys.schemas AS s
        JOIN sys.tables AS t ON s.schema_id = t.schema_id
            JOIN sys.indexes AS i ON i.object_id = t.object_id
                JOIN sys.index_columns AS ic 
                  ON ic.index_id = i.index_id 
                 AND ic.object_id = i.object_id
                    JOIN sys.columns AS c 
                      ON c.column_id = ic.column_id 
                     AND c.object_id = ic.object_id 
                    JOIN CTE_Indexes cte 
                      ON cte.Column_index_ID + 1 = ic.index_column_id  
                    --JOIN CTE_Indexes cte ON cte.ColumnID + 1 = ic.index_column_id  
                     AND cte.IndexID = i.index_id AND cte.ObjectID = ic.object_id
)
select 
    IndexName, 
    ColumnNames, 
    IndexType
from 
    CTE_Indexes
where 
    SchemaName = '{schema}' and TableName = '{table}'
"""

GET_PG_INDEX_QUERY = """
SELECT 
    indexname,
    indexdef 
FROM 
    pg_indexes 
WHERE
    schemaname='{schema}' 
    AND tablename='{table}';
"""
