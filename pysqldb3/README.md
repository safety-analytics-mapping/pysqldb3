## Pysqldb3 Library Components 

## 1. Pysqldb3 (DbConnect) - Pysqldb3.py
Pysqldb contains DbConnect, a class built off of pymssql/psycopg2 that makes it easy to connect to Postgres/Sql Server databases in Python. 
### 1.1 Simple Example/Setup  
To create a DbConnect object:
1. Import 
   `from pysqldb3 import pysqldb3`
2. `db = pysqldb.DbConnect(user=None, password=None, ldap=False, type=None, server=None, database=None, port=5432,
                 allow_temp_tables=False, use_native_driver=True, default=False, quiet=False))` 
    + Replace server_name and db_name with the actual values. 
    + You must specify the type, either `PG` (Postgres) or `MS` (SQL Server). 
    + You can also add your username using `user='my_username'` 
    + You can add your password similarly using `password='mypassword'`
    + Other inputs include LDAP and ports. If you're unsure about these, feel free to ask around. 
3. Now, you can query on your database by using `db.query('select * from my_schema.my_table')`

### 1.2 Functions 
In Jupyter or Python shell, use help(pysqldb) to show all public functions and their inputs. <br>
#### pysqldb3 public functions:


1. [`DbConnect`](#connect): Connects to database
1. [`disconnect`](#disconnect): Disconnects from database.  
1. [`check_conn`](#check_conn): Checks and reconnects to connection if not currently connected.
1. [`log_temp_table`](#log_temp_table): Writes tables to temp log to be deleted after expiration date.
1. [`check_logs`](#check_logs): Queries the temp log associated with the user's login and returns a pandas DataFrame of results. 
1. [`clean_up_new_tables`](#clean_up_new_tables): Drops all newly created tables from this DbConnect instance (current session).
1. [`blocking_me`](#blocking_me): Queries database to check for queries or users currently blocking the user ()defined in the connection). *Postgres Only.*
1. [`kill_blocks`](#kill_blocks): Will kill any queries that are blocking, that the user (defined in the connection) owns. *Postgres Only*.
1. [`my_tables`](#my_tables): Get a list of tables for which user (defined in the connection) is the owner *Postgres Only*.
1. [`table_exists`](#table_exists): Checks if table exists in the database. 
1. [`get_schemas`](#get_schemas): Gets a list of schemas available in the database
1. [`query`](#query): Runs query from input SQL string, calls Query object. 
1. [`drop_table`](#drop_table):  Drops table from database and removes from the temp log table
1. [`rename_column`](#rename_column): Renames a column to the new column name on the specified table.
1. [`dfquery`](#dfquery): Runs from input SQL string, calls Query object with `return_df=True`; returns Pandas DataFrame
1. [`print_last_query`](#print_last_query): Prints latest query run with basic formatting
1. [`dataframe_to_table_schema`](#dataframe_to_table_schema): Translates Pandas DataFrame into empty database table.
1. [`dataframe_to_table`](#dataframe_to_table): Adds data from Pandas DataFrame to existing table
1. [`csv_to_table`](#csv_to_table): Imports csv file to database. This uses pandas datatypes to generate the table schema.
1. [`xls_to_table`](#xls_to_table): Imports xls file to database. This uses pandas datatypes to generate the table schema.
1. [`query_to_csv`](#query_to_csv): Exports query results to a csv file.
1. [`query_to_map`](#query_to_map): Generates Plotly choropleth map using the query results.
1. [`query_to_shp`](#query_to_shp): Exports query results to an ESRI Shapefile file.
1. [`table_to_shp`](#table_to_shp): Exports database table to an ESRI Shapefile file.
1. [`table_to_csv`](#table_to_csv): Exports database table to a csv file.
1. [`shp_to_table`](#shp_to_table): Imports ESRI Shapefile to database, uses GDAL to generate the table.
1. [`feature_class_to_table`](#feature_class_to_table): Imports shape file feature class to database, uses GDAL to generate the table.








## Details 
### connect
**`pysqldb3.DbConnect(quiet=False)`**
Creates database connection instance.  
###### Parameters:
 - **`quiet`: bool, default False**: When true, does not print database connection information.
 
 
**Sample** 
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')


>>> db.connect()

Database connection (PG) to ris on dotdevrhpgsql01 - user: user_name
Connection established 2023-01-13 09:41:52, /
- ris version 0.0.3 -

>>> db.connect(quiet = True)) #nothing will return
```
[Back to Table of Contents](#pysqldb3-public-functions)
<br>

### disconnect
**`DbConnect.disconnect(quiet=False)`**
Disconnects from database. When called this will print database connection information and closed timestamp. 
###### Parameters:
 - **`quiet`: bool, default False**: When true, does not print database connection information. 

**Sample**
```
>>> from pysqldb3 import pysqldb3
>>> db = db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')

>>> db.disconnect()

Database connection (PG) to ris on dotdevrhpgsql01 - user: user_name
Connection closed 2023-01-13 10:07:15

>>> db.disconnect(quiet = True) #nothing will return

```
[Back to Table of Contents](#pysqldb3-public-functions)
<br>

### check_conn
**`DbConnect.check_conn()`**
Checks and reconnects to connection if not currently connected. 

###### Parameters: 
- None

**Sample**
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> db.check_conn() #nothing will return

```
[Back to Table of Contents](#pysqldb3-public-functions)
<br>

### log_temp_table
**`DbConnect.log_temp_table(schema, table, owner, server=None, database=None, expiration=datetime.datetime.now() + datetime.timedelta(days=7))`**
Writes tables to temp log to be deleted after expiration date. This method gets called automatically when a table is created by a DbConnect query. 
###### Parameters:
 - **`schema`: str**: Database schema name 
 - **`table`: str**: Table name to log 
  - **`owner`: str**: User name of table owner
  - **`server`: str, default None**: Name of server, this is needed for queries that create tables on servers that are different from the DbConnect instance's server connection
  - **`database`: str, default None**: Name of database, this is needed for queries that create tables on servers that are different from the DbConnect instance's server connection
  - **`expiration`: datetime, default datetime.datetime.now() + datetime.timedelta(days=7)**: Date where table should be removed from the database
```
>>> db.log_temp_table(schema="working", table="doc_test", owner='user_name')
#nothing will return
```
[Back to Table of Contents](#pysqldb3-public-functions)
<br>

### check_logs
**`DbConnect.check_logs(schema=None)`**
Queries the temp log associated with the user's login and returns a pandas DataFrame of results.  
###### Parameters:
 - **`schema`: str, default None**: Database schema name 

**Sample**
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')

>>> db.check_logs(schema="working")

   tbl_id table_owner table_schema table_name          created_on     expires
0       1   user_name      working   doc_test 2023-01-16 12:44:00  2023-01-23
```
[Back to Table of Contents](#pysqldb3-public-functions)
<br>

### cleanup_new_tables
**`DbConnect.clean_up_new_tables()`**
Drops all newly created tables from this DbConnect instance (current session).
###### Parameters:
 - None

**Sample**
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')

>>> db.check_logs(schema="working")

Empty DataFrame
Columns: [tbl_id, table_owner, table_schema, table_name, created_on, expires]
Index: []

>>> db.query("create table working.haos_temp_test_table as select 1 as dta")

- Query run 2023-01-16 13:01:04.224953
 Query time: Query run in 2091 microseconds
 * Returned 0 rows *
 
>>> db.check_logs(schema='working')

   tbl_id table_owner table_schema            table_name          created_on     expires
0       2   user_name      working  haos_temp_test_table 2023-01-16 13:01:00  2023-01-23

>>> db.cleanup_new_tables()

Dropped 1 tables

>>> db.check_logs(schema='working')

Empty DataFrame
Columns: [tbl_id, table_owner, table_schema, table_name, created_on, expires]
Index: []
```
[Back to Table of Contents](#pysqldb3-public-functions)
<br>

### blocking_me
**`DbConnect.blocking_me()`**
QRuns dfquery to find which queries or users are blocking the user defined in the connection. Postgres Only.
###### Parameters:
- None

**Sample**
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> db.blocking_me()

Empty DataFrame
Columns: [blocked_pid, blocked_user, blocking_pid, blocking_user, blocked_statement, current_statement_in_blocking_process]
Index: []
```
[Back to Table of Contents](#pysqldb3-public-functions)
<br>

### kill_blocks
**`DbConnect.kill_blocks()`**
Will kill any queries that are blocking, that the user (defined in the connection) owns. Postgres Only.
###### Parameters:
- None

**Sample**
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> db.kill_blocks()
#nothing will return if there's no block
```
[Back to Table of Contents](#pysqldb3-public-functions)
<br>

### my_tables
**`DbConnect.my_tables(schema='public')`**
Get a list of tables for which user (defined in the connection) is the owner Postgres Only.
###### Parameters:
 - **`schema`: str, default None**: Database schema name

**Sample**
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> db.my_tables(schema='working')

                       tablename     tableowner
0  __temp_log_table_user_name__       user_name
```
[Back to Table of Contents](#pysqldb3-public-functions)
<br>

### table_exists
**`DbConnect.table_exists(table, **kwargs)`**
Checks if table exists in the database
###### Parameters:
 - **`table`: str** Table name 
 - **`schema`: str, default Database's default schema**: Database schema name 
 
**Sample**
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> db.table_exists('bike_inj', schema='working')

True
```
[Back to Table of Contents](#pysqldb3-public-functions)
<br>

### get_schemas
**`DbConnect.get_schemas()`**
Gets a list of schemas available in the database
###### Parameters:
 - None 
 
**Sample**
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> db.get_schemas()

['pg_catalog', 'information_schema', 'public', 'topology', 'working', 'staging', 'archive', 'visionzero']
```
[Back to Table of Contents](#pysqldb3-public-functions)
<br>

### query
**`DbConnect.query(query, strict=True, permission=True, temp=True, timeme=True, no_comment=False, comment='',
              lock_table=None, return_df=False, days=7, internal=False)`**

Runs query from input SQL string, calls Query object.
###### Parameters:
 - **`query` str**: String sql query to be run
 - **`strict` bool, defaut True**: If True will run sys.exit on failed query attempts
 - **`permission` bool, default True**: If False it will override default will automatically grant select permissions on any tables created in the query
 - **`temp` bool, default True**: If False overrides default behavior where new tables will be logged for deletion at a future date
 - **`timeme` bool, default True**: If False overrides default behavior that automatically prints query durration time
 - **`no_comment` bool, default False**: If True overrides default behavior to automatically generate a comment on any tables created in query (Postgres only)
 - **`comment` str, default ''**: If provided, appends to automatic table generation comment
 - **`lock_table` str, default None**: ??? Table schema and name to be locked in formate `'schema.table'`
 - **`return_df` bool, default False**: If False overrides default behavior where query results are stored and not returned, if True returns pandas DataFrame
 - **`days` int, default 7**: Defines the lifespan (number of days) of any tables created in the query, before they are automatically deleted  
 - **`internal` Boolean, default False**, flag for internal processes

*Sample**

Create a table using query and verify that the table was recorded in the `tables_created` attribute
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')

>>> db.query("create table working.haos_temp_test_table as select 1 as dta, 2 as other_field")

- Query run 2023-01-16 13:48:35.622299
 Query time: Query run in 14815 microseconds
 * Returned 0 rows * * Returned 0 rows *
 
 >>> db.tables_created

['working.haos_temp_test_table']
```

Basic select query and verify that the results are stored in the `data` attribute
```
>>> db.query("select * from working.haos_temp_test_table")

- Query run 2023-01-16 13:50:54.711544
 Query time: Query run in 2000 microseconds
 * Returned 1 rows *
 
>>> db.data

[(1, 2)]
```

Multiple queries tied together 
```
>>> db.query("""
    drop table if exists working.haos_temp_test_table; 
    create table working.haos_temp_test_table as select 1 as dta, 2 as other_field;
    alter table working.haos_temp_test_table add column typ varchar;
    update working.haos_temp_test_table set typ = 'old';
    insert into working.haos_temp_test_table values (8, 9, 'new');
    select typ, count(*) as cnt from working.haos_temp_test_table   group by typ;
""")

- Query run 2023-01-16 13:53:40.288881
 Query time: Query run in 15613 microseconds
 * Returned 2 rows *

>>> db.data

[('old', 1L), ('new', 1L)]
```

Failed query in non-strict mode
```
>>> db.query("drop table working.haos_temp_test_table; select * from working.haos_temp_test_table", strict=False)
- Query failed: relation "working.haos_temp_test_table" does not exist
LINE 1: ...able working.haos_temp_test_table; select * from working.ha...
                                                             ^
- Query run 2023-01-16 13:59:58.584221
        drop table working.haos_temp_test_table; select * from working.haos_temp_test_table

```
[Back to Table of Contents](#pysqldb3-public-functions)
<br>

### drop_table
**`DbConnect.drop_table(schema, table, cascade=False, strict=True, server=None, database=None, internal=False)`**
Drops table if it exists from database and removes from the temp log table. If a table uses "" or [] because of case, spaces, or periods, they (""/[]) must be inputted explicitly.

###### Parameters:
 - **`schema`: str** Database schema name 
 - **`table`: str** Table name 
 - **`cascade`: bool, default False**: To drop a table that is referenced by a view or a foreign-key constraint of another table, `cascade=True` must be specified. Cascade will remove a dependent view entirely, but in the foreign-key case it will only remove the foreign-key constraint, not the other table entirely. (Postgres only)
 - **`strict` bool, default True**: May not be needed, but if set to False will prevent sys.exit on failed attempts
 - **`server`: str, default None**: Name of server, this is needed if tables to drop exist on servers that are different from the DbConnect instance's server connection
 - **`database`: str, default None**: Name of database, this is needed if tables to drop exist on servers that are different from the DbConnect instance's server connection
 - **`internal`: Boolean, default False**: flag for internal processes

**Sample**
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> db.query("drop table if exists working.haos_temp_test_table;")

- Query run 2023-01-16 14:07:24.284742
 Query time: Query run in 7616 microseconds
 * Returned 0 rows *
 
>>> db.query("create table working.haos_temp_test_table as select 1 as dta, 2 as other_field;")

- Query run 2023-01-16 14:08:10.347278
 Query time: Query run in 15645 microseconds
 * Returned 0 rows *
  
 >>> db.table_exists('haos_temp_test_table', schema='working')
 
True
 
 >>> db.drop_table('working', 'haos_temp_test_table')
 
>>> db.table_exists('haos_temp_test_table', schema='working')
 
False
```
[Back to Table of Contents](#pysqldb3-public-functions)
<br>

### rename_column
**`DbConnect.rename_column(schema, table, old_column, new_column)`**
Renames a column to the new column name on the specified table
###### Parameters:
 - **`schema`: str** Database schema name 
 - **`table`: str** Table name 
 - **`old_column`: str** Name of column to be renamed
 - **`new_column`: str** New column name
 
**Sample**
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> db.query("drop table if exists working.haos_temp_test_table;")

- Query run 2023-01-16 14:47:18.821074
 Query time: Query run in 0 microseconds
 * Returned 0 rows *
 
>>> db.query("create table working.haos_temp_test_table as select 1 as dta, 2 as other_field;")

- Query run 2023-01-16 14:47:45.821073
 Query time: Query run in 10505 microseconds
 * Returned 0 rows *
 
 >>> db.rename_column('working', 'haos_temp_test_table', 'dta', 'new_column') 
 
 - Query run 2021-08-05 09:57:44.154000
 Query time: Query run in 35000 microseconds
 * Returned 0 rows *
 
 >>> db.dfquery("select * from working.haos_temp_test_table")
 
   new_column  other_field
0           1            2
```
[Back to Table of Contents](#pysqldb3-public-functions)
<br>

### dfquery
**`DbConnect.dfquery(query, strict=True, permission=True, temp=True, timeme=True, no_comment=False, comment='',
              lock_table=None, return_df=False, days=7)`**

This is a wrapper for query, to return a pandas DataFram. This runs query from input SQL string, calls Query object with return_df=True. 
###### Parameters:
 - **`query` str**: String sql query to be run
 - **`strict` bool, defaut True**: If True will run sys.exit on failed query attempts
 - **`permission` bool, default True**: If False it will override default will automatically grant select permissions on any tables created in the query
 - **`temp` bool, default True**: If False overrides default behavior where new tables will be logged for deletion at a future date
 - **`timeme` bool, default True**: If False overrides default behavior that automatically prints query durration time
 - **`no_comment` bool, default False**: If True overrides default behavior to automatically generate a comment on any tables created in query (Postgres only)
 - **`comment` str, default ''**: If provided, appends to automatic table generation comment
 - **`lock_table` str, default None**: ??? Table schema and name to be locked in formate `'schema.table'` 
 - **`days` int, default 7**: Defines the lifespan (number of days) of any tables created in the query, before they are automatically deleted  
 - **`internal` Boolean, default False**: flag for internal processes 

**Sample**

Use query to set up sample table and dfquery to explore results in pandas.
```
>>> from ris import pysqldb
>>> db = pysqldb.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> db.query("""
    drop table if exists working.haos_temp_test_table; 
    create table working.haos_temp_test_table as select 1 as dta, 2 as other_field;
    alter table working.haos_temp_test_table add column typ varchar;
    update working.haos_temp_test_table set typ = 'old';
    insert into working.haos_temp_test_table values (8, 9, 'new');
    select typ, count(*) as cnt from working.haos_temp_test_table   group by typ;
""")

- Query run 2023-01-16 14:53:12.834803
 Query time: Query run in 14840 microseconds
 * Returned 2 rows *
 
>>> df = db.dfquery("select * from working.haos_temp_test_table;")
>>> df.columns

Index(['dta', 'other_field', 'typ'], dtype='object')

>>> df

   dta  other_field  typ
0    1            2  old
1    8            9  new

>>> df.dta

0    1
1    8
Name: dta, dtype: int64

```

[Back to Table of Contents](#pysqldb3-public-functions)
<br>

### print_last_query
**`DbConnect.print_last_query()`**

Prints latest query run with basic formatting. 
###### Parameters:
 - None
**Sample**

```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> db.query("""
    select n.nodeid, street
    from node n
    join (
        select nodeidfrom nodeid, street from lion
        union
        select nodeidto nodeid, street from lion
    ) l
    on n.nodeid=l.nodeid::int
    limit 5
""")

- Query run 2023-01-16 14:59:15.959425
 Query time: Query run in 3 seconds
 * Returned 5 rows *
 
>>> db.print_last_query()

    select n.nodeid, street
    from node n
    join (
        select nodeidfrom nodeid, street from lion
        union
        select nodeidto nodeid, street from lion
    ) l
    on n.nodeid=l.nodeid::int
    limit 5

>>>

```

[Back to Table of Contents](#pysqldb3-public-functions)
<br>

### dataframe_to_table_schema
**`DbConnect.dataframe_to_table_schema(df, table, schema=None, overwrite=False, temp=True, allow_max_varchar=False,
                                  column_type_overrides=None, days=7)`**

Translates Pandas DataFrame into empty database table. Generates an empty database table using the column names and panda's datatype inferences.
Returns table schema that was created from DataFrame.
 
###### Parameters:
 - **`df` DataFrame**: Pandas DataFrame to be added to database
 - **`table` str**: Table name to be used in the database
 - **`schema` str, default None**: Database schema to use for destination in database (defaults database object's default schema)
 - **`overwrite` bool, default False**: If table exists in database will overwrite if True (defaults to False)
 - **`temp` bool, default True**: Optional flag to make table as not-temporary (defaults to True)
 - **`allow_max_varchar` bool, default False**: Boolean to allow unlimited/max varchar columns; defaults to False
 - **`column_type_overrides` Dict, default None'**: Dict of type {key=column name, value=column type}. Will manually set the
                raw column name as that type in the query, regardless of the pandas/postgres/sql server automatic
                detection.
 - **`days` int, default 7**: Defines the lifespan (number of days) of any tables created in the query, before they are automatically deleted  
 
**Sample**

```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> df = db.dfquery("select 1 as int_col, 'text' as text_col, now() as timstamp_col;")
 
>>> db.dataframe_to_table_schema(df, 'haos_temp_test_table', schema='working', overwrite=True)
[['int_col', 'bigint'], ['text_col', 'varchar (500)'], ['timstamp_col', 'varchar (500)']]

>>> db.dfquery("select * from working.haos_temp_test_table")

Empty DataFrame
Columns: [int_col, text_col, timstamp_col]
Index: []

```

[Back to Table of Contents](#pysqldb3-public-functions)
<br>

### dataframe_to_table
**`DbConnect.dataframe_to_table(df, table, table_schema=None, schema=None, overwrite=False, temp=True,
                           allow_max_varchar=False, column_type_overrides=None, days=7)`**

Translates Pandas DataFrame into populated database table. This uses dataframe_to_table_schema to generate an empty database table 
and then inserts the dataframe's data into it. 
 
###### Parameters:
 - **`df` DataFrame**: Pandas DataFrame to be added to database
 - **`table` str**: Table name to be used in the database
 - **`table_schema` list, default None**:  schema of dataframe (returned from `dataframe_to_table_schema`)
 - **`schema` str, default None**:  Database schema to use for destination in database (defaults database object's default schema)
 - **`overwrite` bool, default False**: If table exists in database will overwrite if True (defaults to False)
 - **`temp` bool, default True**: Optional flag to make table as not-temporary (defaults to True)
 - **`allow_max_varchar` bool, default False**: Boolean to allow unlimited/max varchar columns; defaults to False
 - **`column_type_overrides` Dict, default None'**: Dict of type {key=column name, value=column type}. Will manually set the
                raw column name as that type in the query, regardless of the pandas/postgres/sql server automatic
                detection.
 - **`days` int, default 7**: Defines the lifespan (number of days) of any tables created in the query, before they are automatically deleted  
 
**Sample**


```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> df = db.dfquery("select 1 as int_col, 'text' as text_col, now() as timstamp_col;")
 
>>> db.dataframe_to_table(df, 'haos_temp_test_table', schema='working', overwrite=True)

Reading data into Database

0it [00:00, ?it/s]
1it [00:00, 23.17it/s]

1 rows added to working.haos_temp_test_table

>>> db.dfquery("select * from working.haos_temp_test_table")

   int_col text_col      timstamp_col
0        1     text  2023-01-16 15:07

```

[Back to Table of Contents](#pysqldb3-public-functions)
<br>

#### csv_to_table

**`DbConnect.csv_to_table(input_file=None, overwrite=False, schema=None, table=None, temp=True, sep=',',
                     long_varchar_check=False, column_type_overrides=None, days=7)`**

Imports csv file to database. This uses pandas datatypes to generate the table schema.
###### Parameters:
 - **`input_file` DataFrame, default None**: File path to csv file; if None, prompts user input
 - **`overwrite` bool, default False**: If table exists in database will overwrite if True (defaults to False)
 - **`schema` str, default None**:  Database schema to use for destination in database (defaults database object's default schema)
 - **`table` str, default None**: Table name to be used in the database; if None will use file name in input_file's path 
 - **`temp` bool, default True**: Optional flag to make table as not-temporary (defaults to True)
 - **`sep` str, default ','**: Separator for csv file, defaults to comma (,)
 - **`long_varchar_check` bool, default False**: Boolean to allow unlimited/max varchar columns; defaults to False
 - **`column_type_overrides` Dict, default None'**: Dict of type {key=column name, value=column type}. Will manually set the
                raw column name as that type in the query, regardless of the pandas/postgres/sql server automatic
                detection.
 - **`days` int, default 7**: Defines the lifespan (number of days) of any tables created in the query, before they are automatically deleted  
 
**Sample**

```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> df = db.dfquery("select 1 as int_col, 'text' as text_col, now() as timstamp_col;")
>>> df.to_csv('haos_sample_file.csv')
>>> db.csv_to_table('haos_sample_file.csv', schema='working')
 
Reading data into Database

0it [00:00, ?it/s]
1it [00:00, 24.57it/s]

1 rows added to working.haos_sample_file
 
>>> db.dfquery("select * from working.haos_sample_file")

  unnamed__0  int_col text_col                      timstamp_col
0           0        1     text  2023-01-16 15:13:57.251890-05:00
```

[Back to Table of Contents](#pysqldb3-public-functions)
<br>

#### xls_to_table
**`DbConnect.xls_to_table(input_file=None, sheet_name=0, overwrite=False, schema=None, table=None, temp=True,
                     column_type_overrides=None, days=7)`**

Imports xls file to database. This uses pandas datatypes to generate the table schema.
###### Parameters:
 - **`input_file` DataFrame, default None**: File path to excel file; if None, prompts user input
 - **`sheet_name` str, int, or None, default 0**: Name or ordenal position of excel sheet/tab to import. If none provided it will default to the 1st sheet
 - **`overwrite` bool, default False**: If table exists in database will overwrite if True (defaults to False)
 - **`schema` str, default None**:  Database schema to use for destination in database (defaults database object's default schema)
 - **`table` str, default None**: Table name to be used in the database; if None will use file name in input_file's path 
 - **`temp` bool, default True**: Optional flag to make table as not-temporary (defaults to True)
 - **`column_type_overrides` Dict, default None'**: Dict of type {key=column name, value=column type}. Will manually set the
                raw column name as that type in the query, regardless of the pandas/postgres/sql server automatic
                detection.
 - **`days` int, default 7**: Defines the lifespan (number of days) of any tables created in the query, before they are automatically deleted  
 
**Sample**

```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> df = db.dfquery("select 1 as int_col, 'text' as text_col, now()::date as timstamp_col;")
>>> df.to_excel('haos_sample_file.xlsx')
>>> db.xls_to_table('haos_sample_file.xlsx', schema='working')

working.haos_sample_file already exists. Use overwrite=True to replace.

>>> db.xls_to_table('haos_sample_file.xlsx', schema='working', overwrite=True)

Bulk loading data...
b''

            1 rows added to working.stg_haos_sample_file.
            The table name may include stg_. This will not change the end result.

- Query run 2021-08-09 10:59:01.520000
 Query time: Query run in 5000 microseconds
 * Returned 0 rows *
  
Reading data into Database

1it [00:00, 58.82it/s]

1 rows added to working.haos_sample_file
 
>>> db.dfquery("select * from working.haos_sample_file")

   ogc_fid  field1  int_col text_col timstamp_col
0        1       0        1     text   2023-01-16
```

[Back to Table of Contents](#pysqldb3-public-functions)
<br>

#### query_to_csv
**`DbConnect.query_to_csv(query, strict=True, output_file=None, open_file=False, sep=',', quote_strings=True,
                     quiet=False)`**
Exports query results to a csv file. 

###### Parameters:
 - **`query` str**: SQL query as string type; the query should ultimatley return data (ie. include a `select` statement) 
 - **`strict` bool, default True**: If true will run sys.exit on failed query attempts
 - **`output_file` str, default None**: File path for resulting csv file, if not provided the output will write a file to current directory named `data_[YYYMMDD].csv`
 - **`open_file` bool, default False**:  If true output file will be automatically opened when complete 
 - **`sep` str, default `,`**: Delimiter for csv; defaults to comma (,) 
 - **`quote_strings` bool, default True**: Defaults to True (csv.QUOTE_ALL); if False, will csv.QUOTE_MINIMAL
 - **`quiet` bool, default False'**: If True will override default behavior which outputs query metrics and output location

**Sample**

```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> db.query_to_csv("select street, segmentid, lboro from lion limit 25;")

- Query run 2023-01-16 15:27:04.686184
 Query time: Query run in 0 microseconds
 * Returned 25 rows *
Writing to C:\Users\hshi\data_202301161527.csv

>>> db.query_to_csv("select street, segmentid, lboro from lion limit 25;", output_file= r'...\hao_sample_data.csv', open_file=True)
- Query run 2023-01-16 15:30:54.233901
 Query time: Query run in 0 microseconds
 * Returned 25 rows *
Writing to ...\hao_sample_data.csv

#will open the file
```

[Back to Table of Contents](#pysqldb3-public-functions)
<br>

#### query_to_map
**`DbConnect.query_to_map(query, value_column, geom_column=None, id_column=None)`**

Generates simple Plotly Choropleth Map from query results. 
If no geom_column is specified, and the results contain columns named `precinct`, `nta`, `ntacode`, `boro`, `borough`, 
or `borocode`, it will automatically link to precinct, NTA, or borough, respectively. **This will only work if the database connected to contains the appropriate tables:**
- districts_police_precincts
- districts_neighborhood_tabulation_areas
- districts_boroughs

###### Parameters:
 - **`query` str**: SQL query as string type; the query should ultimatley return data (ie. include a `select` statement with polygon geometry or must contain appropriate join attrbute) 
 - **`value_column` str**: The name of column with the value that is being mapped
 - **`geom_column` str, default None**: the column with the geom that is being mapped;
            if not filled in, columns must contain `precinct`, `nta`, `ntacode`, `boro`, `borough`, or `borocode`
            *Must be used in conjunction with an id_column*
 - **`id_column` str, default False**: The name of the column that contains the ID of the geography being mapped (ex. precinct, nta, boro);
            if not filled in, columns must contain `precinct`, `nta`, `ntacode`, `boro`, `borough`, or `borocode`
            *Must be used in conjunction with an geom_column*

**Sample**

```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> db.query_to_map("""
select 
    lntacode as nta, 
    count(*) cnt, 
    n.geom 
from lion l 
join districts_neighborhood_tabulation_areas n 
    on l.lntacode=n.ntacode 
group by 
    lntacode, n.geom
""", 'cnt', id_column='nta', geom_column='geom')

#will return a website with the map
```
Rely on on geom from districts table 
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> db.query_to_map("select lntacode as nta, count(*) cnt from lion group by lntacode;", 'cnt')

#will return a website with the map
```


![alt text](https://github.com/safety-analytics-mapping/ris/blob/docs/Capture.JPG?raw=true)

[Back to Table of Contents](#pysqldb3-public-functions)
<br>

#### query_to_shp
**`DbConnect.query_to_shp(self, query, path=None, shp_name=None, cmd=None, gdal_data_loc=GDAL_DATA_LOC,
                     print_cmd=False, srid=2263)`**

Generates shapefile output from the data returned from a query. 

###### Parameters:
 - **`query` str**: SQL query as string type; the query should ultimatley return data (ie. include a `select` statement with polygon geometry or must contain appropriate join attrbute) 
 - **`shp_name` str, default None**: Output filename to be used for shapefile (should end in .shp)
 - **`path` str, default None**: Folder path where the output shapefile will be written to, if none provided user input is required
 - **`cmd` str, default None**: GDAL command to overwrite default behavior 
 - **`gdal_data_loc` str, default None**: Path to gdal data, if not stored in system env correctly
 - **`print_cmd` str, default None**: Option to print ogr command (without password)
 - **`srid` int, default 2263**: SRID to manually set output to

**Sample**

```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> db.query_to_shp("select street, segmentid, geom from lion where street ='WATER STREET'", path=r'C:\Users\HShi\Documents', shp_name='sample_shp.shp', print_cmd=True)

- Query failed: table "tmp_query_to_shp_hshi_2023_01_16_1544" does not exist


- Query run 2023-01-16 15:44:01.916732
        drop table tmp_query_to_shp_hshi_2023_01_16_1544
 ogr2ogr --config GDAL_DATA "C:\Program Files (x86)\GDAL\gdal-data" -overwrite -f "ESRI Shapefile" "C:\Users\HShi\Documents\sample_shp.shp"  -a_srs "EPSG:2263" PG:"host=dotdevrhpgsql01 user=hshi dbname=ris password=*********" -sql "SELECT * FROM (select \"street\" , \"segmentid\" , \"geom\" from (select street, segmentid, geom from lion where street ='WATER STREET') q ) x"
b''
sample_shp.shp shapefile
written to: C:\Users\HShi\Documents
generated from: select \"street\" , \"segmentid\" , \"geom\" from (select street, segmentid, geom from lion where street ='WATER STREET') q

```

[Back to Table of Contents](#pysqldb3-public-functions)
<br>

#### table_to_shp
**`DbConnect.table_to_shp(table, schema=None, strict=True, path=None, shp_name=None, cmd=None,
                     gdal_data_loc=GDAL_DATA_LOC, print_cmd=False)`**

Generates shapefile output from the data returned from a query. 

###### Parameters:
 - **`table` str**: SQL query as string type; the query should ultimatley return data (ie. include a `select` statement with polygon geometry or must contain appropriate join attrbute) 
 - **`schema` str, default None**:  Database schema to use for destination in database (defaults database object's default schema)
 - **`shp_name` str, default None**: Output filename to be used for shapefile (should end in .shp)
 - **`strict` bool, default True**: If True will run sys.exit on failed query attempts
 - **`path` str, default None**: Folder path where the output shapefile will be written to, if none provided user input is required
 - **`cmd` str, default None**: GDAL command to overwrite default behavior 
 - **`gdal_data_loc` str, default None**: Path to gdal data, if not stored in system env correctly
 - **`print_cmd` str, default None**: Option to print ogr command (without password)
 - **`srid` int, default 2263**: SRID to manually set output to

**Sample**

```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> db.query("create table working.sample as select segmentid, number_travel_lanes, corridor_street, carto_display_level, created, geom from lion")
 
 - Query run 2023-01-16 16:04:12.159604
 Query time: Query run in 1 seconds
 * Returned 0 rows *
 
>>> db.table_to_shp('sample', schema='working', path=r'C:\Users\HShi\Documents', shp_name='lion_sample_shp.shp')
 
-- Query failed: table "tmp_query_to_shp_hshi_2023_01_16_1605" does not exist


- Query run 2023-01-16 16:05:21.866019
        drop table tmp_query_to_shp_hshi_2023_01_16_1605

            The following columns are of type datetime/timestamp:

            ['created']

            Shapefiles don't support datetime/timestamps with both the date and time. Each column will be split up
            into colname_dt (of type date) and colname_tm (of type **string/varchar**).

b"Warning 6: Normalized/laundered field name: 'number_travel_lanes' to 'number_tra'\r\nWarning 6: Normalized/laundered field name: 'corridor_street' to 'corridor_s'\r\nWarning 6: Normalized/laundered field name: 'carto_display_level' to 'carto_disp'\r\n"
lion_sample_shp.shp shapefile
written to: C:\Users\HShi\Documents
generated from: select \"segmentid\" , \"number_travel_lanes\" , \"corridor_street\" , \"carto_display_level\" , \"geom\" , cast(\"created\" as date) \"created_dt\", cast(cast(\"created\" as time) as varchar) \"created_tm\"  from (select * from working.sample) q

```

[Back to Table of Contents](#pysqldb3-public-functions)
<br>

#### table_to_csv
**`DbConnect.table_to_csv(table, schema=None, strict=True, output_file=None, open_file=False, sep=',',
                     quote_strings=True)`**

Generates shapefile output from the data returned from a query. 

###### Parameters:
 - **`table` str**: Name of database table to be used  
 - **`schema` str, default None**:  Database schema to use for destination in database (defaults database object's default schema)
 - **`strict` bool, default True**: If True will run sys.exit on failed query attempts
 - **`output_file` str, default None**: String for csv output file location and file name. If none provided defaults to current directory and table name
 - **`open_file` bool, default False**: Option to automatically open output file when finished; defaults to False
 - **`sep` str, default ','**: Seperator to use for csv (defaults to `,`) 
 - **`quote_strings` bool, default True**: PaBoolean flag for adding quote strings to output (defaults to true, QUOTE_ALL)

**Sample**

```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> db.query("create table working.hs_sample as select segmentid, number_travel_lanes, corridor_street, carto_display_level, created, geom from lion")
 
 - Query run 2023-01-16 16:14:02.326916
 Query time: Query run in 1 seconds
 * Returned 0 rows *
 
>>> db.table_to_csv('hs_sample', schema='working', output_file=r'C:\Users\HShi\Documents\lion_sample_shp.csv')

 - Query run 2021-08-10 10:41:26.242000
 Query time: Query run in 2000 microseconds
 * Returned 0 rows *
Writing to C:\Users\HShi\Documents\lion_sample_shp.csv

```

[Back to Table of Contents](#pysqldb3-public-functions)
<br>

#### shp_to_table

**`DbConnect.shp_to_table(path=None, table=None, schema=None, shp_name=None, cmd=None,
                     port=None, gdal_data_loc=GDAL_DATA_LOC, precision=False, private=False, temp=True,
                     shp_encoding=None, print_cmd=False, days=7)`**

Imports ESRI Shapefile to database, uses GDAL to generate the table.

###### Parameters:
 - **`path` str, default None**: File path of the shapefile; if None, prompts user input
 - **`table` str, default None**: Table name to be used in the database; if None will use shapefile's name 
 - **`schema` str, default None**:  Database schema to use for destination in database (defaults database object's default schema)
 - **`shp_name` str, default None**: Output filename to be used for shapefile (should end in .shp)
 - **`cmd` str, default None**: GDAL command to overwrite default behavior 
 - **`port` str, default None**:  Database port to use, defaults database connection port
 - **`gdal_data_loc` str, default None**: Path to gdal data, if not stored in system env correctly
 - **`precision` bool, default False**:  Sets precision flag in ogr (defaults to `-lco precision=NO`)
 - **`private` bool, default False**: Flag for permissions output table in database (Defaults to False - will grant select to public)
 - **`temp` bool, default True**: Optional flag to make table as not-temporary (defaults to True)
 - **`shp_encoding` str, default None**: If not None, sets the PG client encoding while uploading the shpfile. Options inlude `LATIN1` or `UTF-8`.
 - **`print_cmd` str, default None**: Option to print ogr command (without password)
 - **`days` int, default 7**: Defines the lifespan (number of days) of any tables created in the query, before they are automatically deleted  
 
**Sample**

```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> db.shp_to_table(path=r'C:\Users\HShi\Documents', shp_name='lion_sample_shp.shp', table='lion_sample_import', schema='working')

b'0...10...20...30...40...50...60...70...80...90...100 - done.\r\n'
- Query run 2023-01-16 16:37:57.418900
 Query time: Query run in 12907 microseconds
 * Returned 0 rows *

>>> db.dfquery("select * from working.lion_sample_import limit 3")

      ogc_fid segmentid number_tra  ...  created_dt created_tm                                               geom
0        1   0294281          2  ...  2022-10-14   10:12:40  0105000020D70800000100000001020000000200000000...
1        2   0043911          2  ...  2022-10-14   10:12:40  0105000020D70800000100000001020000000200000000...
2        3   0118892          1  ...  2022-10-14   10:12:40  0105000020D70800000100000001020000000200000080...

[3 rows x 8 columns]

```

[Back to Table of Contents](#pysqldb3-public-functions)
<br>



#### feature_class_to_table

**`DbConnect.feature_class_to_table(path, table, schema=None, shp_name=None, gdal_data_loc=GDAL_DATA_LOC,
                               private=False, temp=True, fc_encoding=None, print_cmd=False,
                               days=7)`**

Imports feature class from ESRI file geodatabase, uses GDAL to generate the table.

###### Parameters:
 - **`path` str**: File path of the geodatabase
 - **`table` str**: Table name to be used in the database
 - **`schema` str, default None**:  Database schema to use for destination in database (defaults database object's default schema)
 - **`shp_name` str, default None**: Output filename to be used for shapefile
 - **`gdal_data_loc` str, default None**: Path to gdal data, if not stored in system env correctly
 - **`private` bool, default False**: Flag for permissions output table in database (Defaults to False - will grant select to public)
 - **`temp` bool, default True**: Optional flag to make table as not-temporary (defaults to True)
 - **`fc_encoding` str, default None**: If not None, sets the PG client encoding while uploading the shpfile. Options inlude `LATIN1` or `UTF-8`.
 - **`print_cmd` str, default None**: Option to print ogr command (without password)
 - **`days` int, default 7**: Defines the lifespan (number of days) of any tables created in the query, before they are automatically deleted  
 - **`srid` int, default 2263**: SRID to manually set output to 
 
**Sample**

```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='user_name', password='*******')
>>> db.feature_class_to_table(path=r'E:\RIS\Staff Folders\Seth\sample\file_geodatabase.gdb', shp_name='Pct81', table='sample_fc_import', schema='working')

, table='sample_fc_import', schema='working')
0...10...20...30...40...50...60...70...80...90...100 - done.

- Query run 2021-08-10 11:22:34.254000
 Query time: Query run in 2000 microseconds
 * Returned 0 rows *
 
>>> db.dfquery("select * from working.sample_fc_import limit 3")
   objectid_1  objectid  loc  ...             x              y                                               geom
0           1     12495  INT  ...  1.001274e+06  186245.989763  0104000020D70800000100000001010000000045FD8F73...
1           2     12496  INT  ...  1.001306e+06  186672.914762  0104000020D70800000100000001010000008080F1B8B3...
2           3     12497  INT  ...  1.001326e+06  186946.733112  0104000020D708000001000000010100000000A8ED4EDB...

[3 rows x 16 columns]

```

[Back to Table of Contents](#pysqldb3-public-functions)
<br>
