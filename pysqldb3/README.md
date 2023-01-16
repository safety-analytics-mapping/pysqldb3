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

## Details 
### connect
**`pysqldb3.DbConnect(quiet=False)`**
Creates database connection instance.  
###### Parameters:
 - **`quiet`: bool, default False**: When true, does not print database connection information.
 
 
**Sample** 
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='****', password='*******')


>>> db.connect()
Database connection (PG) to ris on dotdevrhpgsql01 - user: ****
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
>>> db = db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='****', password='*******')

>>> db.disconnect()
Database connection (PG) to ris on dotdevrhpgsql01 - user: ****
Connection closed 2023-01-13 10:07:15

>>> db.disconnect(quiet = True) #nothing will return

```
[Back to Table of Contents](#pysqldb-public-functions)
<br>

### check_conn
**`DbConnect.check_conn()`**
Checks and reconnects to connection if not currently connected. 

###### Parameters: 
- None

**Sample**
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='****', password='*******')
>>> db.check_conn() #nothing will return

```
[Back to Table of Contents](#pysqldb-public-functions)
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
>>> db.log_temp_table(schema="working", table="doc_test", owner='****')
#nothing will return
```
[Back to Table of Contents](#pysqldb-public-functions)
<br>

### check_logs
**`DbConnect.check_logs(schema=None)`**
Queries the temp log associated with the user's login and returns a pandas DataFrame of results.  
###### Parameters:
 - **`schema`: str, default None**: Database schema name 

**Sample**
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='****', password='*******')

>>> db.check_logs(schema="working")
   tbl_id table_owner table_schema table_name          created_on     expires
0       1        ****      working   doc_test 2023-01-16 12:44:00  2023-01-23
```
[Back to Table of Contents](#pysqldb-public-functions)
<br>

### cleanup_new_tables
**`DbConnect.clean_up_new_tables()`**
Drops all newly created tables from this DbConnect instance (current session).
###### Parameters:
 - None

**Sample**
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='****', password='*******')

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
0       2        hshi      working  haos_temp_test_table 2023-01-16 13:01:00  2023-01-23

>>> db.cleanup_new_tables()
Dropped 1 tables

>>> db.check_logs(schema='working')
Empty DataFrame
Columns: [tbl_id, table_owner, table_schema, table_name, created_on, expires]
Index: []
```
[Back to Table of Contents](#pysqldb-public-functions)
<br>

### blocking_me
**`DbConnect.blocking_me()`**
QRuns dfquery to find which queries or users are blocking the user defined in the connection. Postgres Only.
###### Parameters:
- None

**Sample**
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='****', password='*******')
>>> db.blocking_me()

Empty DataFrame
Columns: [blocked_pid, blocked_user, blocking_pid, blocking_user, blocked_statement, current_statement_in_blocking_process]
Index: []
```
[Back to Table of Contents](#pysqldb-public-functions)
<br>

### kill_blocks
**`DbConnect.kill_blocks()`**
Will kill any queries that are blocking, that the user (defined in the connection) owns. Postgres Only.
###### Parameters:
- None

**Sample**
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='****', password='*******')
>>> db.kill_blocks()
#nothing will return if there's no block
```
[Back to Table of Contents](#pysqldb-public-functions)
<br>

### my_tables
**`DbConnect.my_tables(schema='public')`**
Get a list of tables for which user (defined in the connection) is the owner Postgres Only.
###### Parameters:
 - **`schema`: str, default None**: Database schema name

**Sample**
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='****', password='*******')
>>> db.my_tables(schema='working')
                 tablename tableowner
0  __temp_log_table_****__       ****
```
[Back to Table of Contents](#pysqldb-public-functions)
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
>>> db = pysqldb3.DbConnect(type='pg', server=server_address, database='ris', user='****', password='*******')
>>> db.table_exists('bike_inj', schema='working')
True
```
[Back to Table of Contents](#pysqldb-public-functions)
<br>

